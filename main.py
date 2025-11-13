import os
import json
import hashlib
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

import mysql.connector
from mysql.connector import Error as MySQLError
from cachetools import TTLCache
from openpyxl import Workbook

# OpenAI SDK
from openai import OpenAI

# ------------------------------
# Configuration via environment
# ------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL_PRIMARY = os.getenv("OPENAI_MODEL", "o1-mini")
OPENAI_MODEL_FALLBACK = os.getenv("OPENAI_MODEL_FALLBACK", "gpt-4o-mini")

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "test")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

CACHE_DIR = os.getenv("CACHE_DIR", "./cache")
MAX_ROWS = int(os.getenv("MAX_ROWS", "2000"))
QUERY_TIMEOUT_SECONDS = int(os.getenv("QUERY_TIMEOUT_SECONDS", "25"))
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "1"))

TRIGGERS = {
    "json": ["fammi json", "mostra json"],
    "table": ["fammi tabella", "mostra tabella", "fammi html", "mostra html"],
    "chart": ["fammi un grafico", "fammi grafico", "genera grafico", "mostra grafico"],
    "excel": ["fammi excel", "esporta excel"]
}

os.makedirs(CACHE_DIR, exist_ok=True)

# ------------------------------
# FastAPI App
# ------------------------------
app = FastAPI(title="AI DB Chatbot API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------
# Helpers: DB Connection & Schema
# ------------------------------

def get_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            connection_timeout=QUERY_TIMEOUT_SECONDS,
        )
        return conn
    except MySQLError as e:
        # Bubble up; callers may catch to degrade gracefully
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")


def load_db_schema() -> Dict[str, List[str]]:
    """Read information_schema to get all tables and columns for anti-hallucination validation."""
    try:
        conn = get_mysql_connection()
    except HTTPException:
        return {}
    schema: Dict[str, List[str]] = {}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY TABLE_NAME, ORDINAL_POSITION
            """,
            (MYSQL_DB,),
        )
        for table, column in cur.fetchall():
            schema.setdefault(table, []).append(column)
        return schema
    finally:
        try:
            conn.close()
        except Exception:
            pass


SCHEMA_CACHE = TTLCache(maxsize=1, ttl=60)  # refresh every 60s


def get_valid_schema() -> Dict[str, List[str]]:
    if "schema" in SCHEMA_CACHE:
        return SCHEMA_CACHE["schema"]
    s = load_db_schema()
    SCHEMA_CACHE["schema"] = s
    return s


# ------------------------------
# Helpers: OpenAI interaction
# ------------------------------
class AIClient:
    def __init__(self, api_key: Optional[str]):
        if not api_key:
            self.client = None
        else:
            self.client = OpenAI(api_key=api_key)

    def build_system_prompt(self, schema: Dict[str, List[str]]) -> str:
        tables_desc = []
        for t, cols in schema.items():
            tables_desc.append(f"- {t}: {', '.join(cols)}")
        schema_text = "\n".join(tables_desc)
        return (
            "Sei un assistente di analytics per MariaDB.\n"
            "Regole importanti:\n"
            "1) NON generare SQL.\n"
            "2) Genera SOLO un JSON strutturato che descriva l'intento di query.\n"
            "3) Usa solo tabelle e colonne esistenti. Se mancano, imposta intent=invalid.\n"
            "4) output_format di default 'text'.\n"
            "5) Non inventare dati o colonne.\n"
            "Schema DB disponibile:\n" + schema_text + "\n\n"
            "Formato JSON valido:\n"
            "{\n"
            "  'intent': 'grouped_query' | 'simple_query' | 'timeseries' | 'invalid',\n"
            "  'table': 'nome_tabella',\n"
            "  'metrics': ['COUNT(*)' | 'SUM(col)' | 'AVG(col)' | 'MIN(col)' | 'MAX(col)'],\n"
            "  'group_by': 'colonna' | null,\n"
            "  'filters': [ { 'column': 'col', 'operator': '>='|'<'|'='|'<=|'!='|'LIKE' , 'value': '...' } ],\n"
            "  'order_by': { 'column': 'col', 'direction': 'ASC'|'DESC' },\n"
            "  'limit': 200,\n"
            "  'output_format': 'text' | 'html' | 'json' | 'chart' | 'excel'\n"
            "}\n"
            "Rispondi SOLO con JSON valido."
        )

    def ask(self, user_message: str, schema: Dict[str, List[str]]) -> Dict[str, Any]:
        if not self.client:
            # Fallback: naive parser that tries to build a minimal JSON
            return {
                "intent": "simple_query",
                "table": list(schema.keys())[0] if schema else "",
                "metrics": ["COUNT(*)"],
                "group_by": None,
                "filters": [],
                "order_by": None,
                "limit": 100,
                "output_format": "text",
                "_debug": "OPENAI_API_KEY not set: returning default JSON"
            }

        system_prompt = self.build_system_prompt(schema)

        # Prefer o1-mini via responses API, fallback to chat.completions
        try:
            if OPENAI_MODEL_PRIMARY.startswith("o1"):
                resp = self.client.responses.create(
                    model=OPENAI_MODEL_PRIMARY,
                    reasoning={"effort": "medium"},
                    input=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                )
                content_text = resp.output_text
            else:
                chat = self.client.chat.completions.create(
                    model=OPENAI_MODEL_PRIMARY,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                    temperature=0.2,
                )
                content_text = chat.choices[0].message.content
        except Exception:
            # fallback model
            chat = self.client.chat.completions.create(
                model=OPENAI_MODEL_FALLBACK,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.2,
            )
            content_text = chat.choices[0].message.content

        try:
            data = json.loads(content_text)
            return data
        except Exception:
            # try to extract JSON substring
            start = content_text.find('{')
            end = content_text.rfind('}')
            if start != -1 and end != -1 and end > start:
                try:
                    return json.loads(content_text[start:end+1])
                except Exception:
                    pass
            return {"intent": "invalid", "error": "AI returned non-JSON"}


ai_client = AIClient(OPENAI_API_KEY)


# ------------------------------
# Helpers: Query validation + SQL build
# ------------------------------
ALLOWED_AGG_FUNCS = {"COUNT(*)", "SUM", "AVG", "MIN", "MAX"}
ALLOWED_OPERATORS = {">=", "<=", ">", "<", "=", "!=", "LIKE"}


def validate_and_build_sql(spec: Dict[str, Any], schema: Dict[str, List[str]]) -> Dict[str, Any]:
    table = spec.get("table")
    if not table or table not in schema:
        raise HTTPException(status_code=400, detail="query non valida: tabella inesistente")

    columns = schema[table]

    metrics: List[str] = spec.get("metrics") or ["COUNT(*)"]
    parsed_metrics: List[str] = []
    for m in metrics:
        m = m.strip()
        if m.upper() == "COUNT(*)":
            parsed_metrics.append("COUNT(*) AS count")
        elif m.upper().startswith("SUM(") and m.endswith(")"):
            col = m[4:-1]
            if col not in columns:
                raise HTTPException(status_code=400, detail=f"query non valida: colonna {col} inesistente")
            parsed_metrics.append(f"SUM(`{col}`) AS sum_{col}")
        elif m.upper().startswith("AVG(") and m.endswith(")"):
            col = m[4:-1]
            if col not in columns:
                raise HTTPException(status_code=400, detail=f"query non valida: colonna {col} inesistente")
            parsed_metrics.append(f"AVG(`{col}`) AS avg_{col}")
        elif m.upper().startswith("MIN(") and m.endswith(")"):
            col = m[4:-1]
            if col not in columns:
                raise HTTPException(status_code=400, detail=f"query non valida: colonna {col} inesistente")
            parsed_metrics.append(f"MIN(`{col}`) AS min_{col}")
        elif m.upper().startswith("MAX(") and m.endswith(")"):
            col = m[4:-1]
            if col not in columns:
                raise HTTPException(status_code=400, detail=f"query non valida: colonna {col} inesistente")
            parsed_metrics.append(f"MAX(`{col}`) AS max_{col}")
        else:
            raise HTTPException(status_code=400, detail=f"query non valida: metrica {m} non consentita")

    select_parts = parsed_metrics.copy()

    group_by = spec.get("group_by")
    if group_by:
        if group_by not in columns:
            raise HTTPException(status_code=400, detail=f"query non valida: group_by {group_by} inesistente")
        select_parts.insert(0, f"`{group_by}`")

    where_clauses: List[str] = []
    params: List[Any] = []
    for f in spec.get("filters", []) or []:
        col = f.get("column")
        op = f.get("operator")
        val = f.get("value")
        if not col or col not in columns:
            raise HTTPException(status_code=400, detail="query non valida: filtro colonna inesistente")
        if op not in ALLOWED_OPERATORS:
            raise HTTPException(status_code=400, detail="query non valida: operatore non consentito")
        where_clauses.append(f"`{col}` {op} %s")
        params.append(val)

    sql = f"SELECT {', '.join(select_parts)} FROM `{table}`"
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    if group_by:
        sql += f" GROUP BY `{group_by}`"

    order_by = spec.get("order_by")
    if order_by:
        ob_col = order_by.get("column")
        ob_dir = (order_by.get("direction") or "ASC").upper()
        if ob_col and ob_col in columns and ob_dir in {"ASC", "DESC"}:
            sql += f" ORDER BY `{ob_col}` {ob_dir}"

    limit = min(int(spec.get("limit") or MAX_ROWS), MAX_ROWS)
    sql += f" LIMIT {limit}"

    return {"sql": sql, "params": params}


# ------------------------------
# Helpers: Execute query safely
# ------------------------------

def execute_query(sql: str, params: List[Any]) -> List[Dict[str, Any]]:
    try:
        conn = get_mysql_connection()
    except HTTPException as e:
        raise HTTPException(status_code=500, detail="Database non raggiungibile. Configura MYSQL_* e riprova.")
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        return rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ------------------------------
# Helpers: Rate limit and caching
# ------------------------------
RATE_MAP: Dict[str, float] = {}


def check_rate_limit(user_id: str):
    now = time.time()
    last = RATE_MAP.get(user_id)
    if last and now - last < 60 / RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Rate limit superato: 1 richiesta al minuto")
    RATE_MAP[user_id] = now


def cache_key(data: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def cache_write(key: str, payload: Dict[str, Any]):
    with open(os.path.join(CACHE_DIR, f"{key}.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def cache_read(key: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(CACHE_DIR, f"{key}.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_excel(key: str, rows: List[Dict[str, Any]]):
    wb = Workbook()
    ws = wb.active
    if not rows:
        ws.append(["Nessun dato"])
    else:
        headers = list(rows[0].keys())
        ws.append(headers)
        for r in rows:
            ws.append([r.get(h) for h in headers])
    xlsx_path = os.path.join(CACHE_DIR, f"{key}.xlsx")
    wb.save(xlsx_path)
    return xlsx_path


# ------------------------------
# Helpers: Response shaping
# ------------------------------

def rows_to_html_table(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "<table class='table'><thead><tr><th>Nessun dato</th></tr></thead><tbody></tbody></table>"
    headers = list(rows[0].keys())
    th = "".join([f"<th>{h}</th>" for h in headers])
    trs = []
    for r in rows:
        tds = "".join([f"<td>{r.get(h)}</td>" for h in headers])
        trs.append(f"<tr>{tds}</tr>")
    body = "".join(trs)
    return f"<table class='table'><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>"


def rows_to_chart(rows: List[Dict[str, Any]], group_by: Optional[str]) -> Dict[str, Any]:
    # Simple heuristic: if group_by present and one metric, produce labels/dataset
    if not rows:
        return {"chart": "bar", "labels": [], "datasets": []}
    keys = list(rows[0].keys())
    metric_cols = [k for k in keys if k != (group_by or "")]
    metric = metric_cols[0] if metric_cols else keys[0]
    labels = [str(r.get(group_by)) if group_by else str(i+1) for i, r in enumerate(rows)]
    data = [float(r.get(metric) or 0) for r in rows]
    return {
        "chart": "bar",
        "labels": labels,
        "datasets": [
            {"label": metric, "data": data}
        ]
    }


# ------------------------------
# API Endpoints
# ------------------------------
@app.get("/")
def root():
    return {"message": "AI DB Chatbot API running", "db": MYSQL_DB}


@app.get("/test")
def test():
    schema = get_valid_schema()
    if not schema:
        return {"backend": "ok", "db": MYSQL_DB, "warning": "database non raggiungibile o vuoto"}
    return {"backend": "ok", "tables": list(schema.keys())[:20]}


@app.get("/api/chatbot/cache/status")
def cache_status():
    items = [f for f in os.listdir(CACHE_DIR) if f.endswith('.json')]
    return {"items": len(items)}


@app.get("/api/chatbot/chart/{item_id}")
def get_chart(item_id: str):
    payload = cache_read(item_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Item non trovato")
    chart = payload.get("chart")
    if not chart:
        raise HTTPException(status_code=400, detail="Nessun grafico disponibile")
    return chart


@app.get("/api/chatbot/download/excel/{item_id}")
def download_excel(item_id: str):
    xlsx = os.path.join(CACHE_DIR, f"{item_id}.xlsx")
    if not os.path.exists(xlsx):
        raise HTTPException(status_code=404, detail="File non trovato")
    return FileResponse(xlsx, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename=f"export_{item_id}.xlsx")


@app.post("/api/chatbot/message")
async def chatbot_message(req: Request):
    body = await req.json()
    user_message: str = (body.get("message") or "").strip()
    session_id: str = (body.get("session_id") or req.client.host or "anon")

    if not user_message:
        raise HTTPException(status_code=400, detail="Messaggio vuoto")

    # Rate limit
    check_rate_limit(session_id)

    # Determine trigger
    trigger: Optional[str] = None
    m_low = user_message.lower()
    for t, variants in TRIGGERS.items():
        if any(v in m_low for v in variants):
            trigger = t
            break

    # Get schema and call AI for JSON spec
    schema = get_valid_schema()
    if not schema:
        return JSONResponse({
            "type": "text",
            "message": "Database non configurato o non raggiungibile. Imposta le variabili MYSQL_* e riprova.",
        })

    ai_spec = ai_client.ask(user_message, schema)

    if ai_spec.get("intent") == "invalid":
        return JSONResponse({
            "type": "text",
            "message": "query non valida: verifica tabelle/colonne",
            "ai_spec": ai_spec
        })

    # Build SQL safely
    try:
        built = validate_and_build_sql(ai_spec, schema)
    except HTTPException as e:
        return JSONResponse({
            "type": "text",
            "message": e.detail,
            "ai_spec": ai_spec
        })

    sql = built["sql"]
    params = built["params"]

    # Cache key for this spec
    item_id = cache_key({"sql": sql, "params": params})

    # Check cache
    cached = cache_read(item_id)
    if cached:
        response_payload = cached
    else:
        # Execute
        rows = execute_query(sql, params)

        # Build derivatives
        html_table = rows_to_html_table(rows)
        chart_data = rows_to_chart(rows, ai_spec.get("group_by"))
        xlsx_path = save_excel(item_id, rows)

        response_payload = {
            "id": item_id,
            "type": "text",
            "message": "Operazione completata",
            "rows": rows[:1000],  # cap for transport
            "html": html_table,
            "chart": chart_data,
            "excel_available": bool(xlsx_path),
            "ai_spec": ai_spec,
        }
        cache_write(item_id, response_payload)

    # Adjust output by trigger or ai_spec.output_format
    out_format = ai_spec.get("output_format") or "text"
    if trigger == "json":
        final = {"type": "json", "id": item_id, "json": response_payload.get("rows", [])}
    elif trigger == "table":
        final = {"type": "html", "id": item_id, "html": response_payload.get("html")}
    elif trigger == "chart":
        final = {"type": "chart", "id": item_id, "chart": response_payload.get("chart")}
    elif trigger == "excel":
        final = {"type": "excel", "id": item_id, "download_url": f"/api/chatbot/download/excel/{item_id}"}
    else:
        # default text response with hint buttons
        final = {
            "type": "text",
            "id": item_id,
            "message": "Pronto. Puoi chiedere: fammi tabella / fammi grafico / fammi excel / fammi json",
            "preview": {
                "rows": response_payload.get("rows", [])[:5],
            },
            "ai_spec": ai_spec,
        }

    return JSONResponse(final)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
