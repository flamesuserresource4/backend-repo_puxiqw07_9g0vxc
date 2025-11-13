AI DB Chatbot - Backend (FastAPI port 8000)

Nota importante per questo ambiente di anteprima: la richiesta originale prevede Slim 4 (PHP) + Vue 3. In questo ambiente standardizzato la parte backend è Python FastAPI. Ho implementato tutte le funzionalità richieste (pipeline IA→JSON→SQL, anti-allucinazioni, rate limit, caching, export Excel, endpoints REST) in FastAPI mantenendo la stessa interfaccia REST prevista. Il frontend è una SPA moderna stile WhatsApp in React per compatibilità con lo stack. Il codice è pronto per collegarsi a MariaDB e alle API OpenAI (o1-mini). Nella sezione Deploy spiego come replicare la stessa architettura in Slim 4 + Vue 3 per produzione.

- Endpoints principali:
  - POST /api/chatbot/message
  - GET  /api/chatbot/chart/{id}
  - GET  /api/chatbot/download/excel/{id}
  - GET  /api/chatbot/cache/status

- Sicurezza e robustezza:
  - Anti-allucinazioni: validazione di tabelle/colonne da information_schema
  - Costruzione SQL sicura parametrizzata, operatori ammessi limitati
  - Limite 1 richiesta/minuto per sessione
  - Caching su filesystem (cache/*.json, *.xlsx)

- OpenAI: o1-mini via Responses API con fallback a gpt-4o-mini. Impostare OPENAI_API_KEY.

- Variabili d’ambiente utili:
  - OPENAI_API_KEY, OPENAI_MODEL, MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD, CACHE_DIR, MAX_ROWS, RATE_LIMIT_PER_MINUTE

- Deploy Slim 4 equivalente: vedere docs/SLIM_PORTING.md
