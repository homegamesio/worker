# worker

Background job worker for the Homegames platform. It consumes jobs from a RabbitMQ queue (`homegames-jobs`) published by the [Homegames API](../api) and handles three job types:

- **`CERT_REQUEST`** — issues a TLS certificate from Let's Encrypt (ACME dns-01 challenge via AWS Route 53) for a self-hosted Homegames server, and stores the cert in MongoDB. Idempotent: if a still-valid cert already exists for the requesting IP, it's reused instead of re-issued.
- **`DOCS_QUESTION`** — the "ask something" box on homegames.io/docs.html. Answers general questions about Homegames grounded in the knowledge doc ([homegames-common/docs/homegames-knowledge.md](../homegames-common/docs/homegames-knowledge.md)); results POST back to the API's `/internal/docs-answer`. It answers questions only — it never generates games.
- **`LLM_REQUEST`** — AI game creation/editing. Takes a game's current `index.js` plus a natural-language prompt, has a local LLM rewrite the game, validates the output, and POSTs the result back to the API. **Currently dormant**: the API gate (`AI_EDITS_ENABLED`) is off because locally-hosted models aren't good/fast enough for full game generation, so nothing enqueues these jobs. The handler remains so it can be re-enabled later.

The worker listens on no ports — it is purely a queue consumer.

## How it works

```
API ──publish──▶ RabbitMQ (homegames-jobs @ api.homegames.io:5672)
                     │
                     ▼
                 worker (node index.js)
                     ├── CERT_REQUEST ──▶ Let's Encrypt (dns-01) ──▶ Route 53 TXT records
                     │                        └──▶ cert stored in MongoDB (`homegames.certs`, 60-day expiry)
                     └── LLM_REQUEST ───▶ llm/model_server.py (Python child process)
                                              └──▶ LM Studio (localhost:1234, OpenAI-compatible API)
                                          result POSTed to ${API_URL}/internal/llm-result
```

### Process model

- `index.js` is the only entry point. A 500ms interval keeps exactly one AMQP connection alive; on connection error/close it lets the next tick reconnect rather than reconnecting inline (avoids overlapping consumers).
- Messages are consumed with manual ack. Failed jobs are **acked (dropped), not requeued** — deliberate, so a failed ACME order doesn't hammer Let's Encrypt rate limits. Only a full process crash triggers redelivery.
- For LLM jobs, Node spawns one long-lived Python child (`llm/model_server.py`) and talks to it over newline-delimited JSON on stdin/stdout (stderr is the child's logging). The child is warmed at boot, restarted if a job wedges it, and killed on SIGINT/SIGTERM.

### The `llm/` subdirectory

`model_server.py` is a stdlib-only Python bridge to an **LM Studio** server running on `localhost:1234` (you start LM Studio yourself; the worker doesn't launch it):

1. Polls `GET /models` until LM Studio is up, runs a small warmup generation, emits `{"ready": true}`.
2. Per game-edit job: builds messages (`llm/prompts.py` — a system prompt embedding the squish.js authoring guide, with `CREATE` and `EDIT` modes), calls `/chat/completions` (temperature 0, max 8192 tokens), extracts the first ```javascript fenced block, and validates it with `node --check`. On validation failure, retries up to `MAX_RETRIES` times, feeding the error back.
3. Per docs question (`kind: 'docs'` jobs): builds a separate system prompt embedding the platform knowledge doc, generates a short answer (`DOCS_MAX_TOKENS`, default 1024), and returns the raw text — no code extraction or validation.

Both grounding docs are resolved from the sibling [homegames-common](../homegames-common) package (`docs/squishjs-game-authoring.md`, `docs/homegames-knowledge.md`), overridable via `AUTHORING_DOC_PATH` / `KNOWLEDGE_DOC_PATH`.

## Running

Requirements: Node (also used by the Python child for `node --check`), Python 3, a checkout of `homegames-common` as a sibling directory (it's a `file:../homegames-common` dependency), and — for LLM jobs — LM Studio serving a model on `localhost:1234`.

In production this runs as a single instance on a home Mac Studio (which also hosts LM Studio); there is no container or orchestration setup.

```sh
npm install
set -a; . ./local.env; set +a
node index.js
```

CLI subcommands (short-lived, then exit):

```sh
node index.js queue-stats   # print message/consumer counts for the queue
node index.js clear-queue   # purge the queue
```

## Configuration

Read from the environment (see `local.env` for a template):

| Variable | Purpose |
|---|---|
| `API_URL` | Base URL for posting LLM results (e.g. `https://api.homegames.io`) |
| `LLM_WORKER_SECRET` | Bearer token for `POST ${API_URL}/internal/llm-result`; must match the API's value |
| `DB_HOST`, `DB_PORT`, `DB_USERNAME`, `DB_PASSWORD`, `DB_NAME` | MongoDB connection (the database is currently hardcoded to `homegames`, collection `certs`) |
| `AWS_ROUTE_53_HOSTED_ZONE_ID` | Hosted zone for `_acme-challenge` TXT records |
| `LLM_PYTHON`, `LLM_SERVER_PATH` | Python interpreter / model server script (defaults: `python3`, `llm/model_server.py`) |
| `AUTHORING_DOC_PATH` / `KNOWLEDGE_DOC_PATH` | Override paths to the authoring guide / platform knowledge doc |
| `DOCS_MAX_TOKENS` | Max tokens for docs answers (default 1024) |
| `LLM_SERVER_URL`, `MODEL`, `MAX_TOKENS`, `TEMPERATURE`, `MAX_RETRIES`, `REQUEST_TIMEOUT_SECONDS` | LLM tuning (defaults: LM Studio at `http://localhost:1234/v1`, first available model, 8192 tokens, temp 0, 2 retries, 900s timeout) |

AWS credentials for Route 53 come from the standard AWS SDK credential chain.

Note: the AMQP URL (`amqp://api.homegames.io:5672?frameMax=0`), queue name (`homegames-jobs`), ACME directory (Let's Encrypt production), and ACME account email are currently **hardcoded** in `index.js`, not env-driven. `local.env` also contains a number of keys copied from other services (`GAME_TABLE`, `COGNITO_*`, `SQS_*`, `ELASTICSEARCH_*`, `JWT_SECRET`, `PORT`, etc.) that this worker never reads.

## Relationship to other Homegames repos

- **[api](../api)** — publishes jobs to `homegames-jobs` and receives results at `/internal/llm-result` (game edits) and `/internal/docs-answer` (docs questions).
- **[homegames-common](../homegames-common)** — `file:` dependency, used only to locate the authoring guide and knowledge doc.
- **[squish](../squish) / [homegames-core](../homegames-core)** — not imported; the LLM generates squish.js game code, but validation is just `node --check`.

## Known cruft (as of this writing)

- `Dockerfile`, `run.sh`, and the root `requirements.txt` belong to an old, unrelated NSFW image-classifier experiment and do not build or run this worker. There is currently no container build for the worker.
- `llm/env/` is a leftover venv from a previous MLX-based implementation; the current `model_server.py` is stdlib-only and doesn't use it.
- `node-llama-cpp` and `uuid` are in `package.json` but unused on `main` (they belong to the older, larger worker on the `local` branch, which handled additional job types like `PUBLISH_REQUEST` and image-approval jobs).
