# homegames worker

A single self-hosted worker that consumes the `homegames-jobs` RabbitMQ queue and
handles two job types:

- **`CERT_REQUEST`** — generates TLS certs via Let's Encrypt (ACME dns-01 over Route 53),
  stores them in Mongo, and reuses a still-valid cert for an IP instead of re-issuing.
- **`LLM_REQUEST`** — runs a local MLX model to rewrite a game's `index.js` from a
  natural-language edit request, then POSTs the result to the API's `/internal/llm-result`.

## Architecture

```
node index.js  (single entry point)
 ├─ one AMQP consumer on homegames-jobs (prefetch 1, manual ack)
 │    ├─ CERT_REQUEST -> ACME / dns-01 flow
 │    └─ LLM_REQUEST  -> Python model server -> POST /internal/llm-result
 └─ manages ONE persistent child: llm/model_server.py
      (warm MLX model; one job JSON line on stdin -> one result JSON line on stdout;
       all diagnostics on stderr)
```

Node owns the queue, acking, result-posting, and the child's lifecycle. The Python
child is a pure compute worker that keeps the model warm (a per-job spawn would reload
the model every time). Protocol between them:

- Node → child (stdin):  `{"id","source","prompt"}`
- child → Node (stdout): `{"id","status":"COMPLETED|FAILED","result?","error?"}` and one `{"ready":true}` at startup

## Setup

Node deps:

```bash
npm install
```

Python model server (bundled under `llm/`):

```bash
cd llm
python3 -m venv env
./env/bin/pip install -r requirements.txt   # mlx-lm
```

The first LLM job loads the model into unified memory before generating (the child
emits `{"ready":true}` once warm). Requires `node` on PATH (the child syntax-checks
generated code via `node --check`).

## Configure

Copy/edit `local.env` and source it. Key vars:

- Cert path: `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USERNAME`/`DB_PASSWORD`, `AWS_ROUTE_53_HOSTED_ZONE_ID`, AWS creds for Route 53.
- LLM path: `API_URL`, `LLM_WORKER_SECRET` (must match the API). Optional model overrides (`MODEL`, `DRAFT_MODEL`, `MAX_TOKENS`, `MAX_RETRIES`) are inherited by the Python child; defaults are in `llm/config.py`.
- `LLM_PYTHON` / `LLM_SERVER_PATH` default to the bundled `llm/env` venv and `llm/model_server.py`; override only if running the interpreter from elsewhere.

## Run

```bash
set -a; . ./local.env; set +a
node index.js
```

CLI helpers (open their own short-lived connection, then exit):

```bash
node index.js queue-stats   # print queue depth + consumer count (consumers should be 1)
node index.js clear-queue   # purge all queued jobs
```

## Notes

- **Unified queue / API dependency.** LLM jobs arrive as `{type:'LLM_REQUEST', ...}` on
  `homegames-jobs`. The API must publish there (it previously used a separate `llm_requests`
  TLS broker). Until that publisher change ships, no LLM jobs reach this worker. This also
  moves LLM traffic onto the plain-AMQP connection, dropping the dedicated-TLS/scoped-cred
  isolation the old `llm_requests` queue had.
- **Serialized jobs.** With one consumer and `prefetch(1)`, jobs run one at a time — a ~30s
  LLM job will delay a cert job queued behind it. Acceptable for a single worker; raise
  prefetch + track in-flight per type if head-of-line blocking becomes a problem.
- **Authoring guide.** `llm/squishjs-game-authoring.md` grounds the model; re-copy it from
  `homegames-core/docs/squishjs-game-authoring.md` when the source changes.
