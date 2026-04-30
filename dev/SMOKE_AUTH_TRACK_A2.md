# Auth track A2 — manual integration smoke

Procedure verified against a stub e2b orchestrator (real
`http://10.20.100.214:3000` was not reachable from the dev environment
this branch was prepared in; substitute the real URL on a host with
network access).

## Steps

1. Seed the dev DB:

   ```
   KREWHUB_DATABASE_PATH=/tmp/krewhub-smoke.db \
     uv run python dev/seed_fake_auth.py
   ```

2. (Optional, for offline smoke) Start a stub orchestrator that always
   returns `{"sandboxID": "sbx_smoke_stub"}`:

   ```python
   # /tmp/e2b_stub.py
   from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
   import json

   class Handler(BaseHTTPRequestHandler):
       def do_POST(self):
           self.send_response(201)
           self.send_header("Content-Type", "application/json")
           self.end_headers()
           self.wfile.write(json.dumps({"sandboxID": "sbx_smoke_stub"}).encode())
       def do_DELETE(self):
           self.send_response(204); self.end_headers()
       def log_message(self, *_): pass

   ThreadingHTTPServer(("127.0.0.1", 9999), Handler).serve_forever()
   ```

   ```
   python3 /tmp/e2b_stub.py
   ```

3. Start krewhub with fake-auth:

   ```
   KREWHUB_DATABASE_PATH=/tmp/krewhub-smoke.db \
   KREWHUB_KREW_DEV_FAKE_AUTH=1 \
   KREWHUB_E2B_API_URL=http://127.0.0.1:9999 \
   KREWHUB_E2B_API_KEY=test-stub-key \
     uv run uvicorn 'krewhub.app:create_app' --factory --port 8420
   ```

4. Create a task via curl:

   ```
   curl -sS -X POST http://127.0.0.1:8420/api/v1/bundles/BUN_DEV1/tasks \
     -H 'Content-Type: application/json' \
     -d '{"title": "echo hello"}'
   ```

   Expected response: `{ "task": {..., "sandbox_id": "sbx_...",
   "assigned_runtime_id": "rt-dev-1"}, "sandbox": {...} }`.

5. Verify sandbox row written:

   ```
   sqlite3 /tmp/krewhub-smoke.db \
     "SELECT id, task_id, e2b_sandbox_id, status FROM sandboxes"
   ```

6. Confirm 400 no_paired_agent on a bundle without
   `default_agent_runtime_id`:

   ```
   sqlite3 /tmp/krewhub-smoke.db \
     "INSERT INTO bundles(id, recipe_id, prompt, status, created_by,
        created_at, owner_account_id)
      VALUES ('BUN_NA', 'r_dev', 'p', 'open', 'dev-user-1',
        '2026-01-01', 'dev-user-1')"
   curl -sS -X POST http://127.0.0.1:8420/api/v1/bundles/BUN_NA/tasks \
     -H 'Content-Type: application/json' -d '{"title":"x"}'
   ```

   Expected: `{"detail": {"code": "no_paired_agent", "message": "Hire
   an agent first"}}`.

## Notes

- Real e2b at `http://10.20.100.214:3000` was unreachable when this
  branch was prepared; the API shape from
  `infra/e2b/scripts/remote-api-create-base-sandbox.sh` is mirrored
  by the stub, so swapping `KREWHUB_E2B_API_URL` should be sufficient.
- The `KREW_DEV_FAKE_AUTH=1` flag bypasses cookie/JWT checks entirely —
  do not enable in production.
- Sweeper runs every 60s by default (`sandbox_idle_timeout_seconds=600`,
  `sandbox_max_age_seconds=3600`). To force-sweep an idle row update
  `last_event_at` to a backdated value and wait for the next tick.
