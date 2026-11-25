# Plan: Event-driven S3 ingestion (ingestion flow that runs on notification, not a timer)

> Status: **implemented** (2026-07-24) on the aero `hurricane` branch (server) and
> aero-client `fhwa` branch (client) — pending live end-to-end verification against a
> configured server/endpoint, and the AWS-side event wiring (out of scope, see below).
> File links are relative to this file's location (`docs/`).

## Context
Register a data source that is a file in an S3 bucket. Instead of a standalone/flowless
source, model it as an **ingestion flow without a timer**: the same ingestion pipeline that
exists today (pull the source file → commit a new version → trigger dependent analyses),
but triggered by a "the file changed" notification from the S3 event pipeline rather than a
periodic Globus Timer.

Why this framing is better than a bare notify→add_version webhook:
- It reuses the existing ingestion machinery end-to-end (`download` pull fn + `database_commit`
  commit fn), so the pulled file lands in the GCS collection and **downstream analysis flows
  read it from GCS exactly as they do today** — no S3-input gap.
- The new version's checksum/size/version come from the actually-pulled file (via the
  existing commit path), not a trusted event payload.
- The source `Data` record is created the normal way — as the ingestion flow's `output_data`
  in `POST /flow/register` — so no new data-registration endpoint is needed.

How today's ingestion works (for reference): `POST /flow/register` with `rule=INGESTION`
creates the source `Data` (output_data) and a `Flow`; `Flow._run_flow()` →
`_start_ingestion_flow()` → `GLOBUS_CLIENT.set_timer(FlowEnum.VERIFY_AND_MODIFY, …)` sets up a
**Globus Timer** that periodically runs the ingestion flow. On each run, `database_commit`
posts `/prov/new` → `Data.add_new_version()` → `Data.rerun_flow()` → runs dependent
`ANY`/`ALL` analysis flows.

Decisions: version metadata comes from the pulled file (existing commit path); source `url`
reuses the existing `Data.url` field (no schema change); the source `url` is the S3 object's
**HTTPS URL** so the existing HTTP `download` works (native `s3://` + boto3 stays out of
scope).

## New trigger type: INGESTION_EVENT
Add a policy meaning "ingestion flow, run on external notification, no timer."

- **Server** [aero/models/flows.py](../aero/models/flows.py): add
  `TriggerEnum.INGESTION_EVENT = 4`. In `Flow._run_flow()`, add a branch for it that does
  **nothing** (no `_start_ingestion_flow`, no timer, no immediate run) — the flow + its
  output source `Data` are created at registration and it simply waits to be notified.
  Add `Flow._run_ingestion_flow(session)` that runs the ingestion flow **once** (parallel to
  `_start_ingestion_flow`, but a direct run instead of a timer), using the same stored fields
  (`user_endpoint`, `function_id`, `pull_function_id`, `commit_function_id`, `function_args`,
  `email`, `id`).
- **Client** [aero_client/utils.py](../../aero-client/aero_client/utils.py): add
  `PolicyEnum.INGESTION_EVENT = 4` (mirrors the server int).

## Server changes
1. [aero/globus/client.py](../aero/globus/client.py): factor the `VERIFY_AND_MODIFY`
   run-input construction out of `set_timer` into a helper, and add
   `run_ingestion_flow(...)` that calls
   `self.specific_flow_clients[FLOW_IDS[VERIFY_AND_MODIFY]].run_flow(body=run_input,
   label=…, run_managers=…)` **once** (no `FlowTimer`/`create_timer`). Reuse the helper in
   `set_timer`'s ingestion branch so the two paths can't drift.
2. [aero/models/flows.py](../aero/models/flows.py): the `INGESTION_EVENT` `_run_flow`
   no-op branch + `_run_ingestion_flow()` (calls `GLOBUS_CLIENT.run_ingestion_flow(...)`).
3. Webhook — [aero/routers/data.py](../aero/routers/data.py): add
   `POST /data/{id}/notify`. Handler: find the `Flow` whose `contributed_to` includes this
   `Data` id **and** `policy == INGESTION_EVENT` (query mirrors `Data.rerun_flow`'s but on
   `FlowContribution.produced_data_id`); 404 if none. Call `flow._run_ingestion_flow(session)`
   and return status. The request body can be minimal (optionally accept/log S3 event fields
   like key/etag; they aren't required since `download` re-pulls from the source url).
   No `main.py` change (data router already registered).

Security: the webhook launches a flow, so guard it with an optional shared secret — compare
an `X-Aero-Token` header against `AERO_WEBHOOK_SECRET` from
[aero/config.py](../aero/config.py) when that env var is set (matches the currently-open,
`@authenticated`-commented endpoints while allowing lock-down in deployment).

## Client changes
1. [aero_client/api.py](../../aero-client/aero_client/api.py): in `register_flow`, treat
   `INGESTION_EVENT` like `INGESTION` for pull/commit functions —
   `if policy in (PolicyEnum.INGESTION, PolicyEnum.INGESTION_EVENT): download + database_commit`.
2. Implement source registration (the real `create_source` that
   [example.py](../../aero-client/aero_client/example.py) already imports): a thin wrapper over
   `register_flow(..., policy=PolicyEnum.INGESTION_EVENT, output_data={name:{collection_uuid,
   collection_url}}, kwargs={"url": <s3 https url>, ...})` — no `timer_delay`. Returns the
   created source (with its `Data` id) for analysis flows to reference as `input_data`.
3. Wire the currently-stub `create` CLI command in
   [cli.py](../../aero-client/aero_client/cli.py) to call it (drop the timer requirement; keep
   `--name/--url/--collection-url/--endpoint-uuid/--email`). Analysis flows continue to use
   the existing `register` path with `input_data={"in":{"id":<source id>,"version":None}}` and
   `policy=ANY`/`ALL`.
4. Optional: a small `notify`/CLI helper (or documented `curl`) that POSTs to
   `/data/{id}/notify` for manual testing.

## Out of scope
- Native `s3://` fetching / boto3 (register the source with the object's HTTPS URL so the
  existing HTTP `download` works).
- AWS-side wiring (S3 event notification → SNS/Lambda/EventBridge → the webhook) — infra, not
  code. Document the JSON body the webhook accepts.

## Verification
Against a configured server (Globus creds present; run per DEV.md / docker-compose):
1. Register the source: client `create_source(...)` (or `aero create …`) with
   `policy=INGESTION_EVENT` → `POST /flow/register`; confirm a `Data` source + a `Flow`
   (policy 4) exist and **no Globus Timer** was created (`timer_job_id` is null).
2. Register an analysis flow referencing the source id as `input_data`, `policy=ANY`.
3. `POST /data/{id}/notify` → server runs the ingestion flow once (logs show
   `run_ingestion_flow` → `run_flow`); the ingestion flow pulls the S3 file, and its
   `database_commit` posts `/prov/new` → new version (`GET /data/{id}/latest`) → `rerun_flow`
   launches the analysis flow.
4. Notify again after the S3 file is unchanged → `add_new_version` dedups on checksum → no
   new version, analyses not re-run.
5. With `AERO_WEBHOOK_SECRET` set, a notify without the matching `X-Aero-Token` is rejected.

Offline caveat: `run_flow`/`add_search_entry` need Globus creds; on a bare dev box those
calls error at that step. Unit-test the routing/no-timer/dedup logic by stubbing
`GLOBUS_CLIENT`, or exercise end-to-end against a configured endpoint.
