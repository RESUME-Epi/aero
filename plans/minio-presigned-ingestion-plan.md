# MinIO presigned-URL ingestion: raw passthrough staging to GCS

> Status: **implemented** on `feature/minio-presigned-ingest` (aero + aero-client), off
> `develop`. Live end-to-end test pending. File links are relative to this file (`docs/`).

## Context
The event-driven ingestion (`INGESTION_EVENT`) already exists: `POST /data/{id}/notify`
runs an ingestion flow that pulls the source, stages it, and records a version that triggers
dependent analyses. This adds: the source is an object in a **MinIO** (S3-compatible)
datastore, accessed via a **presigned URL supplied in the notify request**, and the ingest is
a **raw passthrough** — the object is staged into the GCS guest collection unchanged, and a
separate analysis flow's compute step operates on it from GCS.

Two gaps closed:
1. **Presigned URL in the notify** — the pull used the registered `Data.url`. Now: use the
   notify's `url` for this run if provided, else fall back to the registered one. Presigned
   URLs are transient, so they are per-run and never persisted.
2. **Raw passthrough staging** — `gcs_save` only runs on a user function's `AeroOutput`, so
   nothing uploaded the *raw* pulled file. A trivial `stage` passthrough bridges this.

Decisions: presigned URL in the notify body (fallback to registered `Data.url`); raw
passthrough (no transform on ingest); MinIO access is the presigned URL only (no boto3/SigV4).

## Server changes (aero)
Thread the presigned URL from the webhook to the pull, transiently.

1. [aero/routers/data.py](../aero/routers/data.py) — `NotifyIn.url: str | None`; `notify_update`
   passes `source_url = payload.url if payload else None` into `_run_ingestion_flow`.
2. [aero/models/flows.py](../aero/models/flows.py) — `_run_ingestion_flow(self, session,
   source_url=None)` forwards it to the client call.
3. [aero/globus/client.py](../aero/globus/client.py) — `run_ingestion_flow(..., source_url=None)`
   **deep-copies** `function_args["kwargs"]` and, if `source_url` is set, injects
   `kwargs["aero"]["source_url"]` before `json.dumps` — so the transient URL is never
   persisted onto `Flow.function_args`.

## Client changes (aero-client)
4. [aero_client/jobs.py](../../aero-client/aero_client/jobs.py) — `download`: after building
   `data` from `flow["contributed_to"][0]`, `override_url = kwargs["aero"].get("source_url")`;
   if set, `data["url"] = override_url` (notify URL wins; registered `Data.url` is the
   fallback). Rest of `download` unchanged; a presigned MinIO URL is a plain HTTPS GET.
5. `jobs.py` — new `stage(*args, **kwargs)` passthrough: returns the downloaded local file as
   an `AeroOutput` (identifies it as the single kwarg whose value is an existing file path), so
   `aero_format`→`gcs_save` uploads the **raw** object into the GCS collection.
6. [aero_client/api.py](../../aero-client/aero_client/api.py) — `create_source`: `function_uuid`
   is now optional; when omitted it registers `stage` **aero_format-wrapped**
   (`register_function as register_aero_function`), so a raw-passthrough source needs no user
   code.

## Why it's minimal / safe
- No changes to `gcs_save`, `aero_format`, `database_commit`, `/prov/new`, or the
  `add_new_version`→`rerun_flow` chain.
- **Dedup preserved:** `download` sets the checksum from the raw bytes; the ingestion
  `output_data` carries a `url`, so `aero_format` keeps that checksum rather than overwriting
  it — a re-notify of an unchanged object → same checksum → no new version.
- Presigned URL is per-run only (deepcopy + inject); expiry is a non-issue and the registered
  `Data.url` fallback is never clobbered.

## Deployment prerequisites
- **The pull runs on the Globus Compute endpoint, not the AERO server.** `download`/`stage`/
  `database_commit` execute on the worker endpoint; the server only orchestrates.
- So **the endpoint must reach MinIO** (route/DNS), and any self-signed-cert/CA-bundle
  handling belongs on the endpoint. `_http_fetch` uses `requests.get` without `verify=False`,
  so a self-signed MinIO cert needs a CA bundle (or a verify toggle) on the endpoint.
- The presigned URL travels notify → server → flow → **endpoint** `download`; its validity
  window must cover that whole hop.
- The GCS guest collection is typically colocated with the endpoint, so `stage`'s upload and
  downstream reads line up on the compute side.

## Verification
Against a configured server + endpoint (Globus creds; per DEV.md):
1. `create_source` with no transform → a `Data` + `INGESTION_EVENT` `Flow` (policy 4), no timer.
2. `POST /data/{id}/notify {"url":"<minio-presigned-GET>"}` → `download` pulls the presigned
   URL; `stage`→`gcs_save` PUTs the raw object into GCS; `GET /data/{id}/latest` shows it.
3. `POST /data/{id}/notify {}` → falls back to registered `Data.url`.
4. Re-notify unchanged object → dedup, no new version.
5. A dependent `ANY`/`ALL` analysis flow reads the staged file from GCS and runs.
6. `Flow.function_args` in the DB does **not** contain `source_url` after a run (deepcopy).

Offline: unit-test the `download` override precedence and the `run_ingestion_flow` deepcopy/
injection in isolation; exercise the rest end-to-end against the live stack.
