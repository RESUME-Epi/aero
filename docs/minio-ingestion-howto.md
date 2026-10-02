# How-to: ingest a MinIO object into AERO and trigger updates with curl

Register a MinIO (S3-compatible) object as an AERO data source, then trigger an ingestion by
POSTing a **presigned URL** to the notify webhook. The object is staged **unchanged** into a
Globus guest collection (raw passthrough); downstream analysis flows read it from there.

Prereqs (see also `minio-presigned-ingestion-plan.md`):
- The **Globus Compute endpoint** (not the AERO server) must be able to reach the MinIO host —
  it performs the HTTPS GET. Self-signed MinIO cert ⇒ install a CA bundle on the endpoint.
- The endpoint's `user_environment.yaml` must set `AERO_PROFILE` (and `AERO_CONFIG_FILE` if
  non-default) to the profile the source was registered against — the jobs resolve `aero_client`
  config on the *worker*, not the launching client. See aero-client `DEV.md` → Config profiles.
- A Globus guest collection (`collection_uuid` + `collection_url`) the endpoint can write to.
- Client configured to talk to your server (profile `server = https://<host>/<prefix>` — e.g.
  `.../osprey-proto`). If `AERO_REQUIRE_AUTH=1` on the server, the client login supplies the
  bearer token automatically.

---

## 1. Register the MinIO source (raw passthrough)

The registered `url` is only a **fallback** used when a notify arrives with no URL; each
event normally carries its own presigned URL. Point it at the object (any form is fine; it's
overridden per-run).

### Option A — CLI
```sh
aero create \
  -n midwest-traffic \
  -u  "https://minio.internal:9000/traffic/LinkTrafficReport.xml.gz" \
  -c  "https://g-abc123.data.globus.org/" \
  --collection-uuid 94d05b66-bf20-435d-a406-2577096b6cb6 \
  -g  ed31cdc5-cbd8-422a-aa9b-bfdaec6a218a \
  -e  you@example.org
```
Omit `--verifier` for raw passthrough — AERO registers the built-in `stage` function that
stores the object unchanged. (Pass `--verifier <fn-uuid>` only if you want a transform on
ingest.) The command prints the registered flow JSON; note the source **Data `id`** in
`contributed_to` — you'll use it to trigger and to wire analyses.

### Option B — CLI from a YAML file
Put the arguments in a YAML file and pass `-f/--file`. Explicit CLI flags override file
values, so you can keep a reusable file and tweak one field on the command line.

```yaml
# source.yaml
name: midwest-traffic
url: https://minio.internal:9000/traffic/LinkTrafficReport.xml.gz   # fallback url
collection_url: https://g-abc123.data.globus.org/
collection_uuid: 94d05b66-bf20-435d-a406-2577096b6cb6
endpoint_uuid: ed31cdc5-cbd8-422a-aa9b-bfdaec6a218a
description: Lake Michigan Interchange traffic (MinIO)
# verifier: <fn-uuid>     # optional; omit for raw passthrough (uses `stage`)
```
```sh
aero create -f source.yaml
# override a single value:
aero create -f source.yaml -n midwest-traffic-dev
```
Accepted keys: `name`, `url`, `collection_url`, `collection_uuid`, `endpoint_uuid`,
`verifier` (or `function_uuid`), `description`, `type`, `no_copy`. Missing required values are
reported before anything is registered.

### Matching many objects with one registration

Give a `type` and the source can own several objects, all driving the same analysis. Register them
individually:

```sh
aero create --type traffic --url https://minio.internal:9000/traffic/a.xml.gz
aero create --type traffic --url https://minio.internal:9000/traffic/b.xml.gz
```

or match them with a glob, which also picks up objects created later:

```sh
# quote it -- otherwise the shell expands the pattern before aero sees it
aero create --type traffic --no-copy \
  --url 'https://minio.internal:9000/traffic/**/*.xml.gz'
```

`*` matches within one path segment and `**` crosses segments, so `traffic/*/x.gz` is exactly one
level deep while `traffic/**/x.gz` is any depth. `?` is a single character.

An exact registration always wins over a pattern, and among patterns the one with the longest
literal prefix wins — so a broad rule plus a narrow exception for one object both work. Each
version records the **concrete** object that changed, so change detection stays per object no
matter which pattern matched it.

`aero types` lists every type with its data id and whether each entry is an object or a pattern.
`aero types <name> --remove-url <url>` detaches one of them again, leaving the type, its data id
and its versions in place. A type with no urls left cannot be resolved by notify at all, so for a
no-copy source that stops ingestion until a url is added back.

### Option C — Python
```python
from aero_client.api import create_source

res = create_source(
    name="midwest-traffic",
    url="https://minio.internal:9000/traffic/LinkTrafficReport.xml.gz",  # fallback
    collection_uuid="94d05b66-bf20-435d-a406-2577096b6cb6",
    collection_url="https://g-abc123.data.globus.org/",
    endpoint_uuid="ed31cdc5-cbd8-422a-aa9b-bfdaec6a218a",
    # function_uuid omitted -> raw-passthrough `stage`
)
data_id = res["contributed_to"][0]["id"]
print("source data id:", data_id)
```

Confirm it registered with **no timer** (event-driven): `aero list -t flow` (policy `4` =
`INGESTION_EVENT`).

---

## 2. Generate a MinIO presigned GET URL

The notify carries a short-lived presigned URL. Its lifetime must cover notify → server →
flow launch → endpoint pull, so give it enough (`1h` is safe).

### With the MinIO client (`mc`)
```sh
mc alias set myminio https://minio.internal:9000 <ACCESS_KEY> <SECRET_KEY>
mc share download --expire 1h myminio/traffic/LinkTrafficReport.xml.gz
# prints a "Share:" URL -> that's your presigned GET URL
```

### With the Python SDK (`minio`)
```python
from datetime import timedelta
from minio import Minio

mc = Minio("minio.internal:9000", access_key="...", secret_key="...", secure=True)
url = mc.presigned_get_object("traffic", "LinkTrafficReport.xml.gz", expires=timedelta(hours=1))
print(url)
```

---

## 3. Trigger an ingestion with curl

POST the presigned URL to the notify webhook for the source's Data `id`. Use your server's
full base (including any path prefix like `/osprey-proto`).

```sh
SERVER="https://aero.cels.anl.gov/osprey-proto"
DATA_ID="<the source data id>"
PRESIGNED="<the presigned GET url from step 2>"

curl -sS -X POST "$SERVER/data/$DATA_ID/notify" \
  -H "Content-Type: application/json" \
  -d "{\"url\": \"$PRESIGNED\"}"
```

If the server sets `AERO_WEBHOOK_SECRET`, add the shared-secret header (the webhook uses this,
**not** a Globus bearer):
```sh
  -H "X-Aero-Token: $AERO_WEBHOOK_SECRET"
```

Expected response:
```json
{"status": "ingestion triggered", "flow_id": "...", "data_id": "..."}
```

What happens: the server runs the ingestion flow → the endpoint HTTPS-GETs the **presigned**
object → `stage` → the raw file is PUT into the guest collection → a new version is recorded →
any dependent analysis flows fire.

### Fallback (no URL)
Omit the body to pull from the **registered** `url` instead:
```sh
curl -sS -X POST "$SERVER/data/$DATA_ID/notify" -H "Content-Type: application/json" -d '{}'
```

---

## 4. Verify

```sh
# newest version of the source
curl -sS "$SERVER/data/$DATA_ID/latest" -H "Authorization: Bearer <access-token>" | jq

# or via the client
aero list -i "$DATA_ID"
```
- A new `version` (with `checksum`, `data_file.file_name`) appears after a successful notify.
- **Dedup:** re-notifying an unchanged object (same bytes) records **no** new version.
- Common failures: `401` = bad/missing `X-Aero-Token`; `404` = wrong Data id or the source
  isn't an `INGESTION_EVENT` flow; endpoint-side errors (unreachable MinIO, expired presigned
  URL, TLS) show up in the flow run / endpoint logs, not the curl response.

> Note: `GET /data/{id}/latest` and `aero list` are Globus-authenticated when
> `AERO_REQUIRE_AUTH=1` (use a bearer access token); only the `/notify` webhook uses the
> `X-Aero-Token` shared secret.
