# Connecting an S3-compatible object store to AERO

AERO learns that an object has changed through its **notify webhook**. This guide covers
connecting an object store to that webhook, so that writing an object is enough to record a new
version and rerun everything that depends on it. It works with AWS S3 and with any store that emits
S3-format bucket notifications, such as MinIO or Ceph RGW.

Registering the source and writing the analysis are covered in the client docs: see
[Getting started](https://github.com/RESUME-Epi/aero-client/blob/main/docs/index.md),
[Examples](https://github.com/RESUME-Epi/aero-client/blob/main/docs/examples.md) (including a
no-copy S3 source), and the
[YAML reference](https://github.com/RESUME-Epi/aero-client/blob/main/docs/yaml-reference.md). This
guide assumes the source already exists, and covers the store-to-server side.

---

## Why a relay

Object stores can't call AERO directly. Their notifications are S3 event JSON, and AERO's webhook
expects a short payload naming the object, with an `X-Aero-Token` header. For a private bucket, where
the code triggered by AERO cannot pull from the bucket, the
object also needs a presigned URL, because AERO holds no object-store credentials. 
If the AERO triggered code can pull from the bucket a presigned URL is not necessary.
In either case, a small relay sits between the two and does the translation:

```
object store                  relay                              AERO
  object written  ──POST──▶   /s3-event                          POST /data/notify
   (S3 event JSON)            - read bucket, key, etag  ──POST──▶  X-Aero-Token
                              - presign a GET (optional)          {file_id, etag, size, url}
```

The relay below is a small FastAPI service. On AWS, where S3 delivers events to Lambda rather than
to a webhook, the same logic runs as a [Lambda function](#aws-s3).

---

## What AERO expects

### The notify request

```
POST <server>/data/notify
Content-Type: application/json
X-Aero-Token: <AERO_WEBHOOK_SECRET>
```

`<server>` is the AERO server's base URL, including any path prefix, the same value as `server` in
the client's `config.toml`. The `X-Aero-Token` header is required only when the server sets
`AERO_WEBHOOK_SECRET`. The webhook does not use Globus authentication.

| Field | |
|---|---|
| `file_id` | The object that changed, as `bucket/key` or a full URL. Required. AERO matches it against the registered sources, including glob patterns on a type |
| `key` | The same identity, recorded on the version. The relay sends `bucket/key` |
| `etag`, `size` | From the event. A no-copy source builds its version from these |
| `url` | A presigned GET for this run's download. Used once and never stored |
| `dedup` | `false` records a new version even when the object is unchanged. Defaults to `true` |

AERO identifies an object by the path of its URL, `bucket/key`, ignoring scheme, host, port and
query string. Register sources with path-style URLs (`https://<host>/<bucket>/<key>`), so the
bucket is part of that path. Use the object's HTTP(S) URL, not an `s3://` URL: AERO downloads
objects over plain HTTP.

### What happens when the notify is received

- **Copy source:** AERO runs the source's ingestion flow. The Globus Compute endpoint downloads the
  object from `url`, or from the registered URL if there is no `url`, and stores it as a new version
  if its contents changed.
- **No-copy source:** AERO records a new version straight from `etag` and `size`, without
  downloading anything. If the `etag` matches the previous version, nothing happens. Otherwise
  every analysis that depends on the source runs.

An object's etag is the MD5 of its contents only for a single-part upload. A multipart upload gets
a different etag scheme, so re-uploading identical contents can still record a new version for a
no-copy source. A copy source compares the downloaded bytes instead, so it isn't affected.

### Responses

| Status | Body | Meaning |
|---|---|---|
| 200 | `{"status": "ingestion triggered", ...}` | Copy source: the ingestion flow is running |
| 200 | `{"status": "version created", ...}` | No-copy source: new version recorded, dependent flows triggered |
| 200 | `{"status": "unchanged", ...}` | No-copy source: same `etag` as last time, nothing triggered |
| 401 | `Invalid or missing webhook token.` | Wrong or missing `X-Aero-Token` |
| 404 | `No source matches file_id ...` | No registered source matches the object. Expected for objects nobody registered |
| 400 | `A no-copy notify needs an etag ...` | No-copy source and no `etag`; see below |

A 200 means AERO accepted the notification, not that the run succeeded. Problems on the endpoint,
such as an expired URL or an unreachable store, show up in the flow run and endpoint logs.

---

## Do you need presigning?

AERO's workers download objects with a plain HTTP GET. For a private bucket, that only works
with a presigned URL. Whether you need one depends on who reads the object:

| Source | Who downloads the object | Private bucket needs `url`? |
|---|---|---|
| Copy source | AERO's ingestion, on the endpoint | Yes |
| No-copy source, analysis input downloaded by AERO (the default) | AERO's analysis wrapper, on the endpoint | Yes |
| No-copy source, analysis input marked `fetch: false` | Your analysis, with its own credentials | No |

A public bucket never needs one. To turn presigning off, set `RELAY_PRESIGN=false`.

Only the input whose notification triggered a run gets a presigned URL. If an analysis has two
no-copy inputs in a private bucket, the other one is downloaded unsigned and fails with a 403.
Marking inputs `fetch: false` avoids this.

The URL has to stay valid from the notification until the endpoint downloads the object, which
includes the time to start a Globus flow. The relay's default of one hour is safe.

Always send the `etag` for a no-copy source. Without it, AERO tries to read it with a `HEAD`
request, and a presigned GET URL doesn't allow `HEAD` because the HTTP method is part of the
signature. The relay takes the `etag` from the event.

---

## The relay

```python title="relay.py"
"""S3 bucket notification -> AERO notify relay.

Receives an S3-format bucket notification (from MinIO, Ceph RGW, or anything
else that emits the S3 event schema), and for each created object POSTs AERO's
notify webhook (``POST <server>/data/notify``) with the object's identity, its
etag and size, and optionally a presigned GET so AERO can download a private
object without holding any object-store credentials.
"""

from __future__ import annotations

import logging
import os
import urllib.parse

import boto3
import requests
from botocore.config import Config as BotoConfig
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("aero-relay")


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


class Config:
    """Relay configuration, read once from the environment."""

    # The object store's S3 API, as the Globus Compute endpoint that downloads the
    # object reaches it -- not necessarily as the relay reaches it, since
    # presigning makes no network call. Leave unset for AWS S3.
    S3_PUBLIC_ENDPOINT = os.getenv("S3_PUBLIC_ENDPOINT")
    # Pinned so presigning stays local. MinIO's default region is us-east-1.
    S3_REGION = os.getenv("S3_REGION", "us-east-1")
    # Credentials come from boto3's usual chain: AWS_ACCESS_KEY_ID and
    # AWS_SECRET_ACCESS_KEY, ~/.aws, or an instance role.
    PRESIGN = _env_bool("RELAY_PRESIGN", True)
    PRESIGN_EXPIRY_SECONDS = int(os.getenv("PRESIGN_EXPIRY_SECONDS", "3600"))

    # AERO's notify route, at the URL the relay reaches it on.
    AERO_NOTIFY_URL = os.getenv("AERO_NOTIFY_URL")
    AERO_WEBHOOK_SECRET = os.getenv("AERO_WEBHOOK_SECRET")
    AERO_VERIFY_TLS = _env_bool("AERO_VERIFY_TLS", True)
    AERO_TIMEOUT_SECONDS = int(os.getenv("AERO_TIMEOUT_SECONDS", "30"))

    # If set, callers must send "Authorization: Bearer <token>". MinIO sends this
    # when its webhook target is configured with auth_token.
    RELAY_AUTH_TOKEN = os.getenv("RELAY_AUTH_TOKEN")

    # If true, an AERO error makes the relay return 502 so the object store
    # retries. Default false: log the error and acknowledge the event.
    RETRY_ON_AERO_ERROR = _env_bool("RETRY_ON_AERO_ERROR", False)

    # Log the notify the relay would send without sending it.
    DRY_RUN = _env_bool("DRY_RUN", False)


_s3 = boto3.client(
    "s3",
    endpoint_url=Config.S3_PUBLIC_ENDPOINT,
    region_name=Config.S3_REGION,
    # Path-style urls (host/bucket/key) work for every S3-compatible store.
    config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
)


def presign_get(bucket: str, key: str) -> str:
    """Return a presigned GET url for ``bucket/key``. Local signing, no I/O."""
    return _s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=Config.PRESIGN_EXPIRY_SECONDS,
    )


def handle_s3_event(body: dict, presign=presign_get) -> list[dict]:
    """Translate an S3 event into AERO notify payloads, one per created object.

    Pure and transport-agnostic, so an AWS Lambda handler can reuse it. Pass
    ``presign=None`` to leave out the presigned url.
    """
    payloads: list[dict] = []
    for record in body.get("Records", []):
        # AWS sends "ObjectCreated:Put", MinIO "s3:ObjectCreated:Put".
        event_name = record.get("eventName") or record.get("EventName") or ""
        if not event_name.removeprefix("s3:").startswith("ObjectCreated:"):
            log.info("skipping non-create event %r", event_name)
            continue

        s3 = record.get("s3", {})
        bucket = s3.get("bucket", {}).get("name")
        obj = s3.get("object", {})
        # Event keys are url-encoded (spaces as '+', '/' as %2F).
        key = urllib.parse.unquote_plus(obj.get("key", ""))
        if not bucket or not key:
            log.warning("record missing bucket/key, skipping: %r", record)
            continue

        file_id = f"{bucket}/{key}"
        payload = {
            "file_id": file_id,
            "key": file_id,
            "etag": obj.get("eTag"),
            "size": obj.get("size"),
        }
        if presign is not None:
            payload["url"] = presign(bucket, key)
        payloads.append(payload)
    return payloads


def forward_to_aero(payload: dict) -> tuple[int, str]:
    """POST one notify payload to AERO; return ``(status_code, body_text)``."""
    if Config.DRY_RUN:
        log.info("DRY_RUN: would POST %s %s", Config.AERO_NOTIFY_URL, payload)
        return 0, "dry-run"

    headers = {"Content-Type": "application/json"}
    if Config.AERO_WEBHOOK_SECRET:
        headers["X-Aero-Token"] = Config.AERO_WEBHOOK_SECRET

    resp = requests.post(
        Config.AERO_NOTIFY_URL,
        json=payload,
        headers=headers,
        verify=Config.AERO_VERIFY_TLS,
        timeout=Config.AERO_TIMEOUT_SECONDS,
    )
    return resp.status_code, resp.text


app = FastAPI(title="AERO S3 relay")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "dry_run": Config.DRY_RUN}


@app.post("/s3-event")
async def s3_event(request: Request) -> dict:
    """Receive a bucket notification and forward each created object to AERO.

    Acknowledges with 200 even when AERO rejects an object, so one bad object
    doesn't block the store's event queue -- unless RETRY_ON_AERO_ERROR is set.
    """
    if Config.RELAY_AUTH_TOKEN:
        if request.headers.get("Authorization") != f"Bearer {Config.RELAY_AUTH_TOKEN}":
            raise HTTPException(status_code=401, detail="bad or missing token")

    body = await request.json()
    payloads = handle_s3_event(body, presign=presign_get if Config.PRESIGN else None)

    results = []
    any_error = False
    for payload in payloads:
        status, text = forward_to_aero(payload)
        if status == 404:
            # An object nobody registered with AERO changed. Expected, and
            # never worth a retry.
            log.info("no AERO source for %s, ignoring", payload["file_id"])
        elif status == 0 or 200 <= status < 300:
            log.info("notified AERO of %s: %s", payload["file_id"], text)
        else:
            any_error = True
            log.error("AERO rejected %s: %s %s", payload["file_id"], status, text[:500])
        results.append({"file_id": payload["file_id"], "status": status, "response": text})

    if any_error and Config.RETRY_ON_AERO_ERROR:
        raise HTTPException(status_code=502, detail=results)

    return {"triggered": len(results), "results": results}


if __name__ == "__main__":
    import uvicorn

    if not Config.AERO_NOTIFY_URL and not Config.DRY_RUN:
        raise SystemExit("set AERO_NOTIFY_URL, or DRY_RUN=1 to test without AERO")
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("RELAY_PORT", "8079")))
```

```text title="requirements.txt"
boto3
fastapi
requests
uvicorn
```

### Configuration

| Variable | Default | |
|---|---|---|
| `AERO_NOTIFY_URL` | — | AERO's notify route, e.g. `https://aero.example.org/aero/data/notify`. Required |
| `AERO_WEBHOOK_SECRET` | unset | Must equal the server's `AERO_WEBHOOK_SECRET`, if it sets one |
| `AERO_VERIFY_TLS` | `true` | `false` for a self-signed certificate on AERO, in testing only |
| `AERO_TIMEOUT_SECONDS` | `30` | Timeout for each notify request |
| `S3_PUBLIC_ENDPOINT` | unset | The store's S3 API, e.g. `https://minio.example.org:9000`. Leave unset for AWS S3 |
| `S3_REGION` | `us-east-1` | Region to sign for. MinIO's default is `us-east-1` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | — | Credentials to sign with; only read access to the bucket is needed. Any source boto3 supports works, including an instance role |
| `RELAY_PRESIGN` | `true` | `false` to send no presigned URL; see [above](#do-you-need-presigning) |
| `PRESIGN_EXPIRY_SECONDS` | `3600` | Lifetime of each presigned URL |
| `RELAY_AUTH_TOKEN` | unset | If set, callers must send `Authorization: Bearer <token>` |
| `RETRY_ON_AERO_ERROR` | `false` | `true` to return 502 when AERO rejects an object, so the store retries the event |
| `DRY_RUN` | `false` | `true` to log each notify instead of sending it |
| `RELAY_PORT` | `8079` | Port to listen on, when run with `python relay.py` |

`S3_PUBLIC_ENDPOINT` is the address **the Globus Compute endpoint** uses to reach the store, since
that is where the presigned URL is used. It need not be reachable from the relay: signing a URL is
a local computation with no network call.

### Running it

```sh
pip install -r requirements.txt
AERO_NOTIFY_URL=https://aero.example.org/aero/data/notify \
AERO_WEBHOOK_SECRET=... \
S3_PUBLIC_ENDPOINT=https://minio.example.org:9000 \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
python relay.py
```

Or in a container:

```dockerfile title="Dockerfile"
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt relay.py ./
RUN pip install --no-cache-dir -r requirements.txt
ENV RELAY_PORT=8079
EXPOSE 8079
CMD ["sh", "-c", "uvicorn relay:app --host 0.0.0.0 --port ${RELAY_PORT}"]
```

```sh
docker build -t aero-s3-relay .
docker run -d --restart unless-stopped -p 8079:8079 --env-file relay.env aero-s3-relay
curl localhost:8079/healthz
```

### Where to run it

Run the relay close to the object store, often on the same machine. It needs to be reachable by
the store, and able to reach AERO's public URL. It doesn't need to be on the same network as AERO.

When an object store dials a webhook, it does so from wherever the store runs. If the store runs in
a container, `localhost` in the webhook address is the store's own container, not the relay. Put
both containers on a shared Docker network and address the relay by its container name.

---

## Sending events to the relay

Configure the store to send object-created events for the bucket, or for just the prefix your
sources cover. A 404 from AERO for an unregistered object is harmless, but narrowing the events
keeps the relay's log readable.

### MinIO

```sh
mc alias set store https://minio.example.org:9000 <ACCESS_KEY> <SECRET_KEY>

# a webhook target named "aero"; auth_token becomes "Authorization: Bearer ..." to the relay
mc admin config set store notify_webhook:aero \
  endpoint=http://relay.example.org:8079/s3-event \
  auth_token=<RELAY_AUTH_TOKEN>
mc admin service restart store

# send put events for one bucket, optionally narrowed by prefix and suffix
mc event add store/flu-surveillance arn:minio:sqs::aero:webhook --event put \
  --prefix wastewater/ --suffix .csv
mc event ls store/flu-surveillance
```

`auth_token` is between MinIO and the relay. It is not AERO's `X-Aero-Token`, which the relay adds.

### Other S3-compatible stores

Any store that can POST S3-format event notifications to an HTTP endpoint can use the relay
unchanged, for example Ceph RGW's bucket notifications with an HTTP push endpoint. Point the
endpoint at `http://<relay>:8079/s3-event`.

### AWS S3

AWS S3 doesn't call webhooks; it delivers events to Lambda, SQS, SNS or EventBridge. The simplest
route is a Lambda function, triggered by the bucket's `s3:ObjectCreated:*` events, that does the
relay's job and posts to AERO's public URL:

```python title="lambda_function.py"
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request

import boto3
from botocore.config import Config as BotoConfig

log = logging.getLogger("aero-relay")
log.setLevel(logging.INFO)

_s3 = boto3.client(
    "s3", config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"})
)


def presign_get(bucket: str, key: str) -> str:
    return _s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=int(os.getenv("PRESIGN_EXPIRY_SECONDS", "3600")),
    )


# --- paste handle_s3_event from relay.py here, unchanged ---


def lambda_handler(event, context):
    presign = presign_get if os.getenv("RELAY_PRESIGN", "true") == "true" else None
    headers = {"Content-Type": "application/json"}
    if os.getenv("AERO_WEBHOOK_SECRET"):
        headers["X-Aero-Token"] = os.environ["AERO_WEBHOOK_SECRET"]

    results = []
    for payload in handle_s3_event(event, presign=presign):
        req = urllib.request.Request(
            os.environ["AERO_NOTIFY_URL"],
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                status = resp.status
        except urllib.error.HTTPError as e:
            status = e.code  # 404: no AERO source for this object
        log.info("notified AERO of %s: %s", payload["file_id"], status)
        results.append({"file_id": payload["file_id"], "status": status})
    return results
```

It uses only boto3 and the standard library, both available in the Lambda Python runtime, so it
needs no packaging. Give its execution role `s3:GetObject` on the bucket, and set
`AERO_NOTIFY_URL` and `AERO_WEBHOOK_SECRET` in its environment.

A URL presigned with the role's temporary credentials stops working when those credentials expire,
which can be sooner than `PRESIGN_EXPIRY_SECONDS`. If downloads fail with an expired signature,
sign with a dedicated IAM user's keys instead, or mark the inputs `fetch: false`.

---

## Testing

### The relay on its own

Run the relay with `DRY_RUN=true` and post it a sample event. It logs the notify it would send,
including the presigned URL, without contacting AERO:

```json title="sample-event.json"
{
  "Records": [
    {
      "eventName": "s3:ObjectCreated:Put",
      "s3": {
        "bucket": {"name": "flu-surveillance"},
        "object": {
          "key": "wastewater%2F2026-11-24.csv",
          "size": 48213,
          "eTag": "e55c21331b43487d86e269e47edcf3a3"
        }
      }
    }
  ]
}
```

```sh
DRY_RUN=true S3_PUBLIC_ENDPOINT=https://minio.example.org:9000 \
  AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... python relay.py &

curl -sS -X POST localhost:8079/s3-event \
  -H "Content-Type: application/json" --data-binary @sample-event.json
```

The log should show `file_id` `flu-surveillance/wastewater/2026-11-24.csv` and a URL starting with
`S3_PUBLIC_ENDPOINT`.

### End to end

1. Register a source for the object with the client.
2. Start the relay with `DRY_RUN=false` and configure the store to send it events.
3. Write the object to the bucket.
4. Check for a new version with `aero list -i <data id>`, and check the relay's log for AERO's
   response.

---

## Notifying AERO by hand

You can send the notify request yourself with `curl`. This is useful for:

- testing the AERO side before setting up the relay
- replaying an event the relay missed, for example while AERO was down
- a store that can't send notifications, from a cron job or the script that writes the object

Notify by object, as the relay does:

```sh
SERVER="https://aero.example.org/aero"

curl -sS -X POST "$SERVER/data/notify" \
  -H "Content-Type: application/json" \
  -H "X-Aero-Token: $AERO_WEBHOOK_SECRET" \
  -d '{
        "file_id": "flu-surveillance/wastewater/2026-11-24.csv",
        "etag": "e55c21331b43487d86e269e47edcf3a3",
        "size": 48213,
        "url": "<presigned GET url>"
      }'
```

Or notify by the source's data id, as printed by `aero create`. This form needs nothing else for an
untyped copy source, since AERO then pulls from the registered URL:

```sh
curl -sS -X POST "$SERVER/data/$DATA_ID/notify" \
  -H "X-Aero-Token: $AERO_WEBHOOK_SECRET"
```

For a typed source, the body must also say which object changed, with `key` or `url`. Add
`"dedup": false` to either form to record a new version even when the object is unchanged.

To make a presigned URL by hand, use either tool:

```sh
# AWS CLI; add --endpoint-url for a store other than AWS
aws s3 presign s3://flu-surveillance/wastewater/2026-11-24.csv --expires-in 3600 \
  --endpoint-url https://minio.example.org:9000

# MinIO client
mc share download --expire 1h store/flu-surveillance/wastewater/2026-11-24.csv
```

---

## Endpoint requirements

When AERO downloads the object (any copy source, and no-copy inputs not marked `fetch: false`),
the download runs on the Globus Compute endpoint, not on the AERO server; the server only
orchestrates the flow. So:

- The endpoint must be able to reach the store at the address in the presigned URL
  (`S3_PUBLIC_ENDPOINT`). The download verifies TLS certificates, so if the store uses a
  self-signed certificate, install its CA on the endpoint, for example by pointing
  `REQUESTS_CA_BUNDLE` at it in the endpoint's environment.
- The endpoint's `user_environment.yaml` must set `AERO_PROFILE`, and `AERO_CONFIG_FILE` if it
  isn't the default, to the client profile for this server. The download job reads the client
  configuration on the endpoint, not on the machine that registered the source.

---

## Security

- The notify route is protected only by the shared `X-Aero-Token`. Set `AERO_WEBHOOK_SECRET` on the
  server, and only send the token to AERO over HTTPS.
- Anyone who can reach the relay can make it notify AERO. Keep it on a network only the store can
  reach, or set `RELAY_AUTH_TOKEN` and configure the store to send it.
- A presigned URL grants read access to one object until it expires. AERO uses it for one download
  and doesn't store it, but it does appear in the relay's log when `DRY_RUN` is on.
