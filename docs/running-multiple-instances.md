# Running multiple aero-server instances on one host

Run several aero-server deployments on one machine, reachable under a **single
hostname** via **path prefixes** — e.g. `https://aero.cels.anl.gov/aero1/docs`,
`https://aero.cels.anl.gov/aero2/docs`. File links are relative to this file (`docs/`).

## Why path-based (not port-based or hostname-based)

The ANL perimeter firewall only allows standard ports (80/443) through; non-standard
ports (8090, 8443, 5433, …) time out from off-host. And provisioning a second DNS name +
cert per instance is extra overhead. Path prefixes keep everything on one hostname, one
cert, port 443 — which is what the firewall permits.

## Architecture

- **One front-door nginx** on host 80/443 (two nginx can't share those ports). It routes
  by path prefix to each instance's `web` container over a shared Docker network:
  `/aero1/…` → `aero1-web:8081`, `/aero2/…` → `aero2-web:8081`
  (see [nginx/aero-app.conf](../nginx/aero-app.conf)).
- **Each instance** is a full Compose project (`web` + `database` + `adminer`) with its own
  Postgres volume and DB host port. Only the **first** instance also runs the nginx front
  door; the others run without nginx.
- **`--root-path`** is passed to each instance's uvicorn so FastAPI emits correctly
  prefixed URLs (Swagger UI at `/aeroN/docs`, schema at `/aeroN/openapi.json`). Without it
  the docs page loads but its `openapi.json` fetch 404s. The app itself needs no code
  changes — it has no hardcoded absolute paths or static mounts.

## What the compose file parameterizes

[docker-compose.yml](../docker-compose.yml) reads these interpolation variables (all have
single-instance defaults, so a plain `docker compose up` still works once the shared
network/volume exist):

| Variable | Purpose | Default |
|---|---|---|
| `ROOT_PATH_ARG` | uvicorn `--root-path` flag for the instance | *(empty)* |
| `WEB_ALIAS` | the `web` container's alias on `aero-shared` | `web` |
| `DB_PORT` | host port for Postgres | `5432` |
| `ADMINER_PORT` | host port for Adminer | `8080` |
| `ADMINER_ALIAS` | the `adminer` container's alias on `aero-shared` | `adminer` |
| `PG_VOLUME` | external Postgres data volume name | `osprey-postgres-data` |

`web`, `adminer`, and `nginx` are attached to an external `aero-shared` network so the
single front door can resolve every instance's `web` and `adminer`.

## Setup

**1. One-time host prerequisites**

```bash
docker network create aero-shared
docker volume  create osprey-postgres-data      # if it doesn't already exist
docker volume  create osprey-postgres-data-2
```

**2. Per-instance env files** (used for Compose interpolation via `--env-file`)

`.env.instance1`
```
ROOT_PATH_ARG=--root-path /aero1
WEB_ALIAS=aero1-web
ADMINER_ALIAS=aero1-adminer
DB_PORT=5432
ADMINER_PORT=8080
PG_VOLUME=osprey-postgres-data
```

`.env.instance2`
```
ROOT_PATH_ARG=--root-path /aero2
WEB_ALIAS=aero2-web
ADMINER_ALIAS=aero2-adminer
DB_PORT=5433
ADMINER_PORT=8081
PG_VOLUME=osprey-postgres-data-2
```

**3. Bring up the instances** (distinct project names via `-p`)

```bash
# Instance 1 = full stack INCLUDING the nginx front door
docker compose -p aero1 --env-file .env.instance1 up -d

# Instance 2 = app + db + adminer only (NO nginx — the front door is shared)
docker compose -p aero2 --env-file .env.instance2 up -d web database adminer
```

Add more instances by copying an env file (new `WEB_ALIAS`, `ADMINER_ALIAS`, `DB_PORT`,
`ADMINER_PORT`, `PG_VOLUME`) and adding matching `location /aeroN/` and
`location /aeroN/adminer/` blocks to `aero-app.conf`.

## Reaching each instance

- `http://aero.cels.anl.gov/aero1/docs`   (and `/aero1/adminer/` for its DB)
- `http://aero.cels.anl.gov/aero2/docs`   (and `/aero2/adminer/` for its DB)

(Use `https://` once TLS is enabled — see below. Always include adminer's trailing slash.)

## nginx routing detail

Each prefix strips itself before proxying and advertises the prefix back to the app:

```nginx
location /aero1/ {
    set $aero1_web http://aero1-web:8081;
    rewrite ^/aero1/(.*)$ /$1 break;        # app sees /docs, not /aero1/docs
    proxy_pass $aero1_web;
    proxy_set_header X-Forwarded-Prefix /aero1;
    ...
}
```

The upstream is a **variable** with a `resolver`, so nginx starts even if a backend is
down (it 502s at request time instead of failing to load — and avoids the "host not found
in upstream" startup crash).

## Enabling TLS

`aero-app.conf` has a commented `listen 443 ssl;` block. Uncomment it and the
`ssl_certificate` / `ssl_certificate_key` lines; the front-door nginx already mounts
`/etc/pki/tls/certs` → `/certs` and `/etc/pki/tls/private` → `/keys`.

## Application-level isolation caveat

Port/volume/prefix isolation gives independent servers + databases, but every `web` still
loads the same app env (`env_file: scripts/setup_env.sh`, which also drives Globus/GCS). If
instances share `SEARCH_INDEX`, the GCS collection IDs, or `GLOBUS_WORKER_UUID`, they share
those resources despite separate Postgres DBs. For genuine isolation give each instance its
own app env with a distinct `SEARCH_INDEX`, GCS collection, and worker identity.

> Side note: `env_file` points at `scripts/setup_env.sh`, which isn't in the repo (only
> `test_env.sh` exists, and it sets `DATABASE_HOST=127.0.0.1` — wrong inside Compose, where
> it must be the service name `database`). Provide a real per-instance env file.
