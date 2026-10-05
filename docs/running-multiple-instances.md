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

- `https://aero.cels.anl.gov/aero1/docs`   (and `/aero1/adminer/` for its DB)
- `https://aero.cels.anl.gov/aero2/docs`   (and `/aero2/adminer/` for its DB)

(Always include adminer's trailing slash.) Configure clients with `https://` — the front end
described under [TLS](#tls) is what serves it, and it sends HSTS, so a browser will not use
`http://` for this hostname anyway.

`aero1`/`aero2` are placeholders in this document. The live front door serves `/osprey-proto`,
`/fhwa` and `/hurricane`.

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

## TLS

**TLS is not terminated on this host.** A CELS front end terminates it and forwards to epivm over
plain http on port 80. It is also what sets `Strict-Transport-Security`. So nginx here listens on
80 only, mounts no certificate or key, and publishes no 443.

That is easy to get wrong in a specific and expensive way. Adding `listen 443 ssl` plus a
`return 308 https://...` on port 80 looks like the standard recipe, and it takes down all three
instances at once: `$scheme` is `http` for a request the client made over https, so nginx
redirects the client back to the front end, which forwards it here again, and the browser stops
with `ERR_TOO_MANY_REDIRECTS`. The symptom of having made this mistake:

```bash
curl -sS -o /dev/null -D - https://aero.cels.anl.gov/fhwa/docs
# HTTP/2 308
# location: https://aero.cels.anl.gov/fhwa/docs     <- the URL that was just requested
```

Two consequences for the config, neither optional:

- **nginx forwards `$client_scheme`, not `$scheme`.** `$scheme` is the scheme *this* nginx was
  reached on, which is `http` for everything. The front end does not send `X-Forwarded-Proto`
  either, so there is nothing to read and the answer comes from the topology — the front end is
  the only route in and it is https-only:

  ```nginx
  map $http_x_forwarded_proto $client_scheme {
      ''      https;
      default $http_x_forwarded_proto;
  }
  ```

  If a second way in ever appears, or CELS starts sending the header, this is the line to
  revisit.

- **Each instance's uvicorn needs `FORWARDED_ALLOW_IPS`** (set in `docker-compose.yml`). uvicorn
  enables `--proxy-headers` by default but trusts `X-Forwarded-Proto` only from `127.0.0.1`, and
  nginx is a different container. Without it FastAPI believes every request is http and emits
  `http://` redirects from https requests — easy to miss, because the pages still load.

Note that `/etc/pki/tls/certs/cels-incommon.anl.gov-bundle.pem` on epivm expired in August 2025.
Nothing uses it; it is mentioned only so that finding it does not suggest this host is supposed to
be serving TLS.

Test before reloading — a config error stops nginx from starting, and on the front door that is
every instance at once:

```bash
docker compose exec nginx nginx -t        # parse
docker compose exec nginx nginx -s reload
docker compose up -d web                  # per instance, for FORWARDED_ALLOW_IPS
```

Then check that the scheme survives the two hops:

```bash
curl -sS -o /dev/null -D - https://aero.cels.anl.gov/fhwa/docs      # 200, not a 308 to itself
curl -sSI https://aero.cels.anl.gov/fhwa/data | grep -i ^location   # must not say http://
```

The second is what catches a missing `FORWARDED_ALLOW_IPS`.

## Application-level isolation caveat

Port/volume/prefix isolation gives independent servers + databases, but every `web` still
loads the same app env (`env_file: scripts/setup_env.sh`, which also drives Globus/GCS). If
instances share `SEARCH_INDEX`, the GCS collection IDs, or `GLOBUS_WORKER_UUID`, they share
those resources despite separate Postgres DBs. For genuine isolation give each instance its
own app env with a distinct `SEARCH_INDEX`, GCS collection, and worker identity.

> Side note: `env_file` points at `scripts/setup_env.sh`, which isn't in the repo (only
> `test_env.sh` exists, and it sets `DATABASE_HOST=127.0.0.1` — wrong inside Compose, where
> it must be the service name `database`). Provide a real per-instance env file.
