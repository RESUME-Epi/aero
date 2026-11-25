# Deploying AERO

How to run the AERO server under Docker Compose: the environment files it needs, starting and
checking it, keeping it up to date, and running several instances on one host. File links are
relative to this file (`docs/`).

## What runs

[docker-compose.yml](../docker-compose.yml) defines four services:

| Service | |
|---|---|
| `web` | The AERO app: uvicorn serving FastAPI on port 8081 inside the Compose network. Publishes no host port |
| `database` | Postgres 17, with its data on an external Docker volume |
| `adminer` | A web UI for the database, published on a host port. The practical way to inspect or patch data |
| `nginx` | The front door on host port 80. Proxies requests to `web` |

```
client ──https──▶ TLS front end ──http:80──▶ nginx ──▶ web:8081 ──▶ database:5432
                  (if TLS is terminated
                   upstream of this host)
```

The server never handles data bytes. Users' functions run on their own Globus Compute endpoints,
which call back to the server's API. So the server must be reachable from those endpoints, and from
wherever users run the `aero` client.

## Before you start

**On the host:**

- Docker Engine with the Compose plugin. The commands below are written without `sudo`; add it if
  your user isn't in the `docker` group.
- A checkout of this repository. The `web` container runs the code from the checkout's `aero/`
  directory, which is mounted into it.

**In Globus**, from the [developer dashboard](https://app.globus.org/settings/developers):

- A **confidential client** for AERO to act as: its `PORTAL_CLIENT_ID` and `PORTAL_CLIENT_SECRET`.
  AERO uses it to start Globus flows and timers, and to index versions in Globus Search. It is also
  the resource server for AERO's API: clients ask for the scope
  `https://auth.globus.org/scopes/<PORTAL_CLIENT_ID>/action_all`, and their `portal_client_id` must
  be this client's id.
- Permission for that client to run AERO's two Globus flows. Their ids are fixed in
  [aero/globus/utils.py](../aero/globus/utils.py); if starting a flow fails with a permission
  error, ask the flows' owner to add the client as a starter.
- Optionally, a Globus Search index for AERO to publish version metadata to. Without one, AERO
  creates an index on first start (see [below](#5-build-and-start)), or you can turn search off.

**For users:** a Globus Compute endpoint with `aero-client` installed, configured as described in
the [aero-client configuration docs](https://github.com/RESUME-Epi/aero-client/blob/main/docs/configuration.md).
That is set up per user or project, not as part of the server.

**TLS:** decide where it is terminated. The Compose file assumes something in front of this host
does it, and nginx serves plain http on port 80; see [TLS and the proxy chain](#tls-and-the-proxy-chain).

---

## 1. Create the shared network and the database volume

Both are declared `external` in the Compose file, so Compose won't create them. You need them even
for a single instance:

```bash
docker network create aero-shared
docker volume create aero-postgres-data
```

The volume holds the database. It survives `docker compose down`, and removing it deletes every
source, flow and version AERO knows about.

## 2. Write the application environment file

This file is passed to the `web` and `database` containers as their environment. It is not in the
repository, and it holds secrets, so keep it out of version control and readable only by the
user that runs Compose.

Use plain `KEY=value` lines, without `export`:

`/etc/aero/aero.env`:

```sh
# --- Postgres, as the web container connects to it ---------------------------
# DATABASE_HOST is the Compose service name, not localhost.
DATABASE_HOST=database
DATABASE_PORT=5432
DATABASE_NAME=aero
DATABASE_USER=aero
DATABASE_PASSWORD=change-me

# --- Postgres, as the database container creates it --------------------------
# Read only when the volume is empty, on the very first start. Must match the
# DATABASE_* values above. Changing them later changes nothing in the database.
POSTGRES_DB=aero
POSTGRES_USER=aero
POSTGRES_PASSWORD=change-me

# --- Globus -------------------------------------------------------------------
# The confidential client AERO acts as.
PORTAL_CLIENT_ID=7408753c-04de-42b9-8ca3-3db94cd2a848
PORTAL_CLIENT_SECRET=...

# Credential for checking whether Globus Compute endpoints are online, used by
# endpoint failover. Use the client the endpoints were started with. Falls back
# to PORTAL_CLIENT_*.
FUNCX_SDK_CLIENT_ID=4b116d3c-1703-4f8f-9f6f-39921e5864df
FUNCX_SDK_CLIENT_SECRET=...

# Globus Search index for version metadata. Leave unset on the first start to
# have AERO create one, then fill it in.
SEARCH_INDEX=d6a4c3f2-1e8b-4c6a-9b7d-2f0e5a8c1b34

# --- Security -----------------------------------------------------------------
# Shared secret for the notify webhooks (X-Aero-Token). Unset = unauthenticated.
AERO_WEBHOOK_SECRET=a-long-random-string
# Require a Globus bearer token on the regular API.
AERO_REQUIRE_AUTH=true

# --- Optional -----------------------------------------------------------------
# Password for the account Globus flows send failure emails from.
# DSAAS_EMAIL_PASSWORD=...
```

| Variable | Required | |
|---|---|---|
| `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_NAME`, `DATABASE_USER`, `DATABASE_PASSWORD` | yes | Postgres connection. Inside Compose the host is `database` |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | yes, on first start | Used by the Postgres image to create the database and user in an empty volume |
| `PORTAL_CLIENT_ID`, `PORTAL_CLIENT_SECRET` | yes | The Globus confidential client AERO acts as. The app won't start without them |
| `FUNCX_SDK_CLIENT_ID`, `FUNCX_SDK_CLIENT_SECRET` | for failover | Credential for endpoint status checks. An endpoint's status only answers to a caller with a role on it, so use the client the endpoints were started with. See [endpoint-failover.md](endpoint-failover.md) |
| `SEARCH_INDEX` | recommended | Globus Search index id. See [step 5](#5-build-and-start) |
| `AERO_WEBHOOK_SECRET` | recommended | Shared secret for `POST /data/notify` and `/data/{id}/notify`. **If unset, those routes accept anyone** |
| `AERO_REQUIRE_AUTH` | recommended | `true` to require a Globus token on the API. Off by default |
| `AERO_AUTH_SCOPE` | no | Overrides the API scope derived from `PORTAL_CLIENT_ID` |
| `DSAAS_EMAIL_PASSWORD` | no | Password for the account flows send failure emails from |

> **Some settings belong in the other file.** `LOG_LEVEL`, `AERO_SEARCH_ENABLED`,
> `AERO_FAILOVER_ENABLED`, `AERO_ENDPOINT_CHECK_TTL` and `AERO_ENDPOINT_CHECK_TIMEOUT` are set in
> the `environment:` block of `docker-compose.yml`, which takes precedence over this file. Setting
> them here has no effect. Put them in the Compose environment file in the next step.

## 3. Write the Compose environment file

This file fills in the `${...}` values in `docker-compose.yml`. It is read by Compose itself, with
`--env-file`, and is never passed into a container:

`.env.aero`:

```sh
PROJECT_NAME=aero
# the file from step 2, and the volume from step 1
SETUP_ENV_SH=/etc/aero/aero.env
PG_VOLUME=aero-postgres-data
DB_PORT=5432
ADMINER_PORT=8080
NGINX_RESTART=always
AERO_APP_CONF=./nginx/single-instance.conf

# App settings that must be set here rather than in SETUP_ENV_SH
LOG_LEVEL=INFO
AERO_SEARCH_ENABLED=true
```

| Variable | Default | |
|---|---|---|
| `PROJECT_NAME` | — | The Compose project name. Required unless you pass `-p` |
| `SETUP_ENV_SH` | — | Path to the application environment file. Required |
| `PG_VOLUME` | `osprey-postgres-data` | The external Postgres volume |
| `DB_PORT` | `5432` | Host port Postgres is published on |
| `ADMINER_PORT` | `8080` | Host port Adminer is published on |
| `NGINX_RESTART` | `no` | nginx's restart policy. Set `always` on the one instance that runs the front door |
| `AERO_APP_CONF` | `./nginx/aero-app.conf` | The nginx configuration to mount |
| `ROOT_PATH_ARG` | empty | uvicorn's `--root-path`, when the instance is served under a path prefix |
| `WEB_ALIAS`, `ADMINER_ALIAS` | `web`, `adminer` | The containers' names on the `aero-shared` network |
| `LOG_LEVEL` | `INFO` | App log level. `DEBUG` traces request handling |
| `AERO_SEARCH_ENABLED` | `true` | `false` to run without Globus Search |
| `AERO_FAILOVER_ENABLED` | `true` | `false` to turn endpoint failover off for every flow |
| `AERO_ENDPOINT_CHECK_TTL` | `30` | Seconds an endpoint status is cached |
| `AERO_ENDPOINT_CHECK_TIMEOUT` | `20` | Seconds to wait for one endpoint status check |

The last six are used only when you serve several instances, or tune failover; see
[Running multiple instances](#running-multiple-instances) and
[endpoint-failover.md](endpoint-failover.md).

## 4. Configure nginx

[nginx/aero-app.conf](../nginx/aero-app.conf) is the configuration for the reference deployment,
which serves three instances under path prefixes. For a single instance at the root of its own
hostname, write a smaller one and point `AERO_APP_CONF` at it:

`nginx/single-instance.conf`:

```nginx
# Docker's DNS. With a variable upstream, nginx starts even if web isn't up yet.
resolver 127.0.0.11 valid=30s ipv6=off;

# The scheme the client used. If TLS is terminated in front of this host and the
# front end sends no X-Forwarded-Proto, an absent header means https.
map $http_x_forwarded_proto $client_scheme {
    ''      https;
    default $http_x_forwarded_proto;
}

server {
    listen 80;
    server_name aero.example.org;

    location / {
        set $aero_web http://web:8081;
        proxy_pass $aero_web;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $client_scheme;
    }
}
```

If clients reach this nginx directly over plain http, with nothing in front of it, replace
`$client_scheme` with `$scheme`. Adminer is on its own host port, `ADMINER_PORT`; to serve it
through nginx as well, see the `adminer` blocks in `aero-app.conf`.

## 5. Build and start

```bash
docker compose --env-file .env.aero build web
docker compose --env-file .env.aero up -d
docker compose --env-file .env.aero logs -f web
```

On start, `web` creates any database tables that don't exist yet, and checks the schema for
problems it can't fix itself (see [Updating](#updating)).

If `SEARCH_INDEX` is unset and search is enabled, `web` also creates a Globus Search index and logs
its id:

```
Created new search index: d6a4c3f2-1e8b-4c6a-9b7d-2f0e5a8c1b34
```

Copy that id into `SEARCH_INDEX` in the application environment file and restart `web`. Otherwise
every restart creates another index. To run without Globus Search, set `AERO_SEARCH_ENABLED=false`
in the Compose environment file instead; nothing but `GET /data/search` reads the index.

## 6. Check it works

```bash
# From the host, through nginx
curl -sS -o /dev/null -w '%{http_code}\n' http://localhost/docs       # 200

# From outside, through the TLS front end
curl -sS -o /dev/null -w '%{http_code}\n' https://aero.example.org/docs
```

`/docs` is the interactive API documentation. Then point a client at the server, with
`server = "https://aero.example.org"` in its `config.toml`, and run `aero list`. See the
[aero-client configuration docs](https://github.com/RESUME-Epi/aero-client/blob/main/docs/configuration.md).

---

## Updating

**Code.** `web` runs the code from the checkout, which is mounted into the container. After a
`git pull`, restart it:

```bash
docker compose --env-file .env.aero restart web
```

**Dependencies.** The image installs the pinned versions in `requirements.lock`. When that file
changes, rebuild:

```bash
docker compose --env-file .env.aero build web
docker compose --env-file .env.aero up -d web
```

**Database schema.** There is no migration tooling. On start, `web` creates tables that don't exist
yet, but it can't change tables that do. When a release adds a column to an existing table, or
changes a column's type, `web` logs an error naming what is out of date and the SQL to apply:

```
Database schema is out of date — missing data.no_copy. create_all() cannot add columns to
existing tables; apply this by hand (adminer) and restart:
ALTER TABLE data        ADD COLUMN no_copy    boolean NOT NULL DEFAULT false;
```

Apply it in Adminer or `psql`, then restart `web`. Check the log after every update.

**nginx configuration.** Test before reloading. A configuration error stops nginx from starting,
and when it is the front door for several instances, that takes all of them down at once:

```bash
docker compose --env-file .env.aero exec nginx nginx -t
docker compose --env-file .env.aero exec nginx nginx -s reload
```

---

## Running multiple instances

Several independent AERO deployments can run on one machine, reachable under a **single hostname**
through **path prefixes**, for example `https://aero.example.org/aero1/docs` and
`https://aero.example.org/aero2/docs`.

### Why path prefixes

The reference deployment's perimeter firewall only lets the standard ports, 80 and 443, through;
anything else times out from off the host. And a separate DNS name and certificate for each
instance is extra overhead. Path prefixes keep every instance on one hostname, one certificate and
port 443.

### Architecture

- **One front-door nginx** on host port 80, because two can't share it. It routes by path prefix
  to each instance's `web` container over the shared `aero-shared` Docker network: `/aero1/…` to
  `aero1-web:8081`, `/aero2/…` to `aero2-web:8081`.
- **Each instance** is a full Compose project (`web`, `database`, `adminer`) with its own Postgres
  volume and host ports. Only the **first** instance also runs nginx.
- **`--root-path`** is passed to each instance's uvicorn, so FastAPI generates URLs with the prefix
  (Swagger UI at `/aeroN/docs`, the schema at `/aeroN/openapi.json`). Without it the docs page
  loads but its fetch of `openapi.json` fails with a 404. The app needs no other changes.

### Setup

**1. Create the network and one volume per instance:**

```bash
docker network create aero-shared
docker volume create aero1-postgres-data
docker volume create aero2-postgres-data
```

**2. Write an application environment file for each instance**, as in
[step 2](#2-write-the-application-environment-file). Give each one its own `SEARCH_INDEX`, and its
own `AERO_WEBHOOK_SECRET`; see [Isolation](#isolation-between-instances).

**3. Write a Compose environment file for each instance:**

`.env.aero1`:

```sh
PROJECT_NAME=aero1
SETUP_ENV_SH=/etc/aero/aero1.env
ROOT_PATH_ARG=--root-path /aero1
WEB_ALIAS=aero1-web
ADMINER_ALIAS=aero1-adminer
DB_PORT=5432
ADMINER_PORT=8080
PG_VOLUME=aero1-postgres-data
NGINX_RESTART=always
AERO_APP_CONF=./nginx/aero-app.conf
```

`.env.aero2`:

```sh
PROJECT_NAME=aero2
SETUP_ENV_SH=/etc/aero/aero2.env
ROOT_PATH_ARG=--root-path /aero2
WEB_ALIAS=aero2-web
ADMINER_ALIAS=aero2-adminer
DB_PORT=5433
ADMINER_PORT=8081
PG_VOLUME=aero2-postgres-data
```

Every instance needs a distinct `PROJECT_NAME`, `WEB_ALIAS`, `ADMINER_ALIAS`, `DB_PORT`,
`ADMINER_PORT` and `PG_VOLUME`.

**4. Add each instance to the front door's nginx configuration**, with a `location /aeroN/` block
for the app and a `location /aeroN/adminer/` block for its Adminer. Each block strips its prefix
before proxying, and tells the app what it was:

```nginx
location = /aero1 { return 308 /aero1/; }
location /aero1/ {
    set $aero1_web http://aero1-web:8081;
    rewrite ^/aero1/(.*)$ /$1 break;        # the app sees /docs, not /aero1/docs
    proxy_pass $aero1_web;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $client_scheme;
    proxy_set_header X-Forwarded-Prefix /aero1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_cache_bypass $http_upgrade;
}

location = /aero1/adminer { return 308 /aero1/adminer/; }
location /aero1/adminer/ {
    set $aero1_adminer http://aero1-adminer:8080;
    rewrite ^/aero1/adminer/(.*)$ /$1 break;   # adminer uses relative URLs
    proxy_pass $aero1_adminer;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $client_scheme;
    proxy_set_header X-Forwarded-Prefix /aero1/adminer;
}
```

The upstream is a variable resolved through Docker's DNS, so nginx starts even when an instance is
down, and answers 502 for it at request time instead of refusing to load. See
[nginx/aero-app.conf](../nginx/aero-app.conf) for the full file, including the `resolver` and the
two `map` blocks it relies on.

**5. Start the instances:**

```bash
# Instance 1: the full stack, including the nginx front door
docker compose --env-file .env.aero1 up -d

# Instance 2: app, database and Adminer only -- the front door is shared
docker compose --env-file .env.aero2 up -d web database adminer
```

To add another instance, repeat steps 1 to 5 with a new prefix and new values.

### Reaching each instance

- `https://aero.example.org/aero1/docs`, and `/aero1/adminer/` for its database
- `https://aero.example.org/aero2/docs`, and `/aero2/adminer/` for its database

Always include Adminer's trailing slash. Configure clients with the prefix in the server URL, for
example `server = "https://aero.example.org/aero1"`.

The repository's `aero-app.conf` serves `/cdph`, `/fhwa` and `/hurricane`; `aero1` and `aero2` are
placeholders here.

### Isolation between instances

Separate projects, volumes and prefixes give each instance its own server and database. But
anything the instances' application environment files have in common, they share. If two instances
use the same `SEARCH_INDEX`, both publish into one index, and if they use the same Globus clients,
those clients' permissions and quotas are shared too. For instances that are truly independent,
give each its own application environment file with its own search index and secrets.

---

## TLS and the proxy chain

In the reference deployment, **TLS is not terminated on the AERO host.** A front end in front of
it terminates TLS and forwards to the host over plain http on port 80, and it also sends
`Strict-Transport-Security`. So nginx listens on port 80 only, mounts no certificate or key, and
publishes no port 443. The front end doesn't send `X-Forwarded-Proto`.

That is easy to get wrong in a specific and expensive way. Adding `listen 443 ssl` and a redirect
from port 80 to https looks like the standard recipe, and it takes every instance down at once:
`$scheme` is `http` for a request the client made over https, so nginx redirects the client back to
the front end, which forwards it here again, and the browser stops with
`ERR_TOO_MANY_REDIRECTS`. The symptom:

```bash
curl -sS -o /dev/null -D - https://aero.example.org/aero1/docs
# HTTP/2 308
# location: https://aero.example.org/aero1/docs     <- the URL that was just requested
```

Two settings follow from this, and neither is optional:

- **nginx forwards `$client_scheme`, not `$scheme`.** `$scheme` is the scheme *this* nginx was
  reached on, which is always `http`. With no `X-Forwarded-Proto` to read, the answer comes from
  the topology: the front end is the only way in, and it is https-only, so an absent header means
  https:

  ```nginx
  map $http_x_forwarded_proto $client_scheme {
      ''      https;
      default $http_x_forwarded_proto;
  }
  ```

  If a second way in ever appears, or the front end starts sending the header, revisit this.

- **`web` needs `FORWARDED_ALLOW_IPS`.** uvicorn trusts `X-Forwarded-Proto` only from `127.0.0.1`
  by default, and nginx is a different container. Without it, FastAPI believes every request is
  http and answers https requests with `http://` redirects. That is easy to miss, because pages
  still load. `docker-compose.yml` sets it to `*`, which is safe because `web` publishes no ports
  and is reachable only from the Compose networks.

After changing either, check that the scheme survives both hops:

```bash
curl -sS -o /dev/null -D - https://aero.example.org/aero1/docs      # 200, not a 308 to itself
curl -sSI https://aero.example.org/aero1/data | grep -i ^location   # must not say http://
```

The second check is the one that catches a missing `FORWARDED_ALLOW_IPS`.

If you terminate TLS on this host instead, nginx needs a certificate, a published port 443, and
`X-Forwarded-Proto $scheme`. The Compose file and the configurations here don't cover that.

---

## Security checklist

- **Set `AERO_WEBHOOK_SECRET`.** Without it, anyone who can reach the server can send notify
  requests and trigger ingestion.
- **Set `AERO_REQUIRE_AUTH=true`.** Without it, the API accepts requests with no Globus token.
- **Keep Postgres and Adminer off the public network.** Compose publishes them on `DB_PORT` and
  `ADMINER_PORT` on every host interface. Block those ports at the firewall, or serve Adminer only
  through nginx.
- **Use real database passwords**, and keep the environment files readable only by the user that
  runs Compose.
- **Serve the API only over https**, since the webhook secret and Globus tokens travel in request
  headers.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `web` exits at start with a Globus or database error | A required variable is missing from the application environment file, or `DATABASE_HOST` isn't `database` |
| `web` logs `Database schema is out of date` | A release added or changed a column; apply the SQL it logs. See [Updating](#updating) |
| A new Globus Search index on every restart | `SEARCH_INDEX` isn't set. See [step 5](#5-build-and-start) |
| `LOG_LEVEL` or `AERO_SEARCH_ENABLED` has no effect | It's in the application environment file; it belongs in the Compose one |
| nginx answers 502 | The `web` container it routes to isn't running, or its alias doesn't match the nginx configuration |
| `ERR_TOO_MANY_REDIRECTS`, or a 308 to the same URL | A redirect to https in nginx, with TLS terminated upstream. See [TLS](#tls-and-the-proxy-chain) |
| Redirects to `http://` URLs | `FORWARDED_ALLOW_IPS` missing from `web`, or nginx forwarding `$scheme` |
| Swagger UI loads but `openapi.json` 404s | The instance's `ROOT_PATH_ARG` is missing or doesn't match its prefix |
| `web` can't reach `database`: `No route to host` | Usually the host firewall: a `ufw` reload removes the forwarding rules Docker added. Check `sudo iptables -L FORWARD -n -v`, and restart Docker to restore them |
| `aero failover status` shows every endpoint `unknown` | `FUNCX_SDK_CLIENT_*` isn't a client with a role on the endpoints. See [endpoint-failover.md](endpoint-failover.md) |
