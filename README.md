# AERO

AERO tracks versioned data sources, records their provenance, and orchestrates
[Globus Flows](https://www.globus.org/globus-flows-service) over them: when a source changes, the
analyses derived from it rerun automatically, and every output is recorded as a new version linked
back to the inputs it came from.

This repository is the **server** — a FastAPI + SQLModel service backed by Postgres. It stores
metadata and submits flow runs; **it never handles data bytes itself**. The functions that pull,
transform and upload files run on a [Globus Compute](https://www.globus.org/compute) endpoint and
live in the companion client repo, [aero-client](https://github.com/NSF-RESUME/DSaaS-client), which
also provides the `aero` command-line tool that users register sources and flows with.

## How it works

A **source** is a `Data` record with a series of `DataVersion`s. Something triggers a flow — a
timer, or a webhook saying an object changed — and from there:

```
trigger ─▶ AERO submits a run of a Globus flow
        ─▶ the flow calls three Globus Compute functions on the endpoint:
             pull the source ─▶ run the user's function ─▶ commit the result
        ─▶ the commit function POSTs provenance back to AERO
        ─▶ AERO records a new version and reruns whatever depends on it
```

That last step is what makes it a pipeline rather than a job runner: recording a version is also
what triggers the next round of flows.

A flow's **policy** decides what starts it — a timer, a change notification, or a new version of
any/all of its inputs.

### Sources that change on their own

For object storage (S3, MinIO), a change notification drives ingestion instead of a timer. Post to
`/data/notify` with the object's identity and AERO resolves it to a source and takes it from there.
Two variations are supported:

- **Types** group several objects under one source, so a change to any of them drives the same
  analysis, and the run is told which object triggered it. Objects can be listed individually or
  matched by a glob pattern — `'test-bucket/**/*.csv'` picks up every CSV at any depth, including
  ones created after the type was registered. `*` stays within a path segment, `**` crosses them.
  Quote the pattern so your shell doesn't expand it.
- **No-copy sources** record a new version from the notification's metadata without moving any
  bytes — nothing is pulled and nothing is staged, and the analysis reads the object directly from
  its URL.

See [docs/typed-notify-sources-plan.md](docs/typed-notify-sources-plan.md) and
[docs/minio-ingestion-howto.md](docs/minio-ingestion-howto.md).

## Running it

Everything runs under Docker Compose: the app (`web`), Postgres (`database`), Adminer, and an nginx
front door.

**1. Create an environment file.** Compose reads it via `SETUP_ENV_SH`, and it is *not* in the
repository — you have to supply it. `scripts/test_env.sh` shows the database variables in the right
shape (note it sets `DATABASE_HOST=127.0.0.1`, which is wrong inside Compose, where it must be the
service name `database`). See [Configuration](#configuration) for what has to be in it.

**2. Build and create the volume:**

```bash
export SETUP_ENV_SH=/path/to/your_env.sh
source scripts/prepare_start.sh      # builds images, creates the postgres volume, checks the DB
```

**3. Start:**

```bash
docker compose up web database adminer nginx
```

Adminer is on `${ADMINER_PORT:-8080}` and is the practical way to inspect or patch the database.

Running several independent deployments behind one hostname is documented in
[docs/running-multiple-instances.md](docs/running-multiple-instances.md).

### Schema management

**There is no migration tooling.** `SQLModel.metadata.create_all()` runs at startup and is the only
mechanism. It creates tables that don't exist yet and never alters ones that do, so:

- a new **table** appears on the next restart;
- a new **column on an existing table** must be added by hand (Adminer, or `psql`). On startup the
  app checks for the columns it needs and logs the exact `ALTER TABLE` statements if any are
  missing.

The `aero/migrations/` directory is a non-functional remnant of an earlier Flask-Migrate setup —
nothing runs it.

### Dependencies

`requirements.lock` is what the server image installs, pinned down to transitive dependencies.
`pyproject.toml` still declares what the app depends on and is the file to read, but it does not
decide versions: the Dockerfile installs the lock first and then the app itself with `--no-deps`,
so nothing can re-resolve.

This exists because an unpinned rebuild once changed which SQLModel version the image got, that
version changed what a `datetime` field compiles to, and registration started failing on an insert
that had worked for months — with no change to this repository.

To add, remove or move a dependency:

```bash
# 1. edit pyproject.toml, then rebuild so the new set is actually resolved
docker compose build web

# 2. freeze what the rebuilt image got
docker compose run --rm --no-deps web pip freeze --exclude-editable > requirements.lock

# 3. run the tests against those versions before trusting them
pip install -r requirements.lock      # in your venv
pytest tests/

# 4. commit pyproject.toml and requirements.lock together
```

Step 3 is the one worth not skipping: the lock's whole purpose is that version changes become
deliberate, which only holds if something checks them. Note that CI runs Python 3.10 while the
image is 3.11, so CI is not that check.

The lock pins versions, not hashes, so it protects against drift rather than against a tampered
package.

## Configuration

All configuration is environment variables, read in `aero/config.py`.

| Variable | Purpose |
|---|---|
| `DATABASE_HOST` / `_USER` / `_PASSWORD` / `_PORT` / `_NAME` | Postgres connection. Inside Compose the host is `database`. |
| `PORTAL_CLIENT_ID` / `PORTAL_CLIENT_SECRET` | Globus confidential client AERO acts as. Required — the app builds a Globus client at import. |
| `GLOBUS_WORKER_UUID` | Globus Compute endpoint that runs the flow functions. |
| `GCS_*` | Guest collection AERO stages data into. |
| `SEARCH_INDEX` | Globus Search index for published metadata. Created on first start if unset. |
| `AERO_SEARCH_ENABLED` | `false` to run without Globus Search. Indexing needs an index plus the ingest role on it; nothing but `GET /data/search` reads it. |
| `AERO_WEBHOOK_SECRET` | Shared secret for the notify webhooks (`X-Aero-Token`). **If unset the webhook routes are unauthenticated.** |
| `AERO_REQUIRE_AUTH` | `true` to require a Globus bearer token on the regular API. Off by default. |
| `AERO_AUTH_SCOPE` | Overrides the derived `action_all` scope. |
| `LOG_LEVEL` | Application log level; `DEBUG` traces request handling. |
| `PROJECT_NAME`, `SETUP_ENV_SH`, `ROOT_PATH_ARG`, `PG_VOLUME`, `DB_PORT`, `ADMINER_PORT` | Compose-level, for running multiple instances. |

## API

| | |
|---|---|
| `GET /data/`, `/data/{id}`, `/data/{id}/versions`, `/data/{id}/latest` | Sources and their version history |
| `GET /data/search` | Globus Search query |
| `GET /data/types`, `/data/types/{name}`, `POST /data/types/{name}/urls` | Notification types and the URLs grouped under them |
| `GET /data/{id}/flows`, `DELETE /data/{id}/flows` | What produces and consumes a source; delete those flows (and their provenance) so they can be re-registered |
| `POST /data/source` | Create a source with no flow attached |
| `POST /data/{id}/notify`, `POST /data/notify` | Change webhooks (shared-secret auth, not Globus) |
| `GET /flow/`, `/flow/{id}`, `POST /flow/register` | Register and inspect flows |
| `GET /prov/`, `POST /prov/new` | Provenance; `/prov/new` is how running flows report back |

Interactive docs are at `/docs` on a running instance.

## Development

```bash
pytest tests/                # needs PORTAL_CLIENT_* and DATABASE_* set; tests run on SQLite
tox                          # as CI runs it, with coverage
pre-commit run --all-files   # ruff, ruff-format, codespell
```

Contributor and agent guidance, including the constraints that are easy to trip over, is in
[CLAUDE.md](CLAUDE.md). Design notes for individual features live in [docs/](docs/).
