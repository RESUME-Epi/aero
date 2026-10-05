# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

AERO server: a FastAPI + SQLModel service that tracks versioned data sources and orchestrates
Globus Flows over them. **The server never touches data bytes.** It stores metadata and provenance
and submits flow runs; the functions that actually pull, transform and upload files live in the
sibling **aero-client** repo and execute on a Globus Compute endpoint.

`README.md` is stale — it documents an `osprey/` package layout, a DSaaS client, and
flask-migrate migrations, none of which exist any more. Don't follow it.

## Commands

The venv is `.venv/aero-py3.11/` (note: not `.venv/bin`).

```bash
# Tests. The env vars are mandatory: aero/__init__.py builds a GlobusClient at import
# time, and DATABASE_PORT is int()-cast at import. Values can be junk; tests use SQLite.
PORTAL_CLIENT_ID=00000000-0000-0000-0000-000000000001 PORTAL_CLIENT_SECRET=secret \
DATABASE_HOST=127.0.0.1 DATABASE_USER=u DATABASE_PASSWORD=p DATABASE_PORT=5432 DATABASE_NAME=d \
  .venv/aero-py3.11/bin/python -m pytest tests/ -q

# one file / one test
  ... -m pytest tests/routes/notify_routes_test.py -q
  ... -m pytest tests/model/data_test.py::test_last_version -q

tox                      # full run as CI does it (coverage + pytest)
tox -e report            # coverage report
pre-commit run --all-files   # ruff, ruff-format, codespell, whitespace hooks
```

**Four tests in `tests/model/flows_test.py` fail on any machine without real Globus
credentials** (`test_create_flow`, `test_start_timer`, `test_start_ingestion`, `test_run_flow`).
That is the baseline — check against it before assuming you broke something. To be sure, run the
suite on a clean worktree (`git worktree add /tmp/base develop`) rather than guessing.

```bash
# Run the server. Needs an env file supplying DATABASE_*, PORTAL_CLIENT_*, GLOBUS_* etc.
sudo docker compose -p <project> --env-file <envfile> up web database adminer
```

`LOG_LEVEL=DEBUG` and `AERO_SEARCH_ENABLED=false` are read from the environment (see
`docker-compose.yml`). Multi-instance deployment is documented in
[docs/running-multiple-instances.md](docs/running-multiple-instances.md).

## Architecture

### The round trip

A source is a `Data` row with N `DataVersion`s (each with one `DataFile`). Everything is driven by
one loop:

```
trigger  ->  Flow._run_flow / _run_ingestion_flow
         ->  GlobusClient submits a run of a PERMANENT Globus flow (aero/globus/utils.py:
             VERIFY_AND_MODIFY for ingestion, USER_FLOW for analysis)
         ->  the flow calls three Globus Compute functions, all defined in aero-client:
             pull (download / get_versions) -> user function (aero_format-wrapped) -> commit
         ->  the commit function POSTs back to /prov/new
         ->  add_record -> Data.add_new_version -> Data.rerun_flow -> dependent flows
```

So `POST /prov/new` is not merely a write endpoint — it is the return leg of every flow, and the
place where the next round of flows is triggered.

`Flow.policy` (`TriggerEnum` in `aero/models/flows.py`) decides what triggers a flow: `INGESTION`
(timer), `INGESTION_EVENT` (the notify webhook, no timer), `TIMER`, and `ANY_INPUT`/`ALL_INPUT`
for analyses that rerun when their inputs get new versions.

### Notify and typed sources

`POST /data/notify` (webhook router, guarded by `X-Aero-Token`, no Globus auth) resolves an object
identity to a `Data`. A **source type** (`aero/models/source_type.py`) groups many object URLs
under one `Data` UUID, so a change to any of them triggers the same analysis. A **no-copy** source
(`Data.no_copy`) records a version directly from the notify's `etag`/`size` without running any
flow — nothing is pulled and no bytes are stored; the analysis fetches the object by URL instead.
Design notes: [docs/typed-notify-sources-plan.md](docs/typed-notify-sources-plan.md).

Per-run values (a presigned URL, the triggering object key) are **deepcopy-injected** into the run
kwargs and never written back to `Flow.function_args` — see `run_ingestion_flow` and `run_flow` in
`aero/globus/client.py`. Anything persisted there would outlive the signature that made it valid.

## Constraints that bite

**There are no migrations.** `SQLModel.metadata.create_all()` at startup is the only schema
mechanism (`aero/database.py`). `aero/migrations/` is dead Flask-Migrate code — its `env.py`
imports `flask` and `alembic.ini` has no `script_location`; nothing runs it. Consequences:

- A **new table** is free: define the model and it appears on the next restart.
- A **new column on an existing table** cannot be created by `create_all` at all. It needs
  hand-written DDL against the live Postgres (adminer service), plus an entry in
  `_REQUIRED_COLUMNS` in `aero/database.py` so `check_schema()` logs a clear error instead of the
  failure surfacing later as an opaque 500.
- A model that nothing imports is **silently never created**: `aero/models/__init__.py` is empty,
  so registration happens transitively via `main.py` -> routers -> models.

**uvicorn runs without `--reload`.** The code is volume-mounted so the file on disk looks current,
but the process holds the old module. Restart `web` after every server change.

**Route declaration order matters.** FastAPI matches in order, so a literal path must be declared
*before* a `/{id}` route that would swallow it — `/data/types` and `/data/search` sit above
`GET /data/{id}` in `aero/routers/data.py` for this reason.

**`dict(model)` includes loaded relationships.** It yields the instance `__dict__`, so a
relationship that something upstream happened to touch appears alongside the columns. Unpacking it
next to an explicit `derived_from=` raises "multiple values for keyword argument". Use the
`_flow_out()` helper pattern in `aero/routers/flow.py` rather than `**dict(f)`.

**Don't infer state from a return type.** This has caused three separate bugs. `add_new_version`
returns the new `DataVersion` or `None` on dedup; `_run_flow` takes an explicit `at_registration`
flag rather than inferring it from `last_executed is None`. Keep signals explicit.

**Datetimes are aware UTC, and compared across components.** Every stored timestamp is an aware
UTC instant — `UTCDateTime` in `aero/models/types.py`, with `utcnow()` to stamp them. Binding a
naive value raises rather than being guessed at. Two things follow:

- Columns name `sa_type=UTCDateTime` explicitly instead of letting sqlmodel infer from a bare
  `datetime` annotation, because what that infers changes between releases. A new timestamp column
  must do the same, and needs an entry in `_UTC_COLUMNS` in `aero/database.py` plus hand-written
  DDL, like any other new column.
- The ANY/ALL rerun gate compares `version.created_at > flow.last_executed`, so both are stamped
  from the *server's* clock. The worker sends its own local `ctime()` and `aero/routers/provenance.py`
  discards it — taking it would make versions look hours old and stop flows rerunning.

**Dependencies come from `requirements.lock`.** The Dockerfile installs the lock and then the app
with `--no-deps`, so `pyproject.toml`'s ranges declare intent but choose nothing. Editing
`pyproject.toml` alone changes what the image installs not at all; the lock has to be regenerated
from a rebuilt image. README's *Dependencies* section has the sequence.

**Globus Search is best-effort.** Indexing needs an index plus the ingest role on it;
`add_search_entry` logs and returns `None` on failure so ingestion continues, and
`AERO_SEARCH_ENABLED=false` skips it entirely. Nothing but `GET /data/search` reads the index.

**Tests mock Globus broadly** (`tests/conftest.py` patches `SearchClient`, `AuthClient`,
`SpecificFlowClient`, `TimersClient`). Anything that only fails against the real service — a
malformed search entry, a missing role — will pass locally.

## Conventions

- One import per line, grouped by package; the repo's existing files are not isort-clean, so
  don't reformat imports in files you touch.
- Tests are `*_test.py` (enforced by the `name-tests-test` pre-commit hook), split into
  `tests/model/` and `tests/routes/`.
- Git flow: feature branches off `develop`, `--no-ff` merge; `main` is the release branch.
  Pushes and merges to `main` are done by hand — don't push.
- Do not put historical information in doc strings. For example, text that explains what a
  function used to return. Just explain what the fuction or method etc. does now. If the
  historical information is relevant for your context, put it elsewhere.
  **Tests are the exception**: a regression test's docstring should say what used to break and
  how, because that is the reason the test exists — see the `Regression:` docstrings in
  `tests/routes/`.
