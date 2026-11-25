# Typed notification sources: many URLs → one type → one Data UUID

> Status: **implemented and live-tested** (2026-08-10) against MinIO, a real Globus Compute
> endpoint and the relay: a typed no-copy source records versions per url and the derived analysis
> flow runs on the object that triggered the notify. Merged to `develop` in aero and aero-client;
> the relay and configs are on `main` in aero-testing. Supersedes the earlier "No-copy ingestion
> source + URL-based analysis input" design, which was never implemented — this keeps its no-copy
> idea and adds the type as the unit of identity. Paths are repo-relative: `aero/...` is this repo,
> `aero-client/...` and `aero-testing/...` are sibling repos.

## Context

Today a notify resolves to exactly one `Data`, and a `Data` carries exactly one `url`. That makes
"one MinIO object = one AERO source = one analysis flow." For the general MinIO case — a datastore
holding many objects that are the same *kind* of thing — we want the opposite grouping: **those
objects should all feed the same analysis flow**, and the analysis should be told *which* object
triggered this run.

A **type** becomes the unit of identity:

- A **type** has a name, is bound to exactly **one** `Data` UUID, and owns **many** URLs.
- `aero create --type X --url ...` creates the source the first time and, on every later call,
  just associates another URL with the existing type — returning the same Data UUID.
- Analysis flows are registered against that one UUID, so a change to *any* URL in the type
  triggers the same analysis.
- The analysis receives **the URL that triggered the notify** — the stable, registered object URL,
  which is the object's identity — and **optionally a signed URL** created by the relay
  (`aero-testing/relay/`), which is transient read credentials for those bytes. Two separate
  values, both threaded through per-run and neither persisted. Not a stored copy.

Type and copy-mode are **orthogonal**: a typed source can either copy each triggering object into
GCS as a new version (today's presigned passthrough, unchanged) or be **no-copy**, where the
version is recorded server-side from the notify metadata and no bytes move.

Two further decisions:
- **A URL that matches nothing 404s.** No auto-registration. URL matching uses the *same*
  normalization scheme as `notify_by_object` (`_normalize_object_key`, with the
  `_normalize_full_url` tie-break), so a presigned URL still matches its registered key.

  > **Superseded 2026-08-10.** This originally read "no globs" — every object had to be registered
  > individually. A type's entries may now be **glob patterns**, so an object nobody registered can
  > still resolve. Semantics are canonical: `*` within a path segment, `**` across segments, `?` one
  > character. An entry containing `*`, `?` or `[` is a pattern; everything else stays an exact
  > indexed lookup. Exact beats pattern, and among patterns the longest literal prefix wins, so a
  > broad rule plus narrow overrides is expressible; an exact tie is a 409. The **concrete** object
  > key is what reaches `DataVersion.source_key`, so per-object dedup is unaffected by how it was
  > matched. A pattern is a matching rule and never a fetchable URL: the trigger URL is rebuilt from
  > the pattern's host plus the concrete key.
- **Dedup is per-URL**, keyed by that same normalized key — re-notifying an unchanged object never
  makes a version, even if sibling URLs changed in between. A payload flag forces a version anyway.

## Schema

[aero/database.py:10](../aero/database.py#L10) — `SQLModel.metadata.create_all()` on startup is the
only schema mechanism (the `aero/migrations/` Alembic dir is dead Flask-Migrate code: `env.py`
imports `flask`, `alembic.ini` has no `script_location`). **`create_all` creates new tables but
never alters existing ones.**

### New tables (auto-created on restart)

New file `aero/models/source_type.py`, both models together (same convention as
[flows.py](../aero/models/flows.py) holding its link tables). Imported by
[aero/routers/data.py](../aero/routers/data.py), which puts them on the `main.py → routers →
models` chain that `create_all` depends on — `aero/models/__init__.py` is empty, so an unimported
model is silently never created.

```
sourcetype
  id          UUID pk
  name        str  unique index      -- the user-facing "type"
  data_id     UUID unique FK data.id -- exactly one Data per type
  created_at  datetime
  data        Relationship -> Data          (back_populates="source_type")
  urls        Relationship -> list[SourceUrl]

sourceurl
  id          UUID pk
  type_id     UUID FK sourcetype.id
  url         str                    -- as registered, full URL
  object_key  str  unique index      -- _normalize_object_key(url); the match + dedup key
  created_at  datetime
  type        Relationship -> SourceType
```

Also add a Python-only `source_type: Optional["SourceType"] = Relationship(back_populates="data")`
to `Data` ([aero/models/data.py:46](../aero/models/data.py#L46) area). No column — the FK lives on
`sourcetype` — so no DDL.

### Two new columns — free on a fresh DB, hand-written ALTERs only when upgrading in place

```
data.no_copy           boolean NOT NULL DEFAULT false
dataversion.source_key varchar NULL
```

- `data.no_copy` — orthogonal to type, per the decision above.
- `dataversion.source_key` — which registered object key produced this version. This is what makes
  per-URL dedup work **uniformly on both paths**: on the copy path `data_file.file_name` is a random
  GCS uuid, so the key has to be recorded separately. NULL on every pre-existing row → untyped
  sources fall back to today's tail-checksum compare, i.e. unchanged behavior.

**On a database with no tables yet there is nothing to do** — `create_all` builds `data` and
`dataversion` from the models with these columns already present. That covers a new instance
brought up on a new `PG_VOLUME` (see
[running-multiple-instances.md](running-multiple-instances.md)) or on a new `DATABASE_NAME` against
an existing volume — `SQLALCHEMY_DATABASE_URI` is composed from it at
[config.py:18](../aero/config.py#L18). Cost of that route: no registered sources/flows/versions
carry over, and the new instance wants its own `SEARCH_INDEX` or the Globus Search index still
points at the old Data UUIDs.

**Upgrading an existing database needs these two by hand**, via adminer
([docker-compose.yml:63-72](../docker-compose.yml#L63-L72)):

```sql
ALTER TABLE data        ADD COLUMN no_copy    boolean NOT NULL DEFAULT false;
ALTER TABLE dataversion ADD COLUMN source_key varchar;
```

Tests are unaffected either way — `tests/conftest.py:85` builds SQLite fresh from the models.

**The partial-upgrade case is the trap.** `create_all` is create-if-missing *per table*, not per
column: on an existing DB it happily creates `sourcetype`/`sourceurl` while silently skipping the
two new columns. Startup looks clean and the failure surfaces later as an opaque 500 (blanket
`except Exception` at [provenance.py:106](../aero/routers/provenance.py#L106)). So add a startup
assertion in the `lifespan` hook ([aero/main.py:14](../aero/main.py#L14)) that both columns exist
and log a clear, actionable error — this matters *more* on the fresh-DB route, not less, because
it's the only thing that catches a half-migrated instance.

## Server changes — `aero`

### 1. Type routes — [aero/routers/data.py](../aero/routers/data.py)

**Register these above `get_data` at [data.py:81](../aero/routers/data.py#L81)**, following the
`/data/search` precedent at [data.py:69](../aero/routers/data.py#L69) — FastAPI matches in
declaration order, so a `/data/types` declared after `/{id}: UUID` is shadowed and 422s.

| Route | Router | Behavior |
|---|---|---|
| `GET /data/types` | authed | list types: `{name, data_id, no_copy, urls[]}` |
| `GET /data/types/{name}` | authed | one type, or 404 |
| `POST /data/types/{name}/urls` | authed | associate a URL with an existing type → `{data_id, url, object_key}`; 409 if the normalized key is already registered (anywhere) |
| `POST /data/source` | authed | create a source `Data` **without a flow** — `{name, url, type?, no_copy, collection_uuid?, collection_url?, description?}` → the created `Data`. Reuses the existing `create_data` helper ([data.py:120](../aero/models/data.py#L120), currently only exercised by tests). |

`POST /data/source` is the no-copy create path: no endpoint, no Globus function, no flow.

### 2. Type on flow registration — [aero/routers/flow.py:158-175](../aero/routers/flow.py#L158-L175)

For the *copy* path the `Data` is still born inside `POST /flow/register`. Accept an optional
`type` on each `output_data` entry and, right after the `Data(...)` is constructed at flow.py:164,
create the `SourceType` + first `SourceUrl` bound to it. 409 if the type name already exists (the
client should have taken the associate path). Keeps first-create atomic in one round trip.

### 3. One resolver, shared by both notify routes — [aero/routers/data.py](../aero/routers/data.py)

Replace the inline scan in `notify_by_object`
([data.py:243-266](../aero/routers/data.py#L243-L266)) with:

```
_resolve_notify_target(session, file_id) -> (Data, object_key)
  target = _normalize_object_key(file_id)
  1. indexed exact lookup: SourceUrl.object_key == target   -> (su.type.data, su.object_key)
  2. else the existing legacy scan over Data.url, skipping any Data that has a source_type
     (its URLs are already covered by step 1), with the same _normalize_full_url 409 tie-break
  3. else 404
```

Step 1 turns today's full-table scan + O(n) Python compare into an indexed lookup, because the
normalized key is computed once at registration instead of once per row per notify.

`POST /data/{id}/notify` ([data.py:205](../aero/routers/data.py#L205)) keeps its PK lookup, then
resolves *which* URL fired: normalize `payload.key or payload.url` and match it against that type's
`SourceUrl`s. **404 if the Data is typed and the URL is not registered.** Untyped Data behaves
exactly as today (any URL accepted), so nothing existing breaks.

Add `dedup: bool = True` to both `NotifyIn` ([data.py:116](../aero/routers/data.py#L116)) and
`NotifyByObjectIn` ([data.py:127](../aero/routers/data.py#L127)) — `{"dedup": false}` forces a
version even when the etag is unchanged. `key`/`etag`/`size` are already accepted on both models
and currently read by nothing; they become load-bearing.

### 4. No-copy branch — `_run_event_ingestion` ([data.py:181-202](../aero/routers/data.py#L181-L202))

Widen to `(session, data, trigger_url, signed_url, object_key, etag, size, dedup)`, where
`signed_url = payload.url` (may be `None`) and `trigger_url`/`object_key` come from the resolver.

- **`data.no_copy`** → no flow lookup, no Globus run:
  - `checksum = etag`; if absent, best-effort `HEAD` on `signed_url or trigger_url` for
    `ETag`/`Content-Length`; still absent → 400.
  - `data.add_new_version(new_file=object_key, format=<key suffix>, checksum=checksum, size=size,
    created_at=datetime.now(), source_key=object_key, dedup=dedup)` — server clock, same one-clock
    rule as the provenance fix at [provenance.py:78](../aero/routers/provenance.py#L78).
  - Dedup hit → return `{"status": "unchanged"}` and **do not** rerun.
  - Else `data.rerun_flow(session, trigger_url=trigger_url, signed_url=signed_url)`.
- **else** → existing `flow._run_ingestion_flow(...)`, now also forwarding `object_key` and `dedup`.
  Its existing per-run `source_url` becomes `signed_url or trigger_url` — the same pull, just
  explicit that the signed URL is the optional overlay rather than the only option.

### 5. Per-URL dedup — `Data.add_new_version` ([data.py:51-100](../aero/models/data.py#L51-L100))

Add `source_key: str | None = None, dedup: bool = True`. When `source_key` is set, compare the
checksum against the latest version *with that same `source_key`*
(`select(DataVersion).where(data_id==self.id, source_key==key).order_by(version.desc()).first()`)
instead of `last_version()`. `dedup=False` skips the compare entirely. Stamp `source_key` onto the
new `DataVersion`. Version numbering stays global per Data. Existing callers pass neither and are
byte-for-byte unaffected.

While here, fix `_conf_search_entry`
([data_version.py:63](../aero/models/data_version.py#L63)): it unconditionally dereferences
`self.data_file.file_name` and builds `collection_url + "/" + file_name`. For a no-copy version
`collection_url` is typically `None`, yielding a junk `"None/traffic/a.xml.gz"`. Guard the
`data_file is None` case (a live `AttributeError` today) and fall back to `source_key`/`data.url`
when there is no collection.

**Also fix `Data.last_version()`** ([data.py:112-117](../aero/models/data.py#L112-L117)). It
returns `self.versions[len(self.versions) - 1]` — the last element in whatever order SQLAlchemy
happened to load the relationship, not the highest `version`. It works today by accident. Replace
the body with `max(self.versions, key=lambda v: v.version or 0)` behind an empty-list guard: same
signature, no session needed, no callers change. This matters more once types exist, because a type
accumulates versions from several URLs and `/data/{id}/latest` — which `get_versions` calls on
every analysis run — has to return the genuinely newest one. It also fixes the ANY/ALL rerun gate,
which compares `last_version().created_at > flow.last_executed`.

### 6. Copy-path plumbing for `source_key` + `dedup`

Mirrors the existing `source_url` deepcopy-injection exactly
([globus/client.py:207-209](../aero/globus/client.py#L207-L209)):

- `Flow._run_ingestion_flow` ([flows.py:114](../aero/models/flows.py#L114)) — add
  `source_key=None, dedup=True`.
- `GlobusClient.run_ingestion_flow` ([client.py:180](../aero/globus/client.py#L180)) — inject
  `kwargs["aero"]["source_key"]` and `["dedup"]` into the same deep copy. Never persisted.
- [provenance.py:82](../aero/routers/provenance.py#L82) — pass `source_key=o.get("source_key")`,
  `dedup=o.get("dedup", True)` into `add_new_version`. `ProvRecord`'s value type is a permissive
  dict, so the extra keys ride along with no model change.

### 7. Threading the trigger URL + optional signed URL into the analysis (no-copy only)

Only the no-copy path injects URLs — a copy source has a real GCS object and keeps reading it from
the collection, unchanged.

**Two values, kept distinct end to end:**

| | source | meaning | persisted? |
|---|---|---|---|
| `trigger_url` | the matched `SourceUrl.url` (or `Data.url` when untyped) | the object's stable identity — which object changed | yes, as a `sourceurl` row |
| `signed_url` | the notify's `payload.url`, i.e. what the relay presigned | transient read credentials for those bytes; **may be absent** (public bucket, or a relay that doesn't presign) | never |

The notify body already carries both: `file_id` resolves to the trigger URL, `url` is the signed
one. Keeping them separate means the analysis can log/branch on *which* object it got without
having to parse a presigned URL, and a signed URL expiring can never corrupt stored state.

- `Data.rerun_flow` ([data.py:102](../aero/models/data.py#L102)) — add
  `trigger_url=None, signed_url=None`; pass them plus `source_data_id=self.id` to `_run_flow`. The
  `/prov/new` caller keeps calling with no args.
- `Flow._run_flow` ([flows.py:141](../aero/models/flows.py#L141)) — add the three; forward from the
  `ANY_INPUT`/`ALL_INPUT` branches.
- `GlobusClient.run_flow` ([client.py:83](../aero/globus/client.py#L83)) — add the three. When
  `trigger_url` is set, `copy.deepcopy(tasks)` and set `entry["trigger_url"]` and (when present)
  `entry["signed_url"]` on the `input_data` entry whose `entry.get("id") ==
  str(source_data_id)`. Never mutate the stored `function_args`. (`copy` is already imported.)
- Defensive fix in the same branches: `s.last_version()` returns `None` for a Data with no
  versions, so the `created_at > last_executed` gate raises `AttributeError`. Registering an
  analysis against a brand-new type before its first notify hits this directly. Treat `None` as
  "no new data".

### 7b. Resolving a no-copy input that was *not* this run's trigger

The injection above only reaches the one `input_data` entry whose id matches the changed source.
That leaves three ways an analysis can face a no-copy input with no URL at all:

1. **The registration-time run** — `create_flow` calls `_run_flow`
   ([flows.py:262](../aero/models/flows.py#L262)), so an `ANY` flow fires immediately at
   registration with no notify in the picture.
2. **Multi-input flows** where two inputs are no-copy — a change to B injects onto B only, and A's
   entry is bare.
3. **`ALL_INPUT`** — same shape as 2.

Left unhandled this is worse than an error: §10's branch falls through to the GCS path,
`collection_url` is `None` for a no-copy source, the fetch of `"None/<key>"` returns an error page,
and the GCS branch has no `raise_for_status()` — so that page is written to the temp file and
handed to the analysis function as if it were data.

**AERO can always reconstruct the stable URL**, so resolve it on every run rather than only on the
triggering one: `dataversion.source_key` holds the object key and `sourceurl.object_key` maps it to
the registered `url`.

- **`GET /data/{id}/latest`** ([data.py:100](../aero/routers/data.py#L100)) — add `source_key`,
  `no_copy`, and a resolved `trigger_url` to `VersionOut`
  ([data.py:46](../aero/routers/data.py#L46)). The server looks up
  `SourceUrl.object_key == version.source_key`, falling back to `data.url`.
- **`get_versions`** (`aero-client/aero_client/jobs.py:214`) — it already calls that endpoint per
  input; when the response says `no_copy`, also set `md["trigger_url"]`. So **every** no-copy input
  resolves to a URL on **every** run, and the notify-time injection becomes a refinement — the
  exact object that fired, plus the signature — rather than the only source of a URL.
- **Skip the registration-time run when any input Data is `no_copy`.** These flows are event-driven
  by nature and the registration run has no signed URL by construction; let the first notify be the
  first run. This subsumes the `last_version() is None` guard above for the no-copy case.

What genuinely cannot be reconstructed is the **signature**, not the URL. AERO holds no MinIO
credentials — deliberately; that is the entire reason the relay exists. So on a *private* bucket, a
non-triggered read of a no-copy input resolves a correct but unsigned URL and gets a 403. With
`raise_for_status()` in §10 that now fails loudly and points at the cause instead of silently
feeding an error page to the analysis.

## Client changes — `aero-client`

### 8. `create_source` — `aero-client/aero_client/api.py:229-284`

Add `type_name: str | None = None, no_copy: bool = False` (param named `type_name`; the CLI flag
and YAML key stay `type`). Dispatch:

| type | state | action |
|---|---|---|
| given | exists (`GET /data/types/{name}`) | `POST /data/types/{name}/urls` → return `{data_id}`. No flow, no function registration. |
| given | new, `no_copy` | `POST /data/source` with `type` + `no_copy=True` |
| given | new, copy | today's `register_flow`, with `type` added to the `output_data` entry |
| none | `no_copy` | `POST /data/source`, `no_copy=True`, no type |
| none | copy | unchanged |

New thin helpers alongside it — `get_source_type(name)`, `add_type_url(name, url)`,
`create_data_source(...)`, `list_source_types()` — using the established pattern: `build_url(...)`
(`aero_client/utils.py:68`) + `Authorization: Bearer {AUTH_ACCESS_TOKEN}`.

### 9. CLI — `aero-client/aero_client/cli.py:234-292`

Add `--type` and `--no-copy` to the `create` parser and to the `_pick` YAML merge (new YAML keys
`type`, `no_copy`). Make the required-args check mode-dependent: `endpoint_uuid` / `verifier` /
`collection_*` are only required on the copy path; associating a URL with an existing type needs
only `type` + `url`. Replace the `result["contributed_to"][0]["id"]` dig at cli.py:283 with a
helper that also handles `{"id": ...}` from `/data/source` and `{"data_id": ...}` from
`/types/{name}/urls`, so every path prints the UUID to paste into an analysis config.

Add an `aero types` subcommand listing each type with its Data UUID and URLs — this is the UUID
users need for `input_data`, and with many URLs per type there's otherwise no way to see the
grouping.

### 10. Fetch-by-URL in `aero_format` — `aero-client/aero_client/utils.py:355-373`

Branch the input-materialization loop: if `val.get("trigger_url")`, fetch with a plain
`requests.get(val.get("signed_url") or val["trigger_url"])` — **no** `get_transfer_token` header —
instead of `collection_url + file_bn`. Signed URL when the relay supplied one, plain trigger URL
otherwise (public bucket / no presigner), which is what makes the signed URL genuinely optional.
Unlike the GCS branch, `raise_for_status()` here: a 403 from an expired signature currently gets
written to the temp file as if it were data.

Name the temp file `<tmp_dir>/<uuid4>/<basename of the trigger URL path>` rather than a bare uuid —
derived from the **trigger** URL, so the name is stable and free of signing query params — so the
analysis receives a path keeping the original filename and extension. The existing cleanup at
utils.py:412-419 unlinks it as before.

**Giving the analysis function the URL itself.** `fn_in[name]` stays the local path, so every
existing analysis function keeps working untouched. Additionally, inspect `fn`'s signature and,
only if it declares them, pass `<name>_url` (the trigger URL) and `<name>_signed_url`. Opt-in by
parameter name, no change for functions that don't want them.

`get_versions` (`aero-client/aero_client/jobs.py:214`) is unchanged: the server-injected URLs ride
alongside the `version`/`file_bn`/`encoding` it fills from `GET /data/{id}/latest`, and a no-copy
version still has a `DataFile` (`file_name` = object key) so that endpoint keeps working.

### 11. `download` — `aero-client/aero_client/jobs.py:133-157`

Alongside the existing `source_url` override at jobs.py:133, copy `kwargs["aero"]["source_key"]`
and `["dedup"]` into `kwargs["aero"]["output_data"][data["name"]]` next to the metadata it already
writes at jobs.py:148-157, so `database_commit` carries them to `/prov/new`. `aero_format`'s
`.update(**metadata)` at utils.py:410 doesn't clobber them.

## Relay — `aero-testing/relay/`

`relay.py` already posts `{file_id, url}`; add `etag`, `size`, and `key` from the MinIO event
record (all present in the event JSON). `file_id` remains the stable identity that resolves to the
trigger URL; `url` is the **signed** URL and stays optional — a relay pointed at a public bucket,
or one run with presigning disabled, simply omits it and AERO falls back to the registered trigger
URL for both the HEAD and the analysis fetch. Worth an explicit `RELAY_PRESIGN=0` switch so that
path is exercisable rather than theoretical.

Treat a **404 from AERO as expected** — an unregistered object under a watched bucket — and
log-and-drop rather than erroring, since strict 404 is now the designed behavior. Add the same
fields plus `dedup` to `scripts/notify.sh` for manual forcing. New `configs/minio_typed.yaml`
demonstrating `type:` + two `aero create` invocations.

## Verification

**Offline, before any live run.** The `aero` repo has real route tests; extend
`tests/routes/notify_routes_test.py` and `tests/model/data_test.py` (SQLite `create_all` picks up
new tables *and* columns for free): type CRUD + 409 on a duplicate key; presigned URL resolves to
its registered key; unregistered URL 404s; per-URL dedup (A,B,A with A unchanged → no third
version) and `dedup: false` forcing one; untyped sources unchanged. Plus the two fixes:
`last_version()` returns the highest `version` even when the relationship loads out of order
(construct versions deliberately out of order — this is the test that fails against today's
positional code), and `GET /data/{id}/latest` returns a resolved `trigger_url` for a no-copy
version. In `aero-client`, unit-test the `aero_format` URL branch against the existing local
uvicorn harness (`tests/conftest.py` + `tests/download_server.py`) — including the §7b case where
only `trigger_url` is present and the case where the fetch 403s, which must raise rather than write
the body — plus `create_source` dispatch and `aero create --help` parsing. Run via
`.venv/bin/pytest`.

**Live, against a configured server + endpoint.**
1. Either bring up a new instance on a fresh `PG_VOLUME`/`DATABASE_NAME` (nothing to migrate), or
   apply the two ALTERs via adminer to the existing DB. Restart the server; `create_all` adds
   `sourcetype`/`sourceurl` and the startup check confirms both columns are present.
2. `aero create --type traffic --no-copy --url .../a.xml.gz` → note the Data UUID.
   `aero create --type traffic --url .../b.xml.gz` → **same** UUID, no new flow. `aero types` shows
   one type, one UUID, two URLs.
3. Register one analysis flow against that UUID (`policy: ANY`).
4. Change object **a** in MinIO → relay posts `{file_id, url, etag, size}` → new version with
   `source_key` = a's key, **no GCS object written** → analysis runs, fetches a via the signed URL,
   and (with a test function declaring `<name>_url`) logs a's **trigger** URL → summary version.
   Repeat for **b** → same analysis, same source Data, b's bytes.
   Then re-run with presigning off (relay omits `url`) → analysis fetches the trigger URL directly.
5. Re-notify **a** unchanged → "unchanged", no version, no analysis run — *including* after b
   changed in between (this is the per-URL dedup that today's tail compare would get wrong).
   Then re-notify a with `{"dedup": false}` → version created and analysis runs.
6. Notify an unregistered URL in the same bucket → 404, nothing happens.
7. Confirm `Flow.function_args` in the DB contains **no** `trigger_url`/`signed_url`/`source_key`/
   `dedup` after all of the above (the deepcopy holds).
8. Regression: the existing untyped copy source (`configs/minio_ingestion.yaml`) still ingests to
   GCS exactly as before.
9. §7b: register a second analysis taking **two** inputs, the no-copy type and an existing copy
   source. Change the copy source → the analysis runs and the no-copy input resolves its
   `trigger_url` from `/latest` with no notify of its own. On a private bucket expect a loud 403
   naming the unsigned URL, not a corrupt input file.

## Git

- `aero`: `feature/typed-notify-sources` off `develop`, `--no-ff` merge when verified.
- `aero-client`: `feature/typed-notify-sources` off `develop`, `--no-ff` merge when verified.
- `aero-testing` (`main`): relay fields, `notify.sh`, new config. No secrets.

## Risks

- **Schema drift on an existing DB is the deployment risk** — and it's avoidable: bring the
  instance up on a fresh `PG_VOLUME`/`DATABASE_NAME` and `create_all` handles everything, at the
  cost of re-registering sources and flows. Upgrading in place needs the two ALTERs by hand, with
  the startup column check as the guard against a half-migrated instance. There is no migration
  tooling and no plan here to add it; standing Alembic up properly (rewriting `env.py` off Flask,
  adding `script_location`) is deliberately out of scope and would be its own task.
- ~~`Data.last_version()` is positional~~ — now fixed in §5 rather than tolerated.
- **ETag ≈ md5 only for single-part uploads.** Multipart objects get a different scheme, so dedup
  degrades to "new etag ⇒ new version" — still correct, just no md5 equality guarantee.
- **A no-copy input can be read without a signed URL.** §7b makes the *URL* always resolvable, so
  the remaining gap is narrow and specific: on a private bucket, any read that wasn't triggered by
  a notify (a multi-input flow where a sibling changed) has a correct but **unsigned** URL and gets
  a 403. AERO cannot close this — it holds no MinIO credentials by design. It now fails loudly via
  `raise_for_status()` instead of writing the error page to a temp file. If this becomes a real
  workflow rather than an edge case, the fix is a re-sign hook the relay exposes, which is its own
  task.
- A typed source's `Data.url` holds only the *first* registered URL. It stays as descriptive
  metadata; resolution goes through `sourceurl`, and the legacy `Data.url` scan explicitly skips
  typed Data so the two can't double-match.

## Related

- [minio-presigned-ingestion-plan.md](minio-presigned-ingestion-plan.md) — the copy path this
  builds on (implemented, live-tested).
- [event-driven-s3-ingestion-plan.md](event-driven-s3-ingestion-plan.md) — the original
  `INGESTION_EVENT` design.
- [minio-ingestion-howto.md](minio-ingestion-howto.md) — how to register + trigger today.
