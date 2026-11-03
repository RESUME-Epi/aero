# Endpoint failover

Running an analysis somewhere else when its Globus Compute endpoint is down.

## The problem

Submitting a run to Globus Flows **succeeds whether or not the compute endpoint is
alive**. The run then stalls or fails inside Globus, long after the HTTP request that
started it has returned 200. And because `Flow.last_executed` is stamped on submission
rather than on success ([models/flows.py](../aero/models/flows.py)), the version that
triggered that run is now older than `last_executed` and will never trigger it again.

So a dead endpoint does not produce an error — it produces a run that quietly never
happened, plus a flow that has forgotten it needed to. The only recovery is a human
noticing and running `aero run --force`.

## What this does

Before submitting a run, AERO asks Globus Compute whether the flow's endpoint is online.
If it explicitly reports **offline** and the flow has a failover configured, the run goes
to the failover instead.

Two run paths are covered, which are the two that submit immediately in response to an
event:

| | |
|---|---|
| `Flow._run_flow` | `ANY_INPUT` / `ALL_INPUT` analyses — triggered from `POST /prov/new` after an ingestion, from `POST /data/notify` for a no-copy source, and from `POST /flow/{id}/run` by hand |
| `Flow._run_ingestion_flow` | `INGESTION_EVENT` ingestion — the copy path of the notify webhook |

Timer-driven flows (`INGESTION`, `TIMER`) are **not** covered. A Globus Timer is created
once and then fires for days; the endpoint's state when the timer was created says nothing
about its state when it fires.

## Failover is per-flow

Each flow names its own fallbacks. Two analyses pinned to the same primary endpoint can
fail over to different places, because what makes a fallback valid is whether it can run
*that* analysis — the right `AERO_PROFILE`, the right Python, reachability to the object
store it reads.

They live in the `flowfailover` table ([models/failover.py](../aero/models/failover.py)),
one row per target:

| Column | |
|---|---|
| `flow_id` | the flow this belongs to |
| `priority` | ascending try order, unique within the flow |
| `endpoint` | the Globus Compute endpoint to use instead |
| `function_id`, `pull_function_id`, `commit_function_id` | optional; `NULL` means reuse the flow's own |
| `enabled` | `false` keeps the row but skips it |

The three function columns exist because a function registered from one Python minor
version does not deserialize on an endpoint running another — the symptom is
`loss of worker 0`. An endpoint on a different Python needs the functions re-registered
there, which gives them new uuids.

A new table rather than new columns on `flow` is also what the no-migrations constraint
allows: `create_all()` creates missing tables but cannot alter existing ones
(see [CLAUDE.md](../CLAUDE.md)). Nothing has to be applied by hand for this.

## Selection

`Flow._select_run_target()` returns a `RunTarget` — an endpoint plus the three functions —
which is passed down to the submit call and **never persisted**. A run that failed over
does not rehome the flow.

1. No enabled failover rows, or `AERO_FAILOVER_ENABLED=false` → the primary, **with no
   status call at all**. This is what makes the feature optional: a flow with nowhere to go
   pays nothing, and the check runs inline in the notify webhook.
2. Primary not explicitly offline → the primary.
3. Otherwise the first enabled failover, by priority, that is not explicitly offline.
4. Everything offline → the primary, with a `WARNING`.

### Three-valued, and why unknown is not down

`endpoint_is_up()` returns `True`, `False`, or `None` — and **only `False` moves a run**.

`None` means the question could not be answered: a 403, a timeout, a network failure. An
unreachable status *service* says nothing about the endpoint, and treating it as down would
reroute every run in the deployment the moment Globus Auth hiccuped.

The same reasoning drives step 4. Submitting to a dead endpoint is bad, but dropping the
trigger is worse: the run at least fails visibly in Globus and `aero run --force` recovers
it, whereas a skipped run leaves nothing to find — and `last_executed` has moved either way.

### Caching

The check sits inline in `POST /data/notify` and `POST /prov/new`, so verdicts are cached
per endpoint for `AERO_ENDPOINT_CHECK_TTL` seconds (default 30). A burst of notifies makes
one Globus call. An `unknown` verdict is held for a quarter of that, since it usually means
something transient.

Each call is bounded by `AERO_ENDPOINT_CHECK_TIMEOUT` (default 20s), applied by waiting on
a worker thread. The timeout is generous on purpose — a slow check should return a real
verdict rather than time out into `unknown`, which never fails over. Worst case (primary
plus a two-entry chain, all unreachable, cold cache) a notify blocks for a minute before
falling back to the primary; lower it if the upstream event pipeline has a shorter deadline.

## The trap: the endpoint is stored twice

`create_flow` bakes `endpoint` and `function` into every task dict in the
`Flow.function_args` JSON, *as well as* storing the endpoint in `Flow.user_endpoint`. For
an analysis flow the task-dict copy is the one that matters most —
[workflow/user-flow/definition.json](../workflow/user-flow/definition.json):

```
getVersions    -> endpoint.$ = $.endpoint                        (top-level run input)
execute        -> tasks.$    = $.VersionResult.details.result[0]  <-- the task dicts
updateMetadata -> endpoint.$ = $.endpoint                        (top-level run input)
```

and `aero_client.jobs.get_versions` returns its `function_params` unchanged. So rewriting
only `run_input["endpoint"]` moves the pull and the commit while leaving **the analysis
itself** on the dead endpoint.

`GlobusClient.run_flow` therefore takes a `primary_endpoint` argument. When the selected
endpoint differs from it, each task still naming the primary has its `endpoint` and
`function` repointed — in a deep copy, following the same rule as the per-run signed url:
anything persisted into `function_args` would outlive the run that made it valid. Tasks
naming some *other* endpoint are left alone; they were put there deliberately.

The ingestion flow has no such duplication. Its run input carries
`osprey-worker-endpoint` / `download-function` / `database-commit-function` /
`user-wrapper-function` as plain keys and no task list.

## Using it

```bash
# At registration, via the `failover:` block in the register YAML
aero register -f analysis.yaml

# Or afterwards
aero failover add <flow-id> -e <backup-endpoint> -p 1
aero failover add <flow-id> -e <second-backup> -p 2 \
    -u <fn-uuid-registered-on-that-endpoint>

aero failover list <flow-id>
aero failover status <flow-id>
aero failover rm <flow-id> <failover-id>
```

```yaml
# analysis.yaml
endpoint_uuid: 7591d0fb-3d12-4ff8-b8fb-7a51146c4871
function_uuid: ...
failover:
  - endpoint: 0ed1ddbc-ae82-428c-8431-989a295eaa6a
    priority: 1
  - endpoint: 3aa0677e-f729-45ba-895c-adc9937fb276
    priority: 2
    # that one is on python 3.12, so its functions are different registrations
    function_uuid: ...
    pull_function_uuid: ...
    commit_function_uuid: ...
```

### Check it before relying on it

```bash
aero failover status <flow-id>
```

```
offline  primary   7591d0fb-3d12-4ff8-b8fb-7a51146c4871
online   failover  0ed1ddbc-ae82-428c-8431-989a295eaa6a
```

**If every line says `unknown`, failover will never fire.** An endpoint's status only
answers to a caller holding a role on it. AERO uses `FUNCX_SDK_CLIENT_ID` /
`FUNCX_SDK_CLIENT_SECRET` for this, falling back to `PORTAL_CLIENT_*` — set the former to
the confidential client the endpoints were started with. This is the one failure mode that
looks exactly like a working configuration, which is what the route is for.

### Preparing the failover endpoint

It is not enough for it to be online:

- `AERO_PROFILE` (and `AERO_CONFIG_FILE` if used) must be set in its
  `~/.globus_compute/<name>/user_environment.yaml`, matching the primary. Worker functions
  resolve `CONF` from the *endpoint's* environment at import, not the client's.
- It must reach whatever the analysis reads — MinIO, the GCS collection — since the
  endpoint, not the server, performs the HTTPS GET. That includes a CA bundle for a
  self-signed cert.
- If it runs a different Python minor version, re-register the functions there and give
  their uuids on the failover row.

An endpoint reporting `online` with no workers, the wrong profile, or a Python mismatch
will still fail the run. This is a liveness check, not a readiness check.

## What is not recorded

AERO persists no Globus run id and no run status, so there is nothing to write "this run
used the failover" onto. The decision appears in two places only: the server log
(`flow <id>: endpoint <x> is offline, failing over to <y>`), and the `endpoint` /
`failover` fields of the `POST /flow/{id}/run` response, which `aero run` prints.

Persisting it would mean tracking runs, which is a larger change than this one.
