# Endpoint failover: server internals

Running an analysis somewhere else when its Globus Compute endpoint is down.

This page covers how the server implements failover. For what failover does from a user's point of
view, and how to set it up and prepare a failover endpoint, see
[Endpoint failover](https://github.com/RESUME-Epi/aero-client/blob/main/docs/failover.md) in the
aero-client docs.

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

## Status checks need a credential with a role on the endpoint

An endpoint's status only answers to a caller holding a role on it. AERO uses
`FUNCX_SDK_CLIENT_ID` / `FUNCX_SDK_CLIENT_SECRET` for this, falling back to
`PORTAL_CLIENT_*` — set the former to the confidential client the endpoints were started with.
Without a role, every check returns `unknown` and failover never fires. This is the one failure
mode that looks exactly like a working configuration; `GET /flow/{id}/endpoint-status`
(`aero failover status`) is there to expose it.

## Configuration

| Variable | Default | |
|---|---|---|
| `AERO_FAILOVER_ENABLED` | `true` | `false` turns off both the pre-submit check and failure-triggered failover |
| `AERO_ENDPOINT_CHECK_TTL` | `30` | Seconds a status verdict is cached per endpoint; `unknown` is held for a quarter of this |
| `AERO_ENDPOINT_CHECK_TIMEOUT` | `20` | Seconds to wait for one status check |
| `FUNCX_SDK_CLIENT_ID` / `FUNCX_SDK_CLIENT_SECRET` | `PORTAL_CLIENT_*` | Credential for the status checks; see above |

## Failure-triggered failover

Everything above happens **before** a run is submitted, and only moves a run off an endpoint
that reports offline. That misses a run that reaches a healthy endpoint and fails there.
The motivating case is an analysis that submits a job to an HPC cluster and polls it: the
submission is rejected, or the job dies, and the analysis knows it while the endpoint still
reports `online`. Re-submitting through `POST /flow/{id}/run` would send it straight back.

So the analysis can ask for the run to go elsewhere:

```
POST /flow/{id}/failover/run
{
  "failed_endpoint": "<the endpoint the run failed on>",   // default: the flow's primary
  "tried_endpoints": ["<endpoints earlier runs in this chain failed on>"],
  "reason": "PBS job 1234 exited 271",
  "input_versions": {"<data id>": "<DataVersion id>"}
}
```

The worker side is `aero_client.failover.request_failover(aero_run, reason)`, which posts
this and then raises so the failed run's commit step never runs. `aero failover run
<flow-id>` posts it by hand. The response is the same shape as `POST /flow/{id}/run`.

### Selection

`Flow._select_failover_target(exclude)` takes the candidates in order — the primary, then
the enabled failovers by priority — and returns the first one that is not in `exclude`
(`failed_endpoint` plus `tried_endpoints`) and not explicitly offline. As before, `unknown`
does not disqualify.

The primary is a candidate. A run that originally failed over because the primary was
offline, and then failed on the failover, has the primary *not* in its tried list (it was
offline, not tried), so once the primary is back the re-run goes home.

`AERO_FAILOVER_ENABLED=false` turns this off too. With nothing to choose, the route answers
`submitted: false`.

### The run context, and why the chain terminates

The analysis cannot see the endpoint it is running on — that lives in the task envelope,
which Globus Compute does not hand to the function. So `GlobusClient.run_flow` injects,
into every analysis task's `aero` block, on every run:

```python
aero["run"] = {"endpoint": "<where this run went>", "tried_endpoints": [...]}
```

The client's `aero_format` wrapper passes this (with `flow_id` and the input version ids) to
a function that declares an `aero_run` parameter. `request_failover` sends `endpoint` back as
`failed_endpoint` and `tried_endpoints` as-is. The route appends the failed endpoint and
gives the result to the new run as *its* `tried_endpoints`.

That list only grows along a chain, and the candidates are finite. A run that fails
everywhere therefore ends with `submitted: false`, "no failover endpoint left to try",
instead of looping.

### Same data, not latest data

A failover re-run is **always forced**, because the failed run already moved
`last_executed` past the data it was meant to process.

It also processes the **same revisions** the failed run did, not whatever is latest by the
time the re-run starts. HPC jobs run for hours, and a newer version may have landed in the
meantime. `input_versions` maps each input to the `DataVersion` id it used. The route looks
each one up (400 unless it really is a version of one of the flow's inputs) and builds
the fields `GET /data/{id}/latest` would have supplied: `version`, `version_id`, `file_bn`,
`encoding`, plus a resolved `trigger_url` for a no-copy input. It passes them to
`run_flow` as `pinned_versions`, which writes them onto the matching `input_data` entries
in the per-run deep copy. A pinned input arrives with `version` set, so the worker's
`get_versions` leaves it alone. Inputs not pinned resolve to their latest as usual.

### Limitations

- **No fresh signed url.** A notify-triggered no-copy run carries a presigned
  `signed_url`; the re-run does not, since AERO holds no object-store credentials to sign
  with. On a private bucket only inputs marked `fetch: false`, which the analysis
  retrieves with its own credentials, survive a failover re-run.
- **Auth is the worker's.** `request_failover` authenticates the way the other worker
  callbacks in `aero_client.jobs` do, with the token stored on the endpoint host. Like
  those callbacks, it is only as good as that token is under `AERO_REQUIRE_AUTH=true`.
- **The endpoint needs the new client.** The function must be re-registered so it picks up
  the wrapper that passes `aero_run`, and the endpoint needs an `aero_client` that has
  `aero_client.failover`.

## What is not recorded

AERO persists no Globus run id and no run status, so there is nothing to write "this run
used the failover" onto. The decision appears in two places only: the server log
(`flow <id>: endpoint <x> is offline, failing over to <y>`, or `flow <id>: <reason> on
[<tried>], re-running on <y>`), and the `endpoint` / `failover` fields of the run
response, which `aero run` and `aero failover run` print.

Persisting it would mean tracking runs, which is a larger change than this one.
