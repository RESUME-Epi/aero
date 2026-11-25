# How an analysis flow dependent on ingestion data gets run

Traced through the aero server code under `aero/`.
File links below are relative to this file's location (`docs/`).

This page covers the server's implementation. For how flows connect and run from a user's point
of view, see [How flows run](https://github.com/RESUME-Epi/aero-client/blob/main/docs/how-flows-run.md)
in the aero-client docs.

## Short version

It's **event-driven but synchronous** — there's no background scheduler or message
queue. When an ingestion flow reports new output data via `POST /prov/new`, that same
request handler inline-detects every analysis flow depending on that data and runs the
ones whose policy is satisfied.

## The dependency link (set up at registration)

When a flow is registered ([routers/flow.py](../aero/routers/flow.py) `POST /flow/register`),
its `input_data` and `output_data` become graph edges in the DB:

- `input_data` → rows in the `FlowDerivation` junction table (`Flow.derived_from` → `Data`)
- `output_data` → rows in `FlowContribution` (`Flow.contributed_to` → `Data`)

So an analysis flow declares "I depend on Data X" by listing X in its `input_data`, and
the ingestion flow declares "I produce Data X" via its `output_data`. They're linked
through the shared `Data` record — no direct flow-to-flow reference.

**`output_data` is optional for an analysis.** Without it no `Data` is created and no
`FlowContribution` row exists, so nothing can be registered downstream of it. AERO still writes
a provenance record capturing which input versions the run consumed.

### Removing the flows around a Data

`DELETE /data/{id}/flows` (`aero delete-flows <data-id>`) removes both sides of the link at
once: the flow that produces the Data and the flows that consume it.

It deletes their **provenance records too** — `Provenance.flow_id` is a non-nullable FK,
so a flow that has ever run cannot go while its records remain — and cancels any Globus
timer driving them. A timer left behind would keep firing at a flow row that no longer
exists. If Globus refuses the cancellation the flow is still deleted and the response names
the orphaned job id, which is the only way to then clear it by hand.

The `Data`, its versions, and any source type/urls are kept, and it follows one level only.

One sharp edge while output-less: the registration dedup hash covers `output_data`
([flow.py](../aero/routers/flow.py) — the comment there explains why it had to). Two
*different* output-less analyses sharing a function, inputs and kwargs therefore hash the
same, and the second gets `501 Flow already exists`. Differing kwargs or a differing
function avoid it.

The trigger behavior is stored per-flow as `policy`
([models/flows.py:37](../aero/models/flows.py#L37) `TriggerEnum`):

| Policy | Value | Meaning |
|---|---|---|
| `INGESTION` | 0 | this flow *is* an ingestion flow (runs on a timer to pull data) |
| `TIMER` | 1 | run on a schedule |
| `ANY_INPUT` | 2 | run when **any** dependency gets a new version |
| `ALL_INPUT` | 3 | run when **all** dependencies have a newer version than last run |
| `INGESTION_EVENT` | 4 | an ingestion flow run on a notification, with no timer |
| `NONE` | -1 | never runs on its own |

## The trigger chain (data ingested → analysis launched)

1. **Ingestion completes** → client posts to
   [routers/provenance.py:57](../aero/routers/provenance.py#L57) `add_record` with the
   produced `output_data`.

2. **New version written** — for each output, `d.add_new_version(...)` appends an immutable
   `DataVersion` to the `Data` record
   ([provenance.py:82](../aero/routers/provenance.py#L82)).

3. **Dependents detected** — immediately after,
   [provenance.py:98](../aero/routers/provenance.py#L98) calls `d.rerun_flow(session)`.
   That method ([models/data.py:128](../aero/models/data.py#L128)) queries:
   ```python
   select(Flow).where(Flow.derived_from.any(id=self.id))
   ```
   i.e. every flow that lists this Data as an input — the analysis flows.

4. **Policy check + launch** — for each such flow it calls `_run_flow()`
   ([models/flows.py:400](../aero/models/flows.py#L400)). For an `ANY_INPUT` analysis it
   checks whether any dependency's `last_version().created_at > self.last_executed`; if so it
   fires:
   ```python
   GLOBUS_CLIENT.run_flow(endpoint_uuid=..., function_uuid=self.function_id,
                          pull_function_uuid=..., commit_function_uuid=...,
                          tasks=function_args, email=self.email)
   ```
   then stamps `last_executed`. `ALL_INPUT` is the same but requires *every* dependency to be
   newer.

5. **Globus launches the analysis** — `run_flow`
   ([globus/client.py:108](../aero/globus/client.py#L108)) submits the `USER_FLOW` Globus
   Flow (`0d8ace1a-…`, [globus/utils.py](../aero/globus/utils.py)) with the
   pull/commit/analysis function UUIDs.

6. **Loop closes** — when the analysis finishes it posts its own results back to
   `POST /prov/new`, which can in turn trigger anything downstream of *its* output.

## Things worth flagging

- **It runs inside the ingestion request.** The launch happens synchronously in the
  `POST /prov/new` handler, so an ingestion post blocks on kicking off dependent flows.
  There's no retry/queue — if `run_flow` fails, the whole request 500s
  ([provenance.py:112](../aero/routers/provenance.py#L112)).
- **`INGESTION`/`TIMER` don't launch from `rerun_flow`.** Only `ANY_INPUT`/`ALL_INPUT` fire
  the analysis via `run_flow`. `INGESTION` and `TIMER` set up timer flows instead — so an
  analysis meant to react to new data must be registered with `rule: 2` (ANY) or `rule: 3`
  (ALL), not `0`.
- **Two `# TODO`s sit right on this path** — `rerun_flow` is marked "Fix implementation"
  ([data.py:134](../aero/models/data.py#L134)) and the trigger call is marked "maybe fix"
  ([provenance.py:97](../aero/routers/provenance.py#L97)), so this mechanism may still be
  in flux.
