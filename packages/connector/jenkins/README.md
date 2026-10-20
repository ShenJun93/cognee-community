# cognee-community-connector-jenkins

A Jenkins data-source connector for [cognee](https://github.com/topoteretes/cognee): sync your
CI history into memory and ask questions like *"why did the nightly build fail last week?"* or
*"which jobs started failing after the dependency bump?"*.

It exposes a `dlt` resource you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path (`resolve_dlt_sources` → `ingest_dlt_source` → `orphan_cleanup`). It provides
incremental sync and conditional hard deletes. **Omitted records are retained by default**:
Jenkins permissions and incomplete listings can hide jobs and builds that still exist.
Deletion requires independent authoritative evidence through `confirm_deleted`.

This source candidate pins **Cognee 1.6.5** and Python **>=3.10,<3.14**. The factory refuses
cores lacking `DOCUMENT_SYNC_VERSION >= 2` or `PIPELINE_SCOPE_ATTR`, before creating a resource
or making requests. The checked-in lockfile was generated on Ubuntu against Cognee 1.6.5;
fresh locked installations and Jenkins/DLT regressions passed on Python 3.10, 3.11 and 3.13.
Real Cognee memory acceptance is still under validation on the isolated CI branch. These
dependency and connector results do not establish automatic deletion acceptance.

## What gets ingested

| Record | Id | Content |
| --- | --- | --- |
| Job | `job:<full name>` | description, parameter names/types/descriptions, enabled/disabled |
| Build | `build:<full name>#<number>` | result, causes, description, and for failed builds the tail of the console log |

Parameter **default values are never read** (they often hold secrets), and the raw `config.xml`
is not ingested. Console logs are fetched only for builds whose result is in `log_results`
(`FAILURE` by default), streamed, cut to the last `max_log_bytes` (64 KiB by default), and values
that match common credential patterns (`token=…`, `password: …`) are masked on a best-effort
basis. Build descriptions and cause descriptions are also masked, independently of `redact_logs`.
Job content is masked as a whole. **This is not general-purpose secret detection or a DLP
guarantee**: arbitrary secrets, encoded values, identifiers, URLs and values split at the log
boundary can remain. Review the trusted Jenkins scope and content before ingestion.

## Install

```bash
uv pip install cognee-community-connector-jenkins
# or, from this monorepo:
cd packages/connector/jenkins && uv sync
```

## Usage

```python
import cognee
from cognee_community_connector_jenkins import jenkins_source

await cognee.remember(
    jenkins_source(
        base_url="https://jenkins.example.com",
        username="you",
        api_token="...",
        job_names=["backend/main", "nightly-e2e"],  # omit to sync every visible job
    ),
    dataset_name="ci_history",
    primary_key="id",
    write_disposition="merge",  # required: incremental upsert by record id
    max_rows_per_table=0,  # unlimited: orphan-cleanup sees the whole corpus
)

answer = await cognee.search(
    query_text="Why did the backend/main build fail most recently?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["ci_history"],
)
```

With a compatible document-mode core, re-running `remember(...)` loads new completed builds.
The example retains omitted history because it supplies no deletion authority. Reuse a stable
`resource_name` for each installation; use different names for distinct installations, including
when they share a dataset. Scoped cleanup and alternating datasets still require end-to-end
verification on the approved runtime.

> **`write_disposition="merge"` is required** — the add pipeline defaults to `"replace"`, which
> would wipe the synced history on the second sync.

### Options

| Argument | Default | Meaning |
| --- | --- | --- |
| `job_names` | `None` | Full job names (`"folder/job"`). `None` discovers every job, descending into folders and multibranch projects. |
| `max_builds_per_job` | `20` | On a job's first sync, ingest only its most recent N builds (`0` = all). Later syncs fetch every new build. |
| `log_results` | `("FAILURE",)` | Build results whose console log is ingested, e.g. `("FAILURE", "UNSTABLE")`. |
| `max_log_bytes` | `65536` | Keep only the last N bytes of each ingested log. |
| `max_folder_depth` | `5` | How deep discovery descends into folders. |
| `redact_logs` | `True` | Best-effort masking of some credential formats in console logs. Not comprehensive DLP. |
| `resource_name` | `jenkins_records` | Stable DLT resource/scope name; choose a unique name per Jenkins source. |
| `service` / `session` | `None` | Requests-compatible client; aliases, pass only one. Caller owns injected clients; connector closes clients it creates. |
| `check_active` | `None` | Synchronous callback; raise to cancel. Checked before requests, retries, log chunks, rows and final state update. A request already in progress can last until its timeout. |
| `confirm_deleted` | `None` | `(full_name, build_number_or_None) -> bool`. Return exactly `True` only with independent authoritative deletion evidence. `None` as the second argument identifies a whole job. |

## How sync and forget-on-delete work

- **Bounded requests.** Build numbers are requested newest first using `allBuilds[number]{start,end}`
  in pages of at most 100. An unchanged job needs one build page; an initial sync stops after
  enough recent builds are found. `max_builds_per_job=0` deliberately scans all history in pages.
  Large incremental backlogs also require multiple pages. Servers returning more than 100 entries,
  duplicate or out-of-order build numbers abort the run.
- **Incremental cursor.** A run pages back to the prior cursor and fetches newer builds. A running
  build holds the cursor so it can be retried. Offset paging assumes a stable Jenkins listing;
  concurrent changes detected as duplicate or out-of-order pages abort. Silent server filtering
  cannot be detected universally. Missing `allBuilds` falls back to `lastBuild`, so intervening
  history may be unavailable; a present null or malformed list aborts.
- **Conditional deletion.** A missing job, window omission, rotation or HTTP 404 is insufficient
  proof of deletion. Without `confirm_deleted`, previously ingested history remains in DLT state
  and storage. Do not implement the callback by checking this same listing or a bare 404: it must
  consult a separate trusted deletion feed or a complete administrative snapshot whose visibility
  and completeness have been established. It is consulted for omitted known jobs/builds only.
  Removing a job from `job_names` also retains it by default. An entirely empty known-job sweep
  retains everything even with a callback. Confirmed deletes emit DLT hard-delete rows; deletion
  of corresponding Cognee documents, graph and vectors remains an acceptance requirement.
- **Fail closed.** Missing or malformed children, partial-listing signals, a newly empty known
  folder, a depth cap, or explicit job/build 404 aborts without committing source state. Unknown
  silent omissions are retained. DLT must complete extraction/load successfully to persist state;
  rows yielded before a later failure are not evidence of a committed transaction.
- **HTTP behavior.** Retry only GET on transient responses, at most four retries. Numeric and HTTP
  date `Retry-After` values are capped at 30 seconds per wait, including cloned retry objects.
  Public HTTP failures omit request URLs and headers; raw exception chains are suppressed.
- **Statistics.** `resource.cognee_sync_stats` resets on each extraction. Integer `scanned` counts
  evaluated jobs and candidate builds; `skipped` counts unchanged jobs and running/already ingested
  candidates. `emitted` and `deleted` count yielded rows. `failed` counts aborted extractions;
  `failed_rate_limit` counts aborts on a final HTTP 429. These are extraction counters, not committed
  DLT loads or stored memory counts.

## Setup

1. In Jenkins, open your user (top-right) → **Security** → **API Token** → **Add new Token**.
   A read-only user with *Overall/Read* and *Job/Read* is enough; the connector only sends `GET`
   requests.
2. Pass `base_url`, `username` and `api_token` (sent as HTTP Basic auth), plus your `LLM_API_KEY`
   like any other cognee run.

To try it locally without a Jenkins server:

```bash
docker run --rm -p 8080:8080 jenkins/jenkins:lts-jdk21
```

## Testing

```bash
uv run --with pytest pytest tests/
```

The source tests use fake Jenkins and include a real DLT SQLite merge candidate, bounded
10,000-build history, malformed/capped listing, masking, cancellation and retry regressions.
They have **not run in this Windows environment**: enforced WDAC blocks the Cognee native
stack (including NumPy, pendulum, aiohttp and multidict). Do not reroute around that policy.
Only Ruff, AST parsing and diff checks are authorized here.

`tests/test_remember_acceptance.py` contains real two-pass `remember()` assertions for documents,
vector payloads, graph entities, two sources and alternating datasets. `tests/remember_worker.py`
implements the real store readers and launches no paid models. On an approved non-Windows runner,
the pytest wrapper starts a fresh process with only explicit test settings and disposable paths.
It rejects unexpected inherited environment settings, disables dotenv loading, verifies provider
configuration and guards SQLite, graph, vector and DLT constructors against shared paths. It also
rejects symlink escapes, blocks Python network connections, requires the bundled graph extension,
and forbids global prune. Model stubs are installed before engine setup. The JSON result file is
written only after all memory assertions succeed; no such result has been produced here.

The memory test skips by default and remains disabled on the WDAC-blocked Windows host, even with
the opt-in. The opt-in cannot bypass the isolation checks. After the owner approves a compliant
runner and installation, run there (not on this host):

```bash
python -m pytest tests/ --run-jenkins-memory -q
```

The DLT failure regression additionally reopens the same disposable SQLite pipeline after partial
extraction fails and checks the persisted cursor, unchanged rows and successful retry. None of
these tests have been executed under the current source-only authorization. Runtime validation
must still confirm the harness, native backend paths and cleanup behavior. A skip is not memory
evidence. The former add-only test and example no longer call global prune.
