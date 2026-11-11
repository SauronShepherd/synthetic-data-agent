# SDA-08: build and test plan

This is the executable completion plan for SDA-08, with the cross-series checks
needed to keep SDA-00 through SDA-08 coherent. The implementation boundary is
deliberate: SDA-08 makes the agent durable and stops at `READY_FOR_GENERATION`.
Synthetic row generation and publication remain downstream workflow stages.

## 1. Completion contract

SDA-08 is complete when all of the following are true:

1. Requests, runs, plans, attempts, approvals, feedback, transitions, steps,
   artifact decisions, and migrations have durable contracts.
2. The local reference repository and PostgreSQL/Lakebase adapter implement the
   same state semantics: idempotency, legal transitions, optimistic concurrency,
   leases, retries, terminal records, and append-only history.
3. The Lakebase bundle declares a project, non-expiring branch, read/write
   endpoint, OAuth role, and operational database without embedding credentials.
4. The migration runner applies ordered, content-addressed SQL exactly once and
   preserves the compatibility runner for existing operators.
5. OAuth connection establishment refreshes the token for every retry, applies
   TLS, uses bounded exponential backoff, and never retries a business effect.
6. The workflow can hand metadata, profile, relationship, and pattern artifacts
   into a durable plan and explain what was produced, reused, invalidated, or
   failed.
7. Local unit, contract, static, package, and integration tests are green.
8. A real Databricks run using profile `sda` proves bundle deployment, Lakebase
   migration, PostgreSQL CRUD, and the SDA-04 through SDA-07 Spark jobs.

## 2. Build work packages

### WP-01 — Storage boundary and schema

- Keep operational state in Lakebase/PostgreSQL and analytical evidence in Delta.
- Create `sda_generation_requests` before `sda_runs`, with a real foreign key.
- Keep requests, runs, and immutable plan versions distinct.
- Persist transition history, audit/feedback records, attempt leases, and artifact
  decisions rather than overwriting evidence.
- Make step retries explicit with `attempt_number`; the uniqueness boundary is
  `(run_id, step_name, attempt_number)`.
- Keep the schema contract in `sql/lakebase_state_schema.sql` and the deployable
  migration in `migrations/versions/001_lakebase_state.sql`.

### WP-02 — Versioned migrations

- Discover migrations by ascending filename.
- Hash the filename and SQL content into the migration identifier.
- Create `sda_schema_migrations` before applying the first migration.
- Apply each migration in one database transaction and record it with
  `ON CONFLICT DO NOTHING`.
- Fail closed when the directory is empty.
- Retain `scripts/apply_state_schema.py <dsn> --schema ...` as a compatibility
  path; new environments use `--migrations-dir migrations/versions`.
- Run migrations from a controlled deployment identity, never on application
  startup and never with a long-lived password in source control.

### WP-03 — Repository semantics

- Preserve the storage-neutral `StateRepository` protocol.
- Make creation idempotent only when the complete identity/content matches.
- Enforce legal state transitions and reject terminal mutation.
- Enforce optimistic version checks on transitions.
- Record actor, reason, and timestamp for every transition.
- Enforce one running attempt per run/stage, renewable leases, stale recovery,
  explicit retry numbers, and immutable completed attempts.
- Make step checkpoints idempotent by step ID and retry-safe by attempt number.
- Normalize PostgreSQL JSONB arrays/objects back to the adapter's JSON-string
  contract so SQLite and PostgreSQL return the same domain objects.

### WP-04 — Lakebase connectivity

- Store only host, database, role, TLS mode, and timeout in configuration.
- Resolve the OAuth token lazily through a provider.
- Call the provider on every connection attempt so an expired token is replaced.
- Retry connection establishment with bounded exponential backoff.
- Do not retry repository mutations; a reconnect must not duplicate a business
  effect.
- Keep tokens out of logs, exception text, reprs, and test output.
- Use `PostgreSQLStateRepository.connect_lakebase(...)` for the production path.

### WP-05 — Declarative Lakebase infrastructure

- Declare `postgres_projects`, `postgres_branches`, `postgres_endpoints`,
  `postgres_roles`, and `postgres_databases` in `bundle/lakebase.yml`.
- Use a non-expiring branch and a read/write endpoint with a bounded autoscaling
  window and explicit scale-to-zero timeout.
- Use the current user OAuth role in dev and a target-configured service-principal
  identity in staging/prod.
- Make the database owner dependency explicit. If ownership changes, transfer
  ownership before allowing the bundle to replace a role.
- Keep project destruction conservative (`purge_on_delete: false`) and protect
  the durable branch from accidental destruction.

### WP-06 — Workflow handoff

- Persist the request before creating its run.
- Persist the scope decision and versioned plan with exact artifact IDs,
  fingerprints, source snapshots, thresholds, privacy policy, assumptions, and
  warnings.
- Record artifact reuse and invalidation decisions separately from artifact data.
- Advance the SDA-08 demo only through `READY_FOR_GENERATION` / the repository's
  equivalent `PLANNED` checkpoint; generation is intentionally a later stage.

## 3. SDA-08 test matrix

| ID | Test | Method | Exit criterion |
| --- | --- | --- | --- |
| ST-01 | Domain validation | Construct every state dataclass with valid and invalid values | Invalid identities, statuses, timestamps, retries, and warnings are rejected |
| ST-02 | Request idempotency | Create the same request twice and then with conflicting content | Same content returns the original; conflict raises `StateError` |
| ST-03 | Run idempotency | Repeat run creation with same and conflicting keys | Same run is reused only for matching request content |
| ST-04 | Transition graph | Execute every declared branch, including failure and retry | Only declared edges succeed; terminal states cannot move |
| ST-05 | Optimistic concurrency | Transition with a stale version | Stale update is rejected and current version is unchanged |
| ST-06 | Append-only history | Query transitions, feedback, audit events, and approvals after updates | Actor, reason, timestamps, metadata, and old decisions remain queryable |
| ST-07 | Plan immutability | Insert plan v1 and corrected v2 | Versions are ordered and v1 is not rewritten |
| ST-08 | Step retry history | Insert attempt 1 and attempt 2 for one named step | Both rows survive; only same attempt number conflicts |
| ST-09 | Attempt lease | Acquire, renew, expire, recover, and complete an attempt | One active worker is allowed; stale worker cannot finish after recovery |
| ST-10 | Artifact decisions | Record produced, reused, invalidated, and rejected decisions | Decisions are run-scoped, idempotent, and auditable |
| ST-11 | Restart recovery | Close and reopen SQLite repository | Requests, runs, plans, steps, attempts, and history survive |
| ST-12 | PostgreSQL translation | Exercise placeholders, table names, timestamps, JSONB | Domain results match SQLite semantics |
| ST-13 | OAuth refresh | Fail first connect with expired token, succeed with fresh token | Provider is called again; only connection setup is retried |
| ST-14 | Secret hygiene | Force connection failure containing a secret | Raised error does not expose the token |
| ST-15 | Migration ordering | Apply versioned directory twice | One migration row exists; second pass is a no-op at the data level |
| ST-16 | Schema contract | Run `check_state_schema.py` and inspect required constraints | 11 tables, foreign keys, checks, indexes, and retry key are present |
| ST-17 | Bundle contract | Validate dev/staging/prod and inspect resource summary | All targets resolve and all Lakebase resource types are present |
| ST-18 | Live infrastructure | Deploy with `--profile sda`, list project/branch/endpoint/role/database, protected root, and snapshot schedule | Resources are READY/ACTIVE, the root branch is protected, and the schedule is configured |
| ST-19 | Live migration | Generate a short-lived credential and apply `migrations/versions` | Migration recorded; all 11 operational tables exist |
| ST-20 | Live repository CRUD | Connect with OAuth and execute request/run/plan/step/attempt/artifact/audit flow | Read-back matches writes; test rows are removed afterward |
| ST-21 | Live scale behavior | Leave the endpoint idle past its five-minute timeout and reconnect with a fresh OAuth credential | The endpoint reports `IDLE` before the new connection, returns `ACTIVE` after it, and the database remains readable |
| ST-22 | Cross-series Spark smoke | Run bundle jobs for metadata, profile, relationship, patterns, and scope | Jobs terminate successfully and produce governed evidence |
| ST-23 | Static and package checks | Pytest, Ruff, mypy, release hygiene, wheel build | All checks are green and generated artifacts are removed before commit |
| ST-24 | Live point-in-time recovery | Insert a unique request marker, record a server timestamp, remove it from `dev`, restore a disposable branch from that timestamp, and read it back | The marker exists only in the restored branch; the original branch remains unchanged |

## 4. Commands

From the repository root:

```powershell
python -m pytest
ruff check src tests scripts
mypy src tests
python scripts/check_state_schema.py
python scripts/check_bundle_contract.py
python scripts/check_release.py
databricks bundle validate -t dev --profile sda
databricks bundle deploy -t dev --profile sda
```

Lakebase verification uses an ephemeral credential generated by the CLI. The
token is passed in memory to `psycopg`, never committed or printed:

```powershell
databricks postgres list-projects --profile sda -o json
databricks postgres list-branches projects/<project> --profile sda -o json
databricks postgres list-endpoints projects/<project>/branches/<branch> --profile sda -o json
databricks postgres generate-database-credential <endpoint> --profile sda --ttl 600s -o json
python scripts/apply_state_schema.py "$env:SDA_POSTGRES_DSN" --migrations-dir migrations/versions
```

The recovery acceptance uses the same short-lived DSN. It first runs
`python scripts/prepare_sda08_restore_point.py`, then creates a temporary branch
with `source_branch` and the printed `source_branch_time`, reads the marker back,
and soft-deletes that temporary branch.

After cleanup, `python scripts/check_sda08_live_state.py` verifies the 11-table
schema, the applied migration rows, and zero disposable request rows without
printing the database credential.

The Spark evidence jobs are run through the bundle, for example:

```powershell
databricks bundle run -t dev spark_contract_smoke --profile sda
databricks bundle run -t dev uc_metadata_reader --profile sda --params run_id=sda08-live
databricks bundle run -t dev table_profiler --profile sda --params run_id=sda08-profile
databricks bundle run -t dev relationship_detector --profile sda --params run_id=sda08-rel
databricks bundle run -t dev pattern_detector --profile sda --params run_id=sda08-pattern,source_table=samples.wanderbricks.bookings,mode=quick
databricks bundle run -t dev analyze_scope --profile sda --params catalog=samples,schema=wanderbricks,tables=users,run_id=sda08-scope,profile=false
```

## 5. Evidence recorded on 2026-11-11

- Local: `455 passed, 5 skipped` after adding regression coverage for scoped
  fixture cleanup, empty artifact evidence, and CLI-safe single-profile input.
- Ruff: passed.
- mypy: passed for 198 source files.
- Bundle contract: passed for 3 targets.
- Bundle deployment with profile `sda`: created/updated the Lakebase project,
  protected `production` branch, dev branch, primary endpoint, OAuth user role,
  `sda_state` database, and a daily seven-day snapshot schedule on the root
  `production` branch.
- Migration: the versioned migration was reapplied idempotently and 11 `sda_*`
  tables were verified directly through PostgreSQL; the reused `dev` branch
  retains two content-addressed migration rows, one from the earlier
  compatibility-schema run and one from the versioned migration; no live test
  rows remained.
- Live repository: request, run, plan, two step attempts, durable audit event,
  artifact reuse, lease renewal, and attempt completion passed.
- Live Spark: `spark_contract_smoke`, `uc_metadata_reader`,
  `table_profiler`, `relationship_detector`, `pattern_detector`, and one-table `analyze_scope`
  terminated successfully.
- Controlled Unity Catalog fixture rerun with profile `sda`: metadata selected two
  managed tables; the profiler was forced through a fresh calculation
  (`reuse_existing=false`); relationship detection reported one-to-many with
  `orphan_rate=0`; `analyze_scope` produced the relationship and dependency-graph
  artifacts; and `pattern_detector` consumed all four upstream artifact IDs and
  produced 13 patterns across four families. Cleanup removed only the two
  run-owned tables and left the shared integration schema intact.
- Live recovery: after the configured five-minute idle window, the dev endpoint
  reported `IDLE`; a new OAuth connection reactivated it to `ACTIVE` and read
  the migration table successfully. A point-in-time restore from `dev` created
  a disposable root branch where the removed marker was present (`1` row) and
  both migration rows were still present (`2` rows); the disposable branch was
  soft-deleted and the original `dev` marker count was `0`.

The five skipped local tests are environment-dependent Spark tests. They are not
counted as passed local tests; the Databricks job evidence above is the stronger
runtime proof for the Spark contract. The current Lakebase API returned `IDLE`
rather than the literal `SUSPENDED` state during the idle observation, so the
evidence records the observable transition `IDLE -> ACTIVE` and does not rename
it. The point-in-time recovery check is reproducible with
`scripts/prepare_sda08_restore_point.py` and a disposable branch.

## 6. Definition of done

SDA-08 is complete for the local contract and the Databricks `dev` acceptance
boundary when ST-01 through ST-24 are green, the infrastructure is deployed
from the bundle, and all disposable data and branches are removed. This is not
the same as production sign-off: staging/prod service-principal permissions,
governed UC grants, and target-owner promotion evidence remain environment
gates outside the `sda` user-profile acceptance run.
