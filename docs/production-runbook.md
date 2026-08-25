# SDA Production Handoff Runbook

This runbook is the acceptance contract for moving the implemented local pipeline into
Databricks. A green local test run is not a substitute for these checks.

## Promotion gates

- [ ] `ruff check .`, `mypy src tests`, `pytest`, wheel build, and bundle validation pass.
- [ ] The Lakebase/PostgreSQL migration in `sql/lakebase_state_schema.sql` is applied
  by the platform migration owner and backed up.
- [ ] The runtime service principal has only the required UC `USE CATALOG`, `USE SCHEMA`,
  `SELECT`, `MODIFY`, `CREATE TABLE`, and audit/state permissions.
- [ ] Source and output allowlists are configured with concrete governed catalogs and
  schemas; no wildcard production scope is permitted.
- [ ] Pattern, profile, relationship, graph, plan, validation, and publication artifact
  tables exist with retention and ownership labels.
- [ ] `standalone_generator` is deployed with an approved plan fingerprint and a finite
  `max_rows` budget; its default `approved=false` must remain unchanged.

## Required configuration

Configure through the deployment secret/configuration mechanism, never source files:

- `SDA_ENVIRONMENT=staging|prod`
- `SDA_CATALOG_ALLOWLIST`
- `SDA_SCHEMA_ALLOWLIST`
- `SDA_ARTIFACT_REGISTRY_TABLE`
- `SDA_PATTERN_REGISTRY_TABLE`
- `SDA_PATTERN_EVIDENCE_TABLE`
- `SDA_PROFILE_CATALOG` and `SDA_PROFILE_SCHEMA`
- Lakebase connection reference and SSL mode
- Gateway model/tool allowlists and per-run request/cost limits
- Audit destination and retention period

Tokens, workspace hosts, service-principal secrets, and warehouse credentials must be
secret references or environment-injected values. They must never appear in bundle
defaults, logs, artifacts, or generated manifests.

## Release sequence

1. Validate the bundle against the target workspace.
2. Deploy the wheel and resources to staging.
3. Apply the Lakebase migration and verify the service account can read/write state.
4. Run metadata discovery on a one-table allowlisted scope.
5. Reuse the resulting evidence to run profiling, relationship detection, and SDA 07
   pattern detection.
6. Create and approve a plan; verify the plan fingerprint is recorded in state and all
   downstream artifacts.
7. Run `standalone_generator` with a small bounded row count.
8. Run validation and privacy review; verify publication is denied when either gate is
   not satisfied.
9. Publish one version to a staging schema and verify the version alias and revocation.
10. Execute the negative smoke tests below before production promotion.

## Required negative smoke tests

- [ ] Unauthorized source catalog is rejected before any source read.
- [ ] Unauthorized output schema is rejected before any write.
- [ ] `approved=false` generation is rejected.
- [ ] A stale or mismatched plan fingerprint is rejected.
- [ ] A failed technical validation cannot publish.
- [ ] A non-approved privacy report cannot publish.
- [ ] A duplicate idempotency key returns the existing run rather than creating a second run.
- [ ] A stale worker cannot complete an attempt after lease expiry/reassignment.
- [ ] Gateway-denied model/tool requests produce an audit event without invoking the tool.
- [ ] Secrets and raw sensitive values are absent from audit events and artifact payloads.

## Recovery and rollback

- For a failed attempt, preserve the attempt error and retry history; do not overwrite the
  original run or append a contradictory artifact.
- For a stale worker, abandon the lease, acquire a new attempt, and resume from the last
  durable checkpoint.
- For a bad publication, revoke the dataset version and remove its aliases before repair.
- For a failed migration, stop promotion, restore the database backup, and do not run
  generation against a partially migrated state.
- For a failed checkpoint or incompatible schema transition, create a new explicitly
  versioned stream run; never silently reuse incompatible state.

## Evidence required for sign-off

- Bundle validation output and deployed resource identifiers.
- Migration version, backup confirmation, and state read/write smoke output.
- Run, plan, artifact, validation, privacy, publication, and audit IDs for the staging run.
- Results of every positive and negative smoke test.
- Output row counts, schema fingerprint, plan fingerprint, validation disposition, privacy
  decision, publication version, and revocation test evidence.
- Cost, duration, retry, and audit-event summaries for the run.
