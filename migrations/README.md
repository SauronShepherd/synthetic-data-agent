# SDA-08 state migrations

`versions/` contains ordered PostgreSQL/Lakebase migrations. Each file is applied
inside the migration runner and recorded by content hash in
`sda_schema_migrations`; rerunning a deployment is therefore idempotent while a
changed migration produces a new identifier instead of silently changing history.

Apply the versioned set from a workstation or a controlled Databricks migration
job after obtaining a short-lived Lakebase credential:

```powershell
python scripts/apply_state_schema.py "$env:SDA_POSTGRES_DSN" `
  --migrations-dir migrations/versions
```

The compatibility command without `--migrations-dir` still applies
`sql/lakebase_state_schema.sql` for existing operators. New deployments should
use the versioned directory.
