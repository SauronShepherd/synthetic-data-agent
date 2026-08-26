# Bundle promotion and rollback

The bundle has three explicit environments: `dev`, `staging`, and `prod`.
Promotion is one-way through the environments and must use the same committed
revision and bundle name at each step.

## Promotion gates

1. Run the local quality gates and `databricks bundle validate -t dev`.
2. Deploy and smoke-test `dev`; verify that metadata and pattern artifacts are
   written only under the `sda_dev` namespace.
3. Deploy `staging` with controlled source tables and run the authorized smoke
   workflow. Confirm service-principal execution, deny-by-default variables,
   artifact fingerprints, and validation/privacy outcomes.
4. Require platform-owner approval before deploying the identical revision to
   `prod`. Production source and output placeholders must be replaced only with
   governed tables permitted to the runtime service principal.

Never promote by copying generated data or editing target files. The revision,
configuration hash, source snapshot, and artifact manifests are the promotion
record.

## Rollback

Pause new runs, preserve the failing run manifest and audit records, and deploy
the last known-good committed revision with the same target. Do not delete
historical artifacts or overwrite a published dataset version. Revoke the
affected publication, if necessary, then publish a new version only after
validation, privacy approval, and human approval succeed. Resume execution only
after the failed-run cause, permissions, and output isolation have been verified.

Production readiness still requires workspace-backed permission, Delta, and
rollback drills; local bundle validation does not prove those controls.
