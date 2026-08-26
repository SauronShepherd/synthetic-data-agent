# SDA 07 pattern artifact retention policy

This policy applies to the governed `pattern_registry` and `pattern_evidence`
Delta tables. Pattern rows are analytical evidence, not source data, and must
remain traceable to the run, source snapshot, input artifacts, and detector
configuration that produced them.

## Required lineage and freshness

Every registry and evidence row must carry the run/analysis identifier, source
references, input artifact IDs, configuration hash, detector/scoring versions,
schema version, and creation timestamp. A pattern is reusable only while all
referenced upstream artifacts are `COMPLETE` and within the configured evidence
freshness window. Missing, failed, or incompatible inputs are rejected; they are
never treated as an empty result.

## Retention and supersession

- Retain the current complete version and its lineage for the lifetime of any
  generation plan or published dataset that references it.
- Retain superseded versions for 90 days after supersession for audit and replay;
  environments may increase this period but must not shorten it without approval.
- Mark a prior analysis `SUPERSEDED` only after the replacement registry and
  evidence are complete and fingerprint-verified. Do not delete or mutate the
  historical payload during supersession.
- After the retention period, delete registry and evidence rows by analysis/run
  scope using an auditable maintenance job; preserve compact audit metadata and
  the artifact fingerprint.
- Raw source values are prohibited in either table. Evidence is limited to
  aggregate metrics, safe categorical labels, redactions, and references.

The policy version is `sda07-retention-v1` and changes require a migration note,
an updated compatibility contract, and a replay/retention test.
