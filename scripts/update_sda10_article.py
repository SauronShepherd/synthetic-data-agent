from pathlib import Path

from docx import Document

SOURCE = Path(r"C:\ANGEL\Personal\Articles\SDA\🤖 SDA 10 Generating Synthetic Tables with Realistic Distributions and Null Behavior.docx")
OUTPUT = SOURCE.with_name(SOURCE.stem + " - Revised v6.docx")

REPLACEMENTS = {
    "🤖 SDA 10: Generating Synthetic Tables with Realistic Distributions and Null Behavior":
        "🤖 SDA 10: Generating Bounded Synthetic Tables with Reproducible Strategies",
    "2. Define the synthetic_data_generator Contract":
        "2. Define the v1 Contract and Future Extensions",
    "10. Build the Tool into the Existing Project":
        "10. Build the Tested v1 Tool and Identify Future Extensions",
    "11. Execution Flow":
        "11. Tested Execution Flow",
    "14. Reproducibility, Retries, and State":
        "14. Reproducibility, Reruns, and Current State",
    "14.2 Make Retries Idempotent":
        "14.2 Define the Current Rerun Policy",
    "In this article, we build synthetic_data_generator v1: a deterministic Spark tool that creates a standalone synthetic table at the requested scale while preserving numerical distributions, categorical frequencies, long tails, timestamp behaviour, string formats, and approved null patterns.":
        "In this article, we build synthetic_data_generator v1: a deterministic, bounded Databricks job that creates a standalone synthetic table from an approved plan. The deployed Databricks path exercises reproducible identifiers, bounded numeric ranges, simple dates and timestamps, format signatures, and deterministic prefixed strings. Local package tests additionally cover empirical samples, weighted categories, and global null masks; those hooks are not claimed as live Databricks behavior here.",
    "This is the first useful dataset—not the finished system. Matching individual distributions can still produce impossible rows, and multi-table referential integrity comes next.":
        "This is a deliberately bounded first dataset—not a finished statistical or relational system. The live evidence covers a small standalone table and technical validation; profile-backed distribution fitting, multi-table integrity, privacy assessment, and quality scoring remain future milestones.",
    "It first learned what exists through uc_metadata_reader, how individual columns behave through table_profiler, how tables connect through relationship_detector, and how values depend on segments, states, and business rules through pattern_detector. Lakebase now preserves requests, runs, approvals, and plan versions, while Unity Catalog and Delta retain the canonical analytical evidence.":
        "The broader Synthetic Data Agent architecture uses uc_metadata_reader, table_profiler, relationship_detector, and pattern_detector to prepare evidence. The tested SDA-10 v1 does not invoke those upstream readers during generation: it receives the approved plan and writes only the bounded synthetic output. Lakebase and durable run-state integration remain future work for this milestone.",
    "This article builds synthetic_data_generator v1 and produces one statistically credible standalone—or parent—table.":
        "This article builds synthetic_data_generator v1 and produces one bounded standalone table through the deployed Databricks job.",
    "It preserves approved numerical distributions, categorical frequencies, long tails, timestamp behaviour, string formats, null rates, and selected conditional-null patterns at the requested scale.":
        "The deployed v1 preserves deterministic identifiers, bounded numeric ranges, format signatures, simple timestamps, and deterministic prefixed strings in the tested Databricks example. Empirical samples, weighted categories, and global null-rate masks have local package coverage but were not exercised by the live example. Profile resolution, long-tail modelling, timezone-aware timestamps, and conditional-null resolution are not part of the deployed v1 contract.",
    "The first milestone is therefore narrower but useful: generate new values that look statistically credible on their own—and leave relationships, topology, noise, and full validation to the tools designed to handle them.":
        "The first milestone is therefore narrower but useful: generate new bounded values from a reviewed plan and validate the table’s basic technical shape. Statistical fidelity, relationships, topology, noise, and full quality validation remain later work.",
    "Version one can guarantee uniqueness within the generated table. Article 11 will use those generated keys to make parent/child reuse explicit rather than relying on accidental value overlap.":
        "The local identifier strategy is designed to guarantee uniqueness within a generated table; the live validator in this milestone checks only row count and required columns. Article 11 will need a separate live referential-integrity test before claiming parent/child key reuse.",
    "Version one can guarantee uniqueness within the generated table. Article 11 will use those generated keys when assigning valid child foreign keys.":
        "The local identifier strategy is designed to guarantee uniqueness within a generated table; the live validator in this milestone checks only row count and required columns. Article 11 will need a separate live referential-integrity test before claiming parent/child key reuse.",
    "This article builds synthetic_data_generator v1 and produces one bounded standalone table through the deployed Databricks job. It preserves approved numerical distributions, categorical frequencies, long tails, timestamp behaviour, string formats, null rates, and selected conditional-null patterns at the requested scale.":
        "The tested v1 produces one bounded standalone table through the deployed Databricks job. The live example exercises deterministic identifiers, bounded numeric ranges, format signatures, simple timestamps, and deterministic prefixed strings. Empirical samples, weighted categories, and global null-rate masks have local package coverage but were not exercised live. Profile resolution, long-tail modelling, timezone-aware timestamps, and conditional-null resolution are not part of the deployed v1 contract.",
    "spark.range(target_rows) creates a DataFrame with a LongType column named id, containing values from 0 to target_rows - 1.":
        "The current deployed adapter resolves the approved count and generates deterministic row coordinates in the Python package before creating the Spark DataFrame. It does not yet use spark.range as the generation skeleton.",
    "Keep Generation Distributed\nCreate the row skeleton in Spark, add columns through dependency-aware DataFrame expressions or joins, and write the result directly to the run-scoped staging table.":
        "Keep the deployed boundary explicit\nThe current v1 creates a bounded Python row collection and then writes it to a Delta table. This is suitable for the tested QA scale and is not a claim of distributed generation for large outputs.",
    "The generator should follow one auditable, dependency-aware sequence:":
        "The deployed v1 follows this auditable sequence:",
    "The Delta artifact and Lakebase record are separate commits, not one cross-system transaction. Registration should therefore be idempotent:":
        "The current job writes the Delta artifact and prints a generation receipt. Lakebase registration and cross-system reconciliation are reserved for a later integration step; the deployed v1 does not claim idempotent repair across those stores.",
    "A retry with the same fingerprint should either reuse the completed artifact or reconcile and complete the same run-scoped output according to policy.":
        "A retry with the same parameters is currently a new job attempt and must be treated as a rerun, not as proof of artifact reuse. The output policy is explicit replacement of the run-scoped table; cross-system repair is not implemented in v1. Append and artifact-reuse policies are not part of the tested contract.",
    "A retry with the same parameters is currently a new job attempt and must be treated as a rerun, not as proof of artifact reuse. The output policy is explicit replacement of the run-scoped table; cross-system repair is not implemented in v1. Append and artifact-reuse policies are not part of the tested contract. It must not append another full dataset or create unrelated tables merely because an acknowledgement failed.":
        "A retry with the same parameters is currently a new job attempt and must be treated as a rerun, not as proof of artifact reuse. The output policy is explicit replacement of the run-scoped table; cross-system repair is not implemented in v1. Append and artifact-reuse policies are not part of the tested contract.",
    "The values are new, the row count is controlled, numerical columns preserve their shape and tails, categories retain their weights, timestamps follow observed patterns, strings keep their structure, and nulls appear according to the approved plan.":
        "The values are new, the row count is controlled, deterministic identifiers and simple formats are reproducible, and bounded numeric and timestamp strategies execute in the tested Databricks example. The local package can apply global null masks, but the live example did not exercise a nullable column. Full distributional, temporal, conditional-null, privacy, and multi-table claims remain outside this v1 milestone.",
    "The generated rows and detailed manifest remain in Unity Catalog and Delta. Lakebase stores the mutable run state, approval history, status, and artifact references needed to locate, explain, retry, or invalidate the result. It should not become a second analytical store.":
        "The tested job writes generated rows to the configured Unity Catalog Delta table and prints a compact receipt and manifest. It does not persist the manifest in Delta or register run state in Lakebase; those are future integrations.",
    "It may write only to configured temporary or synthetic-output schemas, preferably using run-specific targets. It must never overwrite or append to a source table, and a completed generation step must remain awaiting validation, not approved for use.":
        "The tested job writes to the configured synthetic-output table and does not read or modify the source table. Its current replacement policy is explicit `CREATE OR REPLACE TABLE`; approval-state registration is outside the v1 entrypoint and the separate technical validator must be run explicitly.",
    "The deployed v1 follows this auditable sequence:":
        "The tested v1 follows this auditable sequence:",
    "Load run and immutable approved plan":
        "Receive job parameters and reconstruct the approved plan",
    "Resolve referenced profiles, patterns, rules, and source versions":
        "Validate the supplied fingerprint, approved flag, row count, and row limit",
    "Validate plan status, target permissions, schema, and row limits":
        "Generate bounded deterministic Python rows from the declared strategies",
    "Create the deterministic synthetic-row skeleton":
        "Create a Spark DataFrame and the configured target schema",
    "Build and validate the column dependency order":
        "Create or replace the run-scoped Delta table",
    "Generate identifiers and driver columns":
        "Build and print the generation receipt and manifest",
    "Resolve null models and generate dependent values": "",
    "Generate remaining numerical, categorical, string, and temporal columns": "",
    "Cast to the declared schema and run generation-time checks": "",
    "Write the run-scoped staging table": "",
    "Persist the generation manifest in Delta": "",
    "Register artifact references and the run outcome in Lakebase": "",
    "Null handling belongs inside the dependency flow rather than as a final blanket transformation: status, segment, or country may determine whether another field is populated at all. Before writing, the tool should verify row count, schema compatibility, required-column nullability, identifier uniqueness, supported values, and configured bounds. These are execution safeguards, not the full quality assessment reserved for Article 14.":
        "The local package contains a global null-rate mask, but the live example did not exercise a nullable column. Conditional dependency ordering and the broader execution safeguards described here are future extensions; the separate validator currently checks expected count and required columns.",
    "Unity Catalog managed tables are Databricks’ default and recommended table type, and DataFrameWriter.saveAsTable accepts a fully qualified catalog.schema.table name. Its behavior when the destination already exists depends on the selected save mode, so the generator should use a validated, run-specific target and an explicit policy—normally fail-if-exists or an idempotent retry path—rather than unrestricted overwrite. (Documentación de Databricks)":
        "The live v1 writes a fully qualified Unity Catalog Delta table using an explicit replacement policy. Fail-if-exists and cross-system idempotent repair are future policy options, not behaviors claimed by this run.",
    "a retry must be able to discover an already committed staging table and manifest, then safely repair the missing Lakebase reference without regenerating the table. This preserves the project’s separation between analytical evidence in Delta and operational coordination in Lakebase.":
        "Cross-system repair is not implemented in v1.",
    "The current job writes the Delta artifact and prints a generation receipt. Lakebase registration and cross-system reconciliation are reserved for a later integration step; the deployed v1 does not claim idempotent repair across those stores. a retry must be able to discover an already committed staging table and manifest, then safely repair the missing Lakebase reference without regenerating the data. This preserves the project’s separation between analytical evidence in Delta and operational coordination in Lakebase.":
        "The current job writes the Delta artifact and prints a generation receipt. Lakebase registration and cross-system reconciliation are future integrations; cross-system repair is not implemented in v1.",
    "The current job writes the Delta artifact and prints a generation receipt. Lakebase registration and cross-system reconciliation are reserved for a later integration step; the deployed v1 does not claim idempotent repair across those stores. a retry must be able to discover an already committed staging table and manifest, then safely repair the missing Lakebase reference without regenerating the table. This preserves the project’s separation between analytical evidence in Delta and operational coordination in Lakebase.":
        "The current job writes the Delta artifact and prints a generation receipt. Lakebase registration and cross-system reconciliation are reserved for a later integration step. A rerun with the same parameters is an explicit replacement-policy rerun, not proof of cross-system idempotent repair.",
    "Use customers for the first end-to-end example. It can be generated independently, so the article can demonstrate realistic single-table generation without introducing foreign-key assignment before Article 11.":
        "Use customers as the live standalone example. The validated run generated 25 rows independently and did not exercise foreign-key assignment.",
    "Example strategies:":
        "The live example used these tested strategies:",
    "customer_id: unique formatted identifier;": "customer_id: deterministic identifier;",
    "customer_type and customer_tier: weighted categories;": "lifetime_value: bounded numeric range;",
    "lifetime_value: empirical numerical distribution with a separate upper tail;": "signup_at: deterministic timestamp string;",
    "country_code: approved low-cardinality category;": "tax_id: deterministic prefixed string;",
    "signup_at: seasonal timestamp model;": "Profile-backed seasonality was not exercised in v1.",
    "phone_number: signature-based string generator;": "Phone-format generation was not exercised in the live run.",
    "tax_id: conditional null model based on country and customer type.": "Conditional null generation was not exercised in the live run.",
    "A human-readable summary might be:": "The live plan summary was:",
    "Generate 500,000 customers using profile version 18. Preserve approved customer-tier, customer-type, and country weights. Model lifetime_value from quantile intervals with a separate upper tail. Reproduce signup_at seasonality using the Europe/Madrid profiling timezone. Create new customer identifiers and phone-shaped values, then apply the approved country- and customer-type-specific null model for tax_id. Write the output to the run-scoped staging table.":
        "Generate 25 customers with an approved fingerprint, seed 1729, one deterministic identifier, one bounded numeric value, one simple timestamp string, and one deterministic prefixed string. Write the output to the configured synthetic-output Delta table.",
    "requested and generated row counts;": "requested and generated row counts;",
    "selected lifetime_value percentiles and tail share;": "the output fingerprint and generator version;",
    "tier, type, and country proportions;": "the declared output columns;",
    "global and conditional tax_id null rates;": "source snapshot and input artifact references;",
    "timestamp range and monthly distribution;": "the plan fingerprint and seed;",
    "phone-format match rate;": "the run identifier and output table;",
    "customer_id uniqueness.": "technical validation was then run separately for expected count and required columns.",
    "Before marking generation successful, the tool should confirm:":
        "The deployed v1 and its separate validator confirm:",
    "exact row count;": "the expected row count;",
    "expected columns, order, and data types;": "the required column names supplied to the validator;",
    "required identifier uniqueness;": "successful table access and report creation;",
    "successful casts and valid required fields;": "",
    "no unknown or unsupported strategy was executed;": "",
    "the table was written to the intended staging schema;": "the generated table was written to the configured output schema;",
    "no source object was modified;": "",
    "the manifest and artifact references were persisted.": "",
    "Article 14 should perform the comparative validation: distribution-distance measures, null and category drift, correlation and business-rule checks, referential integrity, topology and noise validation, privacy-oriented tests, and the final quality and publication decision.":
        "The separate validator used here performs technical checks only. Distribution-distance measures, conditional drift, referential integrity, topology, noise, privacy, and publication decisions remain later validation work.",
    "The generation manifest should capture the exact plan, profile, pattern, and source versions; generator and strategy versions; global and per-column seeds; target row count; timezone; allocation and rounding rules; fallback decisions; configuration fingerprint; output table; and warnings.":
        "The tested receipt contains the plan identity, plan fingerprint, row count, output fingerprint, and generator version. The printed manifest also contains the run, output table, seed, source snapshot references, input artifact references, and output columns. Profile, timezone, fallback, and Lakebase state are not persisted by v1.",
    "Each execution attempt should remain visible in Lakebase. Failed attempts are preserved with their status, checkpoints, and errors; retries create a new attempt rather than rewriting history. If Delta committed successfully but Lakebase registration failed, the retry should discover the existing manifest and repair the missing reference instead of regenerating the table.":
        "Databricks records the job attempts; the generator does not write attempt history to Lakebase. A rerun is visible as a new job run and uses the explicit replacement policy.",
    "A completed output should be registered as generated and awaiting validation.":
        "A completed output is written as a generated Delta table; the separate validation job must be invoked explicitly.",
    "Only the later validation and approval workflow may promote it into the approved synthetic-dataset registry. Successful generation proves that an approved plan executed—not that the result is accurate, useful, private, or ready to publish.":
        "Successful generation proves only that the bounded plan executed and wrote a table. It does not prove statistical quality, privacy, usefulness, or publication readiness.",
    "Approved conditional nulls improve version one, but they capture only selected dependencies. Referential integrity across generated tables arrives in Article 11, while broader cross-column rules, temporal behaviour, and final quality assessment still require explicit modelling and validation.":
        "Conditional null dependencies are future work for this v1. Referential integrity across generated tables arrives in Article 11, while broader cross-column rules, temporal behaviour, and final quality assessment still require explicit modelling and validation.",
}


def main() -> None:
    document = Document(str(SOURCE))
    changed = 0
    for paragraph in document.paragraphs:
        text = paragraph.text
        replacement_text = text
        for old, new in REPLACEMENTS.items():
            if old in replacement_text:
                replacement_text = replacement_text.replace(old, new)
                changed += 1
        if replacement_text != text:
            for run in paragraph.runs:
                run.text = ""
            if paragraph.runs:
                paragraph.runs[0].text = replacement_text
    for paragraph in document.paragraphs:
        if paragraph.text == "2. Define the v1 Contract and Future Extensions":
            note = document.add_paragraph(
                "Reading boundary: present-tense statements in this article describe the tested v1. "
                "Requirements expressed as should or must are explicitly design targets for later milestones "
                "unless the validation section states that they were executed in Databricks."
            )
            paragraph._p.addnext(note._p)
            break
    document.add_heading("Validation status and scope", level=1)
    document.add_paragraph(
        "The implementation was deployed and executed with the Databricks profile sda. "
        "The standalone generator completed a 25-row Delta write and the separate technical "
        "validation job completed successfully against that output. Local regression tests also pass. "
        "The live evidence supports the bounded v1 behavior described above; it does not support claims "
        "of distributed large-scale generation, Lakebase registration, automatic recovery, or full quality validation."
    )
    document.add_paragraph(
        "The next milestone is to add profile-backed distribution samplers, distributed Spark expressions, "
        "conditional null dependencies, durable manifest and run-state registration, and idempotent recovery. "
        "Those capabilities must be tested in Databricks before they are described as part of this article."
    )
    document.save(str(OUTPUT))
    print(f"updated={changed}")


if __name__ == "__main__":
    main()
