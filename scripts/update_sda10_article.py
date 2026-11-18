from pathlib import Path

from docx import Document

SOURCE = Path(r"C:\ANGEL\Personal\Articles\SDA\🤖 SDA 10 Generating Synthetic Tables with Realistic Distributions and Null Behavior.docx")
OUTPUT = SOURCE.with_name(SOURCE.stem + " - Revised v2.docx")

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
    "In this article, we build synthetic_data_generator v1: a deterministic Spark tool that creates a standalone synthetic table at the requested scale while preserving numerical distributions, categorical frequencies, long tails, timestamp behaviour, string formats, and approved null patterns.":
        "In this article, we build synthetic_data_generator v1: a deterministic, bounded Databricks job that creates a standalone synthetic table from an approved plan. The tested implementation supports reproducible identifiers, bounded numeric ranges, simple dates and timestamps, format signatures, optional empirical samples, weighted categories supplied to the local API, and global null-rate masks.",
    "This is the first useful dataset—not the finished system. Matching individual distributions can still produce impossible rows, and multi-table referential integrity comes next.":
        "This is a deliberately bounded first dataset—not a finished statistical or relational system. The live evidence covers a small standalone table and technical validation; profile-backed distribution fitting, multi-table integrity, privacy assessment, and quality scoring remain future milestones.",
    "This article builds synthetic_data_generator v1 and produces one statistically credible standalone—or parent—table.":
        "This article builds synthetic_data_generator v1 and produces one bounded standalone table through the deployed Databricks job.",
    "It preserves approved numerical distributions, categorical frequencies, long tails, timestamp behaviour, string formats, null rates, and selected conditional-null patterns at the requested scale.":
        "The tested v1 preserves deterministic identifiers, bounded numeric ranges, empirical samples supplied by the plan, format signatures, simple timestamps, weighted categories supplied to the local API, and global null-rate masks. Profile resolution, long-tail modelling, timezone-aware timestamps, and conditional-null resolution are not part of the deployed v1 contract.",
    "This article builds synthetic_data_generator v1 and produces one bounded standalone table through the deployed Databricks job. It preserves approved numerical distributions, categorical frequencies, long tails, timestamp behaviour, string formats, null rates, and selected conditional-null patterns at the requested scale.":
        "The tested v1 produces one bounded standalone table through the deployed Databricks job. It preserves deterministic identifiers, bounded numeric ranges, empirical samples supplied by the plan, format signatures, simple timestamps, weighted categories supplied to the local API, and global null-rate masks. Profile resolution, long-tail modelling, timezone-aware timestamps, and conditional-null resolution are not part of the deployed v1 contract.",
    "spark.range(target_rows) creates a DataFrame with a LongType column named id, containing values from 0 to target_rows - 1.":
        "The current deployed adapter resolves the approved count and generates deterministic row coordinates in the Python package before creating the Spark DataFrame. It does not yet use spark.range as the generation skeleton.",
    "Keep Generation Distributed\nCreate the row skeleton in Spark, add columns through dependency-aware DataFrame expressions or joins, and write the result directly to the run-scoped staging table.":
        "Keep the deployed boundary explicit\nThe current v1 creates a bounded Python row collection and then writes it to a Delta table. This is suitable for the tested QA scale and is not a claim of distributed generation for large outputs.",
    "The generator should follow one auditable, dependency-aware sequence:":
        "The deployed v1 follows this auditable sequence:",
    "The Delta artifact and Lakebase record are separate commits, not one cross-system transaction. Registration should therefore be idempotent:":
        "The current job writes the Delta artifact and prints a generation receipt. Lakebase registration and cross-system reconciliation are reserved for a later integration step; the deployed v1 does not claim idempotent repair across those stores.",
    "A retry with the same fingerprint should either reuse the completed artifact or reconcile and complete the same run-scoped output according to policy.":
        "A retry with the same parameters is currently a new job attempt and must be treated as a rerun, not as proof of artifact reuse. The output policy is explicit replacement of the run-scoped table; cross-system repair is not implemented in v1.",
    "The values are new, the row count is controlled, numerical columns preserve their shape and tails, categories retain their weights, timestamps follow observed patterns, strings keep their structure, and nulls appear according to the approved plan.":
        "The values are new, the row count is controlled, deterministic identifiers and simple formats are reproducible, bounded numeric and timestamp strategies execute, and global null masks can be applied according to the approved plan. Full distributional, temporal, conditional-null, privacy, and multi-table claims remain outside this v1 milestone.",
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
