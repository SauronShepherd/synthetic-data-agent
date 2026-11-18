# SDA 10 Validation Audit

## Scope, method, and verdict

This audit performs the two required independent checks for the SDA-10 article, the
repository implementation, and the Databricks deployment executed with profile `sda`
and target `dev` on branch `codex/SDA-10`.

The review is deliberately bounded to the deployed v1 contract. Present-tense claims
in the revised article describe only behavior that is implemented and evidenced below.
The larger architecture—profile resolution, distributed large-scale generation,
conditional null dependencies, durable Lakebase registration, quality assessment, and
cross-system repair—is retained only as explicitly labelled future work.

**Verdict: the revised article is consistent and ready as an accurate description of
the tested bounded v1. The branch is implemented and the bounded flow is tested in
Databricks. The future extensions listed in the article are not claimed as delivered.**

## Evidence set

- Branch: `codex/SDA-10`.
- Databricks profile and target: `sda`, `dev`.
- Bundle validation and deployment completed. Validation reports the existing
  workspace-permissions warning because the bundle does not configure that folder’s
  permissions; the warning is recorded and is not treated as a clean permission gate.
- Local `pytest -q` passes, including the regression test that protects the plan
  fingerprint reconstruction.
- `ruff check src tests scripts` and `git diff --check` pass.
- The deployed generator produced a 25-row Unity Catalog Delta table from an approved,
  fingerprinted plan with a 100-row safety limit.
- The separate validator succeeded against that table, checking the expected count and
  required columns and creating its validation report.
- A second live run with the same output and parameters succeeded under the explicit
  replacement policy. This is a rerun, not evidence of artifact reuse or
  cross-system idempotent repair.
- A live run with `approved=false` failed before writing, with the expected
  `generation requires approved=true` error.
- The original row-count/fingerprint mismatch was reproduced, fixed by including
  `requested_row_count` in the reconstructed `GenerationPlan`, and protected by a
  local regression test.
- Word visual QA of the final DOCX reports 18 pages. The first page shows the bounded
  v1 title and scope; the final page shows the future-work boundary without visible
  clipping or overlap, and Word reports accessibility as correct. The headless DOCX
  renderer could not be used because LibreOffice `soffice.exe` is unavailable in the
  runtime.

## 1. Article → implementation → evidence

| afirmación o capacidad | implementación | prueba ejecutada | resultado | sección del artículo | estado | discrepancia o límite |
|---|---|---|---|---|---|---|
| Plan aprobado, acotado y con fingerprint reproducible | `GenerationPlan`, `build_generation_plan`, `generate_spark.py` | Suite local; ejecución Databricks con `approved=true`; regresión del fingerprint | PASS | 2, 9, 14 | PASS | La ruta live reconstruye el plan suministrado; no carga aún una aprobación persistida desde Lakebase. |
| Despliegue del bundle y jobs conectados | `databricks.yml`, `bundle/resources.yml`, jobs `standalone_generator` y `validate_generated` | `databricks bundle validate -t dev --profile sda`; deploy; ejecuciones live | PASS WITH WARNING | 10, 11 | PASS WITH WARNING | Persiste el warning de permisos de carpeta del workspace. No se presenta como prueba de una política de permisos exhaustiva. |
| Rechazo de planes no aprobados | Guardia `approved` en `generate_spark.py` | Ejecución live con `approved=false`; tests locales | PASS | 2, 9, 11 | PASS | El caso negativo comprueba el guard de entrada; no sustituye una prueba de autorización UC completa. |
| Límite de filas solicitado y límite máximo | Validación de `row_count` y `max_rows` antes de generar | Tests locales; ejecución live de 25 filas con máximo 100 | PASS | 2, 9, 12 | PASS | La evidencia live es un caso pequeño; no demuestra rendimiento ni escalado grande. |
| IDs reproducibles y valores derivados de coordenadas | `_stable_id`, `_coordinate_seed`, estrategias de generación | Tests de determinismo/unicidad; salida live con `customer_id` | PASS | 3, 6 | PASS | El validador live no comprueba unicidad; la garantía está cubierta por la estrategia y los tests locales. |
| Generación numérica acotada | Estrategia `uniform` en `src/sda/generation.py` | Tests locales; columna live `lifetime_value` | PASS | 3, 4, 12 | PASS | No es un ajuste empírico de un perfil ni una validación de distancia distributiva. |
| Fechas y timestamps simples | Modelo determinista de fecha/timestamp de la generación v1 | Tests locales; columna live `signup_at` como valor string | PASS | 7, 12 | PASS | No incluye calendario observado, zona horaria, DST ni tipos LTZ/NTZ de Databricks. |
| Firmas de formato e identificadores prefijados | `_render_signature`, estrategia de identificador | Tests locales; columna live `tax_id` prefijada | PASS | 6, 12 | PASS | El ejemplo live no ejerce teléfono ni firmas complejas. |
| Muestras empíricas y vocabularios ponderados del API local | Parámetros `empirical_samples`, `vocabularies` y `weighted_vocabularies` | Tests locales | LOCAL ONLY | 4, 5 | PARTIAL | No se ejecutó un caso live que suministre esas colecciones por `columns-json`; no se presenta como capacidad live probada. |
| Distribuciones de perfiles, colas y frecuencias aprobadas | No hay resolver live de perfiles conectado al entrypoint | Inspección de código y tests locales del resto del repositorio | NOT DELIVERED IN V1 | 4, 5 | FUTURE | El artículo revisado lo marca como extensión; no afirma ajuste estadístico completo. |
| Texto no soportado falla claramente | `GenerationError` para modelos no soportados | Tests locales | LOCAL ONLY | 6.3 | PARTIAL | El contrato de exclusión/placeholder/especializado a nivel de plan aún no está cableado al job live. |
| Máscara global de nulos | Campo `null_rate` en la generación local | Tests locales | LOCAL ONLY | 8 | PARTIAL | El ejemplo live no incluye una columna nullable; los nulos condicionales quedan como trabajo futuro. |
| Dependencias de nulos y precedencia condicional | No hay resolver de modelos condicionales en el entrypoint | Inspección de código; tests locales de reglas relacionadas | NOT DELIVERED IN V1 | 8 | FUTURE | La revisión sustituyó cualquier afirmación presente por una descripción explícita de extensión futura. |
| Tabla Delta de salida, escrita en el destino configurado | `createDataFrame` + `saveAsTable` con política `CREATE OR REPLACE TABLE` | Ejecución live; lectura posterior por el validator | PASS | 2, 11, 12, 14 | PASS | La v1 reemplaza explícitamente el destino run-scoped; fail-if-exists y reparación idempotente no están implementados. |
| Receipt y manifest de generación | Construcción y `print` de receipt/manifest desde el entrypoint | Logs/output de la ejecución live; tests de manifest locales | PASS | 12, 14 | PASS | Se imprimen; no se persisten actualmente en Delta ni se registran en Lakebase desde este job. |
| Validación separada de generación | Job `validate_generated_spark.py` | Ejecución live sobre la tabla generada | PASS | 11, 13 | PASS | La validación live es técnica: count, columnas, acceso y reporte. No es la evaluación completa de calidad, privacidad o publicación. |
| Estado “awaiting validation” y aprobación posterior | Jobs separados, sin transición de estado en el generator | Inspección de código y flujo live | NOT DELIVERED IN V1 | 2, 13, 14 | FUTURE | El artículo solo afirma que la generación no equivale a calidad/publicación; no afirma una transición live de estado. |
| Rerun del mismo plan y destino | Política explícita de reemplazo en el generator | Segundo job live con los mismos parámetros | PASS | 14 | PASS | Es un nuevo intento que reemplaza el destino; no prueba reutilización de artifact ni reparación tras fallo de Lakebase. |
| Recuperación tras fallo parcial y reparación cross-system | No hay reconciliación generator↔Lakebase | Inspección de código; no existe prueba live positiva de reparación | NOT DELIVERED IN V1 | 11, 14 | FUTURE | Se documenta como extensión futura, no como comportamiento actual. |
| Limpieza segura de fixtures/outputs | `cleanup_fixture` existe para fixtures de integración y tiene tests de alcance | Tests locales de cleanup | LOCAL ONLY | 10, 14 | PARTIAL | No se ejecutó limpieza destructiva live del output generado; el audit preserva la evidencia y no declara una limpieza Databricks no observada. |
| Permisos y protección de fuentes | Bundle declara recursos; el generator apunta al output configurado y no modifica la fuente | Bundle validate/deploy; inspección; ejecución live | PARTIAL | 9, 10, 14 | PARTIAL | La advertencia de permisos requiere configuración explícita si se exige una aceptación de permisos sin warning. |

## 2. Implementación → artículo

This reverse check starts from behavior actually implemented and tested rather than
from the article’s original requirements.

| implementación o capacidad probada | dónde se explica | coincidencia | límite editorial documentado |
|---|---|---|---|
| Reconstructed plan includes requested row count in its fingerprint | Sections 2 and 14; validation-status paragraph | Exacta | The article describes the observable approved-plan contract, not an accidental job/run identifier. |
| Bounded Python row generation followed by Spark DataFrame creation | Sections 3 and 9, with the explicit deployed-boundary note | Exacta | It is described as bounded QA-scale generation, not distributed large-scale generation. |
| Deterministic identifiers, bounded numeric values, simple timestamps, prefixed strings | Sections 3, 6, 7, and live example in section 12 | Exacta | The live example names only the strategies actually exercised; other strategies remain local or future. |
| Local empirical/weighted inputs and null-rate behavior | Sections 4, 5, and 8 | Qualified | The article separates local API support from the Databricks example and does not imply live profile-backed execution. |
| Fully qualified Delta output with explicit replacement | Sections 2.2, 11, 12, and 14 | Exacta | Append, fail-if-exists, artifact reuse, and cross-system repair are not claimed. |
| Printed receipt/manifest fields | Section 12 and manifest discussion in section 14 | Exacta | Printed evidence is distinguished from durable Delta/Lakebase persistence. |
| Separate structural validator | Sections 11, 13, and final validation-status section | Exacta | The article explicitly excludes full statistical, privacy, relational, and publication validation. |
| Negative approved=false path | Sections 2, 9, 11, and validation status | Exacta | It is a generator guard, not a claim of complete UC authorization testing. |
| Successful rerun under replacement policy | Section 14 | Exacta | It is not described as idempotent reuse or partial-failure repair. |
| Future architecture retained from the SDA series | Boundary note after section 2 and final validation-status section | Exacta | Future requirements use “should/must” language and are explicitly excluded from present-tense v1 claims. |

## Reproducible commands and invocation boundary

The validated deployment commands are:

```text
databricks bundle validate -t dev --profile sda
databricks bundle deploy -t dev --profile sda
```

The live jobs were invoked with explicit `jobs run-now` parameters so that the approved
plan identity, row count, maximum row count, output table, and run identifier were not
lost in shell/bundle parameter propagation. An earlier `bundle run` attempt propagated
empty plan parameters and failed with `plan_id must not be empty`; that is recorded as a
harness invocation limitation, not as a generator behavior.

## Iteration checkpoints

The matrix was rerun after the implementation fix, after the first article rewrite,
after the v3 boundary clarification, and after the v4 editorial pass. The final pass
rechecked: claim scope, code linkage, local evidence, live Databricks evidence, success
and negative paths, rerun semantics, cleanup boundaries, article placement, and visual
rendering. No remaining present-tense article claim requires an unexecuted v1 feature.

## Final closure

The article deliberately does not claim that the broader original design is complete.
It now describes exactly the bounded system that exists and has been tested, while
retaining future work with explicit limits. The matrix is therefore bidirectionally
consistent for the delivered v1 scope:

1. Every present-tense article capability maps to implementation and evidence.
2. Every implemented/tested capability is described in the appropriate article
   section with its observed boundary.
3. Unimplemented or untested capabilities are not presented as delivered.
