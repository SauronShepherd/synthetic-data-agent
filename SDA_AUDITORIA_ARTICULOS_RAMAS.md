# Auditoría SDA de artículos y ramas — línea base inicial

**Fecha:** 26 de octubre de 2026 (Europe/Madrid)  
**Estado:** **AUDITORÍA ABIERTA**  
**Workspace / rama:** `synthetic-data-agent`, `SDA-07`  
**SHA fijado al inicio:** `21226c6af7e57554bdcfa41532fcecb7a91b7b7a`  
**HEAD al terminar:** pendiente de volver a comprobar; las comprobaciones locales no editaron archivos versionados.

## 1. Alcance y fuentes

Se adaptó la especificación pegada de FGA para revisar las correspondencias SDA indicadas por el usuario, las ramas remotas presentes y los cuatro originales DOCX proporcionados (SDA-07 Final, 08, 09 y 10). Las fechas se interpretan como 27 y 29 de octubre y 3 y 5 de noviembre de 2026.

Artículos públicos consultados íntegramente por HTTP (código 200; el extractor de búsqueda no podía abrir directamente la ruta corta `/sda/`):

- SDA 01: https://angelic-articles.com/sda/01-why-build-a-synthetic-data-agent-on-databricks/
- SDA 02: el índice lo lista, pero la ruta canónica probada `https://angelic-articles.com/articles/sda--02-designing-the-synthetic-data-agent/` devuelve HTTP 404; URL definitiva pendiente.
- SDA 03: https://angelic-articles.com/sda/03-bootstrapping-with-declarative-automation-bundles/
- SDA 04: https://angelic-articles.com/sda/04-reading-unity-catalog-metadata-the-agents-first-tool/
- SDA 05: https://angelic-articles.com/sda/05-profiling-source-data-distributions-nulls-formats-and-outliers/
- SDA 06: https://angelic-articles.com/sda/06-detecting-primary-keys-foreign-keys-and-join-relationships/
- SDA 07: https://angelic-articles.com/sda/07-capturing-cross-column-patterns-and-business-rules/

Las páginas fueron descargadas y extraídas a `.tmp/published-01.txt` … `.tmp/published-07.txt`; DOCX a `.tmp/SDA-07.txt` … `.tmp/SDA-10.txt`. Estos son artefactos locales temporales, no fuentes normativas.

## 2. Estado de inventario

El índice público consultado encontró SDA-07 como el artículo SDA más reciente. La lista de pares explícita cubre 01 y 02 en la rama, luego 03–07; SDA-02 sí existe en el índice y en Git, aunque faltaba su URL en la solicitud. No apareció artículo 00 en el índice revisado. Git remoto (`git ls-remote --heads origin`) expone `SDA-01` … `SDA-07` y `main`; no existen ramas remotas 08–10 en esta observación. Los DOCX 08–10 son borradores, no publicaciones verificadas.

| SDA | Artículo público | Rama remota | SHA fijado | Estado de primera pasada |
|---|---|---|---|---|
| 01 | Sí | SDA-01 | `79373f1f78d251ee69b93bb6a90aacee010fa71d` | Revisado a nivel inicial; conteos 0/10 |
| 02 | El índice lo lista; URL probada da 404 | SDA-02 | `26532465cdc7a65ef3aa12f03cb0c360757ae8f2` | Artículo bloqueado; rama pendiente de contraste detallado |
| 03 | Sí | SDA-03 | `b1b668cbc3c7ad900082dbe46e1dd7586c574424` | Revisado a nivel inicial; conteos 0/10 |
| 04 | Sí | SDA-04 | `4dff97bc1fd33e6c9cb368a718cf9f4130265097` | Revisado a nivel inicial; conteos 0/10 |
| 05 | Sí | SDA-05 | `912b92931496a4c33064f084fd8b5560c0781c33` | Revisado a nivel inicial; conteos 0/10 |
| 06 | Sí | SDA-06 | `637c2576b80b49d81a91ad7a4d80e7aba86c26c8` | Revisado a nivel inicial; conteos 0/10 |
| 07 | Sí | SDA-07 | `21226c6af7e57554bdcfa41532fcecb7a91b7b7a` | Revisado a nivel inicial; conteos 0/10 |
| 08 | No se observó | No existe en remoto | — | Borrador DOCX solamente |
| 09 | No se observó | No existe en remoto | — | Borrador DOCX solamente |
| 10 | No se observó | No existe en remoto | — | Borrador DOCX solamente |

## 3. Hallazgos iniciales

### SDA-CROSS-001 — Suite local filtrada falla en seis pruebas Delta

- **Estado:** nuevo; **prioridad:** P1; **confianza:** confirmado para la ejecución; causa raíz exacta pendiente.
- **Alcance:** HEAD SDA-07 fijado arriba, persistencia de artefactos/manifest.
- **Comando:** `python -m pytest -q -m "not spark and not databricks and not integration" --tb=short` desde el workspace, 26-10-2026.
- **Resultado observado:** seis tests fallidos y el resto de la suite filtrada completó. Los fallos incluyen `test_artifact_lifecycle_persistence.py` (2), `test_delta_persistence_contract.py` (2), `test_artifact_registry.py` y `test_run_manifest_persistence.py`. En algunos dobles, el código intenta `createOrReplaceTempView` que no existe; en otros termina en `DeltaTable.forName` y el doble no posee `_sc`.
- **Archivos:** `src/sda/artifacts/delta.py`, `tests/test_artifact_lifecycle_persistence.py`, `tests/test_delta_persistence_contract.py`, `tests/test_artifact_registry.py`, `tests/test_run_manifest_persistence.py`.
- **Impacto:** contradice el gate local declarado como reproducible si ese gate incluye este marcador y el entorno actual; bloquea afirmar verde el release local desde este checkout.
- **Siguiente comprobación:** repetir con dependencias fijadas del proyecto/CI y revisar compatibilidad de dobles versus imports Delta; comprobar test por test y decidir si es defecto de producción, test obsoleto o dependencia ambiental. No se cambió código.

### SDA-CROSS-002 — Inventario de pares proporcionado omite SDA-02 y no incluye SDA-00

- **Estado:** nuevo; **prioridad:** P2; **confianza:** confirmado por índice y `ls-remote`.
- **Evidencia:** índice público incluye SDA-02; la rama SDA-02 existe. No se encontró SDA-00 en el índice consultado ni rama `Iteration-00` remota. Se añadió SDA-02 al inventario y SDA-00 queda no descubierto, sin inventarlo.
- **Acción:** confirmar el inventario en cada ejecución futura y añadir los nuevos pares solo con fuentes verificables.

### SDA-08-PAIR-001 — Artículo describe entrega Lakebase que sigue parcialmente pendiente

- **Estado:** nuevo; **prioridad:** P1; **confianza:** confirmado por el caveat vigente del repo.
- **Afirmación DOCX SDA-08:** Lakebase project/database desplegado por bundle, migraciones, OAuth con identidad de servicio, workflow de demostración y recuperación.
- **Evidencia en checkout:** `docs/acceptance-matrix.md` y `docs/sda-00-08-capability-matrix.md` limitan SDA-08 a contratos locales/in-memory/SQLite y declaran pendientes la migración Lakebase en vivo, OAuth, backup/restore y scale-to-zero. No hay rama/artículo publicados de SDA-08 verificados.
- **Impacto editorial:** el texto de entrega debe distinguir objetivo/implementación local de evidencia de producción; no anunciar como hecho un camino live pendiente.
- **Siguiente paso:** al crear rama 08, validar despliegue, migración y OAuth con evidencia autorizada y actualizar el artículo antes de publicarlo.

### SDA-10-PAIR-001 — Alcance de “deterministic Spark tool” supera la generación ejecutable observada

- **Estado:** nuevo; **prioridad:** P1; **confianza:** confirmado en contrato del código actual.
- **Afirmación DOCX SDA-10:** `synthetic_data_generator v1` como herramienta Spark determinista preserva distribuciones, long tails, timestamps, formatos y nulls.
- **Evidencia:** `src/sda/generation.py` se describe como implementación pura local y acotada; `generate_rows` consume plan aprobado y muestras/vocabularios inyectados. El docstring dice que el adaptador distribuido Databricks puede venir después. `docs/acceptance-matrix.md`/capability matrix no incluyen generación en SDA 08 y el plan actual solo afirma la base local.
- **Impacto:** artículo promete Spark y preservación estadística amplia que no está respaldada por la implementación observada en este SHA; la generación multi-tabla también queda fuera del camino standalone.
- **Siguiente paso:** acotar el texto a capacidades ejecutadas en la rama SDA-10 y probar estrategias/calidad por distribución antes de publicar. No inferir que añadir el adaptador sea necesario si el alcance final cambia.

### SDA-09-EDITORIAL-001 — Comparativa Lakebase necesita fuentes y resultados reproducibles

- **Estado:** nuevo; **prioridad:** P2; **confianza:** probable.
- **Evidencia:** el DOCX presenta una decisión comparativa dependiente de benchmarks, integración y operación. La versión preparada que se leyó plantea criterios, pero el informe todavía no inspeccionó tablas/resultados completos y no halló rama 09.
- **Siguiente paso:** exigir condiciones de benchmark, versiones, configuración, repeticiones y resultados; respaldar afirmaciones actuales sobre Lakebase con documentación primaria vigente antes de cerrar la revisión.

## 4. Matriz de trazabilidad inicial

La revisión de los pares 01, 03–06 fue de lectura y contraste general, no una auditoría línea por línea completa. SDA-02 aparece en el índice y su rama existe, pero la URL probada devuelve 404, por lo que su revisión de artículo está bloqueada. No se asignan porcentajes porque aún no hay un denominador estable de afirmaciones verificables.

| Par | Afirmación principal | Evidencia inicial | Estado |
|---|---|---|---|
| 01 | Alcance del agente sintético y motivación Databricks | Artículo público completo, README/arquitectura y rama SDA-01 aún por comparación detallada | No verificable en detalle |
| 02 | Arquitectura por capas y toolbox | Rama SDA-02 existe; artículo no recuperado (URL probada 404) | Artículo no verificable; pareja bloqueada |
| 03 | Bootstrap con Declarative Automation Bundles | `databricks.yml`, `bundle/`, contratos estáticos mencionados en acceptance matrix | Implementado sin validación live completa |
| 04 | Lectura de metadatos UC | `src/sda/tools/uc_metadata_reader.py`; el repo declara contratos locales, UC live pendiente | Implementado sin validación live |
| 05 | Perfilado y evidencia segura | `src/sda/tools/table_profiler.py`, profiling; rendimiento a escala pendiente | Implementado sin validación a escala |
| 06 | PK/FK, relaciones y dependencias | `src/sda/relationships/`, `relational.py`, `topology.py`; generación de grafo distribuida pendiente | Parcialmente implementado |
| 07 | Patrones y reglas cross-column | `src/sda/patterns/` y job Spark; extensos tests locales; integración live nueva bloqueada según docs | Implementado sin validación live completa |
| 08 | Estado duradero Lakebase | SQLite/Postgres adapter y esquema; live Lakebase/OAuth/recuperación pendientes | Parcialmente implementado |
| 09 | Elección de base operativa | Aún no existe rama; artículo borrador compara criterios; evidencia de benchmarks pendiente | No verificable |
| 10 | Generación de tablas con distribuciones/nulls | Generador Python local por plan aprobado; contrato de Spark/distribuciones amplio pendiente | Parcialmente implementado / contradictorio con promesa actual |

## 5. Pruebas y evidencia

| Comando | Resultado | Limitación |
|---|---|---|
| `git ls-remote --heads origin` | ramas SDA-01…SDA-07 y main | Solo observación puntual, no verifica ramas futuras después del momento de captura |
| `python -m pytest -q -m "not spark and not databricks and not integration" --tb=short` | falla en 6 tests de persistencia Delta; el resto completó | Ejecutado con entorno global disponible, no en entorno limpio reconstruido; no equivale a release/CI |
| Requests HTTP GET a páginas SDA 01,03–07 | 200 para las 6 URLs dadas | La lectura se guardó localmente; sin auditoría editorial detallada de cada sección en esta primera iteración |
| Lectura del índice público | SDA-07 más reciente; SDA-02 aparece publicado | Índice dinámico; revisar de nuevo cada ejecución |

No se ejecutaron tests Spark, Databricks, PostgreSQL de integración, despliegues ni benchmarks externos. No se validaron afirmaciones actuales de documentación Databricks; requieren fuentes oficiales vigentes.

## 6. Preparación editorial de SDA-07–10

Los artículos 08–10 no se publicaron ni se escribieron en el repositorio. Los DOCX originales permanecen intactos.

### SDA-07 — publicación prevista 27 de octubre de 2026

El DOCX “Final” cubre patrones condicionales, faltantes, temporales, reglas, evidencia, aprobación y pruebas adversariales. La narrativa delimita adecuadamente patrón observado versus regla aprobada. Antes de publicar, reconciliar referencias que afirman “deployed”/integración con el caveat reciente de presupuesto Free y revisar que la fecha/versiones del estado live sigan vigentes. El artículo público ya existe y no se modificó.

### SDA-08 — publicación prevista 29 de octubre de 2026

El ángulo editorial es sólido: Lakebase como plano operativo y Delta/UC como evidencia analítica; el workflow se detiene en `READY_FOR_GENERATION`. Debe corregirse la afirmación de entrega live descrita arriba o adjuntar evidencia reciente suficiente. Revisar coherencia con branch SDA-07 en su estado final, no solo con el diseño.

### SDA-09 — publicación prevista 3 de noviembre de 2026

El artículo evita basar la decisión solo en latencia y define criterios de consistencia, despliegue y operación. Para dejarlo listo, incorporar resultados de benchmark verificables y comparables, documentar costos/limitaciones de la cuenta/región y fuentes oficiales actuales. No convertir preferencias de arquitectura en hechos universales.

### SDA-10 — publicación prevista 5 de noviembre de 2026

El borrador explica con cuidado muestreo probabilístico frente a cuotas exactas, tipos, colas, formatos, timestamps, nulls, idempotencia y manifest. Sin embargo, el repositorio revisado aún no respalda la promesa de herramienta Spark completa. Alinear título/intro con el código entregable o esperar la rama con el adaptador y las estrategias correspondientes. Mantener separada la validación estadística completa (artículo posterior) de la comprobación estructural de ejecución.

## 7. Contadores e iteraciones

Solo hay una lectura inicial de cada material; encontró hallazgos nuevos. Por tanto no hay pasada limpia y todos los contadores empiezan en 0/10.

| Elemento | Limpias artículo | Limpias rama | Limpias correspondencia | Estado |
|---|---:|---:|---:|---|
| SDA-01 | 0/10 | 0/10 | 0/10 | Abierto |
| SDA-02 | 0/10 | 0/10 | 0/10 | Abierto |
| SDA-03 | 0/10 | 0/10 | 0/10 | Abierto |
| SDA-04 | 0/10 | 0/10 | 0/10 | Abierto |
| SDA-05 | 0/10 | 0/10 | 0/10 | Abierto |
| SDA-06 | 0/10 | 0/10 | 0/10 | Abierto |
| SDA-07 | 0/10 | 0/10 | 0/10 | Abierto |
| SDA-08 | 0/10 | 0/10 | 0/10 | Borrador, sin rama |
| SDA-09 | 0/10 | 0/10 | 0/10 | Borrador, sin rama |
| SDA-10 | 0/10 | 0/10 | 0/10 | Borrador, sin rama |

**Historial de pasada 1:** inventario de workspace y remoto; HTTP artículo completo; extracción DOCX completa; lectura de matrices de aceptación y auditoría DOD; inspección inicial del generador y Postgres adapter; suite pytest filtrada. Hallazgos nuevos: SDA-CROSS-001/002, SDA-02-ACCESS-001, SDA-08-PAIR-001, SDA-10-PAIR-001 y SDA-09-EDITORIAL-001. Ninguna pasada limpia contabilizada.

## 8. Pendientes prioritarios y plan

1. Resolver o clasificar los seis fallos de la suite en una instalación reproducible, separando defecto real de fixture obsoleto o entorno.
2. Hacer comparación artículo↔rama de SDA-01–06, con afirmaciones y ubicaciones de código/tests concretas.
3. Revisar SDA-07 Final frente a artículo público y el SHA; cotejar inventario de reglas, muestras, diagramas/enlaces y claims live.
4. Para SDA-08/09/10, esperar ramas verificables y revisar fuentes oficiales Databricks actuales, costos y benchmark antes de fijar texto final.
5. Continuar pasadas independientes con contador por artículo, rama y pareja; cualquier hallazgo nuevo reinicia solo el elemento afectado.

**Próxima revisión sugerida:** tras disponibilidad de ramas SDA-08 y SDA-09 o antes de la publicación del 29 de octubre, lo que ocurra primero. La auditoría permanece abierta; este informe es una línea base inicial, no un cierre.

## 9. Actualización de la iteración SDA-07 — 29 de octubre de 2026

Esta sección supersede las observaciones operativas de la línea base anterior para
SDA-07. La rama comprobada es `SDA-07`; el commit de esta iteración queda en el
historial de la rama. Las eliminaciones de
`.github/workflows/ci.yml` y `.github/workflows/databricks-integration.yml` se
mantienen porque CI no forma parte del flujo solicitado.

La revisión contrastó el DOCX de SDA-07, el artículo publicado y la implementación
actual. Se verificó que el flujo consume los artefactos de SDA-04–06, limita
candidatos por roles, mantiene separados patrones observados y reglas declaradas,
preserva evidencia de soporte/violaciones/estabilidad/proveniencia, y cubre las
familias condicional, missingness, fan-out, orden temporal, lag, transiciones y
reglas con períodos efectivos. La evaluación de reglas ahora acepta
`rules_as_of_date`, lo incorpora al `analysis_id` y lo persiste explícitamente en
`rule_provenance_json`, evitando una fecha implícita o un fingerprint opaco.

La autenticación del perfil `sda` fue renovada y `databricks bundle validate` y
`databricks bundle deploy -t dev --profile sda` finalizaron correctamente. La
ejecución live de reglas con fecha efectiva terminó correctamente en el job
`803864714810420`, task `555844789496495`; produjo cinco patrones, incluida una
regla business-rule en estado `review_required`, con ventana efectiva
`2026-01-01`–`2026-12-31` y `rules_as_of_date=2026-10-22`. La consulta SQL de
verificación fue `01f1d351-b686-1195-9996-dc08d47b2c7e`. El resto de IDs live y la
matriz completa están en `docs/sda07-definition-of-done-audit.md`.

Los seis fallos de persistencia reportados en la línea base fueron corregidos en
la implementación y se añadió regresión para lotes vacíos y para la fecha de
evaluación de reglas. La suite no-Spark, Ruff, formato, mypy, contratos de bundle,
esquema y release se vuelven a ejecutar al cerrar esta iteración. El Spark local
en Windows sigue siendo un entorno no soportado por la combinación Python 3.13 /
PySpark; la evidencia Spark de SDA-07 se obtiene mediante Databricks `sda`.

No se afirma el requisito formal de diez pasadas limpias por modelo: esta
actualización documenta la corrección y verificación de esta iteración, pero no
convierte retrospectivamente la línea base en 10/10. Tampoco se afirma readiness
productivo: aprobación gobernada, service principal, promoción multi-entorno,
Lakebase/Genie y pruebas de escala siguen fuera del alcance de SDA-07.
