# Plan de Implementacion: Incremento 3 - LSDP Medalla de Bronce

**Branch**: `003-lsdp-bronce-pipeline` | **Fecha**: 2026-04-04 | **Spec**: [spec.md](spec.md)
**Entrada**: Especificacion desde `/specs/003-lsdp-bronce-pipeline/spec.md`

## Resumen

Crear el pipeline Lakeflow Spark Declarative Pipelines (LSDP) con el procesamiento exclusivo de la medalla de bronce. El pipeline ingesta de forma incremental los parquets AS400 (CMSTFL, TRXPFL, BLNCFL) usando AutoLoader con schema evolution activo, crea tres streaming tables en `bronce.regional` con liquid cluster, marca de tiempo `FechaIngestaDatos` y propiedades Delta optimizadas. Todo el codigo usa exclusivamente la API `pyspark.pipelines` (importada como `from pyspark import pipelines as dp`) con decoradores `@dp.table`, es 100% compatible con Serverless Compute, y sigue el patron Closure para parametrizacion. Incluye tres utilidades reutilizables y un conjunto de pruebas TDD.

## Contexto Tecnico

**Lenguaje/Version**: Python 3.x / PySpark (Databricks Runtime Serverless)
**Dependencias Principales**: `pyspark.pipelines` (decorador `@dp.table`, importado como `from pyspark import pipelines as dp`), `pyspark.sql.functions`, `pyspark.sql.types`
**Almacenamiento**: Unity Catalog Managed Volumes (`/Volumes/...`) o Amazon S3 (`s3://...`) segun parametro `TipoStorage`
**Pruebas**: TDD con `spark.createDataFrame()` ejecutable desde Databricks Extension para VS Code en Serverless Compute
**Plataforma Objetivo**: Databricks Free Edition — Serverless Compute exclusivamente
**Tipo de Proyecto**: Pipeline de datos declarativo (LSDP) con utilidades modulares
**Metas de Rendimiento**: Ingesta incremental de ~50,000 clientes + ~150,000 transacciones + ~50,000 saldos por ejecucion
**Restricciones**: Sin `sparkContext`, sin `.cache()`, sin `.persist()`, sin RDD, sin `import dlt`, sin hardcode, sin ZOrder/PartitionBy
**Escala/Alcance**: 3 streaming tables, 3 utilidades, 1 archivo TDD — 7 archivos Python totales

## Verificacion de Constitution

*GATE: Debe pasar antes del Phase 0 research. Re-verificar despues del Phase 1 design.*

| # | Principio | Estado | Evidencia |
|---|-----------|--------|-----------|
| I | Plataforma Exclusiva: Databricks Free Edition | PASA | Sin dependencias Azure. Solo Unity Catalog + Serverless. |
| II | Storage Dinamico via TipoStorage | PASA | `LsdpConstructorRutas` construye rutas Volume o S3 segun parametro. |
| III | Idempotencia Diferenciada | PASA | AutoLoader garantiza exactly-once. Streaming tables append-only. Utilidades son idempotentes. |
| IV | Observabilidad desde el Minuto Cero | PASA | RF-018 exige prints al inicio y fin de cada script. RF-004/RF-005 imprimen parametros y rutas. |
| V | Parametrizacion Total (No Hard-Coded) | PASA | 9 parametros de pipeline + 11 claves de tabla Parametros. Cero valores hardcodeados (RF-017). |
| VI | Compatibilidad Maxima Serverless | PASA | Prohibido sparkContext, RDD, cache, persist (RF-014). Patron Closure obligatorio (RF-013). |
| VII | Optimizacion Recursos Limitados | PASA | Sin shuffle.partitions manual (RF-024). AutoLoader optimizado para Serverless. |
| VIII | TDD | PASA | RF-023 define TDD exclusivamente para las 3 utilidades de `utilities/` (Python puro). Los notebooks de `transformations/` NO son testeables via TDD (codigo de nivel de modulo requiere pipeline LSDP desplegado). |
| IX | Paradigma Declarativo LSDP Estricto | PASA | Solo `@dp.table` de `pyspark.pipelines` (`from pyspark import pipelines as dp`) (RF-016). Sin `import dlt`. |
| R | Restricciones Tecnologicas | PASA | Sin abfss://, /mnt/, DBFS root, Azure SQL, ZOrder, PartitionBy, errores silenciosos, emojis. |

**Resultado**: TODOS LOS GATES PASAN. No hay violaciones.

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/003-lsdp-bronce-pipeline/
├── plan.md              # Este archivo
├── research.md          # Phase 0: investigacion
├── data-model.md        # Phase 1: modelo de datos
├── quickstart.md        # Phase 1: guia rapida
├── contracts/           # Phase 1: contratos
│   └── nb-bronce-pipeline.md
└── tasks.md             # Phase 2: tareas (generado por /speckit.tasks)
```

### Codigo Fuente (raiz del repositorio)

```text
src/LSDP_Laboratorio_Basico/
├── utilities/
│   ├── LsdpConexionParametros.py          # Python puro. Lee tabla Parametros -> diccionario Python
│   ├── LsdpConstructorRutas.py            # Python puro. Construye rutas dinamicas (Volume o S3)
│   └── LsdpReordenarColumnasLiquidCluster.py  # Python puro. Reordena columnas del liquid cluster
├── transformations/
│   ├── LsdpBronceCmstfl.py               # Streaming table cmstfl (Maestro Clientes)
│   ├── LsdpBronceTrxpfl.py               # Streaming table trxpfl (Transaccional)
│   └── LsdpBronceBlncfl.py               # Streaming table blncfl (Saldos)
└── explorations/
    └── LSDP_Laboratorio_Basico/
        └── NbTddBroncePipeline.py         # Pruebas TDD para utilidades (Python puro). NO cubre transformations/
```

**Decision de Estructura**: Se usa la estructura estandar del LSDP: `utilities/` para funciones transversales reutilizables en formato Python puro (modulos importables, NO notebooks), `transformations/` para scripts con decoradores `@dp.table` en formato notebook Databricks, y `explorations/LSDP_Laboratorio_Basico/` para pruebas TDD en formato notebook Databricks. Cada script de transformacion es autocontenido (patron Closure independiente). **IMPORTANTE**: LSDP resuelve automaticamente los imports entre carpetas del pipeline, por lo que los scripts de transformacion importan utilidades directamente (`from utilities.LsdpConexionParametros import obtener_parametros`) sin `sys.path.insert()`. Solo los notebooks de exploracion/TDD (que se ejecutan fuera del pipeline) necesitan manipulacion manual de `sys.path`. **CRITICO**: Los notebooks de Databricks NO son scripts Python estandar — `__file__` no esta definido. Los notebooks TDD deben resolver la ruta base via `dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()` con prefijo `/Workspace`.

## Seguimiento de Complejidad

> No hay violaciones de la constitution. Esta seccion queda vacia.
