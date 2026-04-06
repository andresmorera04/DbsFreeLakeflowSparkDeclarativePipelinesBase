# Plan de Implementacion: Incremento 4 - LSDP Medalla de Plata - Vistas Materializadas

**Branch**: `004-lsdp-plata-vistas` | **Fecha**: 2026-04-05 | **Spec**: [spec.md](spec.md)
**Entrada**: Especificacion desde `/specs/004-lsdp-plata-vistas/spec.md`

## Resumen

Incrementar el pipeline Lakeflow Spark Declarative Pipelines (LSDP) agregando la medalla de plata mediante dos vistas materializadas en `plata.lab1`. La vista `clientes_saldos_consolidados` consolida las streaming tables de bronce `cmstfl` (Maestro de Clientes, 72 columnas) y `blncfl` (Saldos de Clientes, 102 columnas) mediante LEFT JOIN por CUSTID con Dimension Tipo 1 (solo el registro mas reciente por cliente), deduplicacion de columnas comunes, 4 campos calculados (clasificacion_riesgo_cliente, categoria_saldo_disponible, perfil_actividad_bancaria, huella_identificacion_cliente) y 5 expectativas de calidad de datos (`@dp.expect`). La vista `transacciones_enriquecidas` lee la streaming table de bronce `trxpfl` (62 columnas) sin filtros para maximizar cargas incrementales automaticas, con 4 campos calculados numericos (monto_neto_comisiones, porcentaje_comision_sobre_monto, variacion_saldo_transaccion, indicador_impacto_financiero) y 4 expectativas de calidad de datos. Ambas vistas descartan las columnas de control (`_rescued_data`, `año`, `mes`, `dia`, `FechaIngestaDatos`), renombran todas las columnas a espanol snake_case, y tienen propiedades Delta optimizadas con liquid cluster. Todo el codigo usa exclusivamente la API `pyspark.pipelines` (`from pyspark import pipelines as dp`) con decoradores `@dp.materialized_view` y `@dp.expect`, es 100% compatible con Serverless Compute y sigue el patron Closure para parametrizacion.

## Contexto Tecnico

**Lenguaje/Version**: Python 3.x / PySpark (Databricks Runtime Serverless)
**Dependencias Principales**: `pyspark.pipelines` (decoradores `@dp.materialized_view`, `@dp.expect`, importado como `from pyspark import pipelines as dp`), `pyspark.sql.functions`, `pyspark.sql.Window`
**Almacenamiento**: Unity Catalog Managed Tables — las vistas materializadas de plata se crean directamente en `plata.lab1` via LSDP. No requieren rutas de archivos.
**Pruebas**: TDD con `spark.createDataFrame()` ejecutable desde Databricks Extension para VS Code en Serverless Compute. Cubre EXCLUSIVAMENTE modulos de `utilities/` (Python puro).
**Plataforma Objetivo**: Databricks Free Edition — Serverless Compute exclusivamente
**Tipo de Proyecto**: Pipeline de datos declarativo (LSDP) con vistas materializadas y utilidades modulares
**Metas de Rendimiento**: Procesamiento de ~50,000 clientes + ~50,000 saldos consolidados + ~150,000 transacciones enriquecidas por ejecucion. Carga incremental automatica para transacciones.
**Restricciones**: Sin `sparkContext`, sin `.cache()`, sin `.persist()`, sin RDD, sin `import dlt`, sin hardcode, sin ZOrder/PartitionBy, sin filtros en vista transaccional
**Escala/Alcance**: 2 vistas materializadas, 2 scripts de transformacion, posible nuevo TDD — ~4 archivos Python nuevos

## Verificacion de Constitution

*GATE: Debe pasar antes del Phase 0 research. Re-verificar despues del Phase 1 design.*

| # | Principio | Estado | Evidencia |
|---|-----------|--------|-----------|
| I | Plataforma Exclusiva: Databricks Free Edition | PASA | Sin dependencias Azure. Solo Unity Catalog + Serverless. Las vistas materializadas residen en catalogos UC gestionados. |
| II | Storage Dinamico via TipoStorage | PASA | Las vistas materializadas de plata NO requieren rutas de archivos — leen directamente de las streaming tables de bronce por nombre (`spark.read.table()`). Las utilidades `LsdpConstructorRutas` no se invocan desde plata. |
| III | Idempotencia Diferenciada | PASA | `@dp.materialized_view` es declarativo e idempotente por naturaleza. Re-ejecutar el pipeline recalcula las vistas sin duplicados. |
| IV | Observabilidad desde el Minuto Cero | PASA | RF-018 exige prints al inicio y fin de cada script: parametros, catalogos, esquemas, campos calculados, liquid cluster. Las expectativas `@dp.expect` registran metricas en la UI de LSDP. |
| V | Parametrizacion Total (No Hard-Coded) | PASA | Catalogos y esquemas de plata via tabla Parametros (`catalogoPlata`, `esquemaPlata`). Streaming tables de bronce leidas por nombre simple (catalogo/esquema por defecto del pipeline). Cero hardcode (RF-017). |
| VI | Compatibilidad Maxima Serverless | PASA | Prohibido sparkContext, RDD, cache, persist (RF-015). Patron Closure obligatorio (RF-013). Funciones nativas PySpark para Window y CASE. |
| VII | Optimizacion Recursos Limitados | PASA | Sin configuracion manual de shuffle.partitions (RF-027). Vista transaccional sin filtros para maximizar carga incremental (RF-006/RF-007). |
| VIII | TDD | PASA | RF-023 define TDD exclusivamente para utilidades nuevas en `utilities/`. Los scripts de `transformations/` NO son testeables via TDD (patron Closure requiere pipeline LSDP desplegado). Utilidades existentes del Inc. 3 mantienen sus pruebas sin regresion. |
| IX | Paradigma Declarativo LSDP Estricto | PASA | Solo `@dp.materialized_view` y `@dp.expect` de `pyspark.pipelines` (RF-001/RF-005/RF-016). Sin `import dlt`. Deduplicacion de columnas en consolidada (constitution v1.2.0). Vista transaccional sin filtros para carga incremental (constitution v1.2.0). |
| R | Restricciones Tecnologicas | PASA | Sin abfss://, /mnt/, DBFS root, Azure SQL, ZOrder, PartitionBy, errores silenciosos, emojis. Liquid cluster via `cluster_by` exclusivamente. |

**Resultado**: TODOS LOS GATES PASAN. No hay violaciones.

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/004-lsdp-plata-vistas/
├── plan.md              # Este archivo
├── research.md          # Phase 0: investigacion
├── data-model.md        # Phase 1: modelo de datos
├── quickstart.md        # Phase 1: guia rapida
├── contracts/           # Phase 1: contratos
│   └── nb-plata-vistas.md
└── tasks.md             # Phase 2: tareas (generado por /speckit.tasks)
```

### Codigo Fuente (raiz del repositorio)

```text
src/LSDP_Laboratorio_Basico/
├── utilities/
│   ├── LsdpConexionParametros.py          # [EXISTENTE Inc. 3] Python puro. Lee tabla Parametros -> diccionario
│   ├── LsdpConstructorRutas.py            # [EXISTENTE Inc. 3] Python puro. Construye rutas dinamicas (Volume o S3)
│   └── LsdpReordenarColumnasLiquidCluster.py  # [EXISTENTE Inc. 3] Python puro. Reordena columnas liquid cluster
├── transformations/
│   ├── LsdpBronceCmstfl.py               # [EXISTENTE Inc. 3] Streaming table cmstfl
│   ├── LsdpBronceTrxpfl.py               # [EXISTENTE Inc. 3] Streaming table trxpfl
│   ├── LsdpBronceBlncfl.py               # [EXISTENTE Inc. 3] Streaming table blncfl
│   ├── LsdpPlataClientesSaldos.py         # [NUEVO Inc. 4] Vista materializada clientes_saldos_consolidados
│   └── LsdpPlataTransacciones.py          # [NUEVO Inc. 4] Vista materializada transacciones_enriquecidas
└── explorations/
    └── LSDP_Laboratorio_Basico/
        ├── NbTddBroncePipeline.py         # [EXISTENTE Inc. 3] TDD bronce (no modificar)
        └── NbTddPlataPipeline.py          # [NUEVO Inc. 4] TDD para utilidades nuevas de plata (si aplica)
```

**Decision de Estructura**: Se mantiene la estructura estandar del LSDP establecida en el Incremento 3. Se agregan dos archivos nuevos en `transformations/` para las vistas materializadas de plata. Cada script es autocontenido (patron Closure independiente). Los scripts de plata NO invocan `LsdpConstructorRutas` porque leen directamente de streaming tables de bronce por nombre (no de archivos parquet). Se reutilizan las utilidades existentes: `LsdpConexionParametros` (leer tabla Parametros) y `LsdpReordenarColumnasLiquidCluster` (reordenar columnas del liquid cluster). No se anticipan utilidades nuevas para plata — la logica de Dimension Tipo 1, deduplicacion de columnas, campos calculados y exclusion de columnas se implementa directamente en los scripts de transformacion usando funciones nativas de PySpark (`Window`, `F.when`, `F.sha2`, operaciones aritmeticas). Si durante la implementacion se identifica logica reutilizable, se extraera a `utilities/` con su TDD correspondiente. El archivo TDD `NbTddPlataPipeline.py` se crea solo si se generan utilidades nuevas.

## Seguimiento de Complejidad

> No hay violaciones de la constitution. Esta seccion queda vacia.
