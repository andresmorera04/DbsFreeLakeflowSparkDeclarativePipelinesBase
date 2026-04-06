# Plan de Implementacion: Incremento 5 - LSDP Medalla de Oro - Vistas Materializadas

**Branch**: `005-lsdp-oro-vistas` | **Fecha**: 2026-04-05 | **Spec**: [spec.md](spec.md)
**Entrada**: Especificacion desde `/specs/005-lsdp-oro-vistas/spec.md`

## Resumen

Incrementar el pipeline Lakeflow Spark Declarative Pipelines (LSDP) agregando la medalla de oro mediante dos vistas materializadas en `oro.regional`. La vista `comportamiento_atm_cliente` agrega las transacciones de la vista materializada de plata `transacciones_enriquecidas` por `identificador_cliente` usando agregacion condicional (sin filtros previos) para calcular 5 metricas: cantidad de depositos ATM (DATM), cantidad de retiros ATM (CATM), promedio de montos de depositos ATM, promedio de montos de retiros ATM y total de pagos al saldo del cliente (PGSL), todas basadas en la columna `monto_principal`. La vista `resumen_integral_cliente` combina datos dimensionales de `clientes_saldos_consolidados` (plata) con metricas ATM de `comportamiento_atm_cliente` (oro) mediante INNER JOIN por `identificador_cliente`, produciendo 22 columnas con `F.coalesce` para defaults en metricas nulas. Solo clientes presentes en ambas fuentes aparecen en el resumen. Ambas vistas se crean en un unico archivo `LsdpOroClientes.py` con patron Closure autocontenido, propiedades Delta optimizadas, liquid cluster, observabilidad completa y compatibilidad 100% con Serverless Compute. Se usa exclusivamente la API `pyspark.pipelines` (`from pyspark import pipelines as dp`) con decoradores `@dp.materialized_view`. Se crea una nueva utilidad `LsdpInsertarTiposTransaccion` para insercion idempotente de tipos de transaccion en tabla Parametros, con TDD en `NbTddOroPipeline.py`. Se reutilizan las demas utilidades existentes de incrementos anteriores.

## Contexto Tecnico

**Lenguaje/Version**: Python 3.x / PySpark (Databricks Runtime Serverless)
**Dependencias Principales**: `pyspark.pipelines` (decorador `@dp.materialized_view`, importado como `from pyspark import pipelines as dp`), `pyspark.sql.functions`
**Almacenamiento**: Unity Catalog Managed Tables — las vistas materializadas de oro se crean directamente en `oro.regional` via LSDP. No requieren rutas de archivos.
**Pruebas**: TDD con `spark.createDataFrame()` ejecutable desde Databricks Extension para VS Code en Serverless Compute. Cubre EXCLUSIVAMENTE modulos de `utilities/` (Python puro). Se crea la utilidad `LsdpInsertarTiposTransaccion` para oro. TDD nuevo en `NbTddOroPipeline.py` cubre insercion idempotente. Se verifica no-regresion de pruebas existentes.
**Plataforma Objetivo**: Databricks Free Edition — Serverless Compute exclusivamente
**Tipo de Proyecto**: Pipeline de datos declarativo (LSDP) con vistas materializadas y utilidades modulares
**Metas de Rendimiento**: Agregacion de ~150,000 transacciones en metricas por ~50,000 clientes. INNER JOIN de ~50,000 clientes plata con metricas oro. Dimension Tipo 1 heredada de plata.
**Restricciones**: Sin `sparkContext`, sin `.cache()`, sin `.persist()`, sin RDD, sin `import dlt`, sin hardcode, sin ZOrder/PartitionBy, sin filtros previos sobre DataFrame de transacciones en vista de agregacion
**Escala/Alcance**: 2 vistas materializadas, 1 script de transformacion — 3 archivos Python nuevos (`LsdpOroClientes.py`, `LsdpInsertarTiposTransaccion.py`, `NbTddOroPipeline.py`)

## Verificacion de Constitution

*GATE: Debe pasar antes del Phase 0 research. Re-verificar despues del Phase 1 design.*

| # | Principio | Estado | Evidencia |
|---|-----------|--------|-----------|
| I | Plataforma Exclusiva: Databricks Free Edition | PASA | Sin dependencias Azure. Solo Unity Catalog + Serverless. Las vistas materializadas residen en catalogos UC gestionados (`oro.regional`). |
| II | Storage Dinamico via TipoStorage | PASA | Las vistas materializadas de oro NO requieren rutas de archivos — leen directamente de vistas materializadas de plata y de la propia vista de oro por nombre (`spark.read.table()`). `LsdpConstructorRutas` no se invoca desde oro. |
| III | Idempotencia Diferenciada | PASA | `@dp.materialized_view` es declarativo e idempotente por naturaleza. Re-ejecutar el pipeline recalcula las vistas sin duplicados. La Dimension Tipo 1 se hereda de plata. |
| IV | Observabilidad desde el Minuto Cero | PASA | RF-018 exige prints al inicio del modulo: parametros del pipeline, valores de tabla Parametros, catalogos y esquemas destino, metricas, liquid cluster. LSDP propaga errores nativamente. |
| V | Parametrizacion Total (No Hard-Coded) | PASA | Catalogos y esquemas de oro via tabla Parametros (`catalogoOro`, `esquemaOro`). Catalogos y esquemas de plata via tabla Parametros (`catalogoPlata`, `esquemaPlata`). Tipos de transaccion leidos de tabla Parametros clave `TiposTransaccionesLabBase` (RF-017). Cero hardcode de rutas, catalogos o esquemas. |
| VI | Compatibilidad Maxima Serverless | PASA | Prohibido sparkContext, RDD, cache, persist (RF-014). Patron Closure obligatorio (RF-012). Funciones nativas PySpark para agregacion condicional. |
| VII | Optimizacion Recursos Limitados | PASA | Sin configuracion manual de shuffle.partitions (RF-026). Agregacion condicional sobre un solo paso GroupBy (eficiente en memoria). INNER JOIN entre dos fuentes — solo clientes presentes en ambas. |
| VIII | TDD | PASA | Utilidad nueva `LsdpInsertarTiposTransaccion` con TDD en `NbTddOroPipeline.py`. Las pruebas TDD existentes de incrementos anteriores deben pasar sin regresion. Los scripts de `transformations/` NO son testeables via TDD (patron Closure). |
| IX | Paradigma Declarativo LSDP Estricto | PASA | Solo `@dp.materialized_view` de `pyspark.pipelines` (RF-001/RF-005/RF-015). Sin `import dlt`. Agregacion condicional sin filtros previos sobre DataFrame (RF-027). INNER JOIN directo. |
| R | Restricciones Tecnologicas | PASA | Sin abfss://, /mnt/, DBFS root, Azure SQL, ZOrder, PartitionBy, errores silenciosos, emojis. Liquid cluster via `cluster_by` exclusivamente. |

**Resultado**: TODOS LOS GATES PASAN. No hay violaciones.

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/005-lsdp-oro-vistas/
├── plan.md              # Este archivo
├── research.md          # Phase 0: investigacion
├── data-model.md        # Phase 1: modelo de datos
├── quickstart.md        # Phase 1: guia rapida
├── contracts/           # Phase 1: contratos
│   └── nb-oro-vistas.md
└── tasks.md             # Phase 2: tareas (generado por /speckit.tasks)
```

### Codigo Fuente (raiz del repositorio)

```text
src/LSDP_Laboratorio_Basico/
├── utilities/
│   ├── LsdpConexionParametros.py          # [EXISTENTE Inc. 3] Python puro. Lee tabla Parametros -> diccionario
│   ├── LsdpConstructorRutas.py            # [EXISTENTE Inc. 3] Python puro. Construye rutas dinamicas (Volume o S3) — NO usado en oro
│   ├── LsdpInsertarTiposTransaccion.py    # [NUEVO Inc. 5] Python puro. Insercion idempotente de tipos de transaccion en tabla Parametros
│   └── LsdpReordenarColumnasLiquidCluster.py  # [EXISTENTE Inc. 3] Python puro. Reordena columnas liquid cluster — NO necesario en oro
├── transformations/
│   ├── LsdpBronceCmstfl.py               # [EXISTENTE Inc. 3] Streaming table cmstfl
│   ├── LsdpBronceTrxpfl.py               # [EXISTENTE Inc. 3] Streaming table trxpfl
│   ├── LsdpBronceBlncfl.py               # [EXISTENTE Inc. 3] Streaming table blncfl
│   ├── LsdpPlataClientesSaldos.py         # [EXISTENTE Inc. 4] Vista materializada clientes_saldos_consolidados
│   ├── LsdpPlataTransacciones.py          # [EXISTENTE Inc. 4] Vista materializada transacciones_enriquecidas
│   └── LsdpOroClientes.py                # [NUEVO Inc. 5] Vistas materializadas comportamiento_atm_cliente y resumen_integral_cliente
└── explorations/
    └── LSDP_Laboratorio_Basico/
        ├── NbTddBroncePipeline.py         # [EXISTENTE Inc. 3] TDD bronce (no modificar, validar no-regresion)
        └── NbTddOroPipeline.py            # [NUEVO Inc. 5] TDD utilidad LsdpInsertarTiposTransaccion
```

**Decision de Estructura**: Se mantiene la estructura estandar del LSDP establecida en incrementos anteriores. Se agrega un unico archivo nuevo `LsdpOroClientes.py` en `transformations/` que define ambas vistas materializadas de oro. Ambas vistas comparten el mismo catalogo y esquema destino (`oro.regional`), los mismos parametros de inicializacion y el mismo contexto de closure, por lo que colocarlas en un solo archivo sigue el principio de cohesion. Se crea la utilidad `LsdpInsertarTiposTransaccion` para insercion idempotente de tipos de transaccion en tabla Parametros. La logica de oro (agregacion condicional + INNER JOIN + coalesce) se implementa directamente con funciones nativas de PySpark. La utilidad `LsdpReordenarColumnasLiquidCluster` NO se necesita en oro porque las columnas son pocas (6 y 22) y se seleccionan explicitamente en el orden deseado. La utilidad `LsdpConstructorRutas` NO se necesita porque las vistas materializadas leen de otras vistas/tablas por nombre, no de archivos. El TDD existente se valida por no-regresion. Se agrega `NbTddOroPipeline.py` con pruebas TDD para `LsdpInsertarTiposTransaccion`.

## Seguimiento de Complejidad

> No hay violaciones de la constitution. Esta seccion queda vacia.
