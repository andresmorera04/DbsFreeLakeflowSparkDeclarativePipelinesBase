# Tasks: Incremento 4 - LSDP Medalla de Plata - Vistas Materializadas

**Input**: Documentos de diseño de `/specs/004-lsdp-plata-vistas/`
**Prerequisites**: plan.md (requerido), spec.md (requerido), research.md, data-model.md, contracts/nb-plata-vistas.md, quickstart.md

**Tests**: CONDICIONALES — El spec (HU5, RF-023) exige pruebas TDD unicamente si se crean utilidades nuevas en `utilities/`. El plan no anticipa utilidades nuevas para plata.

**Organizacion**: Tareas agrupadas por historia de usuario para permitir implementacion y pruebas independientes.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos diferentes, sin dependencias)
- **[Story]**: Historia de usuario asociada (US1, US2, US5)
- Rutas exactas de archivos en cada descripcion

## Absorcion de Historias

Las siguientes historias no generan fases independientes porque sus requisitos se implementan dentro de las mismas tareas y archivos de otras historias:

- **HU3 (Propiedades Delta y Liquid Cluster, P2)**: Absorbida en HU1 (parametros `table_properties` y `cluster_by` del decorador `@dp.materialized_view` en T001) y HU2 (mismos parametros en T004). Las 5 propiedades Delta y las columnas del liquid cluster se configuran directamente en los decoradores de cada vista.
- **HU4 (Observabilidad Completa, P2)**: Absorbida en HU1 (prints a nivel de modulo en T001 + prints de procesamiento en T003) y HU2 (prints a nivel de modulo en T004 + prints de procesamiento en T006). La observabilidad es integral a cada script de transformacion.

---

## Phase 1: Setup

**Proposito**: No se requiere. Los directorios `transformations/` y `utilities/` ya existen desde el Incremento 3. No se crean directorios nuevos ni dependencias adicionales.

---

## Phase 2: Foundational (Prerequisitos Bloqueantes)

**Proposito**: No se requiere. Las utilidades reutilizables (`LsdpConexionParametros`, `LsdpReordenarColumnasLiquidCluster`) fueron implementadas en el Incremento 3 y estan disponibles para importacion directa. Los scripts de plata NO invocan `LsdpConstructorRutas` (leen de streaming tables por nombre, no de archivos parquet). No se anticipan utilidades nuevas.

---

## Phase 3: HU1 — Vista Materializada Consolidada de Clientes y Saldos (Prioridad: P1) 🎯 MVP

**Objetivo**: Crear la vista materializada `clientes_saldos_consolidados` en `plata.regional` que consolida cmstfl y blncfl mediante LEFT JOIN con Dimension Tipo 1, 173 columnas renombradas a espanol snake_case, 4 campos calculados, 5 expectativas de calidad de datos, propiedades Delta optimizadas y liquid cluster activo.

**Prueba Independiente**: Desplegar pipeline LSDP, verificar que `plata.regional.clientes_saldos_consolidados` existe con 173 columnas, sin duplicados entre cmstfl y blncfl, Dimension Tipo 1 aplicada (un solo registro por CUSTID), campos calculados correctos, expectativas registradas en la UI de LSDP, propiedades Delta activas, liquid cluster configurado.

**Incorpora**: HU3 (`table_properties` + `cluster_by` del decorador), HU4 (prints de observabilidad).

### Implementacion HU1

- [X] T001 [P] [US1] Crear archivo `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py` — formato notebook Databricks (`# Databricks notebook source` + `# COMMAND ----------`). Patron Closure autocontenido: imports (`from pyspark import pipelines as dp`, `from pyspark.sql import functions as F`, `from pyspark.sql import Window`, `from pyspark.sql import DataFrame`, `from utilities.LsdpConexionParametros import obtener_parametros`, `from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster`). A nivel de modulo: lee 3 parametros del pipeline via `spark.conf.get("pipelines.parameters.catalogoParametro/esquemaParametro/tablaParametros")`, invoca `obtener_parametros()` para leer tabla Parametros, extrae `catalogoPlata` y `esquemaPlata` del diccionario retornado. Define 2 diccionarios de mapeo de columnas a nivel de modulo: `mapeo_cmstfl` con 70 entries (CUSTID→identificador_cliente, CUSNM→nombre_cliente, CUSLN→apellido_cliente, ..., CUSND→fecha_notificacion — todas las 70 columnas segun data-model.md seccion CMSTFL) y `mapeo_blncfl` con 99 entries excluyendo CUSTID (BLSQ→secuencia_saldo, BLACT→tipo_cuenta, ..., BLTP2→fecha_tercero_pago — todas las 99 columnas segun data-model.md seccion BLNCFL). Define lista constante de columnas a excluir: `["FechaIngestaDatos", "_rescued_data", "año", "mes", "dia"]`. Prints de observabilidad a nivel de modulo: nombre de la vista a crear, parametros del pipeline recibidos, catalogo/esquema plata destino (de Parametros), campos del liquid cluster (`huella_identificacion_cliente`, `identificador_cliente`), nombres de los 4 campos calculados. Decorador `@dp.materialized_view(name=f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados", table_properties={"delta.enableChangeDataFeed":"true", "delta.autoOptimize.autoCompact":"true", "delta.autoOptimize.optimizeWrite":"true", "delta.deletedFileRetentionDuration":"interval 30 days", "delta.logRetentionDuration":"interval 60 days"}, cluster_by=["huella_identificacion_cliente","identificador_cliente"])` + 5 decoradores `@dp.expect` apilados: `("limite_credito_positivo","limite_credito > 0")`, `("identificador_cliente_no_nulo","identificador_cliente IS NOT NULL")`, `("limite_credito_no_nulo","limite_credito IS NOT NULL")`, `("fecha_apertura_cuenta_valida","fecha_apertura_cuenta > '2020-12-31'")`, `("fecha_nacimiento_valida","fecha_nacimiento < '2009-01-01'")`. Funcion decorada `clientes_saldos_consolidados()` con esqueleto placeholder (`pass`). (RF-001, RF-003, RF-009, RF-010, RF-012, RF-013, RF-014, RF-016, RF-017, RF-018, RF-019, RF-020, RF-021, RF-022, RF-029)

- [X] T002 [US1] Implementar logica de Dimension Tipo 1 y LEFT JOIN dentro de la funcion decorada `clientes_saldos_consolidados()` en `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py` — (1) Leer cmstfl via `spark.read.table("cmstfl")` (nombre simple, resuelto por el catalogo/esquema por defecto del pipeline). Aplicar Window `Window.partitionBy("CUSTID").orderBy(F.col("FechaIngestaDatos").desc(), F.col("CUSTID").desc())` con `F.row_number().over(ventana)` como columna temporal `numero_fila`. Filtrar `F.col("numero_fila") == 1`, drop `numero_fila`. Eliminar columnas de control con manejo gracioso: `columnas_excluir_existentes = [c for c in columnas_excluir if c in df.columns]`, `df.drop(*columnas_excluir_existentes)`. Resultado: DataFrame cmstfl con 70 columnas (CUSTID + 69 CUS*). (2) Repetir lectura + Dim Tipo 1 + exclusion de control para blncfl via `spark.read.table("blncfl")`. Resultado: DataFrame blncfl con 100 columnas (CUSTID + 99 BL*). (3) Ejecutar LEFT JOIN: `df_cmstfl_reciente.join(df_blncfl_reciente, on="CUSTID", how="left")` — `on="CUSTID"` deduplica automaticamente la columna de JOIN. Resultado: DataFrame con 169 columnas. (4) Imprimir cantidad de columnas de cada DataFrame intermedio y confirmacion del JOIN exitoso. (RF-002, RF-024, RF-025, RF-028)

- [X] T003 [US1] Implementar seleccion/renombrado de columnas, campos calculados, liquid cluster y observabilidad final dentro de la funcion decorada en `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py` — Sobre el DataFrame resultante del JOIN: (1) Construir lista de seleccion combinando ambos diccionarios de mapeo: `columnas_seleccion = [F.col(old).alias(new) for old, new in {**mapeo_cmstfl, **mapeo_blncfl}.items() if old in df_joined.columns]`. Aplicar `.select(columnas_seleccion)`. Resultado: 169 columnas renombradas a espanol snake_case. (2) Agregar 4 campos calculados con `.withColumn()`: `clasificacion_riesgo_cliente` = `F.when((F.col("score_cliente") >= 750) & (F.col("nivel_riesgo").isin("L","LOW")) & (F.col("calificacion_crediticia").isin("A","AA","AAA")), F.lit("RIESGO_BAJO")).when((F.col("score_cliente") >= 500) & (F.col("nivel_riesgo").isin("M","MED")), F.lit("RIESGO_MEDIO")).when((F.col("score_cliente") < 500) | (F.col("nivel_riesgo").isin("H","HIGH")), F.lit("RIESGO_ALTO")).otherwise(F.lit("SIN_CLASIFICAR"))` con manejo explicito de nulos; `categoria_saldo_disponible` = CASE sobre `saldo_disponible >= 100000 AND limite_credito >= 50000 AND saldo_total >= 150000 → PREMIUM`, `saldo_disponible >= 25000 AND limite_credito >= 10000 → ESTANDAR`, `saldo_disponible > 0 → BASICO`, else `SIN_SALDO`; `perfil_actividad_bancaria` = CASE sobre `cantidad_transacciones >= 100 AND cantidad_cuentas >= 3 AND ranking_prestamos >= 5 → MUY_ACTIVO`, `cantidad_transacciones >= 30 AND cantidad_cuentas >= 2 → ACTIVO`, `cantidad_transacciones >= 1 → MODERADO`, else `INACTIVO`; `huella_identificacion_cliente` = `F.sha2(F.col("identificador_cliente").cast("string"), 256)`. Usar `F.coalesce()` con `F.lit(0)` para proteger columnas numericas nulas en las condiciones CASE. (3) Invocar `reordenar_columnas_liquid_cluster(df, ["huella_identificacion_cliente", "identificador_cliente"])` para colocar campos del liquid cluster como primeras columnas. (4) Imprimir cantidad total de columnas (esperada: 173), nombres de los 4 campos calculados, campos del liquid cluster, confirmacion de deduplicacion exitosa y creacion de la vista. (5) Retornar DataFrame final. (RF-003, RF-004, RF-012, RF-026, RF-028)

**Checkpoint**: La vista materializada `clientes_saldos_consolidados` esta completamente implementada. El pipeline puede desplegarse y validarse con `DESCRIBE plata.regional.clientes_saldos_consolidados`.

---

## Phase 4: HU2 — Vista Materializada Transaccional Enriquecida (Prioridad: P1)

**Objetivo**: Crear la vista materializada `transacciones_enriquecidas` en `plata.regional` a partir de trxpfl SIN filtros para maximizar carga incremental automatica de LSDP, 64 columnas renombradas a espanol snake_case, 4 campos calculados numericos con proteccion de nulos y division por cero, 4 expectativas de calidad de datos, propiedades Delta optimizadas y liquid cluster activo.

**Prueba Independiente**: Desplegar pipeline LSDP, verificar que `plata.regional.transacciones_enriquecidas` existe con 64 columnas, sin filtros aplicados, campos calculados numericos correctos, expectativas registradas en la UI de LSDP, propiedades Delta activas, liquid cluster configurado.

**Incorpora**: HU3 (`table_properties` + `cluster_by` del decorador), HU4 (prints de observabilidad).

### Implementacion HU2

- [X] T004 [P] [US2] Crear archivo `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py` — formato notebook Databricks. Patron Closure autocontenido: imports (`from pyspark import pipelines as dp`, `from pyspark.sql import functions as F`, `from pyspark.sql import DataFrame`, `from utilities.LsdpConexionParametros import obtener_parametros`, `from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster`). NO importar Window (no se usa Dimension Tipo 1). A nivel de modulo: lee 3 parametros del pipeline via `spark.conf.get()`, invoca `obtener_parametros()`, extrae `catalogoPlata` y `esquemaPlata` del diccionario retornado. Define diccionario de mapeo `mapeo_trxpfl` con 60 entries (TRXID→identificador_transaccion, CUSTID→identificador_cliente, TRXSQ→secuencia_transaccion, TRXTYP→tipo_transaccion, ..., TRXUS→timestamp_actualizacion — todas las 60 columnas segun data-model.md seccion TRXPFL). Define lista constante de columnas a excluir: `["FechaIngestaDatos", "_rescued_data", "año", "mes", "dia"]`. Prints de observabilidad a nivel de modulo: nombre de la vista a crear, parametros del pipeline recibidos, catalogo/esquema plata destino (de Parametros), campos del liquid cluster (`fecha_transaccion`, `identificador_cliente`, `tipo_transaccion`), nombres de los 4 campos calculados numericos. Decorador `@dp.materialized_view(name=f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas", table_properties={"delta.enableChangeDataFeed":"true", "delta.autoOptimize.autoCompact":"true", "delta.autoOptimize.optimizeWrite":"true", "delta.deletedFileRetentionDuration":"interval 30 days", "delta.logRetentionDuration":"interval 60 days"}, cluster_by=["fecha_transaccion","identificador_cliente","tipo_transaccion"])` + 4 decoradores `@dp.expect` apilados: `("moneda_transaccion_no_nula","moneda_transaccion IS NOT NULL")`, `("monto_neto_no_nulo","monto_neto IS NOT NULL")`, `("monto_neto_positivo","monto_neto > 0")`, `("identificador_cliente_no_nulo","identificador_cliente IS NOT NULL")`. Funcion decorada `transacciones_enriquecidas()` con esqueleto placeholder. (RF-005, RF-009, RF-011, RF-012, RF-013, RF-014, RF-016, RF-017, RF-018, RF-019, RF-020, RF-021, RF-022, RF-030)

- [X] T005 [US2] Implementar seleccion/exclusion/renombrado de columnas SIN FILTROS dentro de la funcion decorada `transacciones_enriquecidas()` en `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py` — (1) Leer trxpfl via `spark.read.table("trxpfl")` (nombre simple, resuelto por el catalogo/esquema por defecto del pipeline) — SIN usar `.filter()`, `.where()`, `groupBy()`, `agg()`, `distinct()` ni funciones no deterministas (`current_timestamp()`, `current_date()`). Estas restricciones garantizan que LSDP pueda optimizar con carga incremental automatica. (2) Eliminar columnas de control con manejo gracioso: `columnas_excluir_existentes = [c for c in columnas_excluir if c in df.columns]`, `df.drop(*columnas_excluir_existentes)`. (3) Construir lista de seleccion: `columnas_seleccion = [F.col(old).alias(new) for old, new in mapeo_trxpfl.items() if old in df.columns]`. Aplicar `.select(columnas_seleccion)`. Resultado: 60 columnas renombradas a espanol snake_case. (4) Imprimir confirmacion de 0 filtros aplicados y cantidad de columnas seleccionadas. (RF-006, RF-007, RF-024, RF-028)

- [X] T006 [US2] Implementar 4 campos calculados numericos, liquid cluster y observabilidad final dentro de la funcion decorada en `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py` — Sobre el DataFrame renombrado: (1) Agregar 4 campos calculados con `.withColumn()`: `monto_neto_comisiones` = `F.coalesce(F.col("monto_principal"), F.lit(0.0)) - F.coalesce(F.col("comision_transaccion"), F.lit(0.0))`; `porcentaje_comision_sobre_monto` = `F.when((F.col("monto_principal") != 0) & (F.col("monto_principal").isNotNull()), (F.coalesce(F.col("comision_transaccion"), F.lit(0.0)) / F.col("monto_principal")) * 100.0).otherwise(F.lit(0.0))` — proteccion division por cero exigida por ANSI mode; `variacion_saldo_transaccion` = `F.coalesce(F.col("saldo_posterior"), F.lit(0.0)) - F.coalesce(F.col("saldo_anterior"), F.lit(0.0))`; `indicador_impacto_financiero` = `F.coalesce(F.col("monto_principal"), F.lit(0.0)) * F.coalesce(F.col("riesgo_transaccion"), F.lit(0.0))`. Todos los campos usan `F.coalesce()` con `F.lit(0.0)` para proteger nulos (RF-026). (2) Invocar `reordenar_columnas_liquid_cluster(df, ["fecha_transaccion", "identificador_cliente", "tipo_transaccion"])` para colocar campos del liquid cluster como primeras columnas. (3) Imprimir cantidad total de columnas (esperada: 64), nombres de los 4 campos calculados numericos, campos del liquid cluster, confirmacion de 0 filtros aplicados, confirmacion de creacion exitosa de la vista. (4) Retornar DataFrame final. (RF-008, RF-012, RF-026)

**Checkpoint**: La vista materializada `transacciones_enriquecidas` esta completamente implementada. Ambas vistas de plata pueden desplegarse y validarse con las consultas SQL del quickstart.md.

---

## Phase 5: HU5 — Pruebas TDD para Utilidades de Plata (Prioridad: P2, Condicional)

**Objetivo**: Cubrir con TDD cualquier utilidad nueva creada en `utilities/` durante las fases anteriores. Si no se crearon utilidades nuevas (escenario anticipado por el plan), esta fase no genera archivos nuevos.

**Prueba Independiente**: Ejecutar NbTddPlataPipeline.py desde extension Databricks para VS Code en Serverless Compute. Todas las pruebas pasan sin errores.

### Implementacion HU5

- [X] T007 [US5] Evaluar si se crearon utilidades nuevas en `src/LSDP_Laboratorio_Basico/utilities/` durante las fases anteriores. **SI se crearon**: crear `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/NbTddPlataPipeline.py` en formato notebook Databricks con pruebas TDD que validen cada utilidad nueva (al menos 1 test por funcion: comportamiento correcto + manejo de errores), ejecutables en Serverless Compute con `spark.createDataFrame()`. **SI NO se crearon** (escenario anticipado por plan.md): verificar que `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/NbTddBroncePipeline.py` del Incremento 3 sigue funcional sin regresiones y documentar que no se requiere TDD adicional para este incremento. (RF-023)

**Checkpoint**: Las utilidades de plata (si existen) tienen cobertura TDD. Las pruebas del Incremento 3 no tienen regresiones.

---

## Phase 6: Polish & Verificacion Cruzada

**Proposito**: Validacion final contra quickstart.md, verificacion de formatos de archivo y compatibilidad Serverless.

- [X] T008 Validar implementacion ejecutando los escenarios SQL de `specs/004-lsdp-plata-vistas/quickstart.md` contra ambas vistas materializadas desplegadas en el pipeline LSDP
- [X] T009 [P] Verificar que ambos archivos nuevos en `src/LSDP_Laboratorio_Basico/transformations/` (`LsdpPlataClientesSaldos.py` y `LsdpPlataTransacciones.py`) tienen formato notebook Databricks (header `# Databricks notebook source`), son 100% compatibles con Serverless Compute (sin `sparkContext`, `.cache()`, `.persist()`, operaciones RDD, `import dlt`), no configuran `spark.sql.shuffle.partitions` (RF-027), y no contienen valores hardcodeados de catalogos, esquemas ni nombres de tablas (RF-015, RF-016, RF-017)

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Setup (Phase 1)**: No aplica — sin tareas de setup
- **Foundational (Phase 2)**: No aplica — sin prerequisitos bloqueantes nuevos
- **HU1 Consolidada (Phase 3)**: Sin dependencias de fase — puede comenzar inmediatamente. Utiliza utilidades existentes del Inc. 3
- **HU2 Transaccional (Phase 4)**: Sin dependencias de fase — puede comenzar inmediatamente. Completamente independiente de HU1
- **HU5 TDD (Phase 5)**: Depende de Phase 3 + Phase 4 — evalua si se crearon utilidades nuevas durante la implementacion
- **Polish (Phase 6)**: Depende de todas las fases anteriores

### Dependencias entre Historias

- **HU1 (P1)** y **HU2 (P1)**: Completamente independientes. Archivos diferentes (`LsdpPlataClientesSaldos.py` vs `LsdpPlataTransacciones.py`), datos fuente diferentes (cmstfl+blncfl vs trxpfl), sin dependencias cruzadas. Pueden ejecutarse en paralelo.
- **HU3 (P2)** y **HU4 (P2)**: Absorbidas en HU1 y HU2. No generan trabajo adicional independiente.
- **HU5 (P2)**: Condicional. Depende del resultado de HU1 y HU2 (evalua si generaron utilidades nuevas).

### Dentro de Cada Historia

- **HU1**: T001 → T002 → T003 (secuencial, mismo archivo `LsdpPlataClientesSaldos.py`)
- **HU2**: T004 → T005 → T006 (secuencial, mismo archivo `LsdpPlataTransacciones.py`)
- No hay tareas [P] dentro de una misma historia (todas operan sobre el mismo archivo)

### Oportunidades de Paralelismo

- **Phase 3 ‖ Phase 4**: HU1 y HU2 pueden ejecutarse completamente en paralelo (archivos independientes, sin dependencias cruzadas)
- **T001 ‖ T004**: Los scaffoldings de ambos archivos pueden crearse simultaneamente
- **T002 ‖ T005**: La logica interna de ambas funciones decoradas puede implementarse simultaneamente
- **T003 ‖ T006**: Los campos calculados y observabilidad de ambos scripts pueden completarse simultaneamente
- **Phase 6**: T009 puede ejecutarse en paralelo con T008

---

## Ejemplo Paralelo: HU1 ‖ HU2

```bash
# Desarrollador A (HU1 — Vista Consolidada):
Task: "Crear scaffolding LsdpPlataClientesSaldos.py con Closure + 169 mappings + decoradores"
Task: "Implementar Dim Tipo 1 + LEFT JOIN + exclusion columnas control"
Task: "Implementar 4 campos calculados CASE + SHA2 + liquid cluster + observabilidad"

# Desarrollador B (HU2 — Vista Transaccional) — simultaneamente:
Task: "Crear scaffolding LsdpPlataTransacciones.py con Closure + 60 mappings + decoradores"
Task: "Implementar seleccion/renombrado SIN FILTROS + exclusion columnas control"
Task: "Implementar 4 campos calculados numericos + liquid cluster + observabilidad"
```

---

## Estrategia de Implementacion

### MVP Primero (Solo HU1)

1. Completar Phase 3: HU1 — Vista consolidada de clientes y saldos
2. **PARAR Y VALIDAR**: Desplegar pipeline LSDP y verificar vista en `plata.regional`
3. Deploy/demo si esta listo

### Entrega Incremental

1. Implementar HU1 → Desplegar → Validar vista consolidada (MVP!)
2. Implementar HU2 → Desplegar → Validar vista transaccional
3. Evaluar HU5 → TDD si aplica
4. Cada historia agrega valor sin romper las anteriores

### Implementacion Secuencial Optima (Agente LLM)

Para un solo agente LLM ejecutando tareas secuencialmente:

1. T001 → T002 → T003 (completar `LsdpPlataClientesSaldos.py`)
2. T004 → T005 → T006 (completar `LsdpPlataTransacciones.py`)
3. T007 (evaluar TDD)
4. T008, T009 (polish)

---

## Notas

- Las tareas [P] = archivos diferentes, sin dependencias cruzadas
- [US?] mapea la tarea a la historia de usuario para trazabilidad
- HU3 y HU4 estan absorbidas — no generan tareas ni fases independientes
- HU5 es **condicional**: el plan NO anticipa utilidades nuevas para plata
- Los diccionarios de mapeo de columnas se obtienen de `data-model.md` (70+99=169 entries para consolidada, 60 para transaccional)
- Todos los scripts usan formato notebook Databricks (`# Databricks notebook source`, `# COMMAND ----------`)
- Ambos scripts leen `catalogoPlata` y `esquemaPlata` de la tabla Parametros para el nombre completo de la vista destino (RF-017)
- La columna `FechaIngestaDatos` se usa internamente para Dimension Tipo 1 pero se descarta antes del JOIN (no se expone en plata)
- Commit despues de cada tarea o grupo logico
- Evitar: tareas vagas, conflictos en el mismo archivo, dependencias cruzadas entre HU1 y HU2
