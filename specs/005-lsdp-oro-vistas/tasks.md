# Tasks: Incremento 5 - LSDP Medalla de Oro - Vistas Materializadas

**Input**: Documentos de diseño de `/specs/005-lsdp-oro-vistas/`
**Prerequisites**: plan.md (requerido), spec.md (requerido), research.md, data-model.md, contracts/nb-oro-vistas.md, quickstart.md

**Tests**: REQUERIDAS — RF-017 y RF-023 exigen TDD para la nueva utilidad `LsdpInsertarTiposTransaccion`. Las pruebas TDD existentes de incrementos anteriores deben pasar sin regresiones.

**Organizacion**: Tareas agrupadas por historia de usuario para permitir implementacion y pruebas independientes.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos diferentes, sin dependencias)
- **[Story]**: Historia de usuario asociada (US1, US2, US5)
- Rutas exactas de archivos en cada descripcion

## Absorcion de Historias

Las siguientes historias no generan fases independientes porque sus requisitos se implementan dentro de las mismas tareas y archivos de otras historias:

- **HU3 (Propiedades Delta y Liquid Cluster, P2)**: Absorbida en HU1 (parametros `table_properties` y `cluster_by` del decorador `@dp.materialized_view` en T002) y HU2 (mismos parametros en T004). Las 5 propiedades Delta y las columnas del liquid cluster se configuran directamente en los decoradores de cada vista.
- **HU4 (Observabilidad Completa, P2)**: Absorbida en HU1 via T002 (prints de inicializacion a nivel de modulo: parametros del pipeline, valores de tabla Parametros, catalogos/esquemas destino, nombres de vistas, metricas, columnas y liquid cluster). Toda la observabilidad se concentra en el codigo de modulo de T002 (nivel de closure). LSDP propaga errores nativamente.

---

## Phase 1: Setup

**Proposito**: No se requiere. Los directorios `transformations/`, `utilities/` y `explorations/LSDP_Laboratorio_Basico/` ya existen desde incrementos anteriores. No se crean directorios nuevos ni dependencias adicionales.

---

## Phase 2: Foundational (Prerequisitos Bloqueantes)

**Proposito**: Crear la utilidad `LsdpInsertarTiposTransaccion` que inserta de forma idempotente la clave `TiposTransaccionesLabBase` en la tabla Parametros. Esta utilidad DEBE existir antes de ejecutar el pipeline LSDP con la medalla de oro, ya que el script de oro lee esta clave para obtener los tipos de transaccion.

**⚠️ CRITICO**: La ejecucion del pipeline de oro no puede validarse hasta que esta fase este completa.

- [X] T001 Crear archivo `src/LSDP_Laboratorio_Basico/utilities/LsdpInsertarTiposTransaccion.py` — formato notebook Databricks (`# Databricks notebook source` + `# COMMAND ----------` + `# MAGIC %md` para titulo). Implementar funcion publica `insertar_tipos_transaccion(spark, catalogo: str, esquema: str, tabla_parametros: str) -> None` que: (1) Construye nombre completo de 3 partes de la tabla Parametros: `f"{catalogo}.{esquema}.{tabla_parametros}"`. (2) Consulta si la clave `TiposTransaccionesLabBase` ya existe: `spark.sql(f"SELECT Valor FROM {nombre_completo_tabla} WHERE Clave = 'TiposTransaccionesLabBase'")`. (3) Si el resultado esta vacio (`.count() == 0`): ejecuta `spark.sql(f"INSERT INTO {nombre_completo_tabla} VALUES ('TiposTransaccionesLabBase', 'DATM,CATM,PGSL')")` e imprime mensaje confirmando la insercion. (4) Si ya existe una fila: imprime mensaje indicando que la clave ya existe con su valor actual y no realiza ninguna operacion (idempotente). (5) Imprime siempre el valor final de la clave para observabilidad. Parametros inyectados via SQL con f-string (la tabla Parametros es interna del pipeline, no expuesta a usuarios — riesgo de inyeccion SQL: ninguno). 100% compatible con Serverless Compute: sin `sparkContext`, sin RDD, sin `.cache()`, sin `.persist()`. (RF-017, RF-014)

**Checkpoint**: La utilidad `LsdpInsertarTiposTransaccion` esta lista para ser invocada desde el notebook LSDP y para ser cubierta por TDD.

---

## Phase 3: HU1 — Vista Materializada de Comportamiento ATM por Cliente (Prioridad: P1) 🎯 MVP

**Objetivo**: Crear la vista materializada `comportamiento_atm_cliente` en `oro.regional` que agrega las transacciones de `transacciones_enriquecidas` (plata) por `identificador_cliente` usando agregacion condicional para calcular 5 metricas ATM: cantidad depositos ATM (DATM), cantidad retiros ATM (CATM), promedio montos depositos ATM, promedio montos retiros ATM y total pagos al saldo (PGSL). 6 columnas totales. Sin filtros previos sobre DataFrame. Tipos de transaccion leidos de tabla Parametros.

**Prueba Independiente**: Desplegar pipeline LSDP, verificar que `oro.regional.comportamiento_atm_cliente` existe con 6 columnas, metricas correctas, sin NULLs (todos 0 o 0.0 via F.coalesce), tipos de transaccion leidos de Parametros, propiedades Delta activas, liquid cluster sobre `identificador_cliente`.

**Incorpora**: HU3 (`table_properties` + `cluster_by` del decorador), HU4 (prints de observabilidad).

### Implementacion HU1

- [X] T002 [P] [US1] Crear archivo `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py` — formato notebook Databricks (`# Databricks notebook source` + `# COMMAND ----------` + `# MAGIC %md` para titulo y secciones). Este archivo contiene AMBAS vistas materializadas de oro (HU1 y HU2) en el mismo contexto de Closure. Implementar el scaffolding completo del modulo: (1) **Imports**: `from pyspark import pipelines as dp`, `from pyspark.sql import functions as F`, `from pyspark.sql import DataFrame`, `from utilities.LsdpConexionParametros import obtener_parametros`. Sin `import dlt`. Sin `sys.path.insert()`. (2) **Parametros del pipeline** a nivel de modulo via `spark.conf.get()`: `catalogo_parametro = spark.conf.get("pipelines.parameters.catalogoParametro")`, `esquema_parametro = spark.conf.get("pipelines.parameters.esquemaParametro")`, `tabla_parametros = spark.conf.get("pipelines.parameters.tablaParametros")`. (3) **Lectura de tabla Parametros**: `parametros = obtener_parametros(spark, catalogo_parametro, esquema_parametro, tabla_parametros)`. (4) **Extraccion de valores** del diccionario: `catalogo_oro = parametros.get("catalogoOro", "oro")`, `esquema_oro = parametros.get("esquemaOro", "regional")`, `catalogo_plata = parametros.get("catalogoPlata", "plata")`, `esquema_plata = parametros.get("esquemaPlata", "regional")`. (5) **Tipos de transaccion desde Parametros**: `tipos_transaccion_str = parametros.get("TiposTransaccionesLabBase", "DATM,CATM,PGSL")`, `tipos_transaccion = tipos_transaccion_str.split(",")`, `TIPO_DEPOSITO_ATM = tipos_transaccion[0]`, `TIPO_RETIRO_ATM = tipos_transaccion[1]`, `TIPO_PAGO_SALDO = tipos_transaccion[2]`. (6) **Diccionario de propiedades Delta**: `propiedades_delta_oro = {"delta.enableChangeDataFeed": "true", "delta.autoOptimize.autoCompact": "true", "delta.autoOptimize.optimizeWrite": "true", "delta.deletedFileRetentionDuration": "interval 30 days", "delta.logRetentionDuration": "interval 60 days"}`. (7) **Prints de observabilidad** al inicio del modulo: parametros del pipeline recibidos (`catalogoParametro`, `esquemaParametro`, `tablaParametros`), valores de tabla Parametros extraidos (`catalogoOro`, `esquemaOro`, `catalogoPlata`, `esquemaPlata`), tipos de transaccion leidos de la clave `TiposTransaccionesLabBase` (valores DATM, CATM, PGSL), nombres de las 2 vistas materializadas a crear (`comportamiento_atm_cliente`, `resumen_integral_cliente`), campos del liquid cluster de cada vista, nombres de las 5 metricas calculadas (`cantidad_depositos_atm`, `cantidad_retiros_atm`, `promedio_monto_depositos_atm`, `promedio_monto_retiros_atm`, `total_pagos_saldo_cliente`), nombres de las 22 columnas del resumen integral. (8) **Declaracion por defecto**: Decorador `@dp.materialized_view(name="comportamiento_atm_cliente", catalog=catalogo_oro, schema=esquema_oro, table_properties=propiedades_delta_oro, cluster_by=["identificador_cliente"])` con funcion placeholder `def comportamiento_atm_cliente() -> DataFrame: pass`. (RF-001, RF-008, RF-009, RF-012, RF-013, RF-014, RF-015, RF-016, RF-017, RF-018, RF-019, RF-020, RF-021, RF-022, RF-025)

- [X] T003 [US1] Implementar logica de la funcion decorada `comportamiento_atm_cliente()` dentro de `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py` — (1) **Lectura**: `df_transacciones = spark.read.table(f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")` — nombre completo de 3 partes, cross-catalog con plata. (2) **Agregacion condicional** sin filtros previos sobre el DataFrame (RF-027): `df_transacciones.groupBy("identificador_cliente").agg(F.count(F.when(F.col("tipo_transaccion") == TIPO_DEPOSITO_ATM, F.col("monto_principal"))).alias("cantidad_depositos_atm"), F.count(F.when(F.col("tipo_transaccion") == TIPO_RETIRO_ATM, F.col("monto_principal"))).alias("cantidad_retiros_atm"), F.avg(F.when(F.col("tipo_transaccion") == TIPO_DEPOSITO_ATM, F.col("monto_principal"))).alias("promedio_monto_depositos_atm"), F.avg(F.when(F.col("tipo_transaccion") == TIPO_RETIRO_ATM, F.col("monto_principal"))).alias("promedio_monto_retiros_atm"), F.sum(F.when(F.col("tipo_transaccion") == TIPO_PAGO_SALDO, F.col("monto_principal"))).alias("total_pagos_saldo_cliente"))`. (3) **F.coalesce** para reemplazar NULLs con valores por defecto: `.withColumn("cantidad_depositos_atm", F.coalesce(F.col("cantidad_depositos_atm"), F.lit(0))).withColumn("cantidad_retiros_atm", F.coalesce(F.col("cantidad_retiros_atm"), F.lit(0))).withColumn("promedio_monto_depositos_atm", F.coalesce(F.col("promedio_monto_depositos_atm"), F.lit(0.0))).withColumn("promedio_monto_retiros_atm", F.coalesce(F.col("promedio_monto_retiros_atm"), F.lit(0.0))).withColumn("total_pagos_saldo_cliente", F.coalesce(F.col("total_pagos_saldo_cliente"), F.lit(0.0)))` — 0 para conteos (LongType), 0.0 para promedios y sumas (DoubleType). (4) **Retorno**: `return df_resultado`. Sin filtros previos sobre el DataFrame de entrada. Sin `.cache()`, `.persist()`, sparkContext, RDD. (RF-002, RF-003, RF-004, RF-014, RF-024, RF-027)

**Checkpoint**: La vista materializada `comportamiento_atm_cliente` esta completamente implementada dentro de `LsdpOroClientes.py`. El pipeline puede desplegarse para validar esta vista con `SELECT COUNT(*) FROM oro.regional.comportamiento_atm_cliente`.

---

## Phase 4: HU2 — Vista Materializada de Resumen Integral del Cliente (Prioridad: P1)

**Objetivo**: Crear la vista materializada `resumen_integral_cliente` en `oro.regional` que combina datos dimensionales de `clientes_saldos_consolidados` (plata) con metricas ATM de `comportamiento_atm_cliente` (oro) mediante INNER JOIN por `identificador_cliente`. 22 columnas (17 plata + 5 oro). Solo incluye clientes presentes en ambas fuentes. Dimension Tipo 1 heredada de plata.

**Prueba Independiente**: Desplegar pipeline LSDP completo, verificar que `oro.regional.resumen_integral_cliente` existe con 22 columnas, INNER JOIN correcto (todos los clientes del resumen tienen contraparte en `comportamiento_atm_cliente`), propiedades Delta activas, liquid cluster sobre `huella_identificacion_cliente` e `identificador_cliente`.

**Incorpora**: HU3 (`table_properties` + `cluster_by` del decorador), HU4 (prints de observabilidad del resumen).

**Depende de**: HU1 (requiere que `comportamiento_atm_cliente` exista en el pipeline — LSDP resuelve dependencias automaticamente).

### Implementacion HU2

- [X] T004 [US2] Implementar vista materializada `resumen_integral_cliente` dentro de `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py` — (1) **Decorador**: `@dp.materialized_view(name="resumen_integral_cliente", catalog=catalogo_oro, schema=esquema_oro, table_properties=propiedades_delta_oro, cluster_by=["huella_identificacion_cliente", "identificador_cliente"])`. (2) **Funcion**: `def resumen_integral_cliente() -> DataFrame`. (3) **Lectura de plata**: `df_plata = spark.read.table(f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados")` — nombre completo de 3 partes, cross-catalog. (4) **Lectura de oro**: `df_oro = spark.read.table(f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente")` — nombre completo de 3 partes, sin excepciones (RF-025). (5) **INNER JOIN**: `df_resultado = df_plata.join(df_oro, on="identificador_cliente", how="inner")` — solo los clientes presentes en ambas fuentes se incluyen. El parametro `on="identificador_cliente"` deduplica automaticamente la columna de JOIN. (6) **Seleccion de 22 columnas**: `.select("huella_identificacion_cliente", "identificador_cliente", "nombre_cliente", "apellido_cliente", "nacionalidad_cliente", "pais_residencia", "ciudad_residencia", "ocupacion_cliente", "nivel_educativo", "clasificacion_riesgo_cliente", "categoria_saldo_disponible", "perfil_actividad_bancaria", "limite_credito", "saldo_disponible", "fecha_apertura_cuenta", "estado_cuenta", "tipo_cuenta", F.coalesce(F.col("cantidad_depositos_atm"), F.lit(0)).alias("cantidad_depositos_atm"), F.coalesce(F.col("cantidad_retiros_atm"), F.lit(0)).alias("cantidad_retiros_atm"), F.coalesce(F.col("promedio_monto_depositos_atm"), F.lit(0.0)).alias("promedio_monto_depositos_atm"), F.coalesce(F.col("promedio_monto_retiros_atm"), F.lit(0.0)).alias("promedio_monto_retiros_atm"), F.coalesce(F.col("total_pagos_saldo_cliente"), F.lit(0.0)).alias("total_pagos_saldo_cliente"))` — 17 columnas de plata (identificacion 4 + sociodemograficos 5 + financieros 6 + estado 2) + 5 metricas ATM de oro con `F.coalesce` defensivo (RF-006). Nota: `F.coalesce` en el select es redundante con el aplicado en `comportamiento_atm_cliente` pero se mantiene como medida defensiva segun RF-006. (7) **Retorno**: `return df_resultado`. Sin `.cache()`, `.persist()`, sparkContext, RDD. (RF-005, RF-006, RF-007, RF-010, RF-014, RF-024, RF-025, RF-028)

**Checkpoint**: Ambas vistas materializadas de oro estan completamente implementadas en `LsdpOroClientes.py`. El pipeline puede desplegarse y validarse con las consultas SQL del quickstart.md.

---

## Phase 5: HU5 — Pruebas TDD para la Utilidad de Oro (Prioridad: P2)

**Objetivo**: Cubrir con TDD la nueva utilidad `LsdpInsertarTiposTransaccion` creada en Phase 2. Las pruebas validan el comportamiento idempotente de la funcion: insercion cuando la clave no existe y no-operacion cuando ya existe.

**Prueba Independiente**: Ejecutar `NbTddOroPipeline.py` desde la extension Databricks para VS Code en Serverless Compute. Todas las pruebas pasan sin errores.

### Implementacion HU5

- [X] T005 [US5] Crear archivo `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/NbTddOroPipeline.py` — formato notebook Databricks (`# Databricks notebook source` + `# COMMAND ----------` + `# MAGIC %md`). Implementar pruebas TDD para `LsdpInsertarTiposTransaccion` ejecutables en Serverless Compute con `spark.sql()` y `spark.createDataFrame()`. (1) **Setup compartido**: definir catalogo, esquema y nombre de tabla de prueba (usar el mismo esquema de pruebas del TDD existente o crear tabla temporal). (2) **Prueba 1 — Insercion cuando la clave no existe**: Crear tabla Parametros temporal con columnas `Clave STRING, Valor STRING`. Verificar que la clave `TiposTransaccionesLabBase` no existe. Invocar `insertar_tipos_transaccion(spark, catalogo, esquema, tabla)`. Verificar que la clave ahora existe con valor `DATM,CATM,PGSL`. Assert con mensaje descriptivo. (3) **Prueba 2 — Idempotencia cuando la clave ya existe**: Con la clave ya insertada por Prueba 1, invocar `insertar_tipos_transaccion(spark, catalogo, esquema, tabla)` de nuevo. Verificar que el valor no cambio y sigue siendo `DATM,CATM,PGSL`. Verificar que no hay filas duplicadas (solo 1 fila con esa clave). Assert con mensaje descriptivo. (4) **Cleanup**: Eliminar tabla temporal de prueba con `spark.sql(f"DROP TABLE IF EXISTS ...")`. (5) **Import**: `from utilities.LsdpInsertarTiposTransaccion import insertar_tipos_transaccion`. (6) Marcar pruebas con prints claros de PASSED/FAILED. (RF-017, RF-023)

- [X] T006 [US5] Verificar no-regresion ejecutando `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/NbTddBroncePipeline.py` desde la extension Databricks para VS Code en Serverless Compute. Confirmar que todas las pruebas existentes de incrementos anteriores (utilidades `LsdpConexionParametros`, `LsdpConstructorRutas`, `LsdpReordenarColumnasLiquidCluster`) siguen pasando sin errores. Documentar resultado. (RF-023)

**Checkpoint**: La utilidad `LsdpInsertarTiposTransaccion` tiene cobertura TDD completa. Las pruebas existentes no presentan regresiones.

---

## Phase 6: Polish & Verificacion Cruzada

**Proposito**: Validacion final contra quickstart.md, verificacion de formatos de archivo y compatibilidad Serverless.

- [X] T007 Validar implementacion ejecutando los escenarios SQL de `specs/005-lsdp-oro-vistas/quickstart.md` contra ambas vistas materializadas desplegadas en el pipeline LSDP: (1) `SELECT COUNT(*) FROM oro.regional.comportamiento_atm_cliente` — verificar cantidad de clientes. (2) `SELECT * FROM oro.regional.comportamiento_atm_cliente WHERE cantidad_depositos_atm IS NULL` — esperado: 0 filas. (3) `SELECT COUNT(*) FROM oro.regional.resumen_integral_cliente` — verificar que solo incluye clientes con actividad transaccional. (4) `DESCRIBE oro.regional.resumen_integral_cliente` — verificar 22 columnas. (5) Verificar INNER JOIN: consulta LEFT JOIN de resumen contra comportamiento donde contraparte es NULL — esperado: 0 filas.
- [X] T008 [P] Verificar que los archivos nuevos en `src/LSDP_Laboratorio_Basico/` (`transformations/LsdpOroClientes.py` y `utilities/LsdpInsertarTiposTransaccion.py`) tienen formato notebook Databricks (header `# Databricks notebook source`), son 100% compatibles con Serverless Compute (sin `sparkContext`, `.cache()`, `.persist()`, operaciones RDD, `import dlt`), no configuran `spark.sql.shuffle.partitions` (RF-026), no contienen valores hardcodeados de catalogos, esquemas ni nombres de tablas (RF-016), las columnas estan en espanol snake_case (RF-011, RF-020, RF-021), y no aplican filtros previos sobre el DataFrame en `comportamiento_atm_cliente` (RF-027)

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Setup (Phase 1)**: No aplica — sin tareas de setup
- **Foundational (Phase 2)**: Sin dependencias de fase — puede comenzar inmediatamente. Crea la utilidad que BLOQUEA la validacion del pipeline
- **HU1 (Phase 3)**: Puede comenzar en paralelo con Phase 2 (la utilidad no se importa desde el script de oro). Sin embargo, la VALIDACION del pipeline requiere Phase 2 completa
- **HU2 (Phase 4)**: Depende de Phase 3 (la funcion `resumen_integral_cliente` se implementa en el mismo archivo `LsdpOroClientes.py` despues de `comportamiento_atm_cliente`). LSDP resuelve la dependencia de datos automaticamente
- **HU5 TDD (Phase 5)**: Depende de Phase 2 (la utilidad debe existir para testearla)
- **Polish (Phase 6)**: Depende de todas las fases anteriores. Requiere pipeline desplegado

### Dependencias entre Historias

- **HU1 (P1)** y **HU2 (P1)**: Secuenciales — mismo archivo `LsdpOroClientes.py`. HU2 depende de que `comportamiento_atm_cliente` exista en el pipeline para la lectura de datos. Sin embargo, como LSDP resuelve dependencias automaticamente, el orden de declaracion en el archivo es suficiente.
- **HU3 (P2)** y **HU4 (P2)**: Absorbidas en HU1 y HU2. No generan trabajo adicional independiente.
- **HU5 (P2)**: Depende de Phase 2 (utilidad creada). Independiente de HU1 y HU2.

### Dentro de Cada Historia

- **HU1**: T002 → T003 (secuencial, mismo archivo `LsdpOroClientes.py`)
- **HU2**: T004 (secuencial con HU1, mismo archivo `LsdpOroClientes.py`)
- **HU5**: T005 → T006 (secuencial, T006 es verificacion de no-regresion)
- No hay tareas [P] dentro de una misma historia que opera sobre el mismo archivo

### Oportunidades de Paralelismo

- **T001 ‖ T002**: La utilidad (`LsdpInsertarTiposTransaccion.py`) y el scaffolding del script de oro (`LsdpOroClientes.py`) se crean en archivos completamente independientes
- **T005 ‖ T007**: El TDD y la validacion SQL pueden ejecutarse en paralelo si el pipeline ya esta desplegado
- **Phase 6**: T008 puede ejecutarse en paralelo con T007

---

## Ejemplo Paralelo: Phase 2 ‖ Phase 3

```bash
# Desarrollador A (Phase 2 — Utilidad):
Task: "Crear LsdpInsertarTiposTransaccion.py con funcion idempotente"

# Desarrollador B (Phase 3 — Script Oro) — simultaneamente:
Task: "Crear LsdpOroClientes.py scaffold con Closure + parametros + observabilidad"
Task: "Implementar comportamiento_atm_cliente con groupBy + 5 agg condicionales"

# Despues de ambos → secuencial:
Task: "Implementar resumen_integral_cliente con INNER JOIN + 22 columnas"
Task: "Crear NbTddOroPipeline.py con pruebas para la utilidad"
```

---

## Estrategia de Implementacion

### MVP Primero (Solo HU1)

1. Completar Phase 2: Foundational (utilidad `LsdpInsertarTiposTransaccion`)
2. Completar Phase 3: HU1 — Vista `comportamiento_atm_cliente`
3. **PARAR Y VALIDAR**: Desplegar pipeline LSDP y verificar vista en `oro.regional`
4. Deploy/demo si esta listo

### Entrega Incremental

1. Implementar utilidad + HU1 → Desplegar → Validar vista de agregacion ATM (MVP!)
2. Implementar HU2 → Desplegar → Validar resumen integral con INNER JOIN
3. Implementar HU5 → TDD para utilidad
4. Cada historia agrega valor sin romper las anteriores

### Implementacion Secuencial Optima (Agente LLM)

Para un solo agente LLM ejecutando tareas secuencialmente:

1. T001 (crear utilidad `LsdpInsertarTiposTransaccion.py`)
2. T002 → T003 (crear scaffolding + implementar `comportamiento_atm_cliente` en `LsdpOroClientes.py`)
3. T004 (implementar `resumen_integral_cliente` en el mismo archivo)
4. T005 → T006 (TDD + no-regresion)
5. T007, T008 (polish)

---

## Notas

- Las tareas [P] = archivos diferentes, sin dependencias cruzadas
- [US?] mapea la tarea a la historia de usuario para trazabilidad
- HU3 y HU4 estan absorbidas — no generan tareas ni fases independientes
- HU5 es **obligatoria**: RF-017 y RF-023 exigen TDD para la nueva utilidad `LsdpInsertarTiposTransaccion`
- Ambas vistas materializadas residen en un solo archivo `LsdpOroClientes.py` (decision aprobada D7 del research)
- Los tipos de transaccion se leen de la tabla Parametros con clave `TiposTransaccionesLabBase` (decision modificada D6 del research)
- INNER JOIN entre plata y oro (decision modificada D3 del research) — solo clientes con actividad transaccional
- Todas las lecturas de tablas/vistas usan nombre completo de 3 partes sin excepcion (decision modificada D3 del research)
- Todos los scripts usan formato notebook Databricks (`# Databricks notebook source`, `# COMMAND ----------`)
- La utilidad se invoca desde el notebook que orquesta el pipeline LSDP, NO desde el script de transformacion
- Commit despues de cada tarea o grupo logico
- Evitar: valores hardcodeados, filtros previos sobre DataFrame en agregacion, `import dlt`, sparkContext, RDD
