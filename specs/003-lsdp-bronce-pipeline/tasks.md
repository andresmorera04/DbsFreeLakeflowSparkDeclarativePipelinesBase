# Tasks: Incremento 3 - LSDP Medalla de Bronce

**Input**: Documentos de diseño de `/specs/003-lsdp-bronce-pipeline/`
**Prerequisites**: plan.md (requerido), spec.md (requerido), research.md, data-model.md, contracts/nb-bronce-pipeline.md, quickstart.md

**Tests**: INCLUIDOS — El spec (HU-6, RF-023) exige pruebas TDD ejecutables en Serverless Compute.

**Organizacion**: Tareas agrupadas por historia de usuario para permitir implementacion y pruebas independientes.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos diferentes, sin dependencias)
- **[Story]**: Historia de usuario asociada (US1, US6)
- Rutas exactas de archivos en cada descripcion

## Absorcion de Historias

Las siguientes historias no generan fases independientes porque sus requisitos se implementan dentro de las mismas tareas y archivos de otras historias:

- **HU-2 (Liquid Cluster, P1)**: Absorbida en Foundational (utilidad `LsdpReordenarColumnasLiquidCluster`) y HU-1 (parametro `cluster_by` del decorador `@dp.table`).
- **HU-4 (Propiedades Delta, P2)**: Absorbida en HU-1 (parametro `table_properties` del decorador `@dp.table`).
- **HU-5 (Observabilidad, P2)**: Absorbida en Foundational (prints en utilidades) y HU-1 (prints en transformaciones).

---

## Phase 1: Setup

**Proposito**: Crear la estructura de directorios del incremento

- [X] T001 Crear directorios `src/LSDP_Laboratorio_Basico/utilities/` y `src/LSDP_Laboratorio_Basico/transformations/`, verificar que `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/` existe

---

## Phase 2: Foundational (Prerequisitos Bloqueantes)

**Proposito**: Implementar las 3 utilidades reutilizables del LSDP en formato Python puro (modulos importables, NO notebooks). Corresponde a HU-3 (Utilidades LSDP) y al componente de utilidad de HU-2 (Liquid Cluster).

**⚠️ CRITICO**: Ninguna tarea de HU-1 puede comenzar hasta que esta fase este completa. Los scripts de transformacion importan estas utilidades.

- [X] T002 [P] Implementar funcion `obtener_parametros(spark, catalogo_parametro, esquema_parametro, tabla_parametros)` que lee tabla Parametros via `spark.read.table()`, retorna `dict{Clave:Valor}`, imprime parametros leidos y lanza excepcion si la tabla no existe en `src/LSDP_Laboratorio_Basico/utilities/LsdpConexionParametros.py` (RF-004, RF-027, CE-005, CE-013)
- [X] T003 [P] Implementar funcion `construir_ruta(diccionario_parametros, ruta_relativa)` que construye ruta completa Volume (`/Volumes/<catalogoVolume>/<esquemaVolume>/<nombreVolume>/<ruta_relativa>`) o S3 (`s3://<bucketS3>/<ruta_relativa>`) segun TipoStorage (el parametro `prefijoS3` NO se utiliza en rutas S3, segun Constitution v1.1.0), imprime ruta construida y lanza ValueError si TipoStorage no es Volume ni AmazonS3 en `src/LSDP_Laboratorio_Basico/utilities/LsdpConstructorRutas.py` (RF-005, RF-027, CE-006, CE-013)
- [X] T004 [P] Implementar funcion `reordenar_columnas_liquid_cluster(df, campos_cluster)` que reordena DataFrame colocando campos_cluster como primeras columnas via `df.select(campos_cluster + columnas_restantes)`, preserva orden del resto y lanza ValueError si algun campo no existe en el DataFrame en `src/LSDP_Laboratorio_Basico/utilities/LsdpReordenarColumnasLiquidCluster.py` (RF-006, RF-027, CE-004, CE-013)

**Checkpoint**: Las 3 utilidades estan implementadas como modulos Python puro importables. Los scripts de transformacion pueden comenzar.

---

## Phase 3: HU-1 - Ingestion Incremental Declarativa de Parquets AS400 en Bronce (Prioridad: P1)

**Objetivo**: Crear 3 streaming tables de bronce con AutoLoader, schema evolution activo, liquid cluster, propiedades Delta optimizadas y observabilidad completa. Cada script es autocontenido (patron Closure independiente).

**Prueba Independiente**: Desplegar pipeline LSDP desde Databricks UI apuntando a parquets del Incremento 2. Verificar: 3 streaming tables en `bronce.lab1` con conteos correctos, `FechaIngestaDatos` presente, campos del liquid cluster primeros en el schema, 5 propiedades Delta activas, y cero duplicados en re-ejecucion.

**Incorpora**: HU-2 (`cluster_by` del decorador + reordenamiento), HU-4 (`table_properties` Delta), HU-5 (prints de observabilidad).

### Implementacion HU-1

- [X] T005 [P] [US1] Implementar streaming table `cmstfl` en `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceCmstfl.py` — formato notebook Databricks. Patron Closure autocontenido: lee 5 parametros de pipeline via `spark.conf.get()` (`catalogoParametro`, `esquemaParametro`, `tablaParametros`, `rutaCompletaMaestroCliente`, `rutaSchemaLocationCmstfl`), invoca `obtener_parametros()` y `construir_ruta()` a nivel de modulo para ruta de parquet y ruta de schema location. Funcion pura `transformar_cmstfl(df)`: agrega `FechaIngestaDatos` con `F.current_timestamp()` e invoca `reordenar_columnas_liquid_cluster(df, ["FechaIngestaDatos", "CUSTID"])`. Decorador `@dp.table(name="cmstfl", table_properties={"delta.enableChangeDataFeed":"true", "delta.autoOptimize.autoCompact":"true", "delta.autoOptimize.optimizeWrite":"true", "delta.deletedFileRetentionDuration":"interval 30 days", "delta.logRetentionDuration":"interval 60 days"}, cluster_by=["FechaIngestaDatos","CUSTID"])`. AutoLoader: `spark.readStream.format("cloudFiles")` con `cloudFiles.format=parquet`, `cloudFiles.schemaEvolutionMode=addNewColumns`, `cloudFiles.schemaLocation=<ruta_absoluta>`. Prints de observabilidad: nombre tabla, parametros, ruta parquet, campos cluster, confirmacion reordenamiento (RF-001 a RF-003, RF-007, RF-010 a RF-014, RF-016 a RF-022, RF-025, RF-026)
- [X] T006 [P] [US1] Implementar streaming table `trxpfl` en `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceTrxpfl.py` — formato notebook Databricks. Mismo patron Closure que T005 pero con parametros `rutaCompletaTransaccional` y `rutaSchemaLocationTrxpfl`. Funcion pura `transformar_trxpfl(df)`: agrega `FechaIngestaDatos` con `F.current_timestamp()` e invoca `reordenar_columnas_liquid_cluster(df, ["TRXDT", "CUSTID", "TRXTYP"])` — notar que `FechaIngestaDatos` queda en posicion 4 (despues de los 3 campos del cluster). Decorador `@dp.table(name="trxpfl", cluster_by=["TRXDT","CUSTID","TRXTYP"])` con mismas 5 table_properties. AutoLoader con `cloudFiles.schemaLocation` apuntando a ruta de schema location trxpfl (RF-001 a RF-003, RF-008, RF-010 a RF-014, RF-016 a RF-022, RF-025, RF-026)
- [X] T007 [P] [US1] Implementar streaming table `blncfl` en `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceBlncfl.py` — formato notebook Databricks. Mismo patron Closure que T005 pero con parametros `rutaCompletaSaldoCliente` y `rutaSchemaLocationBlncfl`. Funcion pura `transformar_blncfl(df)`: agrega `FechaIngestaDatos` con `F.current_timestamp()` e invoca `reordenar_columnas_liquid_cluster(df, ["FechaIngestaDatos", "CUSTID"])` — identico patron que cmstfl. Decorador `@dp.table(name="blncfl", cluster_by=["FechaIngestaDatos","CUSTID"])` con mismas 5 table_properties (RF-001 a RF-003, RF-009, RF-010 a RF-014, RF-016 a RF-022, RF-025, RF-026)

**Checkpoint**: Las 3 streaming tables estan implementadas. El pipeline puede desplegarse y validarse contra parquets del Incremento 2.

---

## Phase 4: HU-6 - Pruebas TDD para el Pipeline de Bronce (Prioridad: P2)

**Objetivo**: Crear archivo de pruebas TDD que valida EXCLUSIVAMENTE las 3 utilidades de `utilities/` (Python puro importable). Los notebooks de `transformations/` NO se incluyen en el TDD porque ejecutan codigo a nivel de modulo (patron Closure) que requiere un pipeline LSDP desplegado.

**Prueba Independiente**: Ejecutar `NbTddBroncePipeline.py` desde extension Databricks para VS Code con "Run on Serverless Compute". Todas las pruebas deben pasar sin errores.

### Implementacion HU-6

- [X] T008 [US6] Implementar pruebas TDD para las 3 utilidades de `utilities/` (Python puro) en `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/NbTddBroncePipeline.py` — formato notebook Databricks. Pruebas requeridas (9 tests): (1) `LsdpConexionParametros`: diccionario retornado contiene 11 claves esperadas, valores son string; (2) `LsdpConstructorRutas`: formato correcto para TipoStorage=Volume (`/Volumes/catalogo/esquema/volume/ruta`), formato correcto para TipoStorage=AmazonS3 (`s3://bucket/ruta_relativa`), lanza excepcion para TipoStorage invalido; (3) `LsdpReordenarColumnasLiquidCluster`: campos del cluster quedan primeros en schema, total de columnas no cambia, orden del resto de columnas preservado, lanza excepcion si campo del cluster no existe. **Excluidos del TDD**: los notebooks de `transformations/` (`transformar_cmstfl`, `transformar_trxpfl`, `transformar_blncfl`) porque ejecutan codigo a nivel de modulo (patron Closure: `spark.conf.get`, `obtener_parametros()`) que requiere pipeline LSDP desplegado y no son importables fuera de ese contexto. **Exclusiones adicionales**: NO validar cantidad total de columnas (incompatible con schema evolution), NO validar uso/ausencia de sparkContext (responsabilidad del entorno Serverless) (RF-023, RF-024, CE-008)

**Checkpoint**: Todas las pruebas TDD pasan en Serverless Compute. El pipeline esta listo para despliegue.

---

## Phase 5: Polish & Verificacion Cruzada

**Proposito**: Validacion final contra quickstart.md y verificacion de formatos de archivo

- [X] T009 Validar implementacion ejecutando los escenarios de `specs/003-lsdp-bronce-pipeline/quickstart.md`
- [X] T010 [P] Verificar que los 3 archivos en `src/LSDP_Laboratorio_Basico/utilities/` son Python puro sin marcadores de notebook (`# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC`) — CE-013
- [X] T011 [P] Verificar que los 4 archivos en `src/LSDP_Laboratorio_Basico/transformations/` y `explorations/LSDP_Laboratorio_Basico/` tienen formato notebook Databricks con header `# Databricks notebook source` — RF-019

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Setup (Phase 1)**: Sin dependencias — puede comenzar inmediatamente
- **Foundational (Phase 2)**: Depende de Setup (Phase 1) — **BLOQUEA Phase 3**
- **HU-1 Ingestion (Phase 3)**: Depende de Foundational (Phase 2) — requiere las 3 utilidades implementadas
- **HU-6 TDD (Phase 4)**: Depende de Phase 2 — requiere las 3 utilidades de `utilities/` implementadas. NO depende de Phase 3 (los notebooks de transformations/ no se incluyen en TDD)
- **Polish (Phase 5)**: Depende de todas las fases anteriores

### Dependencias entre Historias

- **HU-3 (Foundational, P1)**: Prerequisito de HU-1 y HU-6. Puede comenzar inmediatamente despues de Setup.
- **HU-1 (P1)**: Depende de HU-3 completada. Los 3 scripts de transformacion son independientes entre si.
- **HU-6 (P2)**: Depende de HU-3 completada. El TDD valida EXCLUSIVAMENTE las utilidades de `utilities/` (Python puro). Los notebooks de `transformations/` no son testeables via TDD.

### Dentro de Cada Fase

- Las 3 utilidades (T002, T003, T004) son independientes entre si → ejecutar en paralelo
- Las 3 transformaciones (T005, T006, T007) son independientes entre si → ejecutar en paralelo
- El TDD (T008) es un unico archivo que consolida todas las pruebas

### Oportunidades de Paralelismo

- **Phase 2**: T002 ‖ T003 ‖ T004 (3 utilidades en archivos diferentes)
- **Phase 3**: T005 ‖ T006 ‖ T007 (3 transformaciones en archivos diferentes)
- **Phase 5**: T010 ‖ T011 (verificaciones independientes)

---

## Ejemplo Paralelo: Foundational (HU-3)

```bash
# Ejecutar en paralelo las 3 utilidades:
Task: "Implementar obtener_parametros() en LsdpConexionParametros.py"
Task: "Implementar construir_ruta() en LsdpConstructorRutas.py"
Task: "Implementar reordenar_columnas_liquid_cluster() en LsdpReordenarColumnasLiquidCluster.py"
```

## Ejemplo Paralelo: HU-1 (Ingestion)

```bash
# Ejecutar en paralelo las 3 transformaciones (despues de Foundational completada):
Task: "Implementar streaming table cmstfl en LsdpBronceCmstfl.py"
Task: "Implementar streaming table trxpfl en LsdpBronceTrxpfl.py"
Task: "Implementar streaming table blncfl en LsdpBronceBlncfl.py"
```

---

## Estrategia de Implementacion

### MVP Primero (Foundational + HU-1)

1. Completar Phase 1: Setup (directorios)
2. Completar Phase 2: Foundational — 3 utilidades Python puro
3. Completar Phase 3: HU-1 — 3 streaming tables de bronce
4. **PARAR Y VALIDAR**: Desplegar pipeline LSDP desde Databricks UI y verificar ingestion
5. Deploy/demo si esta listo

### Entrega Incremental

1. Setup → Estructura de directorios creada
2. Foundational (HU-3) → Utilidades listas → Validar independientemente con datos de prueba
3. HU-1 → Transformaciones listas → Desplegar pipeline LSDP → Validar ingestion contra parquets del Incremento 2
4. HU-6 → TDD completo → Ejecutar en Serverless Compute → Confirmar todas las pruebas pasan
5. Polish → Verificacion final con quickstart.md → Listo para produccion

---

## Notas

- [P] = archivos diferentes, sin dependencias → ejecutable en paralelo
- [US?] = historia de usuario asociada para trazabilidad
- HU-2, HU-4, HU-5 absorbidas en Foundational y HU-1 (ver seccion "Absorcion de Historias")
- Archivos `utilities/` → Python puro, modulos importables (RF-027, CE-013)
- Archivos `transformations/` y `explorations/` → notebooks Databricks (RF-019)
- Todo el codigo → 100% compatible Serverless Compute (RF-014): sin sparkContext, sin RDD, sin cache/persist
- Cero valores hardcodeados → todo parametrizado via pipeline parameters y tabla Parametros (RF-017)
- Cero `import dlt` → solo `pyspark.pipelines` (`from pyspark import pipelines as dp`) con `@dp.table` (RF-016)
- Variables en `snake_case`, archivos en `PascalCase` con prefijo `Lsdp` (RF-020, RF-021)
- Codigo y comentarios en idioma espanol (RF-020)
- Commit despues de cada tarea o grupo logico
