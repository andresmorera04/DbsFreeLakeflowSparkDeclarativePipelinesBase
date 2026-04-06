# Tasks: Incremento 6 - Documentacion, README y Sustitucion de Esquemas

**Entrada**: Documentos de diseno de `/specs/006-docs-readme-esquema/`
**Prerrequisitos**: plan.md (requerido), spec.md (requerido), research.md, data-model.md, contracts/

**Tests**: No aplican para este incremento. Las entregas son exclusivamente documentos markdown y modificaciones de valores de configuracion. La verificacion se realiza mediante busqueda exhaustiva de "regional" post-sustitucion y revision de estructura de documentos.

**Organizacion**: Tareas agrupadas por historia de usuario para permitir implementacion y verificacion independiente de cada historia.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos distintos, sin dependencias)
- **[Story]**: Historia de usuario a la que pertenece (US1, US2, US3, US4, US5)
- Rutas de archivo exactas incluidas en cada descripcion

## Convenciones de Rutas

- **Raiz del repositorio**: `DbsFreeLakeflowSparkDeclarativePipelinesBase/`
- **Documentacion nueva**: `docs/`, `demo/`
- **Codigo a modificar**: `src/`, `conf/`
- **Especificaciones a modificar**: `specs/001-*` a `specs/005-*`
- **EXCLUIDOS de modificacion**: `SYSTEM.md`, `.specify/memory/constitution.md`

### Convenciones de Terminologia

- **ModeladoDatos.md**: nombre del archivo/documento
- **Diccionario de datos**: contenido principal del documento (tablas con campos, tipos y descripciones)

---

## Fase 1: Setup (Infraestructura Compartida)

**Proposito**: Creacion de directorios y estructura base para documentacion. Nota: estos directorios se crean implicitamente al crear los archivos en fases posteriores (T023, T038). Si la herramienta de creacion de archivos crea directorios automaticamente, estas tareas se completan sin accion explicita.

- [x] T001 Crear directorio `docs/` en la raiz del repositorio
- [x] T002 [P] Crear directorio `demo/` en la raiz del repositorio

---

## Fase 2: Fundacional (Prerrequisitos Bloqueantes)

**Proposito**: La sustitucion "regional" -> "lab1" es un cambio transversal que DEBE completarse antes de generar documentacion nueva, para que toda la documentacion use los valores correctos desde el inicio.

**CRITICO**: La HU4 (sustitucion) debe completarse antes de HU1, HU2, HU3 y HU5 para evitar inconsistencias.

### Sustitucion en codigo fuente (.py) — RF-015, RF-017

- [x] T003 [US4] Sustituir "regional" por "lab1" en `conf/NbConfiguracionInicial.py` (7 ocurrencias: widget default + 5 INSERT values de esquemas + 1 comentario con esquema)
- [x] T004 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py` (1 ocurrencia: widget default esquemaParametro)
- [x] T005 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarSaldosCliente.py` (1 ocurrencia: widget default esquemaParametro)
- [x] T006 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py` (1 ocurrencia: widget default esquemaParametro)
- [x] T007 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py` (1 ocurrencia: widget default esquemaParametro)
- [x] T008 [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/NbTddBroncePipeline.py` (4 ocurrencias: datos esperados en asserts y diccionarios de prueba)
- [x] T009 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/NbTddOroPipeline.py` (2 ocurrencias: valor de esquema en pruebas)
- [x] T010 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceCmstfl.py` (3 ocurrencias: comentarios magic %md y print)
- [x] T011 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceTrxpfl.py` (3 ocurrencias: comentarios magic %md y print)
- [x] T012 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceBlncfl.py` (3 ocurrencias: comentarios magic %md y print)
- [x] T013 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py` (2 ocurrencias: comentario magic %md y default en .get())
- [x] T014 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py` (2 ocurrencias: comentario magic %md y default en .get())
- [x] T015 [P] [US4] Sustituir "regional" por "lab1" en `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py` (3 ocurrencias: comentario magic %md y 2 defaults en .get())

### Verificacion de sustitucion en codigo — CE-004

- [x] T016 [US4] Ejecutar verificacion exhaustiva: `grep -rn "regional" --include="*.py" src/ conf/` debe retornar 0 resultados

### Sustitucion en especificaciones (.md en specs/) — RF-016, RF-017

**Regla operativa (RF-017)**: Sustituir toda ocurrencia de "regional" que aparezca como valor de esquema en nombres de 3 partes (ej: `bronce.regional.cmstfl`), parametros de widgets, defaults, o ejemplos de configuracion. NO sustituir en frases narrativas que documenten la decision historica del cambio de nombre.

- [x] T017 [US4] Sustituir "regional" por "lab1" en todos los archivos .md de `specs/001-incremento1-config-inicial/` (~14 ocurrencias en data-model.md, contracts/, quickstart.md, spec.md, tasks.md)
- [x] T018 [P] [US4] Sustituir "regional" por "lab1" en todos los archivos .md de `specs/002-generar-parquets-as400/` (~12 ocurrencias en contracts/, quickstart.md)
- [x] T019 [P] [US4] Sustituir "regional" por "lab1" en todos los archivos .md de `specs/003-lsdp-bronce-pipeline/` (~15 ocurrencias en data-model.md, contracts/, quickstart.md, spec.md, plan.md)
- [x] T020 [P] [US4] Sustituir "regional" por "lab1" en todos los archivos .md de `specs/004-lsdp-plata-vistas/` (~15 ocurrencias en data-model.md, contracts/, quickstart.md, spec.md, plan.md)
- [x] T021 [P] [US4] Sustituir "regional" por "lab1" en todos los archivos .md de `specs/005-lsdp-oro-vistas/` (~15 ocurrencias en data-model.md, contracts/, quickstart.md, spec.md, plan.md)

### Verificacion de sustitucion en specs — CE-004

- [x] T022 [US4] Ejecutar verificacion exhaustiva: `grep -rn "regional" --include="*.md" specs/00[1-5]*/` y confirmar que no quedan ocurrencias como valor de esquema de Unity Catalog

**Checkpoint Fase 2**: Todas las ocurrencias de "regional" como valor de esquema han sido sustituidas por "lab1" en codigo y specs. El proyecto esta listo para generar documentacion con los valores correctos.

---

## Fase 3: Historia de Usuario 1 - Manual Tecnico Completo (Prioridad: P1)

**Objetivo**: Crear `docs/ManualTecnico.md` con las 7 secciones requeridas que centralicen todo el conocimiento tecnico del proyecto.

**Prueba Independiente**: Verificar que el documento existe, cubre las 7 secciones (decoradores, paradigma declarativo, propiedades Delta, operaciones DataFrame, parametros, tabla Parametros, dependencias) y que toda la informacion es consistente con el codigo implementado.

### Implementacion de Historia de Usuario 1

- [x] T023 [US1] Crear seccion 1 de `docs/ManualTecnico.md`: Decoradores LSDP — documentar `@dp.table` y `@dp.materialized_view` con importacion (`from pyspark import pipelines as dp`), parametros de cada decorador (name, comment, table_properties, cluster_by), y ejemplos reales extraidos de los archivos de `src/LSDP_Laboratorio_Basico/transformations/` (RF-001)
- [x] T024 [US1] Crear seccion 2 de `docs/ManualTecnico.md`: Paradigma Declarativo LSDP — documentar el patron Closure (parametros leidos a nivel de modulo, capturados por closure), cloudpickle, resolucion automatica de imports entre carpetas del pipeline, y compatibilidad con Serverless Compute (RF-002)
- [x] T025 [US1] Crear seccion 3 de `docs/ManualTecnico.md`: Propiedades de Tablas Delta y Vistas Materializadas — documentar Change Data Feed, autoOptimize.autoCompact, autoOptimize.optimizeWrite, Liquid Cluster (campos por entidad), expectativas de calidad de datos (9 total: 5 plata clientes + 4 plata transacciones), retencion de archivos (30 dias) y logs (60 dias) (RF-003)
- [x] T026 [US1] Crear seccion 4 de `docs/ManualTecnico.md`: Operaciones con API de DataFrames de Spark — documentar lecturas (readStream con cloudFiles, read.table), joins (LEFT JOIN en plata, INNER JOIN en oro), groupBy con agregaciones condicionales (F.count, F.avg, F.sum con F.when), transformaciones (F.coalesce, F.when, F.sha2, F.concat, F.current_timestamp), y protecciones ANSI (F.hash().cast("long") antes de F.abs()) (RF-004)
- [x] T027 [US1] Crear seccion 5 de `docs/ManualTecnico.md`: Parametros del Pipeline LSDP y Notebooks — tabla completa de los 12 parametros del pipeline (via spark.conf.get("pipelines.parameters.*")) y los widgets de cada notebook (catalogoParametro, esquemaParametro, tablaParametros, etc.) con tipo, ejemplo y descripcion (RF-005)
- [x] T028 [US1] Crear seccion 6 de `docs/ManualTecnico.md`: Tabla Parametros — documentar la tabla `control.lab1.Parametros` con estructura (Clave/Valor), los 15 registros de configuracion + TiposTransaccionesLabBase, con descripcion del uso de cada clave (RF-006)
- [x] T029 [US1] Crear seccion 7 de `docs/ManualTecnico.md`: Dependencias — documentar Databricks Free Edition con Unity Catalog, 4 catalogos (bronce, plata, oro, control), esquema lab1, Volume bronce.lab1.datos_bronce, y opcionalmente Amazon S3 (RF-007)

**Checkpoint Fase 3**: `docs/ManualTecnico.md` existe con 7 secciones completas en espanol, usando "lab1" como valor de esquema, con informacion consistente con el codigo fuente.

---

## Fase 4: Historia de Usuario 2 - Modelado de Datos Completo (Prioridad: P1)

**Objetivo**: Crear `docs/ModeladoDatos.md` con el diccionario de datos completo de las 10 entidades (~740 filas de documentacion) y diagrama de linaje.

**Prueba Independiente**: Verificar que el documento existe, documenta las 10 entidades con TODOS los campos (nombre, tipo, descripcion), incluye logica de campos calculados y diagrama de linaje.

### Implementacion de Historia de Usuario 2

- [x] T030 [US2] Crear encabezado y resumen de entidades en `docs/ModeladoDatos.md` — tabla resumen con las 10 entidades (nombre, capa, tipo, cantidad de columnas) y diagrama de linaje completo (AS400 -> Bronce -> Plata -> Oro con transformaciones entre capas) (RF-010)
- [x] T031 [US2] Crear seccion Parquets AS400 en `docs/ModeladoDatos.md` — documentar CMSTFL (70 columnas), TRXPFL (60 columnas), BLNCFL (100 columnas) con tablas detalladas de nombre de campo, tipo PySpark (StringType, DateType, LongType, DoubleType, TimestampType) y descripcion para cada campo (RF-008, RF-009)
- [x] T032 [US2] Crear seccion Streaming Tables Bronce en `docs/ModeladoDatos.md` — documentar bronce.lab1.cmstfl (75 cols), bronce.lab1.trxpfl (65 cols), bronce.lab1.blncfl (105 cols) indicando que replican las columnas fuente + 5 columnas de control (FechaIngestaDatos, _rescued_data, anio, mes, dia) con propiedades Delta y Liquid Cluster (RF-008, RF-009)
- [x] T033 [US2] Crear seccion Vistas Materializadas Plata en `docs/ModeladoDatos.md` — documentar plata.lab1.clientes_saldos_consolidados (173 cols) con mapeo de columnas CMSTFL y BLNCFL a nombres espanol, 4 campos calculados con logica detallada (clasificacion_riesgo_cliente, categoria_saldo_disponible, perfil_actividad_bancaria, huella_identificacion_cliente), y 5 expectativas de calidad (RF-008, RF-009)
- [x] T034 [US2] Completar seccion Plata en `docs/ModeladoDatos.md` — documentar plata.lab1.transacciones_enriquecidas (64 cols) con mapeo de columnas TRXPFL a nombres espanol, 4 campos calculados con logica (monto_neto_comisiones, porcentaje_comision_sobre_monto, variacion_saldo_transaccion, indicador_impacto_financiero), y 4 expectativas de calidad (RF-008, RF-009)
- [x] T035 [US2] Crear seccion Vistas Materializadas Oro en `docs/ModeladoDatos.md` — documentar oro.lab1.comportamiento_atm_cliente (6 cols con 5 metricas ATM: cantidad_depositos_atm, cantidad_retiros_atm, promedio_monto_depositos_atm, promedio_monto_retiros_atm, total_pagos_saldo_cliente) y oro.lab1.resumen_integral_cliente (22 cols con detalle de cada columna seleccionada de plata y oro) (RF-008, RF-009)
- [x] T036 [US2] Crear seccion Tabla Parametros en `docs/ModeladoDatos.md` — documentar control.lab1.Parametros (2 columnas x 16 registros incluyendo TiposTransaccionesLabBase) (RF-008)

**Checkpoint Fase 4**: `docs/ModeladoDatos.md` existe con las 10 entidades documentadas, ~740 filas de documentacion con nombre/tipo/descripcion, campos calculados con logica, expectativas de calidad, y diagrama de linaje. Todo en espanol con "lab1".

---

## Fase 5: Historia de Usuario 3 - Actualizacion del README Profesional (Prioridad: P1)

**Objetivo**: Reescribir `README.md` con formato profesional al nivel de empresas de referencia, con 8+ secciones, enlaces a documentacion y mencion de IA asistida.

**Prueba Independiente**: Verificar que `README.md` tiene formato profesional, contiene al menos 6 secciones diferenciadas, menciona GitHub Copilot y spec-kit, y enlaza a ManualTecnico.md, ModeladoDatos.md y SYSTEM.md.

### Implementacion de Historia de Usuario 3

- [x] T037 [US3] Reescribir `README.md` con formato profesional: Header con titulo descriptivo del proyecto, seccion de descripcion extendida (proposito, plataforma Databricks Free Edition, alcance), seccion de arquitectura medallion (diagrama del flujo Bronce -> Plata -> Oro), seccion de stack tecnologico (tabla con componentes y tecnologias), seccion de estructura del proyecto (arbol de directorios actualizado con docs/ y demo/), seccion de guia rapida de inicio, seccion de documentacion con enlaces a `docs/ManualTecnico.md`, `docs/ModeladoDatos.md` y `SYSTEM.md`, seccion de desarrollo con IA asistida mencionando GitHub Copilot y spec-kit para Spec-Driven Development (RF-011, RF-012, RF-013, RF-014, RF-021, RF-022)

**Checkpoint Fase 5**: `README.md` tiene formato profesional con 8 secciones, enlaces correctos, mencion de IA asistida, todo en espanol con "lab1".

---

## Fase 6: Historia de Usuario 5 - Template de Configuracion Inicial (Prioridad: P2)

**Objetivo**: Crear `demo/ConfiguracionInicial.md` con guia paso a paso para replicar el laboratorio en Databricks Free Edition.

**Prueba Independiente**: Verificar que el documento existe, contiene al menos 5 pasos secuenciales y verificables (CE-005), y usa "lab1" como valor de esquema.

### Implementacion de Historia de Usuario 5

- [x] T038 [US5] Crear `demo/ConfiguracionInicial.md` con guia paso a paso: prerrequisitos (cuenta Databricks Free Edition, VS Code con extensiones), paso 1 importar repositorio en workspace, paso 2 configurar extensiones VS Code (Databricks Extension + Driver for SQLTools), paso 3 ejecutar NbConfiguracionInicial.py con parametros (catalogoParametro=control, esquemaParametro=lab1, tablaParametros=Parametros), paso 4 ejecutar notebooks de generacion de parquets (NbGenerarMaestroCliente, NbGenerarTransaccionalCliente, NbGenerarSaldosCliente), paso 5 crear pipeline LSDP con los 12 parametros listados, paso 6 ejecutar pipeline (bronce -> plata -> oro), paso 7 verificar resultados en vistas de oro, paso 8 consultas SQL de validacion (RF-019, RF-020, RF-021, RF-022)

**Checkpoint Fase 6**: `demo/ConfiguracionInicial.md` existe con 8 pasos secuenciales, usa "lab1", referencia parametros correctos.

---

## Fase 7: Pulido y Verificacion Cruzada

**Proposito**: Verificacion final de consistencia y completitud

- [x] T039 [P] Re-confirmar sustitucion de "regional" (post-documentacion): ejecutar `grep -rn "regional" --include="*.py" src/ conf/` y `grep -rn "regional" --include="*.md" specs/00[1-5]*/` confirmando que los 0 resultados de T016/T022 se mantienen tras la generacion de documentos (CE-004)
- [x] T040 [P] Verificar que `docs/ManualTecnico.md` usa "lab1" consistentemente en todas las secciones y que los ejemplos de codigo son correctos (RF-021, CE-006)
- [x] T041 [P] Verificar que `docs/ModeladoDatos.md` usa "lab1" en todos los nombres de entidad de 3 partes y que las 10 entidades tienen campos completos (RF-021, CE-002, CE-006)
- [x] T042 [P] Verificar que `README.md` tiene al menos 6 secciones diferenciadas, menciona GitHub Copilot y spec-kit, y enlaces a docs/ y SYSTEM.md son correctos (CE-003)
- [x] T043 [P] Verificar que `demo/ConfiguracionInicial.md` tiene al menos 5 pasos secuenciales con "lab1" (CE-005)
- [x] T044 Ejecutar validacion del quickstart.md: recorrer los 7 pasos del quickstart para confirmar que la secuencia de implementacion fue correcta

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Fase 1 (Setup)**: Sin dependencias — puede comenzar inmediatamente
- **Fase 2 (Fundacional/HU4)**: Depende de Fase 1 — BLOQUEA todas las historias de documentacion
- **Fases 3-6 (HU1, HU2, HU3, HU5)**: Todas dependen de la Fase 2 (sustitucion completada)
  - HU1 (ManualTecnico) y HU2 (ModeladoDatos) pueden ejecutarse en paralelo
  - HU3 (README) depende idealmente de HU1 y HU2 (para enlazar documentos completos)
  - HU5 (ConfiguracionInicial) depende idealmente de HU1 (para referenciar parametros correctos)
- **Fase 7 (Pulido)**: Depende de que todas las fases anteriores esten completas

### Dependencias entre Historias de Usuario

- **HU4 (Sustitucion)**: PRIMERA — bloquea todo lo demas (Fase 2)
- **HU1 (ManualTecnico)**: Despues de HU4 — sin dependencia de otras HU
- **HU2 (ModeladoDatos)**: Despues de HU4 — sin dependencia de otras HU
- **HU3 (README)**: Despues de HU4, idealmente despues de HU1 y HU2 (para enlaces)
- **HU5 (ConfiguracionInicial)**: Despues de HU4, idealmente despues de HU1 (para parametros)

### Dentro de la Fase 2 (Sustitucion)

- T003 a T015 (sustitucion en .py): pueden ejecutarse en paralelo entre si (archivos distintos)
- T016 (verificacion .py): depende de T003-T015
- T017 a T021 (sustitucion en specs): pueden ejecutarse en paralelo entre si
- T022 (verificacion specs): depende de T017-T021

### Oportunidades de Paralelismo

- T001 y T002 (Setup): paralelo
- T004-T007, T009-T015 (sustitucion .py): todos paralelos (archivos distintos)
- T018-T021 (sustitucion specs): todos paralelos (directorios distintos)
- T023-T029 (ManualTecnico): secuenciales (mismo archivo, secciones consecutivas)
- T030-T036 (ModeladoDatos): secuenciales (mismo archivo, secciones consecutivas)
- Fase 3 (HU1) y Fase 4 (HU2): paralelo entre si (archivos distintos)
- T039-T043 (verificacion final): todos paralelos

---

## Ejemplo de Paralelismo: Fase 2 (Sustitucion)

```bash
# Ronda 1 — Sustitucion en .py (todos en paralelo, archivos distintos):
T003: conf/NbConfiguracionInicial.py
T004: NbGenerarMaestroCliente.py
T005: NbGenerarSaldosCliente.py
T006: NbGenerarTransaccionalCliente.py
T007: NbTddGenerarParquets.py
T008: NbTddBroncePipeline.py
T009: NbTddOroPipeline.py
T010: LsdpBronceCmstfl.py
T011: LsdpBronceTrxpfl.py
T012: LsdpBronceBlncfl.py
T013: LsdpPlataClientesSaldos.py
T014: LsdpPlataTransacciones.py
T015: LsdpOroClientes.py

# Ronda 2 — Verificacion .py:
T016: grep "regional" en .py = 0 resultados

# Ronda 3 — Sustitucion en specs (todos en paralelo, directorios distintos):
T017: specs/001-incremento1-config-inicial/
T018: specs/002-generar-parquets-as400/
T019: specs/003-lsdp-bronce-pipeline/
T020: specs/004-lsdp-plata-vistas/
T021: specs/005-lsdp-oro-vistas/

# Ronda 4 — Verificacion specs:
T022: grep "regional" en specs = 0 resultados
```

## Ejemplo de Paralelismo: Documentacion (Fases 3-6)

```bash
# HU1 y HU2 en paralelo (archivos distintos):
  Desarrollador A: T023-T029 (ManualTecnico.md)
  Desarrollador B: T030-T036 (ModeladoDatos.md)

# Despues de HU1 y HU2:
  T037 (README.md) — depende de que existxan ManualTecnico y ModeladoDatos
  T038 (ConfiguracionInicial.md) — depende idealmente de ManualTecnico
```

---

## Estrategia de Implementacion

### MVP Primero (Solo HU4 + HU3)

> **NOTA**: Esta estrategia produce enlaces rotos temporales en el README (a ManualTecnico.md y ModeladoDatos.md). Se recomienda seguir el "Orden de Entrega Recomendado" a continuacion.

1. Completar Fase 1: Setup (crear directorios)
2. Completar Fase 2: Sustitucion "regional" -> "lab1" (HU4)
3. Completar Fase 5: README profesional (HU3)
4. **PARAR Y VALIDAR**: Repositorio tiene README profesional y valores actualizados
5. Continuar con HU1, HU2, HU5

### Orden de Entrega Recomendado

1. Setup + Sustitucion (Fases 1-2) -> Valores consistentes en todo el proyecto
2. ManualTecnico (Fase 3) -> Conocimiento tecnico centralizado
3. ModeladoDatos (Fase 4) -> Diccionario de datos completo
4. README (Fase 5) -> Primera impresion profesional con enlaces a docs
5. ConfiguracionInicial (Fase 6) -> Guia de adopcion
6. Verificacion cruzada (Fase 7) -> Consistencia final

---

## Notas

- Las tareas [P] pueden ejecutarse en paralelo (archivos distintos, sin dependencias)
- Las etiquetas [US#] mapean cada tarea a su historia de usuario para trazabilidad
- Cada historia de usuario es verificable de forma independiente
- SYSTEM.md y constitution.md estan EXCLUIDOS de sustitucion (RF-018)
- Toda la documentacion generada debe estar en espanol (RF-022) y usar "lab1" (RF-021)
- Hacer commit despues de cada fase completada para facilitar rollback
