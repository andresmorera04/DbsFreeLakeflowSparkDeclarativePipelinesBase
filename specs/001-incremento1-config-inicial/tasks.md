# Tasks: Incremento 1 - Research Inicial y Configuracion Base

**Input**: Design documents from `/specs/001-incremento1-config-inicial/`
**Prerequisites**: plan.md (cargado), spec.md (cargado), research.md (cargado), data-model.md (cargado), contracts/ (cargado), quickstart.md (cargado)

**Tests**: Excluidas para Incremento 1 (TDD aplica desde Incremento 2).

**Organizacion**: Tareas agrupadas por historia de usuario para implementacion y validacion independiente de cada historia.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos diferentes, sin dependencias)
- **[Story]**: Historia de usuario a la que pertenece (US1, US2, US3)
- Todas las rutas son relativas a la raiz del repositorio

---

## Fase 1: Setup (Infraestructura Inicial)

**Proposito**: Creacion de la estructura de directorios del proyecto

- [X] T001 Crear directorio `conf/` y archivo esqueleto `conf/NbConfiguracionInicial.py` con header de notebook Databricks y comentarios de seccion segun el flujo del contrato (contracts/nb-configuracion-inicial.md)

---

## Fase 2: US1 - Investigacion Inicial del Proyecto (Prioridad: P1)

**Objetivo**: Documentar las decisiones de investigacion aprobadas en SYSTEM.md y verificar que las extensiones de Databricks para VS Code estan funcionales.

**Prueba Independiente**: Verificar que SYSTEM.md contiene las decisiones documentadas de LSDP, AutoLoader, Unity Catalog DDL, extensiones VS Code y dbutils.widgets. Verificar que las extensiones de VS Code se conectan al workspace y listan computos.

### Implementacion para US1

- [X] T002 [P] [US1] Documentar las 5 decisiones aprobadas de research.md en SYSTEM.md, enriqueciendo las secciones existentes o agregando seccion de decisiones (RF-014, CE-006)
- [X] T003 [P] [US1] Verificar que las extensiones Databricks Extension for Visual Studio Code y Databricks Driver for SQLTools estan instaladas, conectadas al workspace y listan computos Serverless disponibles (CE-007)

**Checkpoint**: Las decisiones de investigacion estan formalmente documentadas en SYSTEM.md y las extensiones de VS Code son funcionales.

---

## Fase 3: US2 - Creacion de la Tabla de Parametros (Prioridad: P1) - MVP

**Objetivo**: Implementar el notebook de configuracion inicial que crea catalogos, esquemas, la tabla Parametros con 15 registros y observabilidad completa.

**Prueba Independiente**: Ejecutar `conf/NbConfiguracionInicial.py` en Databricks Free Edition y verificar con `SELECT * FROM control.regional.Parametros ORDER BY Clave` que retorna 15 filas. Re-ejecutar 3 veces y confirmar idempotencia (CE-001, CE-002, CE-003).

### Implementacion para US2

- [X] T004 [US2] Implementar definicion de 3 widgets con `dbutils.widgets.text()` (catalogoParametro, esquemaParametro, tablaParametros) y captura inmediata de valores en variables en conf/NbConfiguracionInicial.py (contrato pasos 1-2, RF-001)
- [X] T005 [US2] Implementar validacion de parametros no vacios con detencion de ejecucion y mensaje explicativo si algun parametro esta vacio en conf/NbConfiguracionInicial.py (contrato paso 3, RF-008)
- [X] T006 [US2] Implementar bloque de resumen inicial que imprime todos los parametros recibidos y la ruta completa de la tabla destino en conf/NbConfiguracionInicial.py (contrato paso 4, RF-010)
- [X] T007 [US2] Implementar creacion de 4 catalogos (control, bronce, plata, oro) con `CREATE CATALOG IF NOT EXISTS` y el esquema de la tabla Parametros `{catalogoParametro}.{esquemaParametro}` con `CREATE SCHEMA IF NOT EXISTS` via `spark.sql()` en conf/NbConfiguracionInicial.py (contrato paso 5, RF-013 parcial — los 4 esquemas medallion se crean en T011)
- [X] T008 [US2] Implementar `CREATE OR REPLACE TABLE {catalogoParametro}.{esquemaParametro}.{tablaParametros} (Clave STRING, Valor STRING)` via `spark.sql()` con cronometro de tiempo en conf/NbConfiguracionInicial.py (contrato paso 6, RF-002, RF-004, RF-009)
- [X] T009 [US2] Implementar INSERT de los 15 registros de parametros (catalogoBronce, esquemaBronce, contenedorBronce, TipoStorage, catalogoVolume, esquemaVolume, nombreVolume, bucketS3, prefijoS3, DirectorioBronce, catalogoPlata, esquemaPlata, catalogoOro, esquemaOro, esquemaControl) con impresion de cada par Clave-Valor insertado en conf/NbConfiguracionInicial.py (contrato paso 7, RF-003, RF-005, RF-011)
- [X] T010 [US2] Implementar lectura de valores de esquemas (esquemaBronce, esquemaPlata, esquemaOro, esquemaControl) y datos de Volume (catalogoVolume, esquemaVolume, nombreVolume) desde la tabla Parametros recien creada en conf/NbConfiguracionInicial.py (contrato paso 8)
- [X] T011 [US2] Implementar creacion de 4 esquemas con `CREATE SCHEMA IF NOT EXISTS` para cada catalogo usando los valores leidos de la tabla (control.{esquemaControl}, bronce.{esquemaBronce}, plata.{esquemaPlata}, oro.{esquemaOro}) en conf/NbConfiguracionInicial.py (contrato paso 9, RF-013)

**Checkpoint**: La tabla Parametros existe con 15 registros, 4 catalogos y 4 esquemas creados. Re-ejecuciones son idempotentes.

---

## Fase 4: US3 - Creacion del Volume en Unity Catalog (Prioridad: P1)

**Objetivo**: Crear el Volume gestionado en Unity Catalog usando los valores leidos de la tabla Parametros.

**Prueba Independiente**: Tras ejecutar el notebook, verificar que el Volume existe con `DESCRIBE VOLUME {catalogoVolume}.{esquemaVolume}.{nombreVolume}` y que la ruta `/Volumes/{catalogoVolume}/{esquemaVolume}/{nombreVolume}/` es accesible (CE-005).

### Implementacion para US3

- [X] T012 [US3] Implementar creacion del Volume gestionado con `CREATE VOLUME IF NOT EXISTS {catalogoVolume}.{esquemaVolume}.{nombreVolume}` usando valores leidos de la tabla, con impresion del nombre completo y estado (creado/ya existente) en conf/NbConfiguracionInicial.py (contrato paso 10, RF-006, RF-007)
- [X] T013 [US3] Implementar bloque de resumen final con metricas de ejecucion (registros insertados, recursos creados, tiempos de operaciones criticas, tiempo total) en conf/NbConfiguracionInicial.py (contrato paso 11, RF-009, RF-010, RF-012)

**Checkpoint**: El Volume gestionado existe y es accesible. El notebook imprime resumen completo de metricas.

---

## Fase 5: Polish y Validacion Cruzada

**Proposito**: Validacion end-to-end y verificacion de criterios de exito

- [X] T014 Ejecutar validacion end-to-end siguiendo quickstart.md en Databricks Free Edition: verificar 15 registros, 4 catalogos, 4 esquemas, 1 Volume, idempotencia en 3 ejecuciones consecutivas, tiempo < 2 min (CE-001, CE-002, CE-003, CE-004, CE-005)
- [X] T015 Revisar que el notebook cumple estandares de codigo: comentarios en espanol, variables snake_case, nombre PascalCase con prefijo "Nb", cero valores hardcodeados, sin emojis, sin errores silenciosos (CE-008, RF-011, RF-012)

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Setup (Fase 1)**: Sin dependencias - puede iniciar inmediatamente
- **US1 (Fase 2)**: Depende de Setup. **Puede ejecutarse en PARALELO con Fase 3 y 4** (archivos diferentes: SYSTEM.md vs conf/NbConfiguracionInicial.py)
- **US2 (Fase 3)**: Depende de Setup (T001). Es la base del notebook
- **US3 (Fase 4)**: Depende de US2 completada (T010 y T011 deben existir para que T012 lea valores de la tabla)
- **Polish (Fase 5)**: Depende de TODAS las fases anteriores completadas

### Dependencias entre Historias de Usuario

- **US1 (Investigacion)**: Independiente - no depende de US2 ni US3
- **US2 (Tabla Parametros)**: Independiente de US1 - depende solo de Setup
- **US3 (Volume)**: Depende de US2 - necesita la tabla Parametros creada para leer valores de Volume

### Dentro de Cada Historia

- **US1**: T002 y T003 son paralelas entre si [P]
- **US2**: Tareas T004-T011 son estrictamente secuenciales (mismo archivo, cada tarea construye sobre la anterior)
- **US3**: T012 depende de T010/T011 (valores de tabla). T013 depende de T012 (metricas incluyen Volume)

### Oportunidades de Paralelismo

- T002 [US1] y T003 [US1] pueden ejecutarse en paralelo entre si
- Toda la Fase 2 (US1) puede ejecutarse en paralelo con las Fases 3 y 4 (US2/US3) ya que son archivos completamente diferentes
- Dentro del notebook (Fases 3-4), las tareas son secuenciales por naturaleza

---

## Ejemplo de Paralelismo: US1 + US2

```text
# Stream A (documentacion):
T002 [US1] Documentar decisiones en SYSTEM.md
T003 [US1] Verificar extensiones VS Code

# Stream B (notebook - secuencial):
T004 [US2] Definir widgets y capturar parametros
T005 [US2] Validar parametros
T006 [US2] Bloque de resumen inicial
T007 [US2] Crear catalogos y esquema inicial
T008 [US2] Crear tabla Parametros
T009 [US2] Insertar 15 registros
T010 [US2] Leer valores de tabla
T011 [US2] Crear 4 esquemas
T012 [US3] Crear Volume
T013 [US3] Bloque de resumen final

# Ambos streams pueden ejecutarse en paralelo.
# Al finalizar ambos: T014 y T015 (Polish).
```

---

## Estrategia de Implementacion

### MVP (US2 solamente)

1. Completar Fase 1: Setup (T001)
2. Completar Fase 3: US2 - Tabla de Parametros (T004-T011)
3. **VALIDAR**: Ejecutar notebook y verificar 15 registros en tabla
4. MVP funcional: la tabla de parametros existe y es consultable

### Entrega Incremental

1. Setup (T001) -> Estructura lista
2. US2 (T004-T011) -> Tabla Parametros funcional -> Validar independientemente (MVP)
3. US3 (T012-T013) -> Volume creado -> Validar independientemente
4. US1 (T002-T003) -> Documentacion y extensiones verificadas (paralelo)
5. Polish (T014-T015) -> Validacion end-to-end completa

---

## Notas

- [P] = archivos diferentes, sin dependencias cruzadas
- [US1/US2/US3] = mapeo a historia de usuario para trazabilidad
- Tests excluidos para Incremento 1 por politica del proyecto
- El notebook tiene un unico archivo: `conf/NbConfiguracionInicial.py`
- Todas las tareas del notebook son secuenciales (construyen sobre codigo previo en el mismo archivo)
- Comprometer (commit) despues de cada tarea o grupo logico completado
- Verificar re-ejecucion idempotente despues de completar cada checkpoint
