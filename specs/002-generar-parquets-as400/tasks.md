# Tasks: Generacion de Parquets Simulando Data AS400

**Input**: Documentos de diseno desde `/specs/002-generar-parquets-as400/`
**Prerequisites**: plan.md (requerido), spec.md (requerido), research.md, data-model.md, contracts/

**Tests**: Incluidas — HU6 requiere pruebas TDD de forma obligatoria segun spec.md (RF-020) y la constitution del proyecto.

**Organization**: Las tareas estan agrupadas por historia de usuario para permitir implementacion y pruebas independientes de cada historia.

## Formato: `[ID] [P?] [Story] Descripcion`

- **[P]**: Puede ejecutarse en paralelo (archivos diferentes, sin dependencias pendientes)
- **[Story]**: Historia de usuario a la que pertenece la tarea (US1, US2, etc.)
- Incluye rutas exactas de archivos en las descripciones

## Phase 1: Setup (Inicializacion del Proyecto)

**Proposito**: Creacion de la estructura de directorios para los notebooks de generacion de parquets

- [X] T001 Crear estructura de directorios `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/`

---

## Phase 2: Foundational (Prerequisitos Bloqueantes)

**Proposito**: Prerequisitos que deben completarse antes de iniciar cualquier historia de usuario

> **Nota**: No existen tareas fundacionales bloqueantes. Toda la logica (lectura de parametros, construccion de rutas, Faker fallback) se implementa directamente dentro de cada notebook segun la decision aprobada de mantener la logica inline (las utilidades LSDP se crearan en el Incremento 3). El Setup (Phase 1) es el unico prerequisito.

**Checkpoint**: Setup completado — la implementacion de historias de usuario puede comenzar.

---

## Phase 3: HU1 — Generacion del Maestro de Clientes CMSTFL (Prioridad: P1) 🎯 MVP

**Objetivo**: Crear el notebook NbGenerarMaestroCliente.py que genera un parquet de 70 columnas (41 StringType, 18 DateType, 9 LongType, 2 DoubleType) simulando la tabla maestra de clientes del sistema AS400, con modelo de mutacion diferenciada en re-ejecuciones, nombres exclusivamente hebreos/egipcios/ingleses, Faker con fallback a listas estaticas, y optimizado para Databricks Free Edition.

**Prueba Independiente**: Ejecutar el notebook con parametros por defecto (incluyendo `rutaMaestroClienteExistente` vacio), verificar que el parquet tenga 70 columnas con la distribucion de tipos correcta, 50,000 registros, nombres exclusivamente hebreos/egipcios/ingleses. En segunda ejecucion (proporcionando ruta del parquet anterior en `rutaMaestroClienteExistente`), verificar `porcentajeNuevos`% registros nuevos y `porcentajeMutacion`% mutados en los campos de `camposMutacion`.

### Implementacion de HU1

- [X] T002 [US1] Crear esqueleto de NbGenerarMaestroCliente.py con encabezado formato Databricks notebook (.py), imports (pyspark.sql.functions, pyspark.sql.types, time, datetime, re), configuracion spark.sql.shuffle.partitions segun parametro `shufflePartitions` (defecto 8) y definicion de 13 widgets (catalogoParametro, esquemaParametro, tablaParametros, cantidadClientes, rutaRelativaMaestroCliente, rutaMaestroClienteExistente, porcentajeMutacion, porcentajeNuevos, camposMutacion, montoMinimo, montoMaximo, numeroParticiones, shufflePartitions) con valores por defecto segun contrato en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T003 [US1] Implementar lectura de tabla Parametros con spark.read.table() + collect() a diccionario Python, y validacion de parametros: widgets no vacios (excepto rutaMaestroClienteExistente que puede estar vacio), cantidadClientes entero positivo, porcentajeMutacion y porcentajeNuevos numericos positivos en rango (0, 1.0], montoMinimo < montoMaximo (ambos numericos positivos), TipoStorage debe ser "Volume" o "AmazonS3" en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T004 [US1] Implementar construccion dinamica de ruta con if/elif segun TipoStorage (Volume: `/Volumes/<cat>/<esq>/<vol>/<ruta>`, S3: `s3://<bucket>/<ruta_relativa>`, otro: ValueError) y bloque de observabilidad inicial con resumen de todos los parametros widgets y tabla Parametros, tipo storage y ruta construida en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T005 [US1] Implementar mecanismo Faker fallback con try/except ImportError: si Faker disponible usar locales he_IL, ar_EG, en_US/en_GB; si no disponible usar listas estaticas embebidas de ~100 nombres y ~80 apellidos por etnia (hebreos, egipcios, ingleses); prohibidos nombres y apellidos latinos en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T006 [US1] Implementar generacion de datos CMSTFL con spark.range(cantidadClientes) y funciones PySpark nativas para las 70 columnas conforme al data-model.md: 41 StringType (CUSNM, CUSLN, CUSMD, etc.), 18 DateType (CUSDB rango 1970-2007 para nacimiento, demas fechas rango 2005-2025), 9 LongType (CUSTID, CUSYR, etc.), 2 DoubleType (CUSIN, CUSBL — valores entre montoMinimo y montoMaximo), con distribuciones de valores segun reglas de validacion en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T007 [US1] Implementar modelo de mutacion diferenciada: si `rutaMaestroClienteExistente` esta vacio, generar registros base; si tiene valor, leer parquet existente desde esa ruta, parsear `camposMutacion` (separados por coma) para obtener la lista de campos a mutar, mutar `porcentajeMutacion`% de registros en esos campos con F.rand()+F.row_number(), agregar `porcentajeNuevos`% nuevos con CUSTID secuencial continuado (max_custid+1..N), union de no-mutados + mutados + nuevos, overwrite en ruta destino `rutaRelativaMaestroCliente` en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`
- [X] T008 [US1] Implementar escritura de parquet con df.coalesce(int(numeroParticiones)).write.mode("overwrite").parquet(ruta_completa) y bloque de observabilidad final con conteo de registros escritos, tiempo de ejecucion total y ruta destino en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`

**Checkpoint**: El notebook CMSTFL genera correctamente el parquet maestro con 70 columnas, modelo de mutacion y nombres exclusivamente hebreos/egipcios/ingleses. Es funcional de forma independiente.

---

## Phase 4: HU2 — Generacion del Transaccional TRXPFL (Prioridad: P1)

**Objetivo**: Crear el notebook NbGenerarTransaccionalCliente.py que genera un parquet de 60 columnas (7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType) simulando las transacciones bancarias del AS400, con 15 tipos de transaccion en 3 grupos de frecuencia, TRXID alfanumerico con prefijo e integridad referencial a CMSTFL.

**Prueba Independiente**: Ejecutar el notebook despues de generar CMSTFL, verificar que el parquet tenga 60 columnas con distribucion de tipos correcta, 150,000 registros, todos los CUSTID existan en CMSTFL, y distribucion de tipos de transaccion ~60%/~30%/~10%.

### Implementacion de HU2

- [X] T009 [US2] Crear esqueleto de NbGenerarTransaccionalCliente.py con encabezado formato Databricks notebook (.py), imports, configuracion shuffle.partitions segun parametro `shufflePartitions` (defecto 8) y definicion de 11 widgets (catalogoParametro, esquemaParametro, tablaParametros, cantidadTransacciones, fechaTransaccion, rutaRelativaTransaccional, rutaRelativaMaestroCliente, montoMinimo, montoMaximo, numeroParticiones, shufflePartitions) con valores por defecto segun contrato en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`
- [X] T010 [US2] Implementar lectura de tabla Parametros a diccionario Python, validacion de parametros (widgets no vacios, cantidadTransacciones entero positivo, formato YYYY-MM-DD de fechaTransaccion con regex, montoMinimo < montoMaximo ambos numericos positivos, TipoStorage valido), y construccion dinamica de ruta (Volume/S3/ValueError) en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`
- [X] T011 [US2] Implementar bloque de observabilidad inicial, verificacion de existencia del parquet CMSTFL en la ruta del Maestro de Clientes, y lectura del parquet para obtener lista de CUSTIDs validos y conteo de clientes en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`
- [X] T012 [US2] Implementar generacion de datos TRXPFL con spark.range(cantidadTransacciones) para las 60 columnas conforme al data-model.md, con distribucion de 15 tipos de transaccion en 3 grupos: alta ~60% (CATM 15%, DATM 14%, CMPR 13%, TINT 10%, DPST 8%), media ~30% (PGSL 7%, TEXT 6%, RTRO 5%, PGSV 5%, NMNA 4%, INTR 3%), baja ~10% (ADSL 3%, IMPT 3%, DMCL 2%, CMSN 2%); montos (TRXAMT y demas DoubleType) generados entre montoMinimo y montoMaximo; fechas (excepto TRXDT que es parametro) en rango 2005-2025; usar Faker con fallback a listas estaticas para TRXDSC (descripcion de transaccion) si disponible en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`
- [X] T013 [US2] Implementar generacion de TRXID con formato alfanumerico (prefijo tipo 4 chars + secuencial 8 digitos zero-padded, ej: "CATM00000001") usando F.concat y F.lpad, y asignacion de CUSTIDs validos del Maestro de Clientes garantizando integridad referencial en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`
- [X] T014 [US2] Implementar escritura de parquet con coalesce(int(numeroParticiones)) y overwrite, y bloque de observabilidad final con conteo de registros, distribucion de tipos de transaccion generada, y tiempo de ejecucion en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`

**Checkpoint**: El notebook TRXPFL genera transacciones con integridad referencial a CMSTFL, TRXID en formato AS400 y distribucion correcta de tipos.

---

## Phase 5: HU3 — Generacion de Saldos BLNCFL (Prioridad: P1)

**Objetivo**: Crear el notebook NbGenerarSaldosCliente.py que genera un parquet de 100 columnas (2 LongType, 29 StringType, 34 DoubleType, 35 DateType) simulando los saldos bancarios del AS400, con relacion 1:1 con el Maestro de Clientes y 4 tipos de cuenta con distribuciones definidas.

**Prueba Independiente**: Ejecutar el notebook despues de generar CMSTFL, verificar que el parquet tenga 100 columnas con distribucion de tipos correcta, exactamente un registro por cliente del Maestro, y distribucion de tipos de cuenta AHRO 40%, CRTE 30%, PRES 20%, INVR 10%.

### Implementacion de HU3

- [X] T015 [P:HU2] [US3] Crear esqueleto de NbGenerarSaldosCliente.py con encabezado formato Databricks notebook (.py), imports, configuracion shuffle.partitions segun parametro `shufflePartitions` (defecto 8) y definicion de 9 widgets (catalogoParametro, esquemaParametro, tablaParametros, rutaRelativaSaldoCliente, rutaRelativaMaestroCliente, montoMinimo, montoMaximo, numeroParticiones, shufflePartitions) con valores por defecto segun contrato en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarSaldosCliente.py`
- [X] T016 [US3] Implementar lectura de tabla Parametros a diccionario Python, validacion de parametros (widgets no vacios, montoMinimo < montoMaximo ambos numericos positivos, TipoStorage valido), construccion dinamica de ruta (Volume/S3/ValueError), bloque de observabilidad inicial, verificacion de existencia del parquet CMSTFL y lectura para obtener CUSTIDs (relacion 1:1) en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarSaldosCliente.py`
- [X] T017 [US3] Implementar generacion de datos BLNCFL con spark.range() para las 100 columnas conforme al data-model.md, con distribucion de 4 tipos de cuenta (AHRO 40%, CRTE 30%, PRES 20%, INVR 10%) y relacion 1:1 con CMSTFL (un registro por cada CUSTID del Maestro); montos (BLAV, BLTB y demas DoubleType) generados entre montoMinimo y montoMaximo; fechas en rango 2005-2025; usar Faker con fallback a listas estaticas para BLMG (gerente) y BLNM (nombre de cuenta) si disponible en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarSaldosCliente.py`
- [X] T018 [US3] Implementar escritura de parquet con coalesce(int(numeroParticiones)) y overwrite, y bloque de observabilidad final con conteo de registros, distribucion de tipos de cuenta generada, y tiempo de ejecucion en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarSaldosCliente.py`

**Checkpoint**: El notebook BLNCFL genera saldos con relacion 1:1 exacta con CMSTFL y distribucion de tipos de cuenta correcta.

---

## Phase 6: HU4 — Observabilidad Completa en la Generacion (Prioridad: P2)

**Objetivo**: Garantizar que los 3 notebooks de generacion tienen observabilidad completa y consistente: resumen de parametros al inicio, conteos durante ejecucion, metricas al final, y manejo claro de condiciones excepcionales.

**Prueba Independiente**: Ejecutar cada notebook y verificar que la salida en consola incluye: parametros recibidos (widgets + tabla), tipo de storage, ruta construida, conteo de registros escritos y tiempo de ejecucion total.

### Implementacion de HU4

- [X] T019 [P] [US4] Verificar y estandarizar consistencia del bloque de observabilidad inicial (parametros widgets, parametros tabla Parametros, TipoStorage, ruta construida) en los 3 notebooks `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`, `NbGenerarTransaccionalCliente.py`, `NbGenerarSaldosCliente.py`
- [X] T020 [P] [US4] Verificar y estandarizar consistencia del bloque de observabilidad final (registros escritos, tiempo de ejecucion, ruta destino) y manejo de condiciones excepcionales (parquet corrupto, ruta inaccesible, recursos insuficientes) con mensajes explicativos en los 3 notebooks

**Checkpoint**: La observabilidad es consistente y completa en los 3 notebooks de generacion.

---

## Phase 7: HU5 — Compatibilidad con Almacenamiento Dinamico (Prioridad: P2)

**Objetivo**: Garantizar que los 3 notebooks manejan correctamente Volume y AmazonS3 como tipos de almacenamiento, con rutas construidas dinamicamente segun TipoStorage, y rechazan valores no reconocidos con un mensaje claro.

**Prueba Independiente**: Verificar que cada notebook construya correctamente rutas para TipoStorage=Volume (`/Volumes/<cat>/<esq>/<vol>/<ruta>`) y TipoStorage=AmazonS3 (`s3://<bucket>/<ruta_relativa>`), y lance ValueError con mensaje descriptivo para un valor invalido.

### Implementacion de HU5

- [X] T021 [P] [US5] Verificar y estandarizar la logica de construccion de rutas dinamicas (Volume: `/Volumes/cat/esq/vol/ruta`, S3: `s3://bucket/ruta_relativa`, otro: ValueError con mensaje de valores validos) en los 3 notebooks, asegurando nomenclatura de variables y formato de ruta identicos en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`, `NbGenerarTransaccionalCliente.py`, `NbGenerarSaldosCliente.py`

**Checkpoint**: Los 3 notebooks son compatibles con ambos tipos de almacenamiento sin modificacion de codigo.

---

## Phase 8: HU6 — Pruebas TDD para la Generacion de Parquets (Prioridad: P2)

**Objetivo**: Crear el notebook NbTddGenerarParquets.py con pruebas TDD que validen la estructura de columnas, tipos de datos, cantidades, distribuciones, integridad referencial y modelo de mutacion de los 3 parquets generados.

**Prueba Independiente**: Ejecutar el notebook TDD despues de generar los 3 parquets y verificar que todas las pruebas pasen exitosamente con resumen PASS/FAIL.

### Implementacion de HU6

- [X] T022 [US6] Crear esqueleto de NbTddGenerarParquets.py con encabezado formato Databricks notebook (.py), imports, definicion de 6 widgets (catalogoParametro, esquemaParametro, tablaParametros, rutaRelativaMaestroCliente, rutaRelativaTransaccional, rutaRelativaSaldoCliente), lectura de parametros y construccion de rutas para los 3 parquets en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py`
- [X] T023 [US6] Implementar funcion auxiliar de reporte de pruebas (PASS/FAIL con descripcion, acumulador de resultados) y lectura de los 3 parquets con manejo de error si alguno no existe en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py`
- [X] T024 [US6] Implementar pruebas TDD para CMSTFL: verificar 70 columnas exactas, distribucion de tipos (41 StringType, 18 DateType, 9 LongType, 2 DoubleType), cantidad de registros >= cantidadClientes, CUSTID secuencial unico y positivo, exclusion de nombres latinos (CUSNM/CUSLN no contienen nombres latinos), y modelo de mutacion diferenciada (verificar 0.6% nuevos con porcentajeNuevos=0.006 y 20% mutados con porcentajeMutacion=0.20 si se ejecuta dos veces) en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py`
- [X] T025 [US6] Implementar pruebas TDD para TRXPFL: verificar 60 columnas exactas, distribucion de tipos (7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType), cantidad de registros, integridad referencial (todos CUSTID existen en CMSTFL), formato TRXID (prefijo 4 chars + 8 digitos), distribucion de tipos de transaccion (~60%/~30%/~10% con tolerancia 5%) en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py`
- [X] T026 [US6] Implementar pruebas TDD para BLNCFL: verificar 100 columnas exactas, distribucion de tipos (2 LongType, 29 StringType, 34 DoubleType, 35 DateType), relacion 1:1 con CMSTFL (misma cantidad de registros, mismos CUSTIDs), distribucion de tipos de cuenta (AHRO 40%, CRTE 30%, PRES 20%, INVR 10% con tolerancia 3%) en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py`
- [X] T027 [US6] Implementar resumen final de pruebas con conteo total de pruebas PASS/FAIL, resultado global (todas pasan o hay fallos), y tiempo total de ejecucion de las pruebas en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py`

**Checkpoint**: Las pruebas TDD cubren todos los escenarios criticos de estructura, tipos, cantidades, distribuciones e integridad referencial y pasan exitosamente.

---

## Phase 9: Polish y Asuntos Transversales

**Proposito**: Validacion final y mejoras transversales

- [X] T028 [P] Validar flujo de ejecucion completo segun quickstart.md: CMSTFL primero, luego TRXPFL y BLNCFL (en cualquier orden entre si), finalmente TDD; verificar que todos los parquets se generan y las pruebas pasan
- [X] T029 Revision final de consistencia de codigo entre los 4 notebooks: convenciones de nombres en espanol, formato snake_case en variables, PascalCase con prefijo Nb en nombres de archivo, comentarios descriptivos en espanol, sin valores hardcodeados, sin spark.sparkContext

---

## Dependencias y Orden de Ejecucion

### Dependencias entre Fases

- **Setup (Phase 1)**: Sin dependencias — puede comenzar inmediatamente
- **Foundational (Phase 2)**: N/A — sin tareas bloqueantes
- **HU1 CMSTFL (Phase 3)**: Depende de Setup (Phase 1) — establece patrones de implementacion
- **HU2 TRXPFL (Phase 4)**: Depende de HU1 completada (reutiliza patrones de lectura de parametros, rutas, observabilidad)
- **HU3 BLNCFL (Phase 5)**: Depende de HU1 completada — **puede ejecutarse en paralelo con HU2** (archivo diferente, sin dependencias mutuas)
- **HU4 Observabilidad (Phase 6)**: Depende de que HU1, HU2 y HU3 esten completadas (verificacion transversal)
- **HU5 Storage Dinamico (Phase 7)**: Depende de que HU1, HU2 y HU3 esten completadas (verificacion transversal)
- **HU6 TDD (Phase 8)**: Depende de que HU1, HU2 y HU3 esten completadas (las pruebas verifican los 3 parquets)
- **Polish (Phase 9)**: Depende de todas las historias completadas

### Dependencias entre Historias de Usuario

- **HU1 (P1)**: Puede comenzar despues de Setup — **sin dependencias de otras historias**
- **HU2 (P1)**: Puede comenzar despues de HU1 — replica patrones de HU1 en archivo diferente
- **HU3 (P1)**: Puede comenzar despues de HU1 — **parallelizable con HU2** (archivo diferente)
- **HU4 (P2)**: Depende de HU1 + HU2 + HU3 (verificacion transversal)
- **HU5 (P2)**: Depende de HU1 + HU2 + HU3 (verificacion transversal)
- **HU6 (P2)**: Depende de HU1 + HU2 + HU3 (pruebas sobre los 3 parquets)

### Dentro de Cada Historia de Usuario

- Esqueleto del notebook primero (widgets, imports)
- Lectura de parametros y validaciones antes de generacion de datos
- Generacion de datos antes de escritura de parquet
- Observabilidad integrada en cada paso
- Escritura y metricas finales al cierre

### Oportunidades de Paralelismo

- HU2 (TRXPFL) y HU3 (BLNCFL) pueden desarrollarse **en paralelo** despues de completar HU1 (archivos diferentes)
- HU4 y HU5 pueden verificarse **en paralelo** (tareas independientes)
- T019 y T020 pueden ejecutarse en paralelo (verificaciones diferentes)
- Todo el TDD (HU6) es un solo archivo y se ejecuta secuencialmente

---

## Ejemplo de Paralelismo: HU2 y HU3 despues de HU1

```text
Despues de completar HU1 (CMSTFL):

Flujo A (HU2):                          Flujo B (HU3):
T009 Esqueleto TRXPFL                   T015 Esqueleto BLNCFL
T010 Parametros + validacion             T016 Parametros + validacion
T011 Observabilidad + lectura CMSTFL     T017 Generacion BLNCFL 100 cols
T012 Generacion TRXPFL 60 cols           T018 Escritura + observabilidad
T013 TRXID + integridad referencial
T014 Escritura + observabilidad
```

---

## Estrategia de Implementacion

### MVP Primero (Solo HU1 — Maestro de Clientes)

1. Completar Phase 1: Setup
2. Completar Phase 3: HU1 — CMSTFL
3. **DETENER Y VALIDAR**: Ejecutar el notebook CMSTFL, verificar 70 columnas, 50,000 registros, nombres correctos
4. Si funciona: CMSTFL es el bloque base para el resto del incremento

### Entrega Incremental

1. Completar Setup → Estructura lista
2. Completar HU1 → Validar CMSTFL independientemente → **MVP listo**
3. Completar HU2 + HU3 (en paralelo) → Validar TRXPFL y BLNCFL independientemente
4. Completar HU4 + HU5 → Verificar consistencia transversal
5. Completar HU6 → Todas las pruebas TDD pasan
6. Polish final → Incremento 2 completo

### Orden de Ejecucion en Databricks (Runtime)

```text
1. NbGenerarMaestroCliente.py       (CMSTFL — sin dependencias)
2. NbGenerarTransaccionalCliente.py  (TRXPFL — requiere CMSTFL)
   NbGenerarSaldosCliente.py         (BLNCFL — requiere CMSTFL, paralelo con TRXPFL)
3. NbTddGenerarParquets.py           (Pruebas — requiere los 3 parquets)
```

---

## Notas

- Todas las tareas se implementan en archivos `.py` con formato de notebook Databricks (celdas separadas por `# COMMAND ----------`)
- Todo el codigo y comentarios en **espanol**, variables en **snake_case**, archivos en **PascalCase con prefijo Nb**
- [P] = archivos diferentes, sin dependencias pendientes
- [US?] = vincula la tarea con la historia de usuario correspondiente del spec.md
- Cada historia de usuario es completable y verificable de forma independiente
- Hacer commit despues de cada tarea o grupo logico de tareas
- Detenerse en cualquier checkpoint para validar la historia de forma independiente
- Sin `spark.sparkContext`, sin acceso al JVM, sin valores hardcodeados, sin abfss://, sin /mnt/
