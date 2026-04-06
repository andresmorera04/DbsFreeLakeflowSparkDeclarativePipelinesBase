# Especificacion de Feature: Incremento 5 - LSDP Medalla de Oro - Vistas Materializadas

**Branch de Feature**: `005-lsdp-oro-vistas`
**Creado**: 2026-04-05
**Estado**: Draft
**Entrada**: Incremento 5 del proyecto DbsFreeLakeflowSparkDeclarativePipelinesBase — Incrementar el pipeline Lakeflow Spark Declarative Pipelines (LSDP) agregando el desarrollo y la transformacion de la medalla de oro a traves de vistas materializadas. La capa de oro constituye el producto de datos final solicitado por el area de negocio de clientes: analizar el comportamiento de los clientes con respecto a sus saldos y al uso de cajeros automaticos (ATM), determinando la cantidad de depositos y retiros, los promedios de montos depositados y retirados, y el total de pagos al saldo por cada cliente, para comprender si existe correlacion entre el uso de ATMs y los pagos al saldo.

## Escenarios de Usuario y Pruebas

### Historia de Usuario 1 - Vista Materializada de Comportamiento ATM por Cliente (Prioridad: P1)

El area de negocio de clientes necesita conocer el comportamiento de cada cliente con respecto al uso de cajeros automaticos (ATM). Al ejecutar el pipeline LSDP con la medalla de oro activa, se crea la vista materializada `comportamiento_atm_cliente` en `oro.regional` a partir de la vista materializada de plata `transacciones_enriquecidas`. Esta vista agrega las transacciones por cliente y calcula 5 metricas clave: cantidad de depositos ATM (DATM), cantidad de retiros ATM (CATM), promedio de montos de depositos ATM, promedio de montos de retiros ATM y total de pagos al saldo del cliente (PGSL). Estas metricas permiten al area de negocio evaluar si el movimiento de retiro o deposito en ATMs esta asociado con los pagos al saldo o si son comportamientos aislados.

**Por que esta prioridad**: Esta vista es el nucleo del producto de datos solicitado por el area de negocio. Sin ella, no es posible analizar el comportamiento ATM ni generar el resumen integral del cliente.

**Prueba Independiente**: Se ejecuta el pipeline LSDP apuntando a las vistas materializadas de plata ya pobladas. Se verifica que la vista `comportamiento_atm_cliente` exista en `oro.regional`, que contenga exactamente 6 columnas (identificador_cliente + 5 metricas), que los valores de las metricas sean coherentes con los datos de plata, y que los tipos de transaccion filtrados sean correctos (DATM para depositos ATM, CATM para retiros ATM, PGSL para pagos al saldo).

**Escenarios de Aceptacion**:

1. **Dado** que la vista materializada de plata `transacciones_enriquecidas` contiene datos con transacciones de tipo DATM, CATM y PGSL, **Cuando** se ejecuta el pipeline LSDP con la medalla de oro, **Entonces** se crea la vista materializada `comportamiento_atm_cliente` en `oro.regional` con 6 columnas: `identificador_cliente`, `cantidad_depositos_atm`, `cantidad_retiros_atm`, `promedio_monto_depositos_atm`, `promedio_monto_retiros_atm` y `total_pagos_saldo_cliente`.
2. **Dado** que un cliente tiene 10 transacciones de tipo DATM con montos de 100, 200, 300, 400, 500, 600, 700, 800, 900 y 1000, **Cuando** LSDP calcula las metricas, **Entonces** `cantidad_depositos_atm` es 10 y `promedio_monto_depositos_atm` es 550.0.
3. **Dado** que un cliente tiene 5 transacciones de tipo CATM con montos variados, **Cuando** LSDP calcula las metricas, **Entonces** `cantidad_retiros_atm` es 5 y `promedio_monto_retiros_atm` refleja el promedio correcto de dichos montos.
4. **Dado** que un cliente tiene transacciones de tipo PGSL (Pagos al Saldo), **Cuando** LSDP calcula las metricas, **Entonces** `total_pagos_saldo_cliente` refleja la suma total de los montos de las transacciones PGSL de ese cliente.
5. **Dado** que un cliente no tiene ninguna transaccion de tipo DATM, CATM o PGSL, **Cuando** LSDP calcula las metricas, **Entonces** las metricas correspondientes son 0 (cantidad) y 0.0 (promedios y totales), no nulas.
6. **Dado** que se agrega un nuevo lote de transacciones y se re-ejecuta el pipeline, **Cuando** LSDP actualiza la vista materializada de oro, **Entonces** las metricas reflejan la totalidad de las transacciones actualizadas por cada cliente.
7. **Dado** que la vista materializada esta creada, **Cuando** se consultan las metricas agrupadas por cliente, **Entonces** el area de negocio puede comparar directamente `cantidad_depositos_atm` y `cantidad_retiros_atm` contra `total_pagos_saldo_cliente` para identificar si existe correlacion o si son comportamientos aislados.

---

### Historia de Usuario 2 - Vista Materializada de Resumen Integral del Cliente (Prioridad: P1)

El area de negocio necesita una vista consolidada que combine los datos dimensionales del cliente (informacion personal, saldos, clasificacion de riesgo) con las metricas de comportamiento ATM, de forma que en una sola consulta se pueda obtener el perfil completo del cliente junto con su actividad en cajeros automaticos. Al ejecutar el pipeline LSDP, se crea la vista materializada `resumen_integral_cliente` en `oro.regional` mediante un INNER JOIN entre `clientes_saldos_consolidados` (plata) y `comportamiento_atm_cliente` (oro) por `identificador_cliente`. La vista tiene 22 columnas seleccionadas estrategicamente para proveer un panorama completo del cliente. Solo se incluyen clientes que existen en ambas fuentes (plata y oro).

**Por que esta prioridad**: Esta vista es el producto de datos definitivo que entrega al area de negocio la vision integral de cada cliente. Al combinar datos dimensionales con metricas transaccionales de ATM, habilita el analisis de correlacion solicitado entre comportamiento ATM y pagos al saldo.

**Prueba Independiente**: Se ejecuta el pipeline LSDP con todas las capas activas. Se verifica que la vista `resumen_integral_cliente` exista en `oro.regional`, que contenga 22 columnas, que el INNER JOIN con `comportamiento_atm_cliente` sea correcto y que solo se incluyan clientes presentes en ambas fuentes.

**Escenarios de Aceptacion**:

1. **Dado** que las vistas materializadas `clientes_saldos_consolidados` (plata) y `comportamiento_atm_cliente` (oro) contienen datos, **Cuando** se ejecuta el pipeline LSDP con la medalla de oro, **Entonces** se crea la vista materializada `resumen_integral_cliente` en `oro.regional` con 22 columnas.
2. **Dado** que la vista usa INNER JOIN entre `clientes_saldos_consolidados` y `comportamiento_atm_cliente`, **Cuando** un cliente existe en plata pero NO tiene ninguna transaccion en `transacciones_enriquecidas`, **Entonces** ese cliente NO aparece en `resumen_integral_cliente` porque no existe en `comportamiento_atm_cliente`. Los clientes que SI aparecen tienen metricas ATM en 0/0.0 si no tuvieron transacciones DATM, CATM o PGSL especificamente.
3. **Dado** que la vista materializada esta creada, **Cuando** se inspeccionan sus columnas, **Entonces** contiene exactamente 22 columnas que incluyen: datos de identificacion del cliente (huella, identificador, nombre, apellido), datos sociodemograficos (nacionalidad, pais, ciudad, ocupacion, nivel educativo), datos financieros (clasificacion de riesgo, categoria de saldo disponible, perfil de actividad bancaria, limite de credito, saldo disponible, fecha apertura cuenta), estado general (estado cuenta, tipo cuenta) y las 5 metricas ATM (cantidad depositos, cantidad retiros, promedio depositos, promedio retiros, total pagos saldo).
4. **Dado** que existe un cliente con huella de identificacion SHA2_256 generada en plata, **Cuando** se consulta el resumen integral, **Entonces** el campo `huella_identificacion_cliente` del resumen coincide exactamente con el de la vista de plata.
5. **Dado** que se actualizan los datos de plata y oro y se re-ejecuta el pipeline, **Cuando** LSDP actualiza la vista materializada, **Entonces** los datos reflejan la informacion mas reciente disponible por cada cliente (Dimension Tipo 1 heredada de plata).

---

### Historia de Usuario 3 - Propiedades Delta y Liquid Cluster en Vistas Materializadas de Oro (Prioridad: P2)

El ingeniero de datos verifica que cada vista materializada de oro fue creada con las propiedades Delta correctas, liquid cluster activo y los nombres de tablas y columnas en espanol con formato snake_case, segun los estandares del proyecto.

**Por que esta prioridad**: Las propiedades Delta y el liquid cluster impactan el rendimiento de las consultas. Los estandares de nomenclatura son requisitos del SDD pero no bloquean la funcionalidad principal.

**Prueba Independiente**: Se consultan las propiedades de cada vista materializada en Unity Catalog y se valida el cumplimiento de las propiedades Delta, el liquid cluster y la nomenclatura.

**Escenarios de Aceptacion**:

1. **Dado** que el pipeline LSDP se ejecuto exitosamente, **Cuando** se consultan las propiedades de `oro.regional.comportamiento_atm_cliente`, **Entonces** tiene activas: `delta.enableChangeDataFeed=true`, `delta.autoOptimize.autoCompact=true`, `delta.autoOptimize.optimizeWrite=true`, `delta.deletedFileRetentionDuration=interval 30 days` y `delta.logRetentionDuration=interval 60 days`.
2. **Dado** que el pipeline LSDP se ejecuto exitosamente, **Cuando** se consultan las propiedades de `oro.regional.resumen_integral_cliente`, **Entonces** tiene las mismas propiedades Delta que `comportamiento_atm_cliente`.
3. **Dado** que la vista `comportamiento_atm_cliente` fue creada, **Cuando** se consulta su liquid cluster, **Entonces** esta definido sobre el campo `identificador_cliente`.
4. **Dado** que la vista `resumen_integral_cliente` fue creada, **Cuando** se consulta su liquid cluster, **Entonces** esta definido sobre los campos `huella_identificacion_cliente` e `identificador_cliente`.
5. **Dado** que las vistas materializadas estan creadas, **Cuando** se inspeccionan los nombres de columnas, **Entonces** todos siguen el formato snake_case en espanol, son intuitivos y con minimo dos palabras.

---

### Historia de Usuario 4 - Observabilidad Completa del Pipeline de Oro (Prioridad: P2)

El ingeniero de datos necesita visibilidad total sobre la ejecucion de la medalla de oro. Al iniciarse cada script de transformacion de oro, se imprimen todos los parametros del pipeline, los valores leidos de la tabla Parametros, los catalogos y esquemas destino, y los nombres de las vistas materializadas que se van a crear.

**Por que esta prioridad**: La observabilidad es una politica fundamental del proyecto. Sin ella, el diagnostico de fallos en las metricas de agregacion o en la logica del INNER JOIN es extremadamente dificil.

**Prueba Independiente**: Se ejecuta el pipeline y se revisan los logs de salida para verificar que contienen: parametros del pipeline, valores de la tabla Parametros, catalogos y esquemas destino, nombres de vistas materializadas, columnas del liquid cluster y nombres de las metricas/columnas.

**Escenarios de Aceptacion**:

1. **Dado** que se inicia el pipeline LSDP de oro, **Cuando** se ejecuta la inicializacion del modulo, **Entonces** se imprimen en pantalla todos los parametros del pipeline recibidos via `spark.conf.get()`, los valores leidos de la tabla Parametros y los catalogos/esquemas destino de las vistas materializadas de oro.
2. **Dado** que se crea la vista `comportamiento_atm_cliente`, **Cuando** el script procesa los datos, **Entonces** se imprime: los nombres de las 5 metricas calculadas, el campo del liquid cluster y los tipos de transaccion filtrados (DATM, CATM, PGSL).
3. **Dado** que se crea la vista `resumen_integral_cliente`, **Cuando** el script procesa los datos, **Entonces** se imprime: la cantidad total de columnas (22), las fuentes de datos (plata clientes_saldos + oro comportamiento_atm) y los campos del liquid cluster.

> *Nota*: LSDP propaga nativamente los errores de las funciones decoradas con `@dp.materialized_view`. No se requiere manejo de errores explicito (`try/except`) dentro de los scripts de transformacion de oro. Los errores de runtime (lectura de Parametros, spark.read.table, JOIN) se reportan automaticamente en los logs del pipeline LSDP.

---

### Historia de Usuario 5 - Pruebas TDD para las Utilidades de Oro (Prioridad: P2)

El ingeniero de datos necesita un conjunto de pruebas TDD que validen el comportamiento de las utilidades reutilizables del pipeline. Las pruebas cubren EXCLUSIVAMENTE los modulos de `utilities/` (Python puro importable). Los notebooks de `transformations/` NO son testeables via TDD porque ejecutan codigo a nivel de modulo (patron Closure) que requiere un pipeline LSDP desplegado. Si se crean nuevas utilidades para la medalla de oro, deben estar cubiertas por TDD. Las utilidades existentes (LsdpConexionParametros, LsdpConstructorRutas, LsdpReordenarColumnasLiquidCluster) ya tienen TDD de incrementos anteriores.

**Por que esta prioridad**: El TDD es obligatorio a partir del Incremento 2 segun las politicas del proyecto. Las pruebas complementan la cobertura existente.

**Prueba Independiente**: Se ejecuta el archivo de TDD desde la extension Databricks para VS Code en Serverless Compute y se verifica que todas las pruebas pasen sin errores.

**Escenarios de Aceptacion**:

1. **Dado** que existen utilidades nuevas creadas para la medalla de oro, **Cuando** se ejecutan las pruebas TDD, **Entonces** cada utilidad nueva tiene al menos una prueba que valida su comportamiento correcto y su manejo de errores.
2. **Dado** que las utilidades existentes de incrementos anteriores ya tienen TDD, **Cuando** se ejecutan las pruebas del Incremento 5, **Entonces** las pruebas anteriores siguen pasando sin regresiones.
3. **Dado** que los notebooks de `transformations/` de oro ejecutan codigo a nivel de modulo (patron Closure), **Cuando** se intenta importar estos modulos desde TDD, **Entonces** se confirma que NO son testeables via TDD y su validacion se realiza unicamente mediante el despliegue del pipeline LSDP.

---

### Casos Borde

- Que sucede cuando la vista materializada de plata `transacciones_enriquecidas` esta vacia al momento de crear `comportamiento_atm_cliente`? La vista de oro debe crearse vacia pero funcional, sin lanzar error. Todas las metricas seran 0 o 0.0.
- Que sucede cuando la vista materializada de plata `clientes_saldos_consolidados` esta vacia? La vista `resumen_integral_cliente` se crea vacia pero funcional.
- Que sucede cuando un cliente existe en `clientes_saldos_consolidados` pero no tiene ninguna transaccion en `transacciones_enriquecidas`? Al usar INNER JOIN, el cliente NO aparece en `resumen_integral_cliente` porque no tiene contraparte en `comportamiento_atm_cliente`. Solo los clientes con al menos una transaccion de cualquier tipo aparecen en el resumen.
- Que sucede cuando un cliente tiene transacciones pero ninguna de tipo DATM, CATM ni PGSL? Las metricas ATM seran 0 (cantidad) y 0.0 (promedios y totales), pero el cliente SI aparece en `resumen_integral_cliente` con sus datos dimensionales de plata porque el `groupBy` sobre `transacciones_enriquecidas` incluye todos los clientes con transacciones.
- Que sucede cuando la tabla Parametros no tiene las claves `catalogoOro` o relacionadas? El script de oro debe usar valores por defecto razonables (oro, regional) e imprimir una advertencia clara indicando que se usaron valores por defecto.
- Que sucede cuando se re-ejecuta el pipeline completo (bronce + plata + oro)? Las vistas de oro deben reflejar los datos mas recientes sin duplicados (Dimension Tipo 1 heredada de plata).
- Que sucede cuando los montos de transacciones DATM o CATM son nulos? La funcion de agregacion debe manejar nulos correctamente: `F.count` excluye nulos, `F.avg` y `F.sum` manejan nulos nativamente en PySpark.
- Que sucede cuando el entorno Serverless de Databricks Free Edition agota sus recursos durante la creacion de las vistas de oro? LSDP reporta el error nativamente en los logs del pipeline.
- Que sucede cuando `comportamiento_atm_cliente` tiene un `identificador_cliente` que no existe en `clientes_saldos_consolidados`? Al usar INNER JOIN, ese cliente no aparecera en el resumen integral porque no tiene contraparte en plata. Este escenario no deberia ocurrir porque las transacciones provienen de clientes del maestro.

## Requisitos

### Requisitos Funcionales

- **RF-001**: El pipeline LSDP DEBE crear una vista materializada `comportamiento_atm_cliente` en el catalogo y esquema de oro (obtenidos de la tabla Parametros mediante las claves `catalogoOro` y `esquemaOro`, con valores por defecto `oro` y `regional` respectivamente si las claves no existen) usando el decorador `@dp.materialized_view` de `pyspark.pipelines` (importado como `from pyspark import pipelines as dp`).
- **RF-002**: La vista materializada `comportamiento_atm_cliente` DEBE leer de la vista materializada de plata `transacciones_enriquecidas` y calcular 5 metricas agregadas agrupadas por `identificador_cliente`:
  - `cantidad_depositos_atm`: conteo de transacciones de tipo DATM (depositos en cajero automatico).
  - `cantidad_retiros_atm`: conteo de transacciones de tipo CATM (retiros en cajero automatico).
  - `promedio_monto_depositos_atm`: promedio de la columna `monto_principal` de transacciones de tipo DATM.
  - `promedio_monto_retiros_atm`: promedio de la columna `monto_principal` de transacciones de tipo CATM.
  - `total_pagos_saldo_cliente`: suma total de la columna `monto_principal` de transacciones de tipo PGSL (pagos al saldo).
- **RF-003**: Las metricas de `comportamiento_atm_cliente` DEBEN calcularse usando agregacion condicional (`F.count(F.when(...))`, `F.avg(F.when(...))`, `F.sum(F.when(...))`) aplicando `groupBy("identificador_cliente")`. Los filtros por tipo de transaccion se aplican DENTRO de las funciones de agregacion (no como filtro previo sobre el DataFrame), lo que asegura que todos los clientes con transacciones aparezcan en el resultado incluyendo aquellos sin actividad ATM.
- **RF-004**: Cuando un cliente no tiene transacciones de un tipo especifico (DATM, CATM o PGSL), las metricas correspondientes DEBEN ser 0 (para cantidades) y 0.0 (para promedios y totales). Esto se logra con `F.coalesce` aplicando un literal por defecto sobre cada metrica despues de la agregacion.
- **RF-005**: El pipeline LSDP DEBE crear una vista materializada `resumen_integral_cliente` en el catalogo y esquema de oro (obtenidos de la tabla Parametros mediante las claves `catalogoOro` y `esquemaOro`, con valores por defecto `oro` y `regional` respectivamente) usando el decorador `@dp.materialized_view`.
- **RF-006**: La vista materializada `resumen_integral_cliente` DEBE combinar datos de `clientes_saldos_consolidados` (plata) y `comportamiento_atm_cliente` (oro) mediante un INNER JOIN por `identificador_cliente`. Solo se incluyen clientes que existen en ambas fuentes. Las 5 columnas de metricas ATM mantienen `F.coalesce` con valores por defecto (0 y 0.0) para clientes cuyos conteos o promedios de un tipo especifico resulten nulos por la agregacion condicional. Los clientes sin ninguna transaccion en `transacciones_enriquecidas` no aparecen en el resumen.
- **RF-007**: La vista materializada `resumen_integral_cliente` DEBE tener exactamente 22 columnas seleccionadas estrategicamente:
  - Identificacion del cliente (4 columnas): `huella_identificacion_cliente`, `identificador_cliente`, `nombre_cliente`, `apellido_cliente`.
  - Datos sociodemograficos (5 columnas): `nacionalidad_cliente`, `pais_residencia`, `ciudad_residencia`, `ocupacion_cliente`, `nivel_educativo`.
  - Datos financieros y clasificacion (6 columnas): `clasificacion_riesgo_cliente`, `categoria_saldo_disponible`, `perfil_actividad_bancaria`, `limite_credito`, `saldo_disponible`, `fecha_apertura_cuenta`.
  - Estado general (2 columnas): `estado_cuenta` y `tipo_cuenta`.
  - Metricas ATM (5 columnas): `cantidad_depositos_atm`, `cantidad_retiros_atm`, `promedio_monto_depositos_atm`, `promedio_monto_retiros_atm`, `total_pagos_saldo_cliente`.
- **RF-008**: Ambas vistas materializadas de oro DEBEN tener activas las siguientes propiedades Delta via el parametro `table_properties` del decorador `@dp.materialized_view`: `delta.enableChangeDataFeed=true`, `delta.autoOptimize.autoCompact=true`, `delta.autoOptimize.optimizeWrite=true`, `delta.deletedFileRetentionDuration=interval 30 days` y `delta.logRetentionDuration=interval 60 days`.
- **RF-009**: La vista `comportamiento_atm_cliente` DEBE tener liquid cluster activo via el parametro `cluster_by` del decorador `@dp.materialized_view` sobre el campo `identificador_cliente`.
- **RF-010**: La vista `resumen_integral_cliente` DEBE tener liquid cluster activo via el parametro `cluster_by` del decorador `@dp.materialized_view` sobre los campos `huella_identificacion_cliente` e `identificador_cliente`.
- **RF-011**: Todas las columnas de las vistas materializadas de oro DEBEN estar en espanol con formato snake_case, con nombres intuitivos y claros, conformados por dos palabras o silabas como minimo.
- **RF-012**: Cada script de oro DEBE implementar el patron Closure de forma autocontenida e independiente: al importar el script, lee sus propios parametros del pipeline via `spark.conf.get()`, invoca `LsdpConexionParametros` para leer la tabla Parametros. Estos valores se calculan una sola vez a nivel de modulo y quedan capturados por closure en la funcion decorada con `@dp.materialized_view`.
- **RF-013**: Los scripts de oro DEBEN importar las utilidades directamente con el prefijo de carpeta (`from utilities.LsdpConexionParametros import obtener_parametros`) SIN usar `sys.path.insert()` ni manipulacion manual de rutas. LSDP resuelve automaticamente los imports entre carpetas del pipeline.
- **RF-014**: Todo el codigo del pipeline DEBE ser 100% compatible con Databricks Serverless Compute. Queda prohibido el uso de `spark.sparkContext`, operaciones RDD, `.cache()`, `.persist()`, broadcasting, acumuladores y cualquier acceso al JVM del driver.
- **RF-015**: El pipeline NO DEBE usar la API legacy `dlt.*` (`import dlt`). Se exige exclusivamente la API nueva con decoradores de `pyspark.pipelines` (importado como `from pyspark import pipelines as dp`).
- **RF-016**: Todo el codigo debe ser dinamico via los parametros del pipeline y la tabla Parametros. Queda prohibido cualquier valor hardcodeado (rutas, nombres de catalogos, esquemas, tablas, campos, tipos de transaccion).
- **RF-017**: Los tipos de transaccion utilizados para las metricas ATM (DATM, CATM, PGSL) DEBEN leerse de la tabla Parametros con la clave `TiposTransaccionesLabBase`, cuyo valor es una cadena de tipos separados por coma (ejemplo: `DATM,CATM,PGSL`). El script de oro DEBE parsear esta cadena para obtener la lista de tipos. DEBE existir una utilidad nueva en `utilities/` llamada `LsdpInsertarTiposTransaccion` que inserte esta clave en la tabla Parametros de forma idempotente (solo si la clave no existe). Esta utilidad recibe como parametros: la sesion de spark, el catalogo, el esquema y el nombre de la tabla Parametros. La utilidad DEBE ser invocada desde el notebook que ejecuta el pipeline LSDP y DEBE tener cobertura TDD.
- **RF-018**: Cada script de transformacion de oro DEBE imprimir al inicio del modulo (nivel de closure, antes de las funciones decoradas): el nombre de las vistas materializadas que va a crear, los parametros del pipeline relevantes, el catalogo y esquema destino, los campos del liquid cluster y los nombres de las metricas o columnas. La observabilidad se concentra en los prints de inicializacion del modulo. LSDP propaga nativamente los errores de las funciones decoradas.
- **RF-019**: El archivo de transformacion de oro DEBE ubicarse en `src/LSDP_Laboratorio_Basico/transformations/` como `LsdpOroClientes.py`. Este unico archivo define ambas vistas materializadas de oro (`comportamiento_atm_cliente` y `resumen_integral_cliente`) ya que ambas comparten el mismo contexto de parametros y catalogo/esquema de oro.
- **RF-020**: El nombre del archivo LSDP DEBE usar formato PascalCase con el prefijo `Lsdp`. Todo el codigo y los comentarios deben estar en idioma espanol.
- **RF-021**: Las variables, constantes, funciones, metodos y objetos en Python DEBEN seguir el formato `snake_case` en minuscula.
- **RF-022**: Las vistas materializadas de oro NO DEBEN usar ZOrder ni PartitionBy. La unica estrategia de optimizacion es Liquid Cluster, definido via el parametro `cluster_by` del decorador `@dp.materialized_view`.
- **RF-023**: DEBE existir un conjunto de pruebas TDD que cubra cualquier utilidad nueva creada para la medalla de oro. Las pruebas deben ser ejecutables desde la extension Databricks para VS Code en Serverless Compute. El TDD cubre EXCLUSIVAMENTE los modulos de `utilities/` (Python puro importable). Los notebooks de `transformations/` de oro NO son testeables via TDD.
- **RF-024**: La lectura de las vistas materializadas de plata desde los scripts de oro DEBE hacerse usando `spark.read.table()` con el nombre completo de la vista (`catalogo.esquema.nombre_vista`) ya que las vistas de plata pertenecen a un catalogo y esquema diferente al de oro. Los nombres de catalogo y esquema de plata se obtienen de la tabla Parametros.
- **RF-025**: La lectura de `comportamiento_atm_cliente` desde el script que define `resumen_integral_cliente` DEBE hacerse usando `spark.read.table()` con el nombre completo de tres partes (`catalogo_oro.esquema_oro.comportamiento_atm_cliente`). Todas las tablas delta y vistas materializadas de plata y oro se leen SIEMPRE con nombre de 3 partes sin excepcion.
- **RF-026**: Los scripts de oro NO configuran `spark.sql.shuffle.partitions` mediante `spark.conf.set()`. El valor se deja al gestionado por defecto del entorno Serverless.
- **RF-027**: La vista materializada `comportamiento_atm_cliente` NO DEBE aplicar filtros previos sobre el DataFrame de entrada para seleccionar tipos de transaccion. Los filtros por tipo de transaccion se aplican EXCLUSIVAMENTE dentro de las funciones de agregacion condicional (`F.when(F.col("tipo_transaccion") == "DATM", ...)`). Esto permite que LSDP optimice la estrategia de carga automaticamente.
- **RF-028**: La vista materializada `resumen_integral_cliente` DEBE heredar la estrategia Dimension Tipo 1 de plata: siempre muestra la informacion mas reciente de cada cliente sin necesidad de logica adicional de deduplicacion, ya que `clientes_saldos_consolidados` ya resuelve esto.

### Entidades Clave

- **Vista Materializada comportamiento_atm_cliente** (`oro.regional.comportamiento_atm_cliente`): Vista materializada que agrega las transacciones por cliente desde `transacciones_enriquecidas` (plata) para calcular 5 metricas de comportamiento ATM. 6 columnas: identificador_cliente + 5 metricas. Liquid Cluster: `identificador_cliente`. Agregacion condicional por tipo de transaccion (DATM, CATM, PGSL), sin filtros previos sobre el DataFrame.
- **Vista Materializada resumen_integral_cliente** (`oro.regional.resumen_integral_cliente`): Vista materializada que combina datos dimensionales de `clientes_saldos_consolidados` (plata) con metricas ATM de `comportamiento_atm_cliente` (oro) mediante INNER JOIN por `identificador_cliente`. 22 columnas: identificacion (4) + sociodemograficos (5) + financieros (6) + estado (2) + metricas ATM (5). Liquid Cluster: `huella_identificacion_cliente`, `identificador_cliente`. Dimension Tipo 1 heredada de plata. Solo incluye clientes con al menos una transaccion.
- **Vista Materializada clientes_saldos_consolidados** (`plata.regional.clientes_saldos_consolidados`): Fuente de datos dimensionales del cliente. Creada en el Incremento 4. Contiene campos calculados de clasificacion e identificacion (huella SHA2_256). Dimension Tipo 1.
- **Vista Materializada transacciones_enriquecidas** (`plata.regional.transacciones_enriquecidas`): Fuente de datos transaccionales. Creada en el Incremento 4. Contiene todos los tipos de transaccion (DATM, CATM, PGSL, DPST, TINT, etc.) con columnas renombradas a espanol snake_case.
- **Tabla Parametros**: Tabla Delta existente en Unity Catalog con columnas `Clave` y `Valor`. Provee la configuracion dinamica del pipeline (catalogoOro, catalogoPlata, esquemaPlata, TiposTransaccionesLabBase). Creada en el Incremento 1. La clave `TiposTransaccionesLabBase` se inserta de forma idempotente via la utilidad `LsdpInsertarTiposTransaccion`.

### Parametros del Pipeline LSDP — Existentes y Nuevos para Oro

| Parametro | Tipo | Ejemplo | Descripcion | Nuevo/Existente |
| --- | --- | --- | --- | --- |
| catalogoParametro | string | control | Catalogo UC donde reside la tabla Parametros | Existente (Inc. 3) |
| esquemaParametro | string | regional | Esquema donde reside la tabla Parametros | Existente (Inc. 3) |
| tablaParametros | string | Parametros | Nombre de la tabla Parametros | Existente (Inc. 3) |

> *Nota*: Los parametros de bronce y plata siguen existiendo y siendo usados por sus respectivos scripts. Los scripts de oro obtienen `catalogoOro` y `esquemaOro` de la tabla Parametros (con valores por defecto `oro` y `regional`), siguiendo el mismo patron establecido en plata. Todas las tablas delta y vistas materializadas de plata y oro se leen SIEMPRE con nombre completo de 3 partes (`catalogo.esquema.nombre_vista`) sin excepcion. Los tipos de transaccion se leen de la tabla Parametros con la clave `TiposTransaccionesLabBase`.

## Criterios de Exito

### Resultados Medibles

- **CE-001**: Las dos vistas materializadas (`oro.regional.comportamiento_atm_cliente` y `oro.regional.resumen_integral_cliente`) se crean exitosamente en la primera ejecucion del pipeline.
- **CE-002**: La vista `comportamiento_atm_cliente` contiene exactamente 6 columnas y las metricas reflejan los valores correctos para el 100% de los clientes.
- **CE-003**: Las 5 metricas ATM (cantidad depositos, cantidad retiros, promedio depositos, promedio retiros, total pagos saldo) producen valores numericos coherentes con los datos de plata para el 100% de los registros.
- **CE-004**: La vista `resumen_integral_cliente` contiene exactamente 22 columnas y el INNER JOIN entre plata y oro es correcto para el 100% de los clientes incluidos.
- **CE-005**: Los clientes con transacciones pero sin actividad ATM especifica (sin DATM, CATM o PGSL) tienen metricas en 0 y 0.0, no en NULL. Los clientes sin ninguna transaccion no aparecen en el resumen (comportamiento esperado del INNER JOIN).
- **CE-006**: Todas las columnas de ambas vistas estan en espanol con formato snake_case y tienen nombres de al menos dos palabras.
- **CE-007**: Las propiedades Delta (Change Data Feed, autoCompact, optimizeWrite, retencion) estan activas en ambas vistas.
- **CE-008**: El pipeline completo (bronce + plata + oro) se ejecuta sin errores en Databricks Free Edition Serverless Compute.
- **CE-009**: Re-ejecutar el pipeline no genera datos duplicados ni errores (idempotencia).
- **CE-010**: Los logs de observabilidad contienen todos los parametros, catalogos, esquemas, metricas, columnas y campos del liquid cluster de cada vista materializada de oro.
- **CE-011**: El area de negocio puede obtener en una sola consulta al `resumen_integral_cliente` el perfil completo del cliente con sus metricas ATM para analizar la correlacion entre depositos/retiros ATM y pagos al saldo.
- **CE-012**: Las pruebas TDD existentes de incrementos anteriores siguen pasando sin regresiones.

## Clarificaciones

### Sesion 2026-04-05

- Q: De donde se obtienen el catalogo y esquema destino de las vistas de oro? La spec mezclaba tabla Parametros y pipeline params. -> A: Leer ambos de la tabla Parametros con claves `catalogoOro` y `esquemaOro`, con valores por defecto `oro` y `regional`. Patron identico a plata.
- Q: Que columna usar para calcular promedios y totales de las metricas ATM: `monto_principal` (bruto) o `monto_neto_comisiones` (neto)? -> A: Usar `monto_principal` (TRXAMT original). Refleja el monto bruto de la operacion ATM, mas directo para comprender el volumen real de actividad. Las comisiones son un concepto separado que no afecta la pregunta de negocio.

### Decisiones del Research 2026-04-05

- Decision 1 (Agregacion Condicional): APROBADA. Usar `groupBy` + `F.count/avg/sum(F.when(...))` sin filtros previos.
- Decision 2 (F.coalesce para Nulos): APROBADA. Aplicar `F.coalesce` con defaults 0/0.0 en ambas vistas.
- Decision 3 (JOIN y Nombres de 3 Partes): MODIFICADA. Se usa INNER JOIN (no LEFT JOIN) entre plata y oro. Todas las tablas delta y vistas materializadas de plata y oro se leen SIEMPRE con nombre completo de 3 partes (`catalogo.esquema.tabla`) sin excepcion.
- Decision 4 (Parametros catalog/schema en Decorador): APROBADA. Usar `catalog=catalogo_oro` y `schema=esquema_oro` del closure.
- Decision 5 (Propiedades Delta y Liquid Cluster): APROBADA. Mismo diccionario de propiedades para ambas vistas. Liquid cluster segun campos definidos.
- Decision 6 (Tipos de Transaccion): MODIFICADA. Los tipos se almacenan en la tabla Parametros con clave `TiposTransaccionesLabBase` y valor separado por comas (`DATM,CATM,PGSL`). Se crea nueva utilidad `LsdpInsertarTiposTransaccion` en `utilities/` idempotente que valida si la clave ya existe antes de insertar. Recibe spark, catalogo, esquema y nombre de tabla Parametros. Se invoca desde el notebook LSDP. Requiere TDD.
- Decision 7 (Archivo Unico): APROBADA. Ambas vistas materializadas de oro se desarrollan en un mismo archivo `LsdpOroClientes.py`.

## Supuestos

- Las vistas materializadas de plata (`clientes_saldos_consolidados`, `transacciones_enriquecidas`) estan creadas y pobladas por el Incremento 4 y disponibles en `plata.regional`.
- La tabla Parametros existe en Unity Catalog con los registros necesarios, incluyendo `catalogoOro`, `esquemaOro`, `catalogoPlata` y `esquemaPlata`.
- Las utilidades existentes (`LsdpConexionParametros`, `LsdpConstructorRutas`, `LsdpReordenarColumnasLiquidCluster`) estan disponibles y funcionales desde incrementos anteriores.
- El computo Serverless esta disponible en el workspace de Databricks Free Edition.
- LSDP resuelve automaticamente los imports entre carpetas del pipeline (utilities, transformations).
- Los catalogos `oro` y su esquema `regional` fueron creados en el Incremento 1 por el notebook de configuracion inicial.
- Los tipos de transaccion DATM (deposito ATM), CATM (retiro ATM) y PGSL (pago al saldo) existen en los datos generados por el Incremento 2 y propagados a traves de bronce y plata.
- La columna `tipo_transaccion` de plata corresponde al renombramiento de `TRXTYP` de bronce y contiene los codigos de tipo de transaccion originales del AS400.
- La columna `monto_principal` de plata corresponde al renombramiento de `TRXAMT` de bronce y es la base para calcular promedios y totales de las metricas ATM.
- Se requiere una nueva utilidad `LsdpInsertarTiposTransaccion` en `utilities/` para insertar de forma idempotente la clave `TiposTransaccionesLabBase` en la tabla Parametros con el valor `DATM,CATM,PGSL`. Esta utilidad debe tener cobertura TDD.
- Los tipos de transaccion se leen de la tabla Parametros (clave `TiposTransaccionesLabBase`) y se parsean como lista separada por comas.
- El catalogo y esquema de oro se leen de la tabla Parametros con las claves `catalogoOro` y `esquemaOro` (con valores por defecto `oro` y `regional`), siguiendo el mismo patron establecido en los scripts de plata.
