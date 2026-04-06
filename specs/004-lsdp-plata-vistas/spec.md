# Especificacion de Feature: Incremento 4 - LSDP Medalla de Plata - Vistas Materializadas

**Branch de Feature**: `004-lsdp-plata-vistas`
**Creado**: 2026-04-05
**Estado**: Draft
**Entrada**: Incremento 4 del proyecto DbsFreeLakeflowSparkDeclarativePipelinesBase — Incrementar el pipeline Lakeflow Spark Declarative Pipelines (LSDP) agregando el desarrollo y la transformacion de la medalla de plata a traves de vistas materializadas. La vista consolidada de clientes y saldos no debe tener columnas duplicadas entre cmstfl y blncfl. La vista transaccional debe tener toda la configuracion necesaria para facilitar cargas incrementales por parte de LSDP.

## Escenarios de Usuario y Pruebas

### Historia de Usuario 1 - Vista Materializada Consolidada de Clientes y Saldos (Prioridad: P1)

El ingeniero de datos despliega el pipeline LSDP con la medalla de plata activa. Al ejecutar el pipeline, LSDP crea la vista materializada `clientes_saldos_consolidados` en `plata.lab1` que consolida las streaming tables de bronce `cmstfl` (Maestro de Clientes) y `blncfl` (Saldos de Clientes) mediante un JOIN por CUSTID, aplicando Dimension Tipo 1 (siempre los datos mas recientes por cada cliente). La vista NO contiene columnas duplicadas: para las columnas que existen en ambas streaming tables de bronce (CUSTID, FechaIngestaDatos, _rescued_data), se aplica la regla de deduplicacion — CUSTID y las columnas de identificacion personal/sociodemografica se toman de cmstfl, y las columnas financieras/de saldos se toman de blncfl. Las columnas `FechaIngestaDatos` y `_rescued_data` de ambas tablas se descartan (RF-028). La vista incluye 4 campos calculados: `clasificacion_riesgo_cliente`, `categoria_saldo_disponible`, `perfil_actividad_bancaria` y `huella_identificacion_cliente`.

**Por que esta prioridad**: La vista consolidada de clientes y saldos es el corazon del producto de datos solicitado por el area de negocio. Sin ella, no es posible construir el `resumen_integral_cliente` de oro que es el entregable final del proyecto.

**Prueba Independiente**: Se ejecuta el pipeline LSDP apuntando a las streaming tables de bronce ya pobladas. Se verifica que la vista materializada exista en `plata.lab1`, que no tenga columnas duplicadas entre cmstfl y blncfl, que contenga los 4 campos calculados, que aplique Dimension Tipo 1 (solo el registro mas reciente por CUSTID) y que los valores de los campos calculados sean correctos segun la logica definida.

**Escenarios de Aceptacion**:

1. **Dado** que las streaming tables de bronce `cmstfl` y `blncfl` contienen datos, **Cuando** se ejecuta el pipeline LSDP con la medalla de plata, **Entonces** se crea la vista materializada `clientes_saldos_consolidados` en `plata.lab1` con la union de columnas de ambas tablas sin duplicados.
2. **Dado** que un CUSTID tiene multiples registros en `cmstfl` (por ingesta historica append-only), **Cuando** LSDP procesa la vista materializada, **Entonces** solo se toma el registro mas reciente de cada CUSTID (determinado por `FechaIngestaDatos` mas alto) tanto de cmstfl como de blncfl, implementando Dimension Tipo 1.
3. **Dado** que la vista materializada esta creada, **Cuando** se inspeccionan sus columnas, **Entonces** no existen columnas duplicadas: CUSTID aparece una sola vez (tomado de cmstfl), las columnas `FechaIngestaDatos` y `_rescued_data` de cada tabla origen fueron descartadas (RF-028), y las columnas exclusivas de cada tabla se incluyen renombradas a espanol snake_case.
4. **Dado** que la vista materializada esta creada, **Cuando** se verifican los campos calculados, **Entonces** existen: `clasificacion_riesgo_cliente` (basado en al menos 3 columnas de bronce con logica CASE), `categoria_saldo_disponible` (basado en al menos 3 columnas de bronce con logica CASE), `perfil_actividad_bancaria` (basado en al menos 3 columnas de bronce con logica CASE) y `huella_identificacion_cliente` (SHA2_256 del identificador de cliente).
5. **Dado** que se agrega un nuevo lote de parquets y se re-ejecuta el pipeline, **Cuando** LSDP actualiza la vista materializada, **Entonces** los datos reflejan los registros mas recientes de cada cliente sin duplicados.
6. **Dado** que la vista materializada esta creada, **Cuando** se inspeccionan las columnas, **Entonces** las columnas `_rescued_data`, `año`, `mes`, `dia` y `FechaIngestaDatos` NO aparecen en la vista; fueron descartadas durante la transformacion.
7. **Dado** que la vista materializada esta creada con datos, **Cuando** se validan las expectativas de calidad de datos, **Entonces** se cumplen las siguientes reglas:
   - El limite de credito es mayor que 0 para todos los registros.
   - El identificador del cliente NO es nulo para ningun registro.
   - El limite de credito NO es nulo para ningun registro.
   - La fecha de apertura de la cuenta es posterior al 31 de diciembre de 2020 para todos los registros.
   - La fecha de nacimiento es anterior al 1 de enero de 2009 para todos los registros.

---

### Historia de Usuario 2 - Vista Materializada Transaccional Enriquecida (Prioridad: P1)

El ingeniero de datos necesita que el pipeline LSDP cree la vista materializada `transacciones_enriquecidas` en `plata.lab1` a partir de la streaming table de bronce `trxpfl`. La vista NO aplica filtros sobre los datos de bronce, lo cual permite que LSDP decida automaticamente realizar cargas incrementales en lugar de cargas completas. La configuracion de la vista debe facilitar al maximo las cargas incrementales. La vista incluye 4 campos calculados numericos: `monto_neto_comisiones`, `porcentaje_comision_sobre_monto`, `variacion_saldo_transaccion` e `indicador_impacto_financiero`.

**Por que esta prioridad**: La vista transaccional enriquecida es la fuente directa de la vista `comportamiento_atm_cliente` de oro, que es el producto de datos central solicitado por el area de negocio para analizar el comportamiento de depositos y retiros en cajeros automaticos.

**Prueba Independiente**: Se ejecuta el pipeline LSDP apuntando a la streaming table de bronce `trxpfl` ya poblada. Se verifica que la vista materializada exista en `plata.lab1`, que no aplique filtros, que contenga los 4 campos calculados numericos y que los valores calculados sean coherentes con los datos fuente.

**Escenarios de Aceptacion**:

1. **Dado** que la streaming table de bronce `trxpfl` contiene datos, **Cuando** se ejecuta el pipeline LSDP con la medalla de plata, **Entonces** se crea la vista materializada `transacciones_enriquecidas` en `plata.lab1` con todas las columnas de la streaming table de bronce mas los 4 campos calculados.
2. **Dado** que la vista materializada no aplica filtros, **Cuando** LSDP evalua la estrategia de carga, **Entonces** LSDP realiza cargas incrementales automaticamente en lugar de cargas completas, procesando solo los datos nuevos desde la ultima ejecucion.
3. **Dado** que la vista materializada esta creada, **Cuando** se verifican los campos calculados, **Entonces** existen: `monto_neto_comisiones` (basado en al menos 2 columnas numericas), `porcentaje_comision_sobre_monto` (basado en al menos 2 columnas numericas), `variacion_saldo_transaccion` (basado en al menos 2 columnas numericas) e `indicador_impacto_financiero` (basado en al menos 2 columnas numericas).
4. **Dado** que se agrega un nuevo lote de transacciones y se re-ejecuta el pipeline, **Cuando** LSDP actualiza la vista materializada, **Entonces** solo los registros nuevos son procesados y enriquecidos con los campos calculados, sin reprocesar los registros previamente cargados.
5. **Dado** que la vista materializada tiene todas las transacciones sin filtrar, **Cuando** el area de negocio consulta los datos, **Entonces** puede filtrar por tipo de transaccion (CATM, DATM, DPST, PGSL, etc.) y fecha directamente sobre la vista.
6. **Dado** que la vista materializada esta creada, **Cuando** se inspeccionan las columnas, **Entonces** las columnas `_rescued_data`, `año`, `mes`, `dia` y `FechaIngestaDatos` NO aparecen en la vista; fueron descartadas durante la transformacion.
7. **Dado** que la vista materializada esta creada con datos, **Cuando** se validan las expectativas de calidad de datos, **Entonces** se cumplen las siguientes reglas:
   - La moneda de la transaccion NO es nula para ningun registro.
   - El monto neto NO es nulo para ningun registro.
   - El monto neto es mayor que 0 para todos los registros.
   - El identificador del cliente NO es nulo para ningun registro.

---

### Historia de Usuario 3 - Propiedades Delta y Liquid Cluster en Vistas Materializadas de Plata (Prioridad: P2)

El ingeniero de datos verifica que cada vista materializada de plata fue creada con las propiedades Delta correctas, liquid cluster activo y los nombres de tablas y columnas en espanol con formato snake_case, segun los estandares del proyecto.

**Por que esta prioridad**: Las propiedades Delta y el liquid cluster impactan el rendimiento de las consultas y la mantenibilidad. Los estandares de nomenclatura son requisitos del SDD pero no bloquean la funcionalidad principal.

**Prueba Independiente**: Se consultan las propiedades de cada vista materializada en Unity Catalog y se valida el cumplimiento de las propiedades Delta, el liquid cluster y la nomenclatura.

**Escenarios de Aceptacion**:

1. **Dado** que el pipeline LSDP se ejecuto exitosamente, **Cuando** se consultan las propiedades de `plata.lab1.clientes_saldos_consolidados`, **Entonces** tiene activas: `delta.enableChangeDataFeed=true`, `delta.autoOptimize.autoCompact=true`, `delta.autoOptimize.optimizeWrite=true`, `delta.deletedFileRetentionDuration=interval 30 days` y `delta.logRetentionDuration=interval 60 days`.
2. **Dado** que el pipeline LSDP se ejecuto exitosamente, **Cuando** se consultan las propiedades de `plata.lab1.transacciones_enriquecidas`, **Entonces** tiene las mismas propiedades Delta que `clientes_saldos_consolidados`.
3. **Dado** que la vista `clientes_saldos_consolidados` fue creada, **Cuando** se consulta su liquid cluster, **Entonces** esta definido sobre los campos `huella_identificacion_cliente` e `identificador_cliente`.
4. **Dado** que la vista `transacciones_enriquecidas` fue creada, **Cuando** se consulta su liquid cluster, **Entonces** esta definido sobre los campos `fecha_transaccion`, `identificador_cliente` y `tipo_transaccion`.
5. **Dado** que las vistas materializadas estan creadas, **Cuando** se inspeccionan los nombres de columnas, **Entonces** todos siguen el formato snake_case en espanol, son intuitivos, con minimo dos palabras y no coinciden con los nombres de las columnas de bronce.

---

### Historia de Usuario 4 - Observabilidad Completa del Pipeline de Plata (Prioridad: P2)

El ingeniero de datos necesita visibilidad total sobre la ejecucion de la medalla de plata. Al iniciarse cada script de transformacion, se imprimen todos los parametros del pipeline, los valores leidos de la tabla Parametros, los catalogos y esquemas destino, y los nombres de las vistas materializadas que se van a crear. Durante la transformacion, se imprime la informacion de los campos calculados y las columnas del liquid cluster. Al finalizar, se imprime un resumen de la transformacion completa.

**Por que esta prioridad**: La observabilidad es una politica fundamental del proyecto. Sin ella, el diagnostico de fallos en los campos calculados o en la logica de Dimension Tipo 1 es extremadamente dificil.

**Prueba Independiente**: Se ejecuta el pipeline y se revisan los logs de salida para verificar que contienen: parametros del pipeline, valores de la tabla Parametros, catalogos y esquemas destino, nombres de vistas materializadas, campos calculados y campos del liquid cluster.

**Escenarios de Aceptacion**:

1. **Dado** que se inicia el pipeline LSDP de plata, **Cuando** se ejecuta la inicializacion del modulo, **Entonces** se imprimen en pantalla todos los parametros del pipeline recibidos via `spark.conf.get()`, los valores leidos de la tabla Parametros y los catalogos/esquemas destino de las vistas materializadas.
2. **Dado** que se crea la vista `clientes_saldos_consolidados`, **Cuando** el script procesa los datos, **Entonces** se imprime: la cantidad de columnas resultantes, los nombres de los 4 campos calculados, los campos del liquid cluster y la confirmacion de que la deduplicacion de columnas fue exitosa.
3. **Dado** que se crea la vista `transacciones_enriquecidas`, **Cuando** el script procesa los datos, **Entonces** se imprime: la cantidad de columnas resultantes, los nombres de los 4 campos calculados, los campos del liquid cluster y la confirmacion de que no se aplicaron filtros.
4. **Dado** que ocurre un error en la transformacion, **Cuando** el error es capturado, **Entonces** se imprime un mensaje descriptivo con el tipo de error y el contexto relevante antes de detener la ejecucion.

---

### Historia de Usuario 5 - Pruebas TDD para las Utilidades de Plata (Prioridad: P2)

El ingeniero de datos necesita un conjunto de pruebas TDD que validen el comportamiento de las utilidades reutilizables del pipeline. Las pruebas cubren EXCLUSIVAMENTE los modulos de `utilities/` (Python puro importable). Los notebooks de `transformations/` NO son testeables via TDD porque ejecutan codigo a nivel de modulo (patron Closure) que requiere un pipeline LSDP desplegado. Si se crean nuevas utilidades para la medalla de plata, deben estar cubiertas por TDD. Las utilidades existentes (LsdpConexionParametros, LsdpConstructorRutas, LsdpReordenarColumnasLiquidCluster) ya tienen TDD del Incremento 3.

**Por que esta prioridad**: El TDD es obligatorio a partir del Incremento 2 segun las politicas del proyecto. Las pruebas complementan la cobertura existente del Incremento 3.

**Prueba Independiente**: Se ejecuta el archivo de TDD desde la extension Databricks para VS Code en Serverless Compute y se verifica que todas las pruebas pasen sin errores.

**Escenarios de Aceptacion**:

1. **Dado** que existen utilidades nuevas creadas para la medalla de plata, **Cuando** se ejecutan las pruebas TDD, **Entonces** cada utilidad nueva tiene al menos una prueba que valida su comportamiento correcto y su manejo de errores.
2. **Dado** que las utilidades existentes del Incremento 3 ya tienen TDD, **Cuando** se ejecutan las pruebas del Incremento 4, **Entonces** las pruebas del Incremento 3 siguen pasando sin regresiones.
3. **Dado** que los notebooks de `transformations/` de plata ejecutan codigo a nivel de modulo (patron Closure), **Cuando** se intenta importar estos modulos desde TDD, **Entonces** se confirma que NO son testeables via TDD y su validacion se realiza unicamente mediante el despliegue del pipeline LSDP.

---

### Casos Borde

- Que sucede cuando la streaming table de bronce `cmstfl` o `blncfl` esta vacia al momento de crear la vista materializada? La vista materializada debe crearse vacia pero funcional, sin lanzar error.
- Que sucede cuando un CUSTID existe en `cmstfl` pero no en `blncfl` (o viceversa)? Al usar LEFT JOIN con cmstfl como base, un cliente sin saldos se incluye con columnas de saldos en NULL. Un saldo sin maestro (CUSTID solo en blncfl) se excluye de la vista consolidada.
- Que sucede cuando los campos fuente de los campos calculados tienen valores nulos? La logica CASE de cada campo calculado debe manejar valores nulos de forma explicita, produciendo un valor por defecto o un resultado coherente.
- Que sucede cuando `FechaIngestaDatos` tiene el mismo valor para multiples registros del mismo CUSTID? La logica de Dimension Tipo 1 debe tener un criterio de desempate determinista.
- Que sucede cuando la tabla Parametros no tiene las claves `catalogoPlata` o relacionadas? El script de plata debe lanzar excepcion explicativa indicando la clave faltante.
- Que sucede cuando el entorno Serverless de Databricks Free Edition agota sus recursos durante la creacion de la vista materializada? El pipeline debe capturar el error y reportarlo claramente.
- Que sucede cuando se re-ejecuta el pipeline y la streaming table de bronce `trxpfl` tiene nuevos registros? LSDP debe procesar incrementalmente solo los nuevos registros en la vista `transacciones_enriquecidas`, sin reprocesar los anteriores.
- Que sucede cuando un registro de bronce tiene el limite de credito en 0 o negativo? La expectativa de calidad de datos (`@dp.expect`) registra la violacion en las metricas de LSDP pero conserva el registro en la vista (modo observacional).
- Que sucede cuando un registro de bronce tiene la fecha de nacimiento posterior al 1 de enero de 2009 (menores de edad)? La expectativa de calidad de datos (`@dp.expect`) registra la violacion pero conserva el registro.
- Que sucede cuando un registro transaccional tiene el monto neto en 0 o negativo? La expectativa de calidad de datos (`@dp.expect`) registra la violacion pero conserva el registro.
- Que sucede cuando las columnas `año`, `mes` o `dia` no existen en la tabla de bronce al momento de aplicar la exclusion? La transformacion debe manejar graciosamente la ausencia de columnas opcionales sin lanzar error.

## Requisitos

### Requisitos Funcionales

- **RF-001**: El pipeline LSDP DEBE crear una vista materializada `clientes_saldos_consolidados` en el catalogo y esquema de plata (obtenidos de la tabla Parametros mediante las claves `catalogoPlata` y `esquemaPlata`) usando el decorador `@dp.materialized_view` de `pyspark.pipelines` (importado como `from pyspark import pipelines as dp`).
- **RF-002**: La vista materializada `clientes_saldos_consolidados` DEBE consolidar las streaming tables de bronce `cmstfl` y `blncfl` mediante un LEFT JOIN por CUSTID (cmstfl como tabla base), aplicando Dimension Tipo 1: se toma unicamente el registro mas reciente de cada CUSTID (determinado por el `FechaIngestaDatos` mas alto) tanto de cmstfl como de blncfl antes del JOIN. Todos los clientes del maestro se incluyen; si un CUSTID no tiene saldos en blncfl, las columnas de saldos quedan en NULL.
- **RF-003**: La vista materializada `clientes_saldos_consolidados` NO DEBE tener columnas duplicadas entre las streaming tables de bronce cmstfl y blncfl. Las columnas comunes se resuelven asi: CUSTID se toma de cmstfl; las columnas `FechaIngestaDatos`, `_rescued_data`, `año`, `mes` y `dia` de ambas tablas se descartan (segun RF-028). No existen otras columnas comunes entre los parquets originales de CMSTFL y BLNCFL. Las columnas exclusivas de cada tabla se incluyen renombradas a espanol con formato snake_case.
- **RF-004**: La vista materializada `clientes_saldos_consolidados` DEBE incluir 4 campos calculados:
  - `clasificacion_riesgo_cliente`: basado en logica CASE con al menos 3 columnas de bronce que representen indicadores de riesgo del cliente.
  - `categoria_saldo_disponible`: basado en logica CASE con al menos 3 columnas de bronce que representen informacion de saldos.
  - `perfil_actividad_bancaria`: basado en logica CASE con al menos 3 columnas de bronce que representen indicadores de actividad del cliente.
  - `huella_identificacion_cliente`: generado con SHA2_256 sobre el identificador de cliente unico.
- **RF-005**: El pipeline LSDP DEBE crear una vista materializada `transacciones_enriquecidas` en el catalogo y esquema de plata (obtenidos de la tabla Parametros mediante las claves `catalogoPlata` y `esquemaPlata`) usando el decorador `@dp.materialized_view` de `pyspark.pipelines`.
- **RF-006**: La vista materializada `transacciones_enriquecidas` DEBE leer de la streaming table de bronce `trxpfl` SIN aplicar filtros sobre los datos, lo cual permite que LSDP decida automaticamente realizar cargas incrementales en lugar de cargas completas. No se DEBE usar `.filter()`, `.where()` ni ninguna condicion que restrinja los registros leidos de bronce.
- **RF-007**: La vista materializada `transacciones_enriquecidas` DEBE tener toda la configuracion necesaria para facilitar al maximo que LSDP realice cargas incrementales y no completas. Esto implica: no aplicar filtros, no usar funciones que fuercen refrescamiento completo y aprovechar el paradigma declarativo para que LSDP optimice automaticamente la estrategia de carga.
- **RF-008**: La vista materializada `transacciones_enriquecidas` DEBE incluir 4 campos calculados numericos:
  - `monto_neto_comisiones`: calculado a partir de al menos 2 columnas numericas de bronce (monto de la transaccion menos comisiones).
  - `porcentaje_comision_sobre_monto`: calculado a partir de al menos 2 columnas numericas de bronce (proporcion de la comision respecto al monto).
  - `variacion_saldo_transaccion`: calculado a partir de al menos 2 columnas numericas de bronce (diferencia entre saldo posterior y anterior a la transaccion).
  - `indicador_impacto_financiero`: calculado a partir de al menos 2 columnas numericas de bronce (indicador compuesto del impacto financiero de la transaccion).
- **RF-009**: Ambas vistas materializadas DEBEN tener activas las siguientes propiedades Delta via el parametro `table_properties` del decorador `@dp.materialized_view`: `delta.enableChangeDataFeed=true`, `delta.autoOptimize.autoCompact=true`, `delta.autoOptimize.optimizeWrite=true`, `delta.deletedFileRetentionDuration=interval 30 days` y `delta.logRetentionDuration=interval 60 days`.
- **RF-010**: La vista `clientes_saldos_consolidados` DEBE tener liquid cluster activo via el parametro `cluster_by` del decorador `@dp.materialized_view` sobre los campos `huella_identificacion_cliente` e `identificador_cliente`.
- **RF-011**: La vista `transacciones_enriquecidas` DEBE tener liquid cluster activo via el parametro `cluster_by` del decorador `@dp.materialized_view` sobre los campos `fecha_transaccion`, `identificador_cliente` y `tipo_transaccion`.
- **RF-012**: Todas las columnas de las vistas materializadas de plata DEBEN renombrarse a espanol con formato snake_case, con nombres intuitivos y claros, conformados por dos palabras o silabas como minimo. Los nombres de las columnas NO DEBEN coincidir con los nombres de las columnas de bronce.
- **RF-013**: Cada script de plata DEBE implementar el patron Closure de forma autocontenida e independiente: al importar el script, lee sus propios parametros del pipeline via `spark.conf.get()`, invoca `LsdpConexionParametros` para leer la tabla Parametros. Estos valores se calculan una sola vez a nivel de modulo y quedan capturados por closure en la funcion decorada con `@dp.materialized_view`.
- **RF-014**: Los scripts de plata DEBEN importar las utilidades directamente con el prefijo de carpeta (`from utilities.LsdpConexionParametros import obtener_parametros`) SIN usar `sys.path.insert()` ni manipulacion manual de rutas. LSDP resuelve automaticamente los imports entre carpetas del pipeline.
- **RF-015**: Todo el codigo del pipeline DEBE ser 100% compatible con Databricks Serverless Compute. Queda prohibido el uso de `spark.sparkContext`, operaciones RDD, `.cache()`, `.persist()`, broadcasting, acumuladores y cualquier acceso al JVM del driver.
- **RF-016**: El pipeline NO DEBE usar la API legacy `dlt.*` (`import dlt`). Se exige exclusivamente la API nueva con decoradores de `pyspark.pipelines` (importado como `from pyspark import pipelines as dp`).
- **RF-017**: Todo el codigo debe ser dinamico via los parametros del pipeline y la tabla Parametros. Queda prohibido cualquier valor hardcodeado (rutas, nombres de catalogos, esquemas, tablas, campos).
- **RF-018**: Cada script de transformacion de plata DEBE imprimir al inicio: el nombre de la vista materializada que va a crear, los parametros del pipeline relevantes, el catalogo y esquema destino, los campos del liquid cluster y los nombres de los campos calculados. Al finalizar, debe imprimir confirmacion de la creacion exitosa.
- **RF-019**: Los archivos de transformacion de plata DEBEN ubicarse en `src/LSDP_Laboratorio_Basico/transformations/` como `LsdpPlataClientesSaldos.py` y `LsdpPlataTransacciones.py`. Si se crean utilidades nuevas, DEBEN ubicarse en `src/LSDP_Laboratorio_Basico/utilities/`.
- **RF-020**: Los nombres de los archivos LSDP DEBEN usar formato PascalCase con el prefijo `Lsdp`. Todo el codigo y los comentarios deben estar en idioma espanol.
- **RF-021**: Las variables, constantes, funciones, metodos y objetos en Python DEBEN seguir el formato `snake_case` en minuscula.
- **RF-022**: Las vistas materializadas NO DEBEN usar ZOrder ni PartitionBy. La unica estrategia de optimizacion de acceso y clustering es Liquid Cluster, definido exclusivamente via el parametro `cluster_by` del decorador `@dp.materialized_view`.
- **RF-023**: DEBE existir un conjunto de pruebas TDD que cubra cualquier utilidad nueva creada para la medalla de plata. Las pruebas deben ser ejecutables desde la extension Databricks para VS Code en Serverless Compute. El TDD cubre EXCLUSIVAMENTE los modulos de `utilities/` (Python puro importable). Los notebooks de `transformations/` de plata NO son testeables via TDD.
- **RF-024**: La lectura de las streaming tables de bronce desde los scripts de plata DEBE hacerse usando `spark.read.table("<nombre_tabla>")`, referenciando las tablas por nombre simple ya que pertenecen al mismo pipeline LSDP y se resuelven automaticamente mediante el catalogo y esquema por defecto asignados al pipeline.
- **RF-025**: La logica de Dimension Tipo 1 en `clientes_saldos_consolidados` DEBE obtener el registro mas reciente de cada CUSTID basandose en `FechaIngestaDatos` (el mas alto). Si multiples registros tienen el mismo `FechaIngestaDatos`, DEBE existir un criterio de desempate determinista.
- **RF-026**: Los campos calculados de las vistas materializadas DEBEN manejar valores nulos de forma explicita en la logica CASE, produciendo un valor por defecto coherente cuando los campos fuente son nulos.
- **RF-027**: Los scripts de plata NO configuran `spark.sql.shuffle.partitions` mediante `spark.conf.set()`. El valor se deja al gestionado por defecto del entorno Serverless.
- **RF-028**: Ambas vistas materializadas de plata (`clientes_saldos_consolidados` y `transacciones_enriquecidas`) DEBEN descartar las siguientes columnas durante la transformacion: `_rescued_data`, `año`, `mes`, `dia` y `FechaIngestaDatos`. Estas columnas son de control o particion de la capa de bronce y NO deben propagarse a la medalla de plata. Si alguna columna no existe en la tabla fuente de bronce al momento de la transformacion, la exclusion debe manejarse sin error.
- **RF-029**: La vista materializada `clientes_saldos_consolidados` DEBE tener las siguientes expectativas de calidad de datos configuradas mediante el decorador `@dp.expect` de LSDP (modo observacional: registra violaciones en metricas pero conserva todos los registros):
  - El limite de credito DEBE ser mayor que 0.
  - El identificador del cliente NO DEBE ser nulo.
  - El limite de credito NO DEBE ser nulo.
  - La fecha de apertura de la cuenta DEBE ser posterior al 31 de diciembre de 2020.
  - La fecha de nacimiento DEBE ser anterior al 1 de enero de 2009.
- **RF-030**: La vista materializada `transacciones_enriquecidas` DEBE tener las siguientes expectativas de calidad de datos configuradas mediante el decorador `@dp.expect` de LSDP (modo observacional: registra violaciones en metricas pero conserva todos los registros):
  - La moneda de la transaccion NO DEBE ser nula.
  - El monto neto NO DEBE ser nulo.
  - El monto neto DEBE ser mayor que 0.
  - El identificador del cliente NO DEBE ser nulo.

### Entidades Clave

- **Vista Materializada clientes_saldos_consolidados** (`plata.lab1.clientes_saldos_consolidados`): Vista materializada que consolida el Maestro de Clientes (cmstfl) y los Saldos (blncfl) de bronce como Dimension Tipo 1. Columnas sin duplicados entre ambas tablas fuente + 4 campos calculados. Excluye: `_rescued_data`, `año`, `mes`, `dia`, `FechaIngestaDatos`. 5 expectativas de calidad de datos (limite de credito > 0, identificador cliente NOT NULL, limite de credito NOT NULL, fecha apertura cuenta > 2020-12-31, fecha nacimiento < 2009-01-01). Liquid Cluster: `huella_identificacion_cliente`, `identificador_cliente`. Todas las columnas renombradas a espanol snake_case.
- **Vista Materializada transacciones_enriquecidas** (`plata.lab1.transacciones_enriquecidas`): Vista materializada a partir de la streaming table transaccional (trxpfl) de bronce. Sin filtros para maximizar carga incremental automatica de LSDP. Incluye 4 campos calculados numericos. Excluye: `_rescued_data`, `año`, `mes`, `dia`, `FechaIngestaDatos`. 4 expectativas de calidad de datos (moneda transaccion NOT NULL, monto neto NOT NULL, monto neto > 0, identificador cliente NOT NULL). Liquid Cluster: `fecha_transaccion`, `identificador_cliente`, `tipo_transaccion`. Todas las columnas renombradas a espanol snake_case.
- **Streaming Table cmstfl** (`bronce.lab1.cmstfl`): Tabla fuente de bronce con 72 columnas (70 originales del parquet CMSTFL + FechaIngestaDatos + _rescued_data). Creada en el Incremento 3.
- **Streaming Table trxpfl** (`bronce.lab1.trxpfl`): Tabla fuente de bronce con 62 columnas (60 originales del parquet TRXPFL + FechaIngestaDatos + _rescued_data). Creada en el Incremento 3.
- **Streaming Table blncfl** (`bronce.lab1.blncfl`): Tabla fuente de bronce con 102 columnas (100 originales del parquet BLNCFL + FechaIngestaDatos + _rescued_data). Creada en el Incremento 3.
- **Tabla Parametros**: Tabla Delta existente en Unity Catalog con columnas `Clave` y `Valor`. Provee la configuracion dinamica del pipeline. Creada en el Incremento 1.

### Parametros del Pipeline LSDP — Existentes y Nuevos para Plata

| Parametro | Tipo | Ejemplo | Descripcion | Nuevo/Existente |
| --- | --- | --- | --- | --- |
| catalogoParametro | string | control | Catalogo UC donde reside la tabla Parametros | Existente (Inc. 3) |
| esquemaParametro | string | lab1 | Esquema donde reside la tabla Parametros | Existente (Inc. 3) |
| tablaParametros | string | Parametros | Nombre de la tabla Parametros | Existente (Inc. 3) |


> *Nota*: Los parametros de bronce (`rutaCompletaMaestroCliente`, `rutaSchemaLocationCmstfl`, etc.) siguen existiendo y siendo usados por los scripts de bronce. Los scripts de plata obtienen `catalogoPlata` y `esquemaPlata` de la tabla Parametros para construir el nombre destino de las vistas materializadas. Las streaming tables de bronce se leen por nombre simple ya que pertenecen al mismo pipeline LSDP (catalogo/esquema resueltos automaticamente por el pipeline).

## Criterios de Exito

### Resultados Medibles

- **CE-001**: Las dos vistas materializadas (`plata.lab1.clientes_saldos_consolidados` y `plata.lab1.transacciones_enriquecidas`) se crean exitosamente en la primera ejecucion del pipeline.
- **CE-002**: La vista `clientes_saldos_consolidados` no contiene columnas duplicadas entre cmstfl y blncfl: cada columna aparece exactamente una vez.
- **CE-003**: Los 4 campos calculados de `clientes_saldos_consolidados` (clasificacion_riesgo_cliente, categoria_saldo_disponible, perfil_actividad_bancaria, huella_identificacion_cliente) producen valores correctos y no nulos para el 100% de los registros con datos fuente validos.
- **CE-004**: Los 4 campos calculados de `transacciones_enriquecidas` (monto_neto_comisiones, porcentaje_comision_sobre_monto, variacion_saldo_transaccion, indicador_impacto_financiero) producen valores numericos coherentes para el 100% de los registros con datos fuente validos.
- **CE-005**: La vista `transacciones_enriquecidas` procesa de forma incremental los nuevos datos al re-ejecutar el pipeline, sin reprocesar los registros previamente cargados.
- **CE-006**: Todas las columnas de ambas vistas estan en espanol con formato snake_case y tienen nombres de al menos dos palabras.
- **CE-007**: Las propiedades Delta (Change Data Feed, autoCompact, optimizeWrite, retencion) estan activas en ambas vistas.
- **CE-008**: El pipeline se ejecuta completamente sin errores en Databricks Free Edition Serverless Compute.
- **CE-009**: Re-ejecutar el pipeline no genera datos duplicados ni errores (idempotencia).
- **CE-010**: Los logs de observabilidad contienen todos los parametros, catalogos, esquemas, campos calculados y campos del liquid cluster de cada vista materializada.
- **CE-011**: Ambas vistas materializadas de plata NO contienen las columnas `_rescued_data`, `año`, `mes`, `dia` ni `FechaIngestaDatos`.
- **CE-012**: Las 5 expectativas de calidad de datos de `clientes_saldos_consolidados` estan configuradas y se validan durante la ejecucion del pipeline, registrando los resultados en la interfaz de observabilidad de LSDP.
- **CE-013**: Las 4 expectativas de calidad de datos de `transacciones_enriquecidas` estan configuradas y se validan durante la ejecucion del pipeline, registrando los resultados en la interfaz de observabilidad de LSDP.

## Supuestos

- Las streaming tables de bronce (`cmstfl`, `trxpfl`, `blncfl`) estan creadas y pobladas por el Incremento 3 y disponibles en `bronce.lab1`.
- La tabla Parametros existe en Unity Catalog con los registros necesarios, incluyendo `catalogoPlata`, `esquemaPlata` y `catalogoOro`.
- Las streaming tables de bronce se leen por nombre simple (`spark.read.table("cmstfl")`) ya que residen en el mismo catalogo y esquema por defecto del pipeline LSDP. Las vistas materializadas de plata SI usan el namespace completo (`catalogo.esquema.vistaMaterializada`) construido con `catalogoPlata` y `esquemaPlata` de la tabla Parametros.
- Las utilidades existentes (`LsdpConexionParametros`, `LsdpConstructorRutas`, `LsdpReordenarColumnasLiquidCluster`) estan disponibles y funcionales desde el Incremento 3.
- El computo Serverless esta disponible en el workspace de Databricks Free Edition.
- LSDP resuelve automaticamente los imports entre carpetas del pipeline (utilities, transformations).
- Los catalogos `plata` y su esquema `lab1` fueron creados en el Incremento 1 por el notebook de configuracion inicial.
- La columna CUSTID es la unica columna estructuralmente comun entre los parquets originales de CMSTFL y BLNCFL. A nivel de bronce, se agregan tambien `FechaIngestaDatos` y `_rescued_data` como columnas comunes adicionales.
- Las columnas `año`, `mes` y `dia` se incluyen en la lista de exclusion por peticion explicita del area de negocio. Si estas columnas no existen en las streaming tables de bronce al momento de implementacion, la transformacion debe manejar su ausencia sin error.
- Las expectativas de calidad de datos usan los nombres de columna de plata (espanol snake_case), no los nombres de bronce (AS400). El mapeo exacto de nombres se define durante la planificacion.

## Clarificaciones

### Sesion 2026-04-05

- Q: Contradiccion entre RF-003 (renombrar FechaIngestaDatos) y RF-028 (descartar FechaIngestaDatos). Cual aplica? → A: Descartar. FechaIngestaDatos se usa internamente para Dimension Tipo 1 (RF-025) pero no se expone en las vistas de plata.
- Q: Que accion tomar cuando un registro viola una expectativa de calidad de datos? → A: `@dp.expect` (modo observacional) para todas las expectativas. Registra violaciones en metricas de LSDP pero conserva todos los registros sin descartar ni detener el pipeline.
- Q: Que tipo de JOIN usar entre cmstfl y blncfl en clientes_saldos_consolidados? → A: LEFT JOIN con cmstfl como tabla base. Todos los clientes del maestro se incluyen; saldos sin maestro se excluyen.
