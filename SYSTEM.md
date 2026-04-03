# Proposito de archivo SYSTEM.md

El Objetivo de este archivo SYSTEM.md es centralizar todo el Spec-Driven
Development (SDD) de la solucion en un solo archivo y que este sirva de base para
alimentar la base de verdad que genera spec-kit a traves de los comandos iniciales de
/speckit.constitution, /speckit.specify y /speckit.plan, facilitando la
implementacion de la solucion y agilizando de cara al Ingeniero de Datos el SDD.

Este proyecto es una adaptacion del proyecto original
[LakeflowSparkDeclarativePipelinesBase v1.6.0](https://github.com/andresmorera04/LakeflowSparkDeclarativePipelinesBase/releases/tag/v1.6.0)
el cual fue disenado para Azure Databricks. Esta version esta rediseñada para
ejecutarse exclusivamente en **Databricks Free Edition**, un entorno totalmente
aislado de Microsoft Azure.

# Dinamica con spec-kit

Speckit debe de seguir la siguiente dinamica de forma estricta:

- Cuando se ejecute el comando /speckit.constitution debe tomar lo especificado en
  la seccion [Constitution](#constitution) de manera exclusiva, de las demas secciones de este archivo solo debe tomar lo
  minimo necesario.
- Cuando se ejecute el comando /speckit.specify debe tomar principalmente lo
  especificado en la seccion [Specify](#specify) de manera exclusiva, de las demas secciones de este archivo solo debe tomar lo
  minimo necesario.
- Cuando se ejecute el comando /speckit.plan debe tomar principalmente lo
  especificado en la seccion [Plan](#plan) de manera exclusiva, de las demas secciones de este archivo solo debe tomar lo
  minimo necesario.

---

# Constitution

A continuacion se definen las politicas que debe seguir el desarrollo de esta
solucion:

## Plataforma: Databricks Free Edition (No Azure)

- Este proyecto se ejecuta exclusivamente en **Databricks Free Edition**, NO en Azure Databricks.
- Databricks Free Edition opera en un entorno **serverless-only** con cuotas limitadas.
- El proyecto esta **totalmente aislado de Microsoft Azure**: no hay Azure Data Lake Storage Gen2,
  no hay Azure SQL, no hay Azure Key Vault, no hay Secret Scopes de Databricks vinculados a Azure.
- El almacenamiento integrado de Databricks Free Edition es **Unity Catalog Managed Storage**
  (provisionado automaticamente al crear el workspace).
- Los mecanismos de almacenamiento disponibles son:
  - **Unity Catalog Managed Volumes**: para archivos parquets y datos no tabulares.
  - **Unity Catalog Managed Tables**: para tablas Delta gestionadas.
  - **Amazon S3** (si se configura acceso externo): para almacenamiento en buckets S3.
- El protocolo `abfss://` queda **prohibido** en este proyecto ya que es exclusivo de Azure.
- Las rutas `/mnt/` (DBFS mounts legacy) quedan **prohibidas** en este proyecto.
- DBFS root esta **deprecado** y **prohibido** en este proyecto.

## Storage Dinamico con Parametro TipoStorage

- Todo el proyecto debe contemplar dos formas de almacenar y leer los archivos parquets,
  determinadas por un parametro llamado **TipoStorage** que solo acepta dos valores:
  1. **"Volume"**: Usa Unity Catalog Volumes como almacenamiento.
  2. **"AmazonS3"**: Usa buckets de Amazon S3 como almacenamiento.
- El comportamiento del storage es **completamente dinamico**:
  - Si `TipoStorage = "Volume"`: Se usara el nombre del Volume (suministrado via parametros)
    para construir la ruta de lectura y escritura de parquets con el formato:
    `/Volumes/<catalogo>/<esquema>/<volume>/<ruta_relativa>`.
  - Si `TipoStorage = "AmazonS3"`: Se construira dinamicamente una ruta S3 con el formato:
    `s3://<bucket>/<ruta_relativa>` y esa ruta sera usada tanto para lectura como para escritura.
- Los parametros adicionales que alimentan la construccion de rutas son:
  - Para Volume: `catalogoVolume`, `esquemaVolume`, `nombreVolume`.
  - Para Amazon S3: `bucketS3`, `prefijoS3`.
- **Todo** el proyecto esta sujeto a este parametro `TipoStorage`. En **todo lugar** donde se haga
  una lectura o escritura de parquets generados, el codigo debe adaptarse dinamicamente
  a este parametro y su comportamiento.
- La utilidad `LsdpConstructorRutas.py` (reemplaza a `LsdpConstructorRutasAbfss.py`) debe
  construir las rutas de forma dinamica segun el `TipoStorage` seleccionado.

## Idempotencia Diferenciada

- La **idempotencia absoluta** aplica de forma exclusiva a: pruebas (TDD), los archivos del
  directorio `utilities/`, los archivos del directorio `transformations/` y los archivos del
  directorio `conf/`. Si estos componentes se ejecutan multiples veces con los mismos
  parametros, el resultado DEBE ser identico y sin errores (sin duplicados, sin datos
  corruptos, sin excepciones).
- El pipeline LSDP debe ser idempotente: re-ejecutar el pipeline no debe generar datos
  duplicados ni errores.
- Los **notebooks generadores de parquets** (directorio `explorations/GenerarParquets/`) NO
  aplican idempotencia absoluta. Estos notebooks utilizan su propio **modelo de mutacion
  diferenciada** que garantiza consistencia sin producir resultados identicos entre
  ejecuciones:
  - CMSTFL: conserva clientes existentes, muta el 20% en campos demograficos y agrega un
    0.60% de clientes nuevos en cada re-ejecucion.
  - TRXPFL: genera un nuevo lote de transacciones por cada ejecucion.
  - BLNCFL: se regenera completamente en cada ejecucion, manteniendo relacion 1:1 con
    CMSTFL.
- Este modelo de mutacion es intencional para simular el comportamiento real de un sistema
  AS400 donde los datos cambian entre extracciones.

## Observabilidad desde el Minuto Cero

- Todo el proyecto debe tener **observabilidad desde el minuto cero**.
- No se permiten **errores silenciosos** bajo ninguna circunstancia.
- Todo debe ser **impreso en pantalla o consola (print)**:
  - Parametros recibidos y sus valores.
  - Tipo de storage seleccionado y ruta construida.
  - Inicio y fin de cada paso relevante del proceso.
  - Conteos de registros procesados (leidos, escritos, transformados).
  - Tiempo de ejecucion de operaciones criticas.
  - Cualquier condicion excepcional, advertencia o situacion inesperada.
- Cada notebook y cada script del LSDP debe imprimir al inicio un bloque de resumen con
  todos los parametros que recibio y las rutas que construyo.
- Al finalizar cada paso, debe imprimirse un resumen con metricas de ejecucion.

## Tabla de Parametros: Parametros en Unity Catalog

- Este proyecto **NO usa Secret Scope de Databricks** ni Azure SQL.
- En su lugar, se creara una tabla Delta llamada `Parametros` (o el nombre de tabla
  suministrado via parametros) dentro de un **catalogo y esquema de Unity Catalog** suministrados
  via parametros del pipeline/notebook.
- Los parametros del pipeline y notebooks que determinan la ubicacion de esta tabla son:
  - `catalogoParametro`: Nombre del catalogo de Unity Catalog donde reside la tabla.
  - `esquemaParametro`: Nombre del esquema dentro del catalogo.
  - `tablaParametros`: Nombre de la tabla (por defecto: `Parametros`).
- La tabla tendra exactamente las mismas columnas que la tabla `Parametros` del Azure SQL
  del proyecto original:
  - `Clave` (string): Nombre del parametro.
  - `Valor` (string): Valor del parametro.
- Los registros de la tabla seran equivalentes a los del proyecto original, adaptados a
  Databricks Free Edition:

| Clave | Valor (ejemplo) | Descripcion |
|---|---|---|
| catalogoBronce | bronce | Catalogo Unity Catalog de bronce |
| contenedorBronce | bronce | Nombre del directorio contenedor de bronce dentro del Volume o prefijo S3 |
| TipoStorage | Volume | Tipo de almacenamiento: "Volume" o "AmazonS3" |
| catalogoVolume | bronce | Catalogo del Volume (si TipoStorage = Volume) |
| esquemaVolume | regional | Esquema del Volume (si TipoStorage = Volume) |
| nombreVolume | datos_bronce | Nombre del Volume (si TipoStorage = Volume) |
| bucketS3 | mi-bucket-datalake | Nombre del bucket S3 (si TipoStorage = AmazonS3) |
| prefijoS3 | lakeflow/bronce | Prefijo dentro del bucket S3 (si TipoStorage = AmazonS3) |
| DirectorioBronce | archivos | Subdirectorio dentro del contenedorBronce donde se almacenan los datos |
| catalogoPlata | plata | Catalogo Unity Catalog de plata |
| catalogoOro | oro | Catalogo Unity Catalog de oro |

- El archivo `conf/NbConfiguracionInicial.py` se encarga de crear esta tabla con las columnas
  correspondientes y los INSERTs para agregar todos los parametros equivalentes.

## Notebook de Configuracion Inicial

- El repositorio debe tener una carpeta nueva llamada **`conf/`** en la cual existira un archivo
  llamado **`NbConfiguracionInicial.py`**.
- Este notebook:
  1. Creara la tabla `Parametros` (usando el catalogo, esquema y nombre de tabla
     suministrados via parametros) con las columnas `Clave` (string) y `Valor` (string).
  2. Insertara todos los registros de parametros equivalentes a los que existen en el Azure SQL
     del proyecto original, adaptados a Databricks Free Edition.
  3. Sera idempotente: si la tabla ya existe, la recrea o la actualiza sin fallar.
  4. Imprimira en pantalla todos los parametros insertados para verificacion.

## Acceso a Parametros via Tabla Parametros

- En **todo el proyecto** (notebooks de generacion de parquets, utilidades LSDP,
  transformaciones, etc.) se debe hacer uso de la tabla de parametros para acceder a los
  valores de configuracion por las columnas `Clave` y `Valor`.
- La tabla sera suministrada via parametros: `catalogoParametro`, `esquemaParametro`,
  `tablaParametros`.
- La utilidad `LsdpConexionParametros.py` (reemplaza a `LsdpConexionAzureSql.py`) leera la
  tabla de parametros directamente desde Unity Catalog usando `spark.read.table()` en lugar
  de JDBC.
- La funcion retornara un diccionario Python con `{Clave: Valor}` para que los scripts
  consuman las claves que necesiten.

## Optimizacion para Databricks Free Edition

- El proyecto debe estar **optimizado para ejecutarse en Databricks Free Edition**, lo que
  implica:
  - **Reduccion de particiones de Spark**: Los notebooks de generacion de parquets deben usar
    un numero reducido de particiones (por ejemplo, 4-8 en lugar de 20-50) dado que el entorno
    Free Edition tiene recursos limitados.
  - **Control de shuffle partitions**: Configurar `spark.sql.shuffle.partitions` a un valor
    bajo (por ejemplo, 8) para evitar exceso de tareas en operaciones de shuffle.
  - **Volumetria reducida para pruebas**: Aunque la logica soporta 5M clientes y 15M
    transacciones, los valores por defecto para Free Edition deben ser menores
    (por ejemplo, 50.000 clientes y 150.000 transacciones) y configurables via parametros
    para escalar segun los recursos disponibles.
  - **Evitar operaciones que excedan la memoria**: Las transformaciones deben ser eficientes
    en memoria, usando operaciones incrementales cuando sea posible.
  - Los notebooks deben detectar y reportar via print si los recursos disponibles son
    insuficientes para la volumetria solicitada.

## Politicas Generales de Desarrollo

- El codigo No debe ser Hardcodeado (No Hard-Coded), en todo momento se debe manejar
  dinamismo en el codigo por medio de parametros claramente establecidos. El uso de valores
  constantes hardcodeadas no deben darse bajo ningun escenario, no hay excepcion que valga.
- Como Requisito de este proyecto, las extensiones Databricks Extension for Visual Studio Code
  y Databricks Driver for SQLTools deben estar correctamente instaladas.
- La configuracion de la extension de Databricks Extension for Visual Studio Code debe estar
  correctamente hecha, de tal manera que sea posible acceder al workspace de Databricks Free
  Edition y enlistar y usar los computos disponibles.
- Excluyendo el incremento 1, todas las demas versiones deben de tener su set de pruebas
  basado en Test-Driven Development (TDD), correctamente definidas y ejecutables
  aprovechando las extensiones de Databricks del Visual Studio Code.
- Se deben generar archivos parquets simulando el landing zone del datalake. Estos scripts de
  python estaran excluidos del Lakeflow Spark Declarative Pipelines, es decir se ejecutan de
  forma aislada como notebooks separados. Los 3 archivos deben tener estas caracteristicas:
  - Uno que simula el Maestro de Clientes de AS400 para una entidad bancaria (CMSTFL),
    incluyendo los nombres de campos lo mas similares a los de AS400 y la cantidad de campos
    debe ser la misma que tienen las tablas originales de AS400.
    - Estructura: 70 columnas (42 StringType, 18 DateType, 8 LongType, 2 DoubleType).
    - Primera ejecucion: genera la cantidad de registros base (configurable via parametro,
      por defecto 50.000 para Free Edition).
    - Re-ejecucion: conserva los clientes existentes, muta el 20% en 15 campos demograficos,
      agrega un 0,60% de clientes nuevos.
  - Uno que simula el Transaccional de AS400 (TRXPFL), el cual se relaciona con el Maestro de
    Clientes a traves de CUSTID.
    - Estructura: 60 columnas (9 StringType, 19 DateType, 2 TimestampType, 2 LongType,
      28 DoubleType).
    - Genera la cantidad de registros configurada via parametro (por defecto 150.000 para
      Free Edition) por cada ejecucion.
    - 15 tipos de transaccion distribuidos: CATM, DATM, CMPR, TINT, DPST (alta ~60%),
      PGSL, TEXT, RTRO, PGSV, NMNA, INTR (media ~30%), ADSL, IMPT, DMCL, CMSN (baja ~10%).
    - La Fecha de la Transaccion se brinda via parametro (formato YYYY-MM-DD).
  - Uno que simula los Saldos de los Clientes de AS400 (BLNCFL), relacion 1:1 con CMSTFL.
    - Estructura: 100 columnas (2 LongType, 29 StringType, 34 DoubleType, 35 DateType).
    - Un registro por cada cliente del Maestro. Se regenera completamente en cada ejecucion.
    - 4 tipos de cuenta: AHRO (40%), CRTE (30%), PRES (20%), INVR (10%).
  - Para la informacion del Maestro de Cliente, los nombres y apellidos deben ser Hebreos,
    Egipcios e Ingleses. No deben usarse bajo ninguna circunstancia nombres y apellidos latinos.
- La Solucion va a usar **Lakeflow Spark Declarative Pipelines (LSDP)** de forma estricta
  (URL: https://docs.databricks.com/aws/en/ldp/). LSDP es la evolucion de Delta Live Tables
  (DLT) con una API renovada. Queda **prohibido** el uso de la API legacy `dlt.*`
  (`import dlt`). Se **exige** el uso exclusivo de la API nueva con decoradores `@dp.table`
  y `@dp.materialized_view` (del modulo `databricks.sdk.pipelines`). Toda referencia a
  documentacion DEBE basarse en la URL oficial de LSDP indicada.
- Los Scripts de LSDP seran codificados en pyspark/python.
- Los Scripts de LSDP seran ejecutados por **Computo Serverless** de forma estricta.
- **Compatibilidad Maxima con Computo Serverless**: Todo el codigo del proyecto debe ser 100%
  compatible con Databricks Serverless Compute. Esta PROHIBIDO el uso de
  `spark.sparkContext` y cualquier acceso directo al JVM del driver
  (error `JVM_ATTRIBUTE_NOT_SUPPORTED`). Esto incluye pero no se limita a:
  `spark.sparkContext.broadcast()`, `spark.sparkContext.parallelize()`,
  `spark.sparkContext.accumulator()`, `spark.sparkContext.setJobGroup()`, y cualquier otro
  metodo de `sparkContext`. En su lugar, se deben usar alternativas nativas de PySpark o
  variables Python capturadas por closure (cloudpickle). Esta regla aplica a todos los
  notebooks del proyecto sin excepcion.
- El Manejo de los Branch de git y github sera controlado por el usuario. Bajo ninguna
  circunstancia el speckit podra crear nuevos branches o hacer commits automaticos.
- LA IA no tomara ninguna decision. Por cada research que se haga debera exigir la decision
  del usuario. La IA plantea una propuesta de cual es la mejor decision que considera, pero
  solo el usuario toma la decision final.
- La IA No debe alucinar. Debe mantenerse bajo el alcance estipulado por cada punto.
- Al Iniciar una nueva version o incremento del proyecto, siempre debe hacer un research de
  Lakeflow Spark Declarative Pipelines y de las extensiones de Databricks para VS Code.
- En el procesamiento y transformacion de los datos a traves de LSDP, se le debe dar el maximo
  provecho a la arquitectura medallion: Bronce, Plata y Oro.
  - Bronce llevara marca de tiempo (Campo FechaIngestaDatos) e ira acumulando datos de forma
    historica, aprovechando al maximo el AutoLoader.
  - En Plata se manejaran vistas materializadas:
    - Para el Maestro de Cliente y los Saldos del cliente se consolidan en una sola vista
      materializada como Dimension Tipo 1 (siempre los datos mas recientes).
    - Para el Transaccional se manejara como una vista materializada separada. El
      procesamiento declarativo de los datos sobre esta vista materializada NO debe aplicar
      filtrados de datos, lo cual permite que LSDP decida automaticamente realizar cargas
      incrementales en lugar de cargas completas (aplicar filtros forzaria una carga
      completa por defecto).
  - En Oro se manejan vistas materializadas que siempre muestren la informacion mas reciente
    (Dimension Tipo 1).
- En todo momento se debe respetar la estructura de archivos y carpetas que usa LSDP
  (utilities, transformations, etc.).
- Para los Research se deben usar los Links Oficiales de Databricks, python y spark.
- Todas las respuestas de la IA deben estar en idioma espanol, incluyendo las respuestas de
  los slash commands que se usen, incluyendo los de spec-kit.

---

# Specify

## Objetivo

El area de negocio de clientes necesita un producto de datos que les permitan
analizar el comportamiento de los clientes con respecto a sus saldos y al uso de
los cajeros automaticos (ATM), por lo que el conocer la cantidad de depositos
(creditos) y retiros (debitos), asi como el promedio de los montos retirados y
depositados es clave tenerlo por cada cliente y, por ultimo, el total sumarizado de
los Pagos al Saldo del cliente. El area de negocio de clientes necesita ver este
comportamiento y comprender si de pronto el movimiento de retiro o deposito de
los ATMs esta asociado con los Pagos al Saldo de cada cliente o si son
comportamientos aislados y sin correlacion.

## Contexto de Plataforma

- Este proyecto se ejecuta en **Databricks Free Edition** (serverless-only, cuotas limitadas).
- No hay dependencia de Microsoft Azure. El proyecto es **cloud-agnostic** dentro de Databricks
  Free Edition.
- El almacenamiento es dinamico: puede ser Unity Catalog Volumes o Amazon S3 segun el
  parametro `TipoStorage`.
- La tabla de configuracion `Parametros` vive como tabla Delta en Unity Catalog, no en
  Azure SQL.

## Versiones del Proyecto

- Incremento 1: Hacer el Research inicial de todo lo necesario del proyecto y tomar las
  decisiones claves para establecer un constitution robusto, un specify robusto y un
  plan robusto, adicionalmente, en este incremento se debe de crear el notebook de configuracion inicial (`conf/NbConfiguracionInicial.py`)
  que crea la tabla `Parametros` en Unity Catalog con todos los registros de parametros, a su vez, este notebook debe tener la creación del Volume en Unity Catalog en caso de que este, el cual deberá tener el nombre que está establecido en la tabla Parametros recientemente creada.
- Incremento 2: Se debe de Crear los notebooks (.py) en lenguaje python que generaran los 3 parquets
  simulando la data de AS400 definida en las politicas del constitution, adaptados al
  storage dinamico (Volume o Amazon S3).
- Incremento 3: Crear el Lakeflow Spark Declarative Pipelines con solo el procesamiento de la
  medalla de bronce, usando el storage dinamico y la tabla de parametros en Unity Catalog.
- Incremento 4: Incrementar el Lakeflow Spark Declarative Pipelines agregando el desarrollo y
  la transformacion de la medalla de plata a traves de vistas materializadas.
- Incremento 5: Incrementar el Lakeflow Spark Declarative Pipelines agregando el desarrollo y
  la transformacion de la medalla de oro a traves de vistas materializadas.
- Incremento 6: Generar los siguientes documentos en formato markdown (.md) en la carpeta docs:
  - ManualTecnico.md: Manual tecnico completo que detalla:
    1. Decoradores @dp.table y @dp.materialized_view.
    2. Paradigma declarativo de LSDP.
    3. Propiedades de las tablas delta streaming y vistas materializadas.
    4. Operaciones con API de DataFrames de Spark.
    5. Todos los parametros del pipeline/notebooks.
    6. Tabla Parametros con detalle de claves y valores.
    7. Dependencias: que el usuario debe tener Databricks Free Edition configurado con Unity
       Catalog, catalogos y esquemas creados, y opcionalmente acceso a Amazon S3.
  - ModeladoDatos.md: Diccionario de datos completo con las 10 entidades (3 Parquets AS400,
    3 Streaming Tables Bronce, 2 Vistas Materializadas Plata, 2 Vistas Materializadas Oro),
    incluyendo campos, tipos de datos, descripciones, logica de campos calculados y diagrama
    de linaje.
  - Actualización del archivo README.md con formato profesional (como lo hacen las grandes empresas en la punta de la tecnologia como Netflix, Spotify, Uber, Microsoft o Amazon) resaltando todos los elementos relevantes del proyecto, haciendo referencia al ManualTecnico.md, al ModeladoDatos.md y al SYSTEM.md, resaltando que el desarrollo de este laboratorio es con IA Asistido usando github copilot y el framework spec-kit.
- Nota Importante: Excluyendo el incremento 1, todas las demas versiones deben de tener su set
  de pruebas basado en Test-Driven Development (TDD).

## Reglas de Negocio

- En la Medalla de Plata, la tabla consolidada de clientes y saldos debe tener como minimo
  3 campos calculados, cada campo calculado debe basarse en lo equivalente a un CASE de SQL
  pero en pyspark, donde deben intervenir como minimo 3 campos/columnas de las tablas delta
  de la medalla de bronce.
- En la Medalla de Plata, en la tabla transaccional, deben crearse al menos 4 campos
  calculados, cada uno basado en minimo 2 o mas campos/columnas de tipo numerica para
  calcular un nuevo valor numerico.
- En la Medalla de Plata, la tabla consolidada de clientes y saldos debe tener un campo
  calculado a partir del identificador de cliente unico, generado a traves del algoritmo
  SHA2_256 (huella_identificacion_cliente).
- Los campos calculados del proyecto original deben mantenerse:
  - Plata clientes_saldos_consolidados (4 campos): clasificacion_riesgo_cliente,
    categoria_saldo_disponible, perfil_actividad_bancaria, huella_identificacion_cliente.
  - Plata transacciones_enriquecidas (4 campos): monto_neto_comisiones,
    porcentaje_comision_sobre_monto, variacion_saldo_transaccion,
    indicador_impacto_financiero.
  - Oro comportamiento_atm_cliente (5 metricas): cantidad_depositos_atm,
    cantidad_retiros_atm, promedio_monto_depositos_atm, promedio_monto_retiros_atm,
    total_pagos_saldo_cliente.
  - Oro resumen_integral_cliente (22 columnas): combinacion de datos dimensionales de plata
    con metricas ATM de oro.

---

# Plan

## Repositorio del Proyecto

El Proyecto tendra la siguiente estructura:

```
DbsFreeLakeflowSparkDeclarativePipelinesBase/
|
|-- README.md
|-- SYSTEM.md
|-- <Archivos Markdown Generados por Speckit en sus directorios o en la raiz>
|
|-- conf/
|   |-- NbConfiguracionInicial.py          # Crea tabla Parametros en Unity Catalog
|
|-- src/
|   |-- LSDP_Laboratorio_Basico/
|       |-- explorations/ 
|           |-- LSDP_Laboratorio_Basico/
|               |-- <Archivos del TDD para el pipeline LSDP>
|           |-- GenerarParquets/
|               |-- NbGenerarMaestroCliente.py
|               |-- NbGenerarTransaccionalCliente.py
|               |-- NbGenerarSaldosCliente.py
|               |-- <Archivos del TDD para generacion de parquets>
|       |-- transformations/
|           |-- LsdpBronceCmstfl.py
|           |-- LsdpBronceTrxpfl.py
|           |-- LsdpBronceBlncfl.py
|           |-- LsdpPlataClientesSaldos.py
|           |-- LsdpPlataTransacciones.py
|           |-- LsdpOroClientes.py
|       |-- utilities/
|           |-- LsdpConexionParametros.py          # Lee Parametros desde Unity Catalog
|           |-- LsdpConstructorRutas.py            # Construye rutas dinamicas (Volume o S3)
|           |-- LsdpReordenarColumnasLiquidCluster.py
|
|-- docs/
|   |-- ManualTecnico.md
|   |-- ModeladoDatos.md

```

## Stack Tecnologico

| Componente | Tecnologia |
|---|---|
| Cloud | Databricks Free Edition (aislado de Azure) |
| Motor de Datos | Databricks con Lakeflow Spark Declarative Pipelines (LSDP) |
| Computo | Databricks Serverless Compute |
| Catalogo | Unity Catalog |
| Almacenamiento (opcion 1) | Unity Catalog Managed Volumes |
| Almacenamiento (opcion 2) | Amazon S3 |
| Configuracion | Tabla Delta Parametros en Unity Catalog |
| Lenguaje | Python / PySpark |
| Pruebas | TDD con Databricks Extension for VS Code |
| Modelado | Arquitectura Medallion (Bronce / Plata / Oro) |
| Metodologia | Spec-Driven Development con spec-kit |

## Estandar de Desarrollo

A continuacion se detalla los estandares y reglas de desarrollo:

- En la medalla de plata y oro, los nombres de las tablas deltas y vistas materializadas
  deben ser en espanol, en formato snake_case y con nombres intuitivos, ademas, deben estar
  comentadas correctamente.
- En la medalla de plata y oro, los nombres de los campos/columnas deben ser en espanol,
  en formato snake_case y con nombres intuitivos, claros, faciles de entender y conformados
  por dos palabras o silabas como minimo. No puede ocurrir que los nombres de los
  campos/columnas sean los mismos que usan en bronce.
- El lenguaje de programacion sera python aprovechando spark a traves de pyspark de forma
  estricta y absoluta.
- Los notebooks seran archivos .py pero con el formato de notebooks compatibles con
  Databricks. Todo el codigo debe estar en idioma espanol y todo comentado al detalle.
  El nombre de los archivos .py debera estar en formato PascalCase y usar "Nb" como prefijo.
- Los scripts de LSDP deben tener todo el codigo en espanol y todo comentado al detalle.
  El nombre de los archivos .py seguira el formato PascalCase con el prefijo "Lsdp".
- Todo el codigo en python debe estar en espanol y seguir el formato snake_case en minuscula.
  Las variables, constantes, clases, funciones, metodos y objetos deben estar estrictamente
  en formato snake_case en minuscula.
- Los bloques de markdown deben seguir un estilo explicativo y profesional.
- En el LSDP, debe crear scripts en la carpeta utilities que faciliten metodos y funciones
  que sigan el principio SOLID de responsabilidad unica y que sean reutilizables.
- El estilo de desarrollo debe ser modular. En el directorio utilities todas las funciones
  o metodos transversales. En transformations, tambien se debe modular en funciones.
- En todo momento se debe realizar codigo altamente optimizado.
- El codigo generado en el LSDP debe ser totalmente compatible con el paradigma declarativo.
- La utilidad `LsdpConexionParametros.py` lee la tabla de parametros desde Unity Catalog:
  - Recibe `spark`, `catalogo_parametro`, `esquema_parametro`, `tabla_parametros`.
  - Ejecuta `spark.read.table(f"{catalogo_parametro}.{esquema_parametro}.{tabla_parametros}")`.
  - Retorna un diccionario Python `{Clave: Valor}`.
  - No usa JDBC, ni Secret Scopes, ni Azure Key Vault.
  - Compatible con Serverless (sin sparkContext).
- La utilidad `LsdpConstructorRutas.py` construye rutas dinamicas segun TipoStorage:
  - Si `TipoStorage = "Volume"`: retorna `/Volumes/<catalogo>/<esquema>/<volume>/<ruta_relativa>`.
  - Si `TipoStorage = "AmazonS3"`: retorna `s3://<bucket>/<ruta_relativa>`.
  - Recibe los parametros necesarios del diccionario de la tabla Parametros.
  - Imprime via print la ruta construida para observabilidad.
- El mismo Pipeline de LSDP debe permitir crear tablas deltas o vistas en esquemas y catalogos
  diferentes al elegido por defecto. El Catalogo y esquema elegido por defecto para el
  pipeline es "bronce" y "regional".
- El Desarrollo del Pipeline de LSDP debe aprovechar al maximo las capacidades del paradigma
  declarativo. Por ejemplo: para crear la vista materializada transaccional de plata no debe
  aplicar filtros en la lectura de la tabla de bronce.
- Las Tablas deltas y vistas materializadas deben tener activas estas propiedades:
  - Change Data Feed (delta.enableChangeDataFeed = true)
  - autoOptimize.autoCompact (delta.autoOptimize.autoCompact = true)
  - autoOptimize.optimizeWrite (delta.autoOptimize.optimizeWrite = true)
  - Liquid Cluster (la IA debe definir los campos basados en buenas practicas)
  - delta.deletedFileRetentionDuration = interval 30 days
  - delta.logRetentionDuration = interval 60 days
- Si se usan tablas delta de tipo apply_change deben ser declaradas como tablas deltas
  temporales y tambien deben tener liquid cluster.
- El patron Closure como reemplazo de Broadcast es obligatorio en Serverless.
  Los parametros de la tabla `Parametros` y las rutas se calculan a nivel de modulo
  (una sola vez al inicializar el pipeline) y quedan capturados por closure en las funciones
  decoradas. cloudpickle serializa automaticamente las variables capturadas.

## Parametros del Pipeline LSDP

Los parametros del pipeline se configuran en la definicion del pipeline LSDP y se leen en
tiempo de ejecucion con `spark.conf.get("pipelines.parameters.nombreParametro")`.

| Parametro | Tipo | Ejemplo | Descripcion |
|---|---|---|---|
| catalogoParametro | string | control | Catalogo UC donde esta la tabla de parametros |
| esquemaParametro | string | regional | Esquema donde esta la tabla de parametros |
| tablaParametros | string | Parametros | Nombre de la tabla de parametros |
| esquema_plata | string | regional | Esquema para vistas materializadas de plata |
| esquema_oro | string | regional | Esquema para vistas materializadas de oro |
| catalogo_plata | string | plata | Catalogo UC para vistas materializadas de plata |
| catalogo_oro | string | oro | Catalogo UC para vistas materializadas de oro |
| rutaCompletaMaestroCliente | string | LSDP_Base/As400/MaestroCliente/ | Ruta relativa del parquet Maestro Clientes |
| rutaCompletaTransaccional | string | LSDP_Base/As400/Transaccional/ | Ruta relativa del parquet Transaccional |
| rutaCompletaSaldoCliente | string | LSDP_Base/As400/SaldoCliente/ | Ruta relativa del parquet Saldos |
| rutaCheckpointCmstfl | string | LSDP_Base/Checkpoints/Bronce/cmstfl/ | Ruta checkpoint AutoLoader cmstfl |
| rutaCheckpointTrxpfl | string | LSDP_Base/Checkpoints/Bronce/trxpfl/ | Ruta checkpoint AutoLoader trxpfl |
| rutaCheckpointBlncfl | string | LSDP_Base/Checkpoints/Bronce/blncfl/ | Ruta checkpoint AutoLoader blncfl |

Las rutas relativas se combinan con los parametros de `Parametros` usando
`LsdpConstructorRutas` para producir rutas completas (Volume o S3) en tiempo de ejecucion.

## Tabla Parametros — Registros Equivalentes

La tabla `Parametros` en Unity Catalog contendra los siguientes registros, adaptados
del Azure SQL original:

| Clave | Valor | Descripcion |
|---|---|---|
| catalogoBronce | bronce | Catalogo Unity Catalog de bronce |
| contenedorBronce | bronce | Nombre del directorio contenedor de bronce dentro del almacenamiento |
| TipoStorage | Volume | Tipo de almacenamiento: "Volume" o "AmazonS3" |
| catalogoVolume | bronce | Catalogo del Volume (solo para TipoStorage=Volume) |
| esquemaVolume | regional | Esquema del Volume (solo para TipoStorage=Volume) |
| nombreVolume | datos_bronce | Nombre del Volume (solo para TipoStorage=Volume) |
| bucketS3 | (vacio) | Bucket S3 (solo para TipoStorage=AmazonS3) |
| prefijoS3 | (vacio) | Prefijo S3 (solo para TipoStorage=AmazonS3) |
| DirectorioBronce | archivos | Subdirectorio dentro del contenedorBronce donde se almacenan los datos |
| catalogoPlata | plata | Catalogo Unity Catalog de plata |
| catalogoOro | oro | Catalogo Unity Catalog de oro |

## Catalogos Unity Catalog

Cada capa del medallion tiene su propio catalogo de Unity Catalog:

| Catalogo | Esquema | Proposito |
|---|---|---|
| bronce | regional | Streaming tables de ingesta desde parquets AS400 |
| plata | regional | Vistas materializadas de transformacion y enriquecimiento |
| oro | regional | Vistas materializadas de agregacion y producto de datos final |
| control | regional | Tablas de parametros, metadatos y control del pipeline |

## Entidades de Datos (10 entidades)

### Parquets AS400 (3 entidades fuente)

| Entidad | Columnas | Clave | Volumetria |
|---|---|---|---|
| CMSTFL (Maestro Clientes) | 70 | CUSTID | 50.000 base (configurable) + 0.60% incremental |
| TRXPFL (Transaccional) | 60 | TRXID, FK: CUSTID | 150.000 por ejecucion (configurable) |
| BLNCFL (Saldos) | 100 | FK: CUSTID | 1 registro por cliente (1:1) |

### Streaming Tables de Bronce (3 entidades)

| Entidad | Columnas | Liquid Cluster | Acumulacion |
|---|---|---|---|
| bronce.regional.cmstfl | 72 | FechaIngestaDatos, CUSTID | Append-only historica |
| bronce.regional.trxpfl | 62 | TRXDT, CUSTID, TRXTYP | Append-only historica |
| bronce.regional.blncfl | 102 | FechaIngestaDatos, CUSTID | Append-only historica |

### Vistas Materializadas de Plata (2 entidades)

| Entidad | Columnas | Liquid Cluster | Estrategia |
|---|---|---|---|
| plata.regional.clientes_saldos_consolidados | 177 | huella_identificacion_cliente, identificador_cliente | Dimension Tipo 1 (mas reciente por CUSTID) |
| plata.regional.transacciones_enriquecidas | 66 | fecha_transaccion, identificador_cliente, tipo_transaccion | Sin filtros (carga incremental automatica por LSDP) |

### Vistas Materializadas de Oro (2 entidades)

| Entidad | Columnas | Liquid Cluster | Fuente |
|---|---|---|---|
| oro.regional.comportamiento_atm_cliente | 6 | identificador_cliente | transacciones_enriquecidas (groupBy) |
| oro.regional.resumen_integral_cliente | 22 | huella_identificacion_cliente, identificador_cliente | clientes_saldos_consolidados + comportamiento_atm_cliente |

## Linaje de Datos

```
Parquets AS400 (CMSTFL, TRXPFL, BLNCFL)
    |  AutoLoader Streaming (rutas dinamicas: Volume o S3)
    v
BRONCE: Streaming Tables (append-only + FechaIngestaDatos)
    |  cmstfl, trxpfl, blncfl
    v
PLATA: Vistas Materializadas
    |  clientes_saldos_consolidados (cmstfl + blncfl, Dim Tipo 1)
    |  transacciones_enriquecidas (trxpfl, sin filtros, carga incremental automatica)
    v
ORO: Vistas Materializadas (producto de datos final)
    |  comportamiento_atm_cliente (groupBy de transacciones)
    |  resumen_integral_cliente (plata + oro ATM, LEFT JOIN)
```

## Que NO debe hacer el proyecto

- No debe usar emojis bajo ninguna circunstancia.
- No debe hacer codigo espaguetti.
- No puede existir codigo que no sea compatible o ejecutable en LSDP.
- Las tablas deltas o vistas materializadas No deben usar ZOrder ni PartitionBy.
- No debe usar protocolo `abfss://` (exclusivo de Azure).
- No debe usar rutas `/mnt/` (legacy DBFS mounts).
- No debe usar DBFS root (deprecado).
- No debe usar Azure SQL, Azure Key Vault ni Secret Scopes de Databricks.
- No debe usar `spark.sparkContext` ni acceso directo al JVM.
- No debe hardcodear valores. Todo debe ser dinamico via parametros.
- No debe tener errores silenciosos. Todo debe imprimirse en pantalla.

---

# Decisiones Tecnicas Aprobadas (Incremento 1)

Las siguientes decisiones fueron investigadas en el Incremento 1 y aprobadas formalmente
por el usuario el 2026-04-03. Rigen para todos los incrementos.

## Decision 1: API de Lakeflow Spark Declarative Pipelines (LSDP)

**Fuente oficial**: https://docs.databricks.com/aws/en/ldp/

**Hallazgos clave**:
- El modulo `databricks.sdk.pipelines` proporciona decoradores `@dp.table` y
  `@dp.materialized_view` para definir tablas y vistas materializadas de forma declarativa.
- La API legacy `dlt.*` (`import dlt`) esta deprecada y NO debe usarse en proyectos nuevos.
- Propiedades soportadas: Change Data Feed, autoOptimize, Liquid Clustering, Expectativas
  de calidad de datos (Data Quality Checks), todo nativo en los decoradores.
- AutoLoader en LSDP: se usa con `cloud_files` como fuente en `@dp.table`. Los checkpoints
  son completamente automaticos (no requieren configuracion manual).
- Para tablas en catalogos y esquemas distintos al default del pipeline, se usan los
  parametros `catalog` y `schema` en los decoradores.
- Compatibilidad con Serverless Compute: TOTALMENTE COMPATIBLE.

**Decision aprobada**: Adoptar exclusivamente LSDP con `@dp.table` y
`@dp.materialized_view`. La API legacy `dlt.*` queda prohibida en todo el proyecto.

## Decision 2: AutoLoader para Ingestion Incremental en Bronce

**Fuente oficial**: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/

**Hallazgos clave**:
- AutoLoader es TOTALMENTE COMPATIBLE con Serverless Compute y esta optimizado para ello.
- Formato de lectura Parquet: `spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(ruta)`.
- Checkpoints AUTOMATICOS en LSDP (no requieren configuracion manual).
- Rutas de Unity Catalog Volumes: `/Volumes/catalogo/esquema/volumen/datos/` soportadas.
- Rutas S3: `s3://bucket/ruta/` soportadas (requiere credenciales IAM configuradas).
- Formatos soportados: JSON, CSV, XML, Parquet, Avro, ORC, Text, BinaryFile.

**Decision aprobada**: Usar AutoLoader en LSDP para toda la ingestion incremental en la
medalla de bronce, con rutas dinamicas segun `TipoStorage` (Volume o S3).

## Decision 3: Unity Catalog DDL para Creacion de Infraestructura

**Fuente oficial**: https://docs.databricks.com/aws/en/sql/language-manual/

**Hallazgos clave**:
- `CREATE CATALOG IF NOT EXISTS nombre`: crea catalogo solo si no existe.
- `CREATE SCHEMA IF NOT EXISTS catalogo.esquema`: requiere que el catalogo padre exista.
- `CREATE VOLUME IF NOT EXISTS catalogo.esquema.volumen`: crea volumen gestionado (managed).
  Ruta de acceso resultante: `/Volumes/catalogo/esquema/volumen/`.
- `CREATE OR REPLACE TABLE catalogo.esquema.tabla (cols)`: recrea la tabla completamente.
- Todos los comandos ejecutables via `spark.sql()` en notebooks y scripts Python.
- TOTALMENTE COMPATIBLE con Databricks Free Edition y Serverless Compute.

**Decision aprobada**: Implementar la creacion automatica de catalogos, esquemas, tabla
Parametros y Volume usando DDL de Unity Catalog via `spark.sql()`. Hive Metastore legacy y
DBFS mounts quedan prohibidos.

## Decision 4: Extensiones de Databricks para Visual Studio Code

**Fuente oficial**: https://docs.databricks.com/aws/en/dev-tools/vscode-ext/

**Hallazgos clave**:
- **Databricks Extension for Visual Studio Code**: permite ejecutar codigo Python en
  Serverless remoto, ejecutar notebooks como jobs, debugging interactivo con Databricks
  Connect, sincronizacion de workspace.
- **Databricks Driver for SQLTools**: extension complementaria para queries SQL nativos.
- Conexion al workspace: via logo Databricks en sidebar, seleccionar host, autenticacion
  (Personal Access Token u OAuth), proporcionar workspace URL.
- Ejecucion en Serverless: seleccionar "Run on Serverless" en el dropdown de computo.

**Decision aprobada**: Verificar que ambas extensiones esten instaladas y configuradas antes
de cada incremento. Usar "Run on Serverless" como modo de ejecucion estandar para todos
los notebooks y scripts del proyecto.

## Decision 5: dbutils.widgets para Parametros de Notebooks

**Fuente oficial**: https://docs.databricks.com/aws/en/dev-tools/databricks-utils.html

**Hallazgos clave**:
- `dbutils.widgets.text(name, defaultValue, label)`: crea widget de texto con valor por
  defecto y etiqueta visible en la UI de Databricks y en VS Code.
- `dbutils.widgets.get(name)`: recupera el valor actual como STRING.
- TOTALMENTE COMPATIBLE con Serverless Compute y con notebooks ejecutados como jobs.
- Los parametros se pasan via job parameters al ejecutar como job automatizado.
- Mejores practicas: definir widgets al inicio, capturar valores inmediatamente, validar
  que no esten vacios antes de cualquier operacion.

**Decision aprobada**: Usar `dbutils.widgets.text()` para todos los parametros de entrada
de notebooks, con valores por defecto que representen la configuracion recomendada.
Validar parametros al inicio del notebook antes de cualquier operacion.
