<!--
Sync Impact Report
==================
Version: 1.0.0 -> 1.1.0
Tipo de cambio: MINOR (expansion material de principios existentes)

Principios modificados:
  - II. Storage Dinamico via TipoStorage:
    Corregida ruta S3 de s3://<bucket>/<prefijo>/<ruta_relativa>
    a s3://<bucket>/<ruta_relativa>
  - III. Idempotencia Absoluta -> Idempotencia Diferenciada:
    Redefinido para distinguir idempotencia absoluta (utilities,
    transformations, conf, TDD, pipeline LSDP) de modelo de
    mutacion diferenciada (notebooks generadores de parquets)
  - VIII. TDD: Eliminada contradiccion "toda la solucion" vs
    "desde incremento 2". Homologado a excluir incremento 1
  - IX. Paradigma Declarativo LSDP Estricto:
    Aclarado que DLT es la API legacy (dlt.*) y LSDP la nueva
    (@dp.table, @dp.materialized_view). Agregada URL oficial.
    Agregada estrategia sin filtros en Plata transaccional
    para carga incremental automatica

Secciones modificadas:
  - Restricciones Tecnologicas: "API legacy dlt.*" reemplaza
    "Delta Live Tables" generico

Templates revisados:
  - .specify/templates/plan-template.md: compatible [OK]
  - .specify/templates/spec-template.md: compatible [OK]
  - .specify/templates/tasks-template.md: compatible [OK]
  - .specify/templates/commands/*.md: no existe [OK]

Elementos diferidos: Ninguno.
-->

# DbsFreeLakeflowSparkDeclarativePipelinesBase Constitution

## Core Principles

### I. Plataforma Exclusiva: Databricks Free Edition

Este proyecto se ejecuta exclusivamente en **Databricks Free Edition**,
un entorno serverless-only con cuotas limitadas, totalmente aislado de
Microsoft Azure.

- El proyecto NO DEBE tener dependencia alguna de Microsoft Azure: no
  hay Azure Data Lake Storage Gen2, no hay Azure SQL, no hay Azure Key
  Vault, no hay Secret Scopes de Databricks vinculados a Azure.
- El almacenamiento integrado es **Unity Catalog Managed Storage**
  (provisionado automaticamente al crear el workspace).
- Los mecanismos de almacenamiento disponibles son:
  - **Unity Catalog Managed Volumes**: para archivos parquets y datos
    no tabulares.
  - **Unity Catalog Managed Tables**: para tablas Delta gestionadas.
  - **Amazon S3** (si se configura acceso externo): para
    almacenamiento en buckets S3.

**Razon**: El proyecto es un laboratorio disenado para demostrar
Lakeflow Spark Declarative Pipelines sin dependencia de servicios
Azure, reduciendo costos y complejidad.

### II. Storage Dinamico via TipoStorage

Todo el proyecto DEBE contemplar dos formas de almacenar y leer
archivos parquets, determinadas por el parametro **TipoStorage**:

- **"Volume"**: Usa Unity Catalog Volumes. Ruta:
  `/Volumes/<catalogo>/<esquema>/<volume>/<ruta_relativa>`.
- **"AmazonS3"**: Usa buckets de Amazon S3. Ruta:
  `s3://<bucket>/<ruta_relativa>`.

Parametros que alimentan la construccion de rutas:

- Para Volume: `catalogoVolume`, `esquemaVolume`, `nombreVolume`.
- Para Amazon S3: `bucketS3`.

> **Nota (v1.1.0)**: El parametro `prefijoS3` existe en la tabla
> Parametros por herencia del Incremento 1 pero NO se utiliza en la
> construccion de rutas S3. La ruta S3 se compone exclusivamente de
> `s3://<bucket>/<ruta_relativa>`. Si en el futuro se requiere un
> prefijo, se incorporara como parte de `<ruta_relativa>`.

La utilidad `LsdpConstructorRutas.py` DEBE construir las rutas de
forma dinamica segun el `TipoStorage` seleccionado. Todo lugar donde
se haga lectura o escritura de parquets DEBE adaptarse dinamicamente
a este parametro.

**Razon**: Permite flexibilidad de almacenamiento sin modificar codigo,
facilitando migracion entre entornos y proveedores.

### III. Idempotencia Diferenciada

La idempotencia en este proyecto aplica de forma diferenciada segun
el componente:

- **Idempotencia absoluta** aplica de forma exclusiva a: pruebas
  (TDD), los archivos del directorio `utilities/`, los archivos del
  directorio `transformations/`, los archivos del directorio `conf/`
  y el pipeline LSDP. Si estos componentes se ejecutan multiples
  veces con los mismos parametros, el resultado DEBE ser identico y
  sin errores (sin duplicados, sin datos corruptos, sin excepciones).
- El pipeline LSDP DEBE ser idempotente: re-ejecutar el pipeline NO
  DEBE generar datos duplicados ni errores.
- Los **notebooks generadores de parquets** (directorio
  `explorations/GenerarParquets/`) NO aplican idempotencia absoluta.
  Estos notebooks utilizan su propio **modelo de mutacion
  diferenciada** que garantiza consistencia sin producir resultados
  identicos entre ejecuciones:
  - CMSTFL: conserva clientes existentes, muta el 20% en campos
    demograficos y agrega un 0.60% de clientes nuevos en cada
    re-ejecucion.
  - TRXPFL: genera un nuevo lote de transacciones por cada ejecucion.
  - BLNCFL: se regenera completamente en cada ejecucion, manteniendo
    relacion 1:1 con CMSTFL.
- Este modelo de mutacion es intencional para simular el
  comportamiento real de un sistema AS400 donde los datos cambian
  entre extracciones.

**Razon**: Garantiza confiabilidad en los componentes del pipeline
mientras permite que los generadores de datos simulen el
comportamiento real de un sistema fuente AS400.

### IV. Observabilidad desde el Minuto Cero

NO se permiten errores silenciosos bajo ninguna circunstancia. Todo
DEBE ser impreso en pantalla o consola (print):

- Parametros recibidos y sus valores.
- Tipo de storage seleccionado y ruta construida.
- Inicio y fin de cada paso relevante del proceso.
- Conteos de registros procesados (leidos, escritos, transformados).
- Tiempo de ejecucion de operaciones criticas.
- Cualquier condicion excepcional, advertencia o situacion inesperada.

Cada notebook y cada script del LSDP DEBE imprimir al inicio un bloque
de resumen con todos los parametros recibidos y las rutas construidas.
Al finalizar cada paso, DEBE imprimirse un resumen con metricas de
ejecucion.

**Razon**: La visibilidad total del comportamiento del pipeline es
esencial para diagnosticar problemas y verificar correctitud en un
entorno de recursos limitados.

### V. Parametrizacion Total (No Hard-Coded)

El codigo NO DEBE ser hardcodeado bajo ningun escenario. En todo
momento se DEBE manejar dinamismo por medio de parametros claramente
establecidos. No hay excepcion que valga.

- Se DEBE crear una tabla Delta llamada `Parametros` dentro de un
  catalogo y esquema de Unity Catalog suministrados via parametros.
- La tabla tiene exactamente dos columnas: `Clave` (string) y
  `Valor` (string).
- Los parametros de ubicacion de la tabla son: `catalogoParametro`,
  `esquemaParametro`, `tablaParametros`.
- La utilidad `LsdpConexionParametros.py` DEBE leer la tabla usando
  `spark.read.table()` y retornar un diccionario Python
  `{Clave: Valor}`.
- El archivo `conf/NbConfiguracionInicial.py` se encarga de crear
  la tabla con las columnas e INSERTs correspondientes.

**Razon**: Elimina la fragilidad de valores constantes en el codigo,
centraliza la configuracion y facilita el cambio de entorno sin
modificar fuentes.

### VI. Compatibilidad Maxima con Computo Serverless

Todo el codigo del proyecto DEBE ser 100% compatible con Databricks
Serverless Compute.

- Esta PROHIBIDO el uso de `spark.sparkContext` y cualquier acceso
  directo al JVM del driver (error `JVM_ATTRIBUTE_NOT_SUPPORTED`).
- Esto incluye pero no se limita a: `spark.sparkContext.broadcast()`,
  `spark.sparkContext.parallelize()`,
  `spark.sparkContext.accumulator()`,
  `spark.sparkContext.setJobGroup()`, y cualquier otro metodo de
  `sparkContext`.
- Se DEBEN usar alternativas nativas de PySpark o variables Python
  capturadas por closure (cloudpickle).
- El patron Closure como reemplazo de Broadcast es obligatorio:
  los parametros de la tabla `Parametros` y las rutas se calculan a
  nivel de modulo (una sola vez) y quedan capturados por closure en
  las funciones decoradas.

**Razon**: Databricks Free Edition ejecuta exclusivamente en
serverless, donde el acceso al JVM esta bloqueado por diseno.

### VII. Optimizacion para Recursos Limitados

El proyecto DEBE estar optimizado para Databricks Free Edition:

- **Reduccion de particiones de Spark**: los notebooks de generacion
  de parquets DEBEN usar un numero reducido de particiones
  (4-8 en lugar de 20-50).
- **Control de shuffle partitions**: configurar
  `spark.sql.shuffle.partitions` a un valor bajo (por ejemplo, 8).
- **Volumetria reducida para pruebas**: valores por defecto de
  50.000 clientes y 150.000 transacciones, configurables via
  parametros.
- **Eficiencia en memoria**: las transformaciones DEBEN ser
  eficientes, usando operaciones incrementales cuando sea posible.
- Los notebooks DEBEN detectar y reportar via print si los recursos
  disponibles son insuficientes para la volumetria solicitada.

**Razon**: El entorno Free Edition tiene recursos severamente
limitados; sin estas optimizaciones los pipelines fallarian o
se ejecutarian con tiempos inaceptables.

### VIII. Test-Driven Development (TDD)

Excluyendo el incremento 1, todas las demas versiones del proyecto
DEBEN tener su set de pruebas basado en Test-Driven Development
(TDD), correctamente definidas y ejecutables aprovechando las
extensiones de Databricks para Visual Studio Code.

- Las extensiones Databricks Extension for Visual Studio Code y
  Databricks Driver for SQLTools DEBEN estar correctamente
  instaladas y configuradas.
- Los notebooks TDD NO pueden usar `__file__` (no esta definido en
  el runtime de Databricks). Para resolver rutas del filesystem se
  DEBE usar `dbutils.notebook.entry_point.getDbutils().notebook()
  .getContext().notebookPath().get()` con prefijo `/Workspace`.
- **Alcance del TDD — REGLA PERMANENTE**: Las pruebas TDD cubren
  EXCLUSIVAMENTE los modulos de `utilities/` (Python puro
  importable). Los notebooks de `transformations/` NO son testeables
  via TDD porque ejecutan codigo a nivel de modulo (patron Closure:
  `spark.conf.get("pipelines.parameters.*")`, `obtener_parametros()`)
  que requiere un pipeline LSDP desplegado. Al importar el modulo
  desde TDD, ese codigo se ejecuta y falla porque no existe el
  contexto del pipeline. La validacion de las transformaciones se
  realiza unicamente mediante el despliegue del pipeline LSDP.

**Razon**: Las pruebas garantizan que cada componente del pipeline
funciona de forma aislada antes de la integracion, reduciendo
defectos en un entorno donde la depuracion es costosa.

### IX. Paradigma Declarativo LSDP Estricto

La solucion DEBE usar **Lakeflow Spark Declarative Pipelines (LSDP)**
de forma estricta (URL oficial:
https://docs.databricks.com/aws/en/ldp/).

- LSDP es la evolucion de Delta Live Tables (DLT) con una API
  renovada. Queda PROHIBIDO el uso de la API legacy `dlt.*`
  (`import dlt`). Se EXIGE el uso exclusivo de la API nueva con
  decoradores `@dp.table` y `@dp.materialized_view` (del modulo
  `pyspark.pipelines`, importado como
  `from pyspark import pipelines as dp`). Toda referencia a
  documentacion DEBE basarse en la URL oficial de LSDP.
- Los scripts de LSDP DEBEN ser codificados en PySpark/Python.
- Los scripts de LSDP DEBEN ser ejecutados por Computo Serverless.
- El codigo generado en LSDP DEBE ser totalmente compatible con el
  paradigma declarativo.
- Se DEBE aprovechar al maximo la arquitectura medallion:
  - Bronce: streaming tables, append-only con FechaIngestaDatos.
  - Plata: vistas materializadas. Para la vista transaccional, el
    procesamiento declarativo NO DEBE aplicar filtrados de datos,
    lo cual permite que LSDP decida automaticamente realizar
    cargas incrementales en lugar de cargas completas (aplicar
    filtros forzaria una carga completa por defecto).
  - Oro: vistas materializadas con producto de datos final.
- Las tablas delta y vistas materializadas DEBEN tener activas:
  Change Data Feed, autoOptimize.autoCompact,
  autoOptimize.optimizeWrite, Liquid Cluster,
  `delta.deletedFileRetentionDuration = interval 30 days`,
  `delta.logRetentionDuration = interval 60 days`.

**Razon**: El paradigma declarativo garantiza reproducibilidad,
optimizacion automatica y gobernanza de datos por parte del motor
de Databricks.

## Restricciones Tecnologicas

Las siguientes restricciones son de cumplimiento obligatorio y no
admiten excepcion:

- **Protocolo `abfss://`**: PROHIBIDO (exclusivo de Azure).
- **Rutas `/mnt/`**: PROHIBIDAS (legacy DBFS mounts).
- **DBFS root**: PROHIBIDO (deprecado).
- **Azure SQL, Azure Key Vault, Secret Scopes**: PROHIBIDOS.
- **`spark.sparkContext`**: PROHIBIDO (incompatible con serverless).
- **ZOrder y PartitionBy**: PROHIBIDOS en tablas delta y vistas
  materializadas. Se DEBE usar Liquid Cluster.
- **API legacy `dlt.*` (Delta Live Tables)**: PROHIBIDA. Se DEBE
  usar exclusivamente la API nueva de LSDP (`@dp.table`,
  `@dp.materialized_view`).
- **Valores hardcodeados**: PROHIBIDOS. Todo DEBE ser parametrizado.
- **Errores silenciosos**: PROHIBIDOS. Todo DEBE imprimirse.
- **Codigo espaguetti**: PROHIBIDO. El desarrollo DEBE ser modular
  siguiendo el principio SOLID de responsabilidad unica.
- **Emojis**: PROHIBIDOS en todo el codigo y documentacion generada.

## Flujo de Desarrollo y Estandares de Codigo

Estandares que rigen todo el desarrollo del proyecto:

- **Idioma del codigo**: Todo el codigo DEBE estar en espanol
  (variables, funciones, comentarios, nombres de tablas y columnas).
- **Formato de nombres en codigo**: snake_case en minuscula para
  variables, constantes, clases, funciones, metodos y objetos.
- **Nombres de archivos .py**: PascalCase con prefijo "Nb" para
  notebooks y "Lsdp" para scripts del pipeline.
- **Nombres de tablas y columnas (Plata/Oro)**: en espanol,
  snake_case, intuitivos, con minimo dos palabras o silabas. NO
  DEBEN coincidir con los nombres de Bronce.
- **Modularidad**: el directorio `utilities/` contiene funciones
  transversales reutilizables. El directorio `transformations/`
  DEBE modularizar en funciones.
- **Bloques markdown**: estilo explicativo y profesional.
- **Respuestas de la IA**: DEBEN estar en idioma espanol.
- **Gestion de branches**: controlada exclusivamente por el usuario.
  La IA NO DEBE crear branches ni hacer commits automaticos.
- **Toma de decisiones**: la IA plantea propuestas, pero solo el
  usuario toma la decision final.
- **La IA NO DEBE alucinar**: DEBE mantenerse dentro del alcance
  estipulado por cada punto.
- **Research**: al iniciar una nueva version, siempre se DEBE
  investigar la documentacion oficial de LSDP y extensiones de
  Databricks para VS Code.
- **Links de research**: DEBEN ser de fuentes oficiales de
  Databricks, Python y Spark.

## Governance

Esta constitution es el documento rector del proyecto
DbsFreeLakeflowSparkDeclarativePipelinesBase. Todas las decisiones
de diseno, implementacion y revision DEBEN verificar cumplimiento
con los principios aqui establecidos.

- **Enmiendas**: Cualquier cambio a esta constitution DEBE ser
  documentado con justificacion, aprobado por el usuario, y
  reflejado en el versionamiento semantico del documento.
- **Versionamiento**: Se usa MAJOR.MINOR.PATCH:
  - MAJOR: Eliminacion o redefinicion incompatible de principios.
  - MINOR: Adicion de principios o expansion material de guias.
  - PATCH: Clarificaciones, correcciones de redaccion, ajustes
    no semanticos.
- **Revision de cumplimiento**: Cada incremento del proyecto DEBE
  iniciar con una verificacion de adherencia a esta constitution.
- **Archivo de referencia**: El archivo SYSTEM.md contiene el
  contexto completo del proyecto y alimenta los comandos de
  spec-kit.

**Version**: 1.1.0 | **Ratified**: 2026-04-03 | **Last Amended**: 2026-04-03
