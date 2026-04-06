# Investigacion Fase 0: Incremento 3 - LSDP Medalla de Bronce

**Feature**: 003-lsdp-bronce-pipeline
**Fecha**: 2026-04-04
**Estado**: Todas las decisiones APROBADAS por el usuario (2026-04-04).

## Decisiones Heredadas del Incremento 1

Las siguientes decisiones fueron investigadas en el Incremento 1, aprobadas por el usuario el 2026-04-03, y aplican directamente a este incremento:

### Decision 1 (heredada): API de Lakeflow Spark Declarative Pipelines (LSDP)

**Estado**: APROBADA por el usuario (2026-04-03)
**Fuente oficial**: https://docs.databricks.com/aws/en/ldp/

**Resumen**: Adoptar exclusivamente LSDP con `@dp.table` y `@dp.materialized_view` del modulo `pyspark.pipelines` (importado como `from pyspark import pipelines as dp`). La API legacy `dlt.*` (`import dlt`) queda prohibida. Los checkpoints de AutoLoader son completamente automaticos en LSDP. El parametro `cluster_by` del decorador soporta Liquid Clustering. Los parametros `catalog` y `schema` permiten crear tablas en catalogos/esquemas distintos al default del pipeline.

**Aplicacion en Incremento 3**: Los tres scripts de bronce (`LsdpBronceCmstfl.py`, `LsdpBronceTrxpfl.py`, `LsdpBronceBlncfl.py`) usan `@dp.table` con `cluster_by` y `table_properties`. No se usa `import dlt` en ningun archivo.

### Decision 2 (heredada): AutoLoader para Ingestion Incremental en Bronce

**Estado**: APROBADA por el usuario (2026-04-03)
**Fuente oficial**: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/

**Resumen**: Usar AutoLoader en LSDP para toda la ingestion incremental en la medalla de bronce. Formato de lectura: `spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(ruta)`. Checkpoints automaticos en LSDP. Rutas de Unity Catalog Volumes (`/Volumes/...`) y S3 (`s3://...`) soportadas.

**Aplicacion en Incremento 3**: Cada script de bronce usa AutoLoader con `cloudFiles.format=parquet`, `cloudFiles.schemaEvolutionMode=addNewColumns` y `cloudFiles.schemaLocation` configurado por parametro del pipeline.

### Decision 4 (heredada): Extensiones de Databricks para Visual Studio Code

**Estado**: APROBADA por el usuario (2026-04-03)
**Fuente oficial**: https://docs.databricks.com/aws/en/dev-tools/vscode-ext/

**Resumen**: Verificar que Databricks Extension for Visual Studio Code y Databricks Driver for SQLTools esten instaladas. Usar "Run on Serverless" para todas las ejecuciones.

**Aplicacion en Incremento 3**: Las pruebas TDD (`NbTddBroncePipeline.py`) se ejecutan desde la extension Databricks para VS Code con "Run on Serverless".

---

## Decisiones Nuevas del Incremento 3

### Decision 6: Schema Evolution en AutoLoader para Bronce

**Contexto**: Las streaming tables de bronce deben soportar cambios en el schema de los parquets AS400 fuente sin modificar el codigo del pipeline. El spec (RF-025) exige `cloudFiles.schemaEvolutionMode=addNewColumns` y `cloudFiles.schemaLocation` en cada lectura AutoLoader.

**Decision**: Configurar AutoLoader con schema evolution activo usando tres opciones obligatorias en cada lectura de streaming:

1. `cloudFiles.format` = `parquet` — formato del archivo fuente.
2. `cloudFiles.schemaEvolutionMode` = `addNewColumns` — si el parquet fuente agrega columnas nuevas, AutoLoader las incorpora automaticamente a la streaming table sin error. Si el parquet elimina columnas, los registros nuevos tendran `null` en esas columnas.
3. `cloudFiles.schemaLocation` = ruta absoluta (Volume o S3) — AutoLoader almacena aqui el schema inferido y su historial de evoluciones. Esta ruta es distinta del checkpoint de streaming (que es automatico en LSDP).

Las rutas de schema location se reciben como parametros del pipeline (rutas relativas: `rutaSchemaLocationCmstfl`, `rutaSchemaLocationTrxpfl`, `rutaSchemaLocationBlncfl`) y se convierten a rutas absolutas via `LsdpConstructorRutas` al inicializar el modulo.

**Razonamiento**: El modo `addNewColumns` es el mas adecuado para un sistema AS400 cuyos parquets pueden evolucionar entre extracciones. El modo `rescue` generaria una columna `_rescued_data` con datos en formato JSON dificiles de procesar. El modo `failOnNewColumns` detendria el pipeline ante cualquier cambio, lo cual es inaceptable en produccion. El modo `none` ignora silenciosamente los cambios de schema, violando el principio de observabilidad.

**Alternativas consideradas**:
- `cloudFiles.schemaEvolutionMode=rescue`: Descartado porque los datos de columnas nuevas quedan en una sola columna JSON `_rescued_data`, dificultando su uso posterior.
- `cloudFiles.schemaEvolutionMode=failOnNewColumns`: Descartado porque detiene el pipeline ante cualquier cambio de schema.
- `cloudFiles.schemaEvolutionMode=none`: Descartado porque ignora silenciosamente los cambios de schema.

**Nota**: Aunque el modo `addNewColumns` esta activo, AutoLoader siempre genera la columna `_rescued_data` para capturar datos que no encajan con el schema vigente (por ejemplo, cambios de tipo de dato). Esto se refleja en los conteos de columnas de las streaming tables: 72 para cmstfl (70 originales + FechaIngestaDatos + _rescued_data), 62 para trxpfl y 102 para blncfl.

---

### Decision 7: Patron Funcion Pura para Testabilidad de Streaming Tables

**Contexto**: El spec (RF-007, RF-008, RF-009, Q7) define que cada script de bronce debe exponer una funcion pura `transformar_<tabla>(df)` separada del decorador `@dp.table`, para permitir TDD sin necesidad de AutoLoader ni pipeline desplegado.

**Decision**: Cada script de bronce implementa el siguiente patron:

```python
# --- Patron de funcion pura + decorador ---
# NOTA: LSDP resuelve automaticamente los imports entre carpetas del pipeline.
# NO usar sys.path.insert() en scripts que se ejecutan dentro del pipeline.
# NOTA: Los notebooks de Databricks NO definen __file__. Para resolver rutas
# de filesystem, usar dbutils.notebook.entry_point...notebookPath().get().
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from utilities.LsdpConexionParametros import obtener_parametros
from utilities.LsdpConstructorRutas import construir_ruta
from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster

def transformar_cmstfl(df: DataFrame) -> DataFrame:
    """Logica pura de transformacion: agrega FechaIngestaDatos y reordena columnas."""
    df_con_timestamp = df.withColumn("FechaIngestaDatos", F.current_timestamp())
    df_reordenado = reordenar_columnas_liquid_cluster(df_con_timestamp, ["FechaIngestaDatos", "CUSTID"])
    return df_reordenado

@dp.table(
    name="cmstfl",
    table_properties={...},
    cluster_by=["FechaIngestaDatos", "CUSTID"]
)
def tabla_bronce_cmstfl():
    df_streaming = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", ruta_schema_location_cmstfl)
        .load(ruta_completa_maestro_cliente))
    return transformar_cmstfl(df_streaming)
```

El TDD NO puede invocar `transformar_cmstfl(df)` directamente porque el modulo ejecuta codigo a nivel de modulo (patron Closure: `spark.conf.get("pipelines.parameters.*")`, `obtener_parametros()`) que requiere un pipeline LSDP desplegado. Al importar el modulo desde TDD, ese codigo falla. El TDD cubre EXCLUSIVAMENTE las utilidades de `utilities/` (Python puro importable).

**Razonamiento**: Separar la logica de transformacion del decorador LSDP facilita la organizacion del codigo. Sin embargo, el patron Closure autocontenido (Decision 8) ejecuta codigo a nivel de modulo que impide la importacion de los notebooks de `transformations/` fuera del contexto LSDP. Por esta razon, las funciones puras de transformacion no son testeables via TDD — la validacion de las transformaciones se realiza mediante el despliegue del pipeline.

**Alternativas consideradas**:
- Todo dentro del decorador `@dp.table`: Descartado porque no permite separacion de responsabilidades.
- Mocking de AutoLoader en TDD: Descartado por complejidad excesiva y fragilidad de los mocks.
- TDD directo sobre funciones puras de transformacion: **Descartado** — el patron Closure (Decision 8) ejecuta `spark.conf.get()` y `obtener_parametros()` a nivel de modulo, haciendo que los archivos no sean importables fuera de LSDP.

---

### Decision 8: Patron Closure Autocontenido por Script

**Contexto**: El spec (RF-013, Q8) define que cada script de bronce es autocontenido: lee sus propios parametros, invoca las utilidades de forma independiente a nivel de modulo. No existe modulo inicializador compartido.

**Decision**: Cada script de bronce sigue el patron Closure independiente:

```python
# --- Nivel de modulo (se ejecuta una vez al inicializar el pipeline) ---
catalogo_parametro = spark.conf.get("pipelines.parameters.catalogoParametro")
esquema_parametro = spark.conf.get("pipelines.parameters.esquemaParametro")
tabla_parametros = spark.conf.get("pipelines.parameters.tablaParametros")
ruta_relativa = spark.conf.get("pipelines.parameters.rutaCompletaMaestroCliente")
ruta_schema_location = spark.conf.get("pipelines.parameters.rutaSchemaLocationCmstfl")

# Lectura de tabla Parametros
diccionario_parametros = obtener_parametros(spark, catalogo_parametro, esquema_parametro, tabla_parametros)

# Construccion de rutas
ruta_completa = construir_ruta(diccionario_parametros, ruta_relativa)
ruta_schema = construir_ruta(diccionario_parametros, ruta_schema_location)

# Los valores quedan capturados por closure en las funciones decoradas
@dp.table(...)
def tabla_bronce_cmstfl():
    # ruta_completa y ruta_schema se leen por closure
    ...
```

Cada script repite la lectura de parametros y la construccion de rutas de forma independiente. cloudpickle serializa automaticamente las variables capturadas por closure al enviarlas al ejecutor.

**Razonamiento**: El patron autocontenido maximiza la independencia de cada script. Si un script falla o necesita cambios, los otros no se ven afectados. Ademas, LSDP puede ejecutar los scripts en paralelo sin dependencias entre modulos. La duplicacion de lectura de parametros es minima (3 llamadas `spark.conf.get()` + 1 lectura de tabla por script) y es preferible a un acoplamiento entre scripts.

**Alternativas consideradas**:
- Modulo inicializador compartido (`__init__.py` o similar): Descartado por acoplamiento entre scripts y posibles problemas de orden de ejecucion en LSDP.
- Variables de entorno compartidas: Descartado porque no estan disponibles en Serverless.

---

### Decision 9: Validacion de Claves en el Consumidor (No en la Utilidad)

**Contexto**: El spec (RF-004, Q9) define que `LsdpConexionParametros` es una utilidad generica de lectura que retorna el diccionario completo sin validar claves. Cada script consumidor valida las claves que necesita.

**Decision**: `LsdpConexionParametros` retorna `{Clave: Valor}` sin validacion. Cada script de bronce accede a las claves que necesita del diccionario y lanza excepcion descriptiva si una clave requerida no se encuentra:

```python
# En cada script de bronce, al consumir el diccionario:
tipo_storage = diccionario_parametros.get("TipoStorage")
if tipo_storage is None:
    raise ValueError("La clave 'TipoStorage' no se encontro en la tabla Parametros.")
```

**Razonamiento**: La utilidad generica no puede conocer a priori que claves necesita cada consumidor. Las claves requeridas pueden variar: bronce necesita rutas de parquets y storage; plata necesitara catalogos adicionales; oro tendra sus propias necesidades. Validar en el consumidor sigue el principio de responsabilidad unica (SRP): la utilidad lee, el consumidor valida.

**Alternativas consideradas**:
- Validacion centralizada con lista de claves obligatorias: Descartado porque acopla la utilidad a los requerimientos especificos de cada consumidor.
- Schema de configuracion con validacion JSON: Descartado por complejidad excesiva para el alcance del proyecto.

---

## Resumen de Decisiones

| # | Tema | Decision | Estado |
|---|------|----------|--------|
| 1 | API LSDP | `@dp.table` de `pyspark.pipelines` (`from pyspark import pipelines as dp`). Prohibir `dlt.*`. | Heredada — APROBADA (2026-04-03) |
| 2 | AutoLoader | Ingestion incremental con checkpoints automaticos en LSDP. | Heredada — APROBADA (2026-04-03) |
| 4 | Extensiones VS Code | Databricks Extension + SQLTools. Run on Serverless. | Heredada — APROBADA (2026-04-03) |
| 6 | Schema Evolution | `addNewColumns` + `schemaLocation` por parametro del pipeline. | APROBADA (2026-04-04) |
| 7 | Funcion Pura | `transformar_<tabla>(df)` separada de `@dp.table` para organizacion de codigo. TDD no puede importar estos modulos (codigo de nivel de modulo requiere LSDP). | APROBADA (2026-04-04) — TDD limitado a utilities/ |
| 8 | Closure Autocontenido | Cada script lee parametros independientemente. Sin inicializador compartido. | APROBADA (2026-04-04) |
| 9 | Validacion de Claves | Consumidor valida. `LsdpConexionParametros` solo lee y retorna. | APROBADA (2026-04-04) |
