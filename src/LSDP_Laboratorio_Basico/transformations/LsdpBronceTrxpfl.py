# Databricks notebook source
# MAGIC %md
# MAGIC # LsdpBronceTrxpfl
# MAGIC
# MAGIC Script LSDP que define la streaming table `trxpfl` en `bronce.lab1`.
# MAGIC Ingesta incremental del parquet Transaccional AS400 via AutoLoader
# MAGIC con schema evolution activo, liquid cluster y propiedades Delta optimizadas.
# MAGIC
# MAGIC Patron Closure autocontenido: los parametros se calculan a nivel de modulo
# MAGIC y quedan capturados por closure en la funcion decorada con @dp.table.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from utilities.LsdpConexionParametros import obtener_parametros
from utilities.LsdpConstructorRutas import construir_ruta
from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inicializacion del modulo — Patron Closure
# MAGIC
# MAGIC Lee parametros del pipeline, conexion a tabla Parametros y construccion de rutas.
# MAGIC Estos valores se calculan una sola vez al importar el script.

# COMMAND ----------

# --- Lectura de parametros del pipeline ---
catalogo_parametro = spark.conf.get("pipelines.parameters.catalogoParametro")
esquema_parametro = spark.conf.get("pipelines.parameters.esquemaParametro")
tabla_parametros = spark.conf.get("pipelines.parameters.tablaParametros")
ruta_relativa_parquet = spark.conf.get("pipelines.parameters.rutaCompletaTransaccional")
ruta_relativa_schema = spark.conf.get("pipelines.parameters.rutaSchemaLocationTrxpfl")

print("[LsdpBronceTrxpfl] Inicializando modulo...")
print(f"  catalogoParametro          = {catalogo_parametro}")
print(f"  esquemaParametro           = {esquema_parametro}")
print(f"  tablaParametros            = {tabla_parametros}")
print(f"  rutaCompletaTransaccional  = {ruta_relativa_parquet}")
print(f"  rutaSchemaLocationTrxpfl   = {ruta_relativa_schema}")

# --- Lectura de la tabla Parametros ---
diccionario_parametros = obtener_parametros(spark, catalogo_parametro, esquema_parametro, tabla_parametros)

# --- Construccion de rutas absolutas ---
ruta_parquet = construir_ruta(diccionario_parametros, ruta_relativa_parquet)
ruta_schema_location = construir_ruta(diccionario_parametros, ruta_relativa_schema)

print(f"[LsdpBronceTrxpfl] Ruta parquet TRXPFL  : {ruta_parquet}")
print(f"[LsdpBronceTrxpfl] Ruta schema location : {ruta_schema_location}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Funcion pura de transformacion
# MAGIC
# MAGIC Separada del decorador @dp.table para permitir TDD sin AutoLoader.

# COMMAND ----------

campos_cluster_trxpfl = ["TRXDT", "CUSTID", "TRXTYP"]


def transformar_trxpfl(df: DataFrame) -> DataFrame:
    """
    Logica pura de transformacion para la streaming table trxpfl.

    Agrega el campo FechaIngestaDatos como marca de tiempo de ingesta
    e invoca LsdpReordenarColumnasLiquidCluster para colocar los campos
    del liquid cluster como las primeras columnas del schema.

    Nota: FechaIngestaDatos queda en posicion 4, despues de los 3 campos
    del liquid cluster (TRXDT, CUSTID, TRXTYP).

    Parametros:
        df: DataFrame de PySpark (streaming o batch)

    Retorna:
        DataFrame con TRXDT en posicion 1, CUSTID en posicion 2, TRXTYP en posicion 3,
        FechaIngestaDatos en posicion 4 y el resto de columnas en su orden original.
    """
    print(f"[LsdpBronceTrxpfl] transformar_trxpfl — tabla destino: bronce.lab1.trxpfl")
    print(f"[LsdpBronceTrxpfl] Campos del liquid cluster: {campos_cluster_trxpfl}")

    df_con_timestamp = df.withColumn("FechaIngestaDatos", F.current_timestamp())
    df_reordenado = reordenar_columnas_liquid_cluster(df_con_timestamp, campos_cluster_trxpfl)

    print(f"[LsdpBronceTrxpfl] Reordenamiento de columnas exitoso. Schema: {df_reordenado.columns[:5]}...")
    return df_reordenado

# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming table trxpfl — Decorador @dp.table
# MAGIC
# MAGIC Define la streaming table en bronce.lab1 con AutoLoader, schema evolution,
# MAGIC liquid cluster y propiedades Delta optimizadas.

# COMMAND ----------

@dp.table(
    name="trxpfl",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
    },
    cluster_by=["TRXDT", "CUSTID", "TRXTYP"],
)
def tabla_bronce_trxpfl():
    """
    Streaming table trxpfl: ingesta incremental del parquet Transaccional AS400.
    AutoLoader con schema evolution activo (addNewColumns).
    """
    df_streaming = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", ruta_schema_location)
        .load(ruta_parquet)
    )
    return transformar_trxpfl(df_streaming)
