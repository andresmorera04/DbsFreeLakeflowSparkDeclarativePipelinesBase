# Databricks notebook source
# MAGIC %md
# MAGIC # LsdpBronceBlncfl
# MAGIC
# MAGIC Script LSDP que define la streaming table `blncfl` en `bronce.regional`.
# MAGIC Ingesta incremental del parquet Saldos de Cliente AS400 via AutoLoader
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
ruta_relativa_parquet = spark.conf.get("pipelines.parameters.rutaCompletaSaldoCliente")
ruta_relativa_schema = spark.conf.get("pipelines.parameters.rutaSchemaLocationBlncfl")

print("[LsdpBronceBlncfl] Inicializando modulo...")
print(f"  catalogoParametro         = {catalogo_parametro}")
print(f"  esquemaParametro          = {esquema_parametro}")
print(f"  tablaParametros           = {tabla_parametros}")
print(f"  rutaCompletaSaldoCliente  = {ruta_relativa_parquet}")
print(f"  rutaSchemaLocationBlncfl  = {ruta_relativa_schema}")

# --- Lectura de la tabla Parametros ---
diccionario_parametros = obtener_parametros(spark, catalogo_parametro, esquema_parametro, tabla_parametros)

# --- Construccion de rutas absolutas ---
ruta_parquet = construir_ruta(diccionario_parametros, ruta_relativa_parquet)
ruta_schema_location = construir_ruta(diccionario_parametros, ruta_relativa_schema)

print(f"[LsdpBronceBlncfl] Ruta parquet BLNCFL  : {ruta_parquet}")
print(f"[LsdpBronceBlncfl] Ruta schema location : {ruta_schema_location}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Funcion pura de transformacion
# MAGIC
# MAGIC Separada del decorador @dp.table para permitir TDD sin AutoLoader.

# COMMAND ----------

campos_cluster_blncfl = ["FechaIngestaDatos", "CUSTID"]


def transformar_blncfl(df: DataFrame) -> DataFrame:
    """
    Logica pura de transformacion para la streaming table blncfl.

    Agrega el campo FechaIngestaDatos como marca de tiempo de ingesta
    e invoca LsdpReordenarColumnasLiquidCluster para colocar los campos
    del liquid cluster como las primeras columnas del schema.

    Parametros:
        df: DataFrame de PySpark (streaming o batch)

    Retorna:
        DataFrame con FechaIngestaDatos en posicion 1, CUSTID en posicion 2
        y el resto de columnas en su orden original.
    """
    print(f"[LsdpBronceBlncfl] transformar_blncfl — tabla destino: bronce.regional.blncfl")
    print(f"[LsdpBronceBlncfl] Campos del liquid cluster: {campos_cluster_blncfl}")

    df_con_timestamp = df.withColumn("FechaIngestaDatos", F.current_timestamp())
    df_reordenado = reordenar_columnas_liquid_cluster(df_con_timestamp, campos_cluster_blncfl)

    print(f"[LsdpBronceBlncfl] Reordenamiento de columnas exitoso. Schema: {df_reordenado.columns[:4]}...")
    return df_reordenado

# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming table blncfl — Decorador @dp.table
# MAGIC
# MAGIC Define la streaming table en bronce.regional con AutoLoader, schema evolution,
# MAGIC liquid cluster y propiedades Delta optimizadas.

# COMMAND ----------

@dp.table(
    name="blncfl",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
    },
    cluster_by=["FechaIngestaDatos", "CUSTID"],
)
def tabla_bronce_blncfl():
    """
    Streaming table blncfl: ingesta incremental del parquet Saldos de Cliente AS400.
    AutoLoader con schema evolution activo (addNewColumns).
    """
    df_streaming = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", ruta_schema_location)
        .load(ruta_parquet)
    )
    return transformar_blncfl(df_streaming)
