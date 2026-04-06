# Databricks notebook source
# MAGIC %md
# MAGIC # LsdpOroClientes
# MAGIC
# MAGIC Script LSDP que define las dos vistas materializadas de oro en `oro.lab1`:
# MAGIC
# MAGIC - `comportamiento_atm_cliente`: agrega transacciones de plata por cliente
# MAGIC   usando agregacion condicional sin filtros previos. Calcula 5 metricas ATM
# MAGIC   (cantidad depositos, cantidad retiros, promedio depositos, promedio retiros,
# MAGIC   total pagos al saldo). Tipos de transaccion leidos de la tabla Parametros.
# MAGIC
# MAGIC - `resumen_integral_cliente`: combina datos dimensionales de
# MAGIC   `clientes_saldos_consolidados` (plata) con metricas ATM de
# MAGIC   `comportamiento_atm_cliente` (oro) via INNER JOIN. 22 columnas totales.
# MAGIC   Solo clientes presentes en ambas fuentes.
# MAGIC
# MAGIC Patron Closure autocontenido: parametros del pipeline y tabla Parametros se
# MAGIC calculan una sola vez al importar el script y quedan capturados por closure
# MAGIC en las funciones decoradas con @dp.materialized_view.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from utilities.LsdpConexionParametros import obtener_parametros

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inicializacion del modulo — Patron Closure
# MAGIC
# MAGIC Lee parametros del pipeline y tabla Parametros. Estos valores se calculan
# MAGIC una sola vez al importar el script y son capturados por closure en las
# MAGIC funciones decoradas, cumpliendo el requisito de compatibilidad con Serverless.

# COMMAND ----------

# --- Lectura de parametros del pipeline ---
catalogo_parametro = spark.conf.get("pipelines.parameters.catalogoParametro")
esquema_parametro  = spark.conf.get("pipelines.parameters.esquemaParametro")
tabla_parametros   = spark.conf.get("pipelines.parameters.tablaParametros")

print("[LsdpOroClientes] Inicializando modulo...")
print(f"  catalogoParametro = {catalogo_parametro}")
print(f"  esquemaParametro  = {esquema_parametro}")
print(f"  tablaParametros   = {tabla_parametros}")

# --- Lectura de la tabla Parametros ---
parametros = obtener_parametros(
    spark, catalogo_parametro, esquema_parametro, tabla_parametros
)

# --- Extraccion de catalogos y esquemas ---
catalogo_oro   = parametros.get("catalogoOro",   "oro")
esquema_oro    = parametros.get("esquemaOro",    "lab1")
catalogo_plata = parametros.get("catalogoPlata", "plata")
esquema_plata  = parametros.get("esquemaPlata",  "lab1")

print(f"[LsdpOroClientes] Catalogo oro    destino : {catalogo_oro}")
print(f"[LsdpOroClientes] Esquema oro     destino : {esquema_oro}")
print(f"[LsdpOroClientes] Catalogo plata  fuente  : {catalogo_plata}")
print(f"[LsdpOroClientes] Esquema plata   fuente  : {esquema_plata}")

# --- Tipos de transaccion desde tabla Parametros ---
tipos_transaccion_str = parametros.get("TiposTransaccionesLabBase", "DATM,CATM,PGSL")
tipos_transaccion     = tipos_transaccion_str.split(",")
TIPO_DEPOSITO_ATM     = tipos_transaccion[0]
TIPO_RETIRO_ATM       = tipos_transaccion[1]
TIPO_PAGO_SALDO       = tipos_transaccion[2]

print(f"[LsdpOroClientes] Tipos de transaccion (clave TiposTransaccionesLabBase): {tipos_transaccion_str}")
print(f"  TIPO_DEPOSITO_ATM = {TIPO_DEPOSITO_ATM}")
print(f"  TIPO_RETIRO_ATM   = {TIPO_RETIRO_ATM}")
print(f"  TIPO_PAGO_SALDO   = {TIPO_PAGO_SALDO}")

# --- Vistas materializadas a crear ---
print(f"[LsdpOroClientes] Vistas materializadas a crear:")
print(f"  1. {catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente")
print(f"     Liquid cluster: ['identificador_cliente']")
print(f"     Metricas: ['cantidad_depositos_atm', 'cantidad_retiros_atm', 'promedio_monto_depositos_atm', 'promedio_monto_retiros_atm', 'total_pagos_saldo_cliente']")
print(f"  2. {catalogo_oro}.{esquema_oro}.resumen_integral_cliente")
print(f"     Liquid cluster: ['huella_identificacion_cliente', 'identificador_cliente']")
print(f"     Columnas (22): ['huella_identificacion_cliente', 'identificador_cliente', 'nombre_cliente', 'apellido_cliente', 'nacionalidad_cliente', 'pais_residencia', 'ciudad_residencia', 'ocupacion_cliente', 'nivel_educativo', 'clasificacion_riesgo_cliente', 'categoria_saldo_disponible', 'perfil_actividad_bancaria', 'limite_credito', 'saldo_disponible', 'fecha_apertura_cuenta', 'estado_cuenta', 'tipo_cuenta', 'cantidad_depositos_atm', 'cantidad_retiros_atm', 'promedio_monto_depositos_atm', 'promedio_monto_retiros_atm', 'total_pagos_saldo_cliente']")

# --- Propiedades Delta optimizadas (compartidas por ambas vistas) ---
propiedades_delta_oro = {
    "delta.enableChangeDataFeed":              "true",
    "delta.autoOptimize.autoCompact":          "true",
    "delta.autoOptimize.optimizeWrite":        "true",
    "delta.deletedFileRetentionDuration":      "interval 30 days",
    "delta.logRetentionDuration":              "interval 60 days",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Vista Materializada 1: comportamiento_atm_cliente
# MAGIC
# MAGIC Agrega las transacciones de `transacciones_enriquecidas` (plata) por
# MAGIC `identificador_cliente` usando agregacion condicional sin filtros previos.
# MAGIC Calcula 5 metricas ATM. Tipos de transaccion leidos de tabla Parametros.

# COMMAND ----------


@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente",
    table_properties=propiedades_delta_oro,
    cluster_by=["identificador_cliente"],
)
def comportamiento_atm_cliente() -> DataFrame:
    df_transacciones = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas"
    )

    df_agrupado = df_transacciones.groupBy("identificador_cliente").agg(
        F.count(F.when(F.col("tipo_transaccion") == TIPO_DEPOSITO_ATM, F.col("monto_principal"))).alias("cantidad_depositos_atm"),
        F.count(F.when(F.col("tipo_transaccion") == TIPO_RETIRO_ATM,   F.col("monto_principal"))).alias("cantidad_retiros_atm"),
        F.avg(F.when(F.col("tipo_transaccion") == TIPO_DEPOSITO_ATM,   F.col("monto_principal"))).alias("promedio_monto_depositos_atm"),
        F.avg(F.when(F.col("tipo_transaccion") == TIPO_RETIRO_ATM,     F.col("monto_principal"))).alias("promedio_monto_retiros_atm"),
        F.sum(F.when(F.col("tipo_transaccion") == TIPO_PAGO_SALDO,     F.col("monto_principal"))).alias("total_pagos_saldo_cliente"),
    )

    df_resultado = (
        df_agrupado
        .withColumn("cantidad_depositos_atm",        F.coalesce(F.col("cantidad_depositos_atm"),        F.lit(0)))
        .withColumn("cantidad_retiros_atm",           F.coalesce(F.col("cantidad_retiros_atm"),           F.lit(0)))
        .withColumn("promedio_monto_depositos_atm",   F.coalesce(F.col("promedio_monto_depositos_atm"),   F.lit(0.0)))
        .withColumn("promedio_monto_retiros_atm",     F.coalesce(F.col("promedio_monto_retiros_atm"),     F.lit(0.0)))
        .withColumn("total_pagos_saldo_cliente",      F.coalesce(F.col("total_pagos_saldo_cliente"),      F.lit(0.0)))
    )

    return df_resultado


# COMMAND ----------
# MAGIC %md
# MAGIC ## Vista Materializada 2: resumen_integral_cliente
# MAGIC
# MAGIC Combina datos dimensionales de `clientes_saldos_consolidados` (plata) con
# MAGIC metricas ATM de `comportamiento_atm_cliente` (oro) via INNER JOIN por
# MAGIC `identificador_cliente`. Produce 22 columnas. Solo incluye clientes presentes
# MAGIC en ambas fuentes.

# COMMAND ----------


@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.resumen_integral_cliente",
    table_properties=propiedades_delta_oro,
    cluster_by=["huella_identificacion_cliente", "identificador_cliente"],
)
def resumen_integral_cliente() -> DataFrame:
    df_plata = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados"
    )
    df_oro = spark.read.table(
        f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente"
    )

    df_resultado = df_plata.join(df_oro, on="identificador_cliente", how="inner")

    return df_resultado.select(
        "huella_identificacion_cliente",
        "identificador_cliente",
        "nombre_cliente",
        "apellido_cliente",
        "nacionalidad_cliente",
        "pais_residencia",
        "ciudad_residencia",
        "ocupacion_cliente",
        "nivel_educativo",
        "clasificacion_riesgo_cliente",
        "categoria_saldo_disponible",
        "perfil_actividad_bancaria",
        "limite_credito",
        "saldo_disponible",
        "fecha_apertura_cuenta",
        "estado_cuenta",
        "tipo_cuenta",
        F.coalesce(F.col("cantidad_depositos_atm"),        F.lit(0)).alias("cantidad_depositos_atm"),
        F.coalesce(F.col("cantidad_retiros_atm"),           F.lit(0)).alias("cantidad_retiros_atm"),
        F.coalesce(F.col("promedio_monto_depositos_atm"),   F.lit(0.0)).alias("promedio_monto_depositos_atm"),
        F.coalesce(F.col("promedio_monto_retiros_atm"),     F.lit(0.0)).alias("promedio_monto_retiros_atm"),
        F.coalesce(F.col("total_pagos_saldo_cliente"),      F.lit(0.0)).alias("total_pagos_saldo_cliente"),
    )
