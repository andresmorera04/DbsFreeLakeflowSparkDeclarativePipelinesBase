# Databricks notebook source
# MAGIC %md
# MAGIC # LsdpPlataTransacciones
# MAGIC
# MAGIC Script LSDP que define la vista materializada `transacciones_enriquecidas`
# MAGIC en `plata.regional`.
# MAGIC
# MAGIC Lee la streaming table de bronce `trxpfl` SIN filtros para maximizar la
# MAGIC carga incremental automatica de LSDP. Excluye columnas de control de bronce,
# MAGIC renombra todas las columnas a espanol snake_case y agrega 4 campos calculados
# MAGIC numericos con proteccion de nulos y division por cero.
# MAGIC
# MAGIC Patron Closure autocontenido: parametros del pipeline y tabla Parametros se
# MAGIC calculan una sola vez al importar el script y quedan capturados por closure
# MAGIC en la funcion decorada con @dp.materialized_view.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from utilities.LsdpConexionParametros import obtener_parametros
from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inicializacion del modulo — Patron Closure
# MAGIC
# MAGIC Lee parametros del pipeline y tabla Parametros. Estos valores se calculan
# MAGIC una sola vez al importar el script y son capturados por closure en la funcion
# MAGIC decorada, cumpliendo el requisito de compatibilidad con Serverless Compute.

# COMMAND ----------

# --- Lectura de parametros del pipeline ---
catalogo_parametro = spark.conf.get("pipelines.parameters.catalogoParametro")
esquema_parametro  = spark.conf.get("pipelines.parameters.esquemaParametro")
tabla_parametros   = spark.conf.get("pipelines.parameters.tablaParametros")

print("[LsdpPlataTransacciones] Inicializando modulo...")
print(f"  catalogoParametro = {catalogo_parametro}")
print(f"  esquemaParametro  = {esquema_parametro}")
print(f"  tablaParametros   = {tabla_parametros}")

# --- Lectura de la tabla Parametros ---
diccionario_parametros = obtener_parametros(
    spark, catalogo_parametro, esquema_parametro, tabla_parametros
)

# --- Extraccion de catalogos y esquemas de plata ---
catalogo_plata = diccionario_parametros.get("catalogoPlata", "plata")
esquema_plata  = diccionario_parametros.get("esquemaPlata",  "regional")

print(f"[LsdpPlataTransacciones] Catalogo plata destino : {catalogo_plata}")
print(f"[LsdpPlataTransacciones] Esquema plata destino  : {esquema_plata}")
print(f"[LsdpPlataTransacciones] Vista materializacion  : {catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")
print(f"[LsdpPlataTransacciones] Campos liquid cluster  : ['fecha_transaccion', 'identificador_cliente', 'tipo_transaccion']")
print(f"[LsdpPlataTransacciones] Campos calculados      : ['monto_neto_comisiones', 'porcentaje_comision_sobre_monto', 'variacion_saldo_transaccion', 'indicador_impacto_financiero']")
print(f"[LsdpPlataTransacciones] Estrategia             : Sin filtros — carga incremental automatica por LSDP")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Diccionario de mapeo de columnas AS400 a espanol snake_case
# MAGIC
# MAGIC mapeo_trxpfl: 60 columnas del Transaccional de Clientes

# COMMAND ----------

# --- Mapeo TRXPFL: 60 columnas ---
# Identificacion Transaccional (3 columnas)
mapeo_trxpfl = {
    "TRXID":  "identificador_transaccion",
    "CUSTID": "identificador_cliente",
    "TRXSQ":  "secuencia_transaccion",
    # Atributos de Transaccion (6 columnas)
    "TRXTYP": "tipo_transaccion",
    "TRXCUR": "moneda_transaccion",
    "TRXST":  "estado_transaccion",
    "TRXCH":  "canal_transaccion",
    "TRXDSC": "descripcion_transaccion",
    "TRXREF": "referencia_externa",
    # Montos y Valores (30 columnas)
    "TRXAMT": "monto_principal",
    "TRXCM":  "comision_transaccion",
    "TRXBA":  "saldo_posterior",
    "TRXBP":  "saldo_anterior",
    "TRXTC":  "cargo_fiscal",
    "TRXAL":  "monto_local",
    "TRXPN":  "monto_pago",
    "TRXBF":  "beneficio_transaccion",
    "TRXRL":  "perdida_tasa",
    "TRXMX":  "monto_maximo",
    "TRXMN":  "monto_minimo",
    "TRXAV":  "monto_promedio",
    "TRXDV":  "desviacion_monto",
    "TRXRK":  "riesgo_transaccion",
    "TRXFR":  "riesgo_fraude",
    "TRXLM":  "limite_transaccion",
    "TRXLP":  "porcentaje_limite",
    "TRXCP":  "cargo_plataforma",
    "TRXCI":  "cargo_institucion",
    "TRXCF":  "cargo_extranjero",
    "TRXCV":  "cargo_varianza",
    "TRXSB":  "subtotal_transaccion",
    "TRXTL":  "total_transaccion",
    "TRXRS":  "residuo_transaccion",
    "TRXIM":  "margen_interes",
    "TRXNT":  "monto_neto",
    "TRXAO":  "monto_original",
    "TRXIN":  "monto_inversion",
    "TRXDS":  "descuento_transaccion",
    "TRXPT":  "monto_principal_prestamo",
    # Fechas (19 columnas)
    "TRXDT":  "fecha_transaccion",
    "TRXVD":  "fecha_valor",
    "TRXPD":  "fecha_procesamiento",
    "TRXSD":  "fecha_liquidacion",
    "TRXCD":  "fecha_compensacion",
    "TRXED":  "fecha_efectiva",
    "TRXRD":  "fecha_reverso",
    "TRXAD":  "fecha_autorizacion",
    "TRXND":  "fecha_notificacion_trx",
    "TRXXD":  "fecha_expiracion_trx",
    "TRXFD":  "fecha_fondeo_trx",
    "TRXGD":  "fecha_gracia_trx",
    "TRXHD":  "fecha_historica_trx",
    "TRXBD":  "fecha_bloqueo_trx",
    "TRXMD":  "fecha_maduracion_trx",
    "TRXLD":  "fecha_limite_trx",
    "TRXUD":  "fecha_actualizacion_trx",
    "TRXOD":  "fecha_origen_trx",
    "TRXKD":  "fecha_kyc_trx",
    # Timestamps (2 columnas)
    "TRXTS":  "timestamp_transaccion",
    "TRXUS":  "timestamp_actualizacion",
}

# --- Columnas de control de bronce a excluir ---
columnas_excluir = ["FechaIngestaDatos", "_rescued_data", "año", "mes", "dia"]

print(f"[LsdpPlataTransacciones] Columnas mapeo_trxpfl : {len(mapeo_trxpfl)} entradas")
print(f"[LsdpPlataTransacciones] Columnas a excluir    : {columnas_excluir}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Vista Materializada: transacciones_enriquecidas
# MAGIC
# MAGIC Lee trxpfl SIN filtros para que LSDP optimice con carga incremental automatica.
# MAGIC Excluye columnas de control, renombra a espanol snake_case y agrega 4 campos
# MAGIC calculados numericos con proteccion de nulos y division por cero.

# COMMAND ----------


@dp.expect("identificador_cliente_no_nulo", "identificador_cliente IS NOT NULL")
@dp.expect("monto_neto_positivo",           "monto_neto > 0")
@dp.expect("monto_neto_no_nulo",            "monto_neto IS NOT NULL")
@dp.expect("moneda_transaccion_no_nula",    "moneda_transaccion IS NOT NULL")
@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas",
    comment="Vista materializada del Transaccional de bronce trxpfl. Sin filtros para "
            "carga incremental automatica de LSDP. 60 columnas renombradas a espanol "
            "snake_case mas 4 campos calculados numericos. 4 expectativas de calidad.",
    table_properties={
        "delta.enableChangeDataFeed":         "true",
        "delta.autoOptimize.autoCompact":     "true",
        "delta.autoOptimize.optimizeWrite":   "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration":         "interval 60 days",
    },
    cluster_by=["fecha_transaccion", "identificador_cliente", "tipo_transaccion"],
)
def transacciones_enriquecidas() -> DataFrame:
    """
    Vista materializada que enriquece la streaming table trxpfl de bronce.

    Aplica:
    - Lectura SIN filtros (garantiza carga incremental automatica de LSDP)
    - Exclusion de columnas de control de bronce (_rescued_data, año, mes, dia, FechaIngestaDatos)
    - Renombrado de todas las columnas AS400 a espanol snake_case
    - 4 campos calculados numericos con proteccion ANSI mode:
        * monto_neto_comisiones
        * porcentaje_comision_sobre_monto
        * variacion_saldo_transaccion
        * indicador_impacto_financiero
    - Reordenamiento del liquid cluster (fecha_transaccion, identificador_cliente, tipo_transaccion)

    Retorna:
        DataFrame con 64 columnas listo para la vista materializada de plata
    """

    # -------------------------------------------------------------------------
    # PASO 1: Lectura de trxpfl SIN filtros
    # Ninguna transformacion de filtrado, agrupamiento ni funcion no determinista.
    # Esto garantiza que LSDP puede optimizar con cargas incrementales automaticas.
    # -------------------------------------------------------------------------
    print("[LsdpPlataTransacciones] Iniciando transformacion transacciones_enriquecidas...")
    print("[LsdpPlataTransacciones] PASO 1: Leyendo trxpfl SIN filtros...")

    df_trxpfl = spark.read.table("trxpfl")

    print(f"[LsdpPlataTransacciones]   trxpfl leido: {df_trxpfl.columns.__len__()} columnas")
    print(f"[LsdpPlataTransacciones]   Filtros aplicados: 0 (carga incremental automatica habilitada)")

    # -------------------------------------------------------------------------
    # PASO 2: Exclusion de columnas de control de bronce (manejo gracioso)
    # -------------------------------------------------------------------------
    print("[LsdpPlataTransacciones] PASO 2: Excluyendo columnas de control de bronce...")

    columnas_excluir_existentes = [c for c in columnas_excluir if c in df_trxpfl.columns]
    df_sin_control = df_trxpfl.drop(*columnas_excluir_existentes)

    print(f"[LsdpPlataTransacciones]   Control excluidas: {columnas_excluir_existentes}")
    print(f"[LsdpPlataTransacciones]   Columnas restantes: {df_sin_control.columns.__len__()}")

    # -------------------------------------------------------------------------
    # PASO 3: Seleccion y renombrado de columnas AS400 a espanol snake_case
    # -------------------------------------------------------------------------
    print("[LsdpPlataTransacciones] PASO 3: Renombrado de columnas a espanol snake_case...")

    columnas_seleccion = [
        F.col(col_as400).alias(col_plata)
        for col_as400, col_plata in mapeo_trxpfl.items()
        if col_as400 in df_sin_control.columns
    ]
    df_renombrado = df_sin_control.select(columnas_seleccion)

    print(f"[LsdpPlataTransacciones]   Columnas renombradas: {df_renombrado.columns.__len__()} (esperado: 60)")
    print(f"[LsdpPlataTransacciones]   Filtros aplicados   : 0 (confirmado)")

    # -------------------------------------------------------------------------
    # PASO 4: Campos calculados numericos
    # F.coalesce() protege nulos en operaciones aritmeticas (ANSI mode activo).
    # Proteccion explícita de division por cero en porcentaje_comision_sobre_monto.
    # -------------------------------------------------------------------------
    print("[LsdpPlataTransacciones] PASO 4: Agregando 4 campos calculados numericos...")

    # Campo 1: monto_neto_comisiones
    # monto_principal (TRXAMT) - comision_transaccion (TRXCM)
    df_con_campos = df_renombrado.withColumn(
        "monto_neto_comisiones",
        F.coalesce(F.col("monto_principal"),       F.lit(0.0))
        - F.coalesce(F.col("comision_transaccion"), F.lit(0.0)),
    )

    # Campo 2: porcentaje_comision_sobre_monto
    # (comision_transaccion / monto_principal) * 100 — con proteccion division por cero
    df_con_campos = df_con_campos.withColumn(
        "porcentaje_comision_sobre_monto",
        F.when(
            (F.col("monto_principal") != 0) & (F.col("monto_principal").isNotNull()),
            (F.coalesce(F.col("comision_transaccion"), F.lit(0.0)) / F.col("monto_principal")) * 100.0,
        ).otherwise(F.lit(0.0)),
    )

    # Campo 3: variacion_saldo_transaccion
    # saldo_posterior (TRXBA) - saldo_anterior (TRXBP)
    df_con_campos = df_con_campos.withColumn(
        "variacion_saldo_transaccion",
        F.coalesce(F.col("saldo_posterior"), F.lit(0.0))
        - F.coalesce(F.col("saldo_anterior"), F.lit(0.0)),
    )

    # Campo 4: indicador_impacto_financiero
    # monto_principal (TRXAMT) * riesgo_transaccion (TRXRK)
    df_con_campos = df_con_campos.withColumn(
        "indicador_impacto_financiero",
        F.coalesce(F.col("monto_principal"),   F.lit(0.0))
        * F.coalesce(F.col("riesgo_transaccion"), F.lit(0.0)),
    )

    print(f"[LsdpPlataTransacciones]   Columnas tras campos calculados: {df_con_campos.columns.__len__()} (esperado: 64)")

    # -------------------------------------------------------------------------
    # PASO 5: Reordenamiento para liquid cluster
    # fecha_transaccion, identificador_cliente, tipo_transaccion en posiciones 1-3
    # -------------------------------------------------------------------------
    print("[LsdpPlataTransacciones] PASO 5: Reordenando columnas para liquid cluster...")

    df_final = reordenar_columnas_liquid_cluster(
        df_con_campos,
        ["fecha_transaccion", "identificador_cliente", "tipo_transaccion"],
    )

    print(f"[LsdpPlataTransacciones] Transformacion completada.")
    print(f"[LsdpPlataTransacciones]   Total columnas finales : {df_final.columns.__len__()} (esperado: 64)")
    print(f"[LsdpPlataTransacciones]   Liquid cluster [0]     : {df_final.columns[0]}")
    print(f"[LsdpPlataTransacciones]   Liquid cluster [1]     : {df_final.columns[1]}")
    print(f"[LsdpPlataTransacciones]   Liquid cluster [2]     : {df_final.columns[2]}")
    print(f"[LsdpPlataTransacciones]   Filtros aplicados      : 0 (carga incremental automatica habilitada)")
    print(f"[LsdpPlataTransacciones]   Vista creada           : {catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")

    return df_final
