# Databricks notebook source
# MAGIC %md
# MAGIC # LsdpPlataClientesSaldos
# MAGIC
# MAGIC Script LSDP que define la vista materializada `clientes_saldos_consolidados`
# MAGIC en `plata.lab1`.
# MAGIC
# MAGIC Consolida las streaming tables de bronce `cmstfl` (Maestro de Clientes) y
# MAGIC `blncfl` (Saldos de Clientes) mediante LEFT JOIN por CUSTID con estrategia
# MAGIC Dimension Tipo 1 (solo el registro mas reciente por cada cliente).
# MAGIC
# MAGIC Columnas de bronce renombradas a espanol snake_case. Sin columnas duplicadas
# MAGIC entre ambas tablas fuente. Incluye 4 campos calculados y 5 expectativas de
# MAGIC calidad de datos en modo observacional.
# MAGIC
# MAGIC Patron Closure autocontenido: parametros del pipeline y tabla Parametros se
# MAGIC calculan una sola vez al importar el script y quedan capturados por closure
# MAGIC en la funcion decorada con @dp.materialized_view.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window
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

print("[LsdpPlataClientesSaldos] Inicializando modulo...")
print(f"  catalogoParametro = {catalogo_parametro}")
print(f"  esquemaParametro  = {esquema_parametro}")
print(f"  tablaParametros   = {tabla_parametros}")

# --- Lectura de la tabla Parametros ---
diccionario_parametros = obtener_parametros(
    spark, catalogo_parametro, esquema_parametro, tabla_parametros
)

# --- Extraccion de catalogos y esquemas de plata ---
catalogo_plata = diccionario_parametros.get("catalogoPlata", "plata")
esquema_plata  = diccionario_parametros.get("esquemaPlata",  "lab1")

print(f"[LsdpPlataClientesSaldos] Catalogo plata destino : {catalogo_plata}")
print(f"[LsdpPlataClientesSaldos] Esquema plata destino  : {esquema_plata}")
print(f"[LsdpPlataClientesSaldos] Vista materializacion  : {catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados")
print(f"[LsdpPlataClientesSaldos] Campos liquid cluster  : ['huella_identificacion_cliente', 'identificador_cliente']")
print(f"[LsdpPlataClientesSaldos] Campos calculados      : ['clasificacion_riesgo_cliente', 'categoria_saldo_disponible', 'perfil_actividad_bancaria', 'huella_identificacion_cliente']")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Diccionarios de mapeo de columnas AS400 a espanol snake_case
# MAGIC
# MAGIC mapeo_cmstfl: 70 columnas del Maestro de Clientes
# MAGIC mapeo_blncfl: 99 columnas de Saldos (excluye CUSTID, resuelto por el JOIN)

# COMMAND ----------

# --- Mapeo CMSTFL: 70 columnas ---
# Identificacion Personal / Sociodemografica (28 columnas)
mapeo_cmstfl = {
    "CUSTID": "identificador_cliente",
    "CUSNM":  "nombre_cliente",
    "CUSLN":  "apellido_cliente",
    "CUSMD":  "nombre_medio_cliente",
    "CUSFN":  "nombre_completo_cliente",
    "CUSSX":  "sexo_cliente",
    "CUSTT":  "tratamiento_cliente",
    "CUSDB":  "fecha_nacimiento",
    "CUSYR":  "anio_nacimiento",
    "CUSAG2": "edad_cliente",
    "CUSAD":  "direccion_calle",
    "CUSA2":  "direccion_apartamento",
    "CUSCT":  "ciudad_residencia",
    "CUSST":  "estado_provincia",
    "CUSZP":  "codigo_postal",
    "CUSCN":  "pais_residencia",
    "CUSNA":  "nacionalidad_cliente",
    "CUSPH":  "telefono_principal",
    "CUSMB":  "telefono_movil",
    "CUSEM":  "correo_electronico",
    "CUSMS":  "estado_civil",
    "CUSOC":  "ocupacion_cliente",
    "CUSED":  "nivel_educativo",
    "CUSDL":  "numero_licencia_conducir",
    "CUSDP":  "tipo_documento_pasaporte",
    "CUSDP2": "cantidad_pasaportes",
    "CUSLG":  "idioma_preferido",
    "CUSRG":  "region_geografica",
    # Comercial / Relacion Bancaria (25 columnas)
    "CUSTP":  "tipo_cliente",
    "CUSSG":  "segmento_cliente",
    "CUSBR":  "sucursal_principal",
    "CUSMG":  "gerente_asignado",
    "CUSRF":  "referencia_interna",
    "CUSRS":  "fuente_referencia",
    "CUSAG":  "grupo_afinidad",
    "CUSPC":  "preferencia_comunicacion",
    "CUSRK":  "nivel_riesgo",
    "CUSVP":  "indicador_vip",
    "CUSPF":  "estado_perfil",
    "CUSKT":  "estado_kyc",
    "CUSFM":  "indicador_flags",
    "CUSLC":  "ultimo_canal",
    "CUSCR":  "calificacion_crediticia",
    "CUSAC":  "cuenta_activa",
    "CUSCL":  "clasificacion_interna",
    "CUSAC2": "cantidad_cuentas",
    "CUSTX":  "cantidad_transacciones",
    "CUSSC":  "score_cliente",
    "CUSLR":  "ranking_prestamos",
    "CUSRC":  "cantidad_registros",
    "CUSIN":  "ingresos_cliente",
    "CUSBL":  "saldo_disponible_maestro",
    "CUSNT":  "nota_cliente",
    # Fechas Administrativas (17 columnas)
    "CUSOD":  "fecha_apertura_relacion",
    "CUSCD":  "fecha_cierre_relacion",
    "CUSLV":  "fecha_ultima_visita",
    "CUSUD":  "fecha_ultima_actualizacion",
    "CUSKD":  "fecha_verificacion_kyc",
    "CUSRD":  "fecha_renovacion",
    "CUSXD":  "fecha_expiracion",
    "CUSFD":  "fecha_primer_producto",
    "CUSLD":  "fecha_ultimo_producto",
    "CUSMD2": "fecha_migracion",
    "CUSAD2": "fecha_activacion",
    "CUSBD":  "fecha_bloqueo",
    "CUSVD":  "fecha_verificacion",
    "CUSPD":  "fecha_promocion",
    "CUSDD":  "fecha_desactivacion",
    "CUSED2": "fecha_educacion_financiera",
    "CUSND":  "fecha_notificacion",
}

# --- Mapeo BLNCFL: 99 columnas (CUSTID excluido — resuelto por el JOIN) ---
# Identificacion de Cuenta (1 columna)
mapeo_blncfl = {
    "BLSQ":  "secuencia_saldo",
    # Atributos de Cuenta (29 columnas)
    "BLACT": "tipo_cuenta",
    "BLACN": "numero_cuenta",
    "BLCUR": "moneda_cuenta",
    "BLST":  "estado_cuenta",
    "BLBR":  "sucursal_cuenta",
    "BLPR":  "producto_cuenta",
    "BLSP":  "subproducto_cuenta",
    "BLNM":  "nombre_cuenta",
    "BLCL":  "clase_cuenta",
    "BLRK":  "riesgo_cuenta",
    "BLTP":  "tipo_producto_cuenta",
    "BLMG":  "gerente_cuenta",
    "BLRF":  "referencia_cuenta",
    "BLCC":  "centro_costos_cuenta",
    "BLAG":  "grupo_afinidad_cuenta",
    "BLPL":  "plan_cuenta",
    "BLRG":  "region_cuenta",
    "BLSF":  "sufijo_cuenta",
    "BLNT":  "nota_cuenta",
    "BLLC":  "ultimo_canal_cuenta",
    "BLPF":  "perfil_cuenta",
    "BLAU":  "autorizado_cuenta",
    "BLTX":  "texto_cuenta",
    "BLGR":  "grupo_cuenta",
    "BLEM":  "email_cuenta",
    "BLFR":  "frecuencia_cuenta",
    "BLKY":  "clave_cuenta",
    "BLVP":  "vip_cuenta",
    "BLFC":  "factor_cuenta",
    # Saldos y Montos (34 columnas)
    "BLAV":  "saldo_disponible",
    "BLTB":  "saldo_total",
    "BLRV":  "saldo_reservado",
    "BLBK":  "saldo_bloqueado",
    "BLCR":  "limite_credito",
    "BLCN":  "credito_utilizado",
    "BLCD":  "credito_disponible",
    "BLOV":  "valor_sobregiro",
    "BLOL":  "limite_sobregiro",
    "BLPD":  "depositos_pendientes",
    "BLPC":  "cargos_pendientes",
    "BLPA":  "ajustes_pendientes",
    "BLDI":  "depositos_ingreso",
    "BLWI":  "retenciones_cuenta",
    "BLTI":  "transferencias_ingreso",
    "BLTC":  "cargos_transferencia",
    "BLCA":  "comisiones_anuales",
    "BLIM":  "intereses_mensuales",
    "BLRF2": "reembolsos_cuenta",
    "BLPN":  "penalidades_cuenta",
    "BLBN":  "bonificaciones_cuenta",
    "BLAP":  "ajustes_positivos",
    "BLAM":  "ajustes_miscelaneos",
    "BLAY":  "ajustes_anuales",
    "BLHI":  "marca_alta_saldo",
    "BLLO":  "marca_baja_saldo",
    "BLVR":  "varianza_saldo",
    "BLRT":  "ratio_cuenta",
    "BLCP":  "porcentaje_aporte",
    "BLCI":  "ingresos_aporte",
    "BLMN":  "saldo_minimo",
    "BLMX":  "saldo_maximo",
    "BLIR":  "tasa_interes",
    "BLPM":  "multiplicador_penalidad",
    # Fechas de Cuenta (35 columnas)
    "BLOD":  "fecha_apertura_cuenta",
    "BLXD":  "fecha_expiracion_cuenta",
    "BLUD":  "fecha_actualizacion_cuenta",
    "BLLD":  "fecha_ultimo_movimiento",
    "BLSD":  "fecha_estado_cuenta",
    "BLPD2": "fecha_penalidad",
    "BLRD":  "fecha_renovacion_cuenta",
    "BLMD":  "fecha_maduracion",
    "BLCD2": "fecha_cierre_cuenta",
    "BLBD":  "fecha_bloqueo_cuenta",
    "BLFD":  "fecha_fondeo",
    "BLGD":  "fecha_gracia",
    "BLHD":  "fecha_historica",
    "BLID":  "fecha_interes",
    "BLJD":  "fecha_ajuste",
    "BLKD":  "fecha_kyc_cuenta",
    "BLND":  "fecha_notificacion_cuenta",
    "BLTD":  "fecha_transferencia",
    "BLVD":  "fecha_verificacion_cuenta",
    "BLWD":  "fecha_retiro",
    "BLYD":  "fecha_rendimiento",
    "BLZD":  "fecha_cierre_periodo",
    "BLED":  "fecha_evaluacion",
    "BLAD2": "fecha_activacion_cuenta",
    "BLDD":  "fecha_desactivacion_cuenta",
    "BLFP":  "fecha_primer_pago",
    "BLLP":  "fecha_ultimo_pago",
    "BLMP":  "fecha_mora_pago",
    "BLNP":  "fecha_proximo_pago",
    "BLOP":  "fecha_origen_pago",
    "BLPP":  "fecha_programacion_pago",
    "BLQP":  "fecha_quiebre_pago",
    "BLRP":  "fecha_regularizacion_pago",
    "BLSP2": "fecha_suspension_pago",
    "BLTP2": "fecha_tercero_pago",
}

# --- Columnas de control de bronce a excluir ---
columnas_excluir = ["FechaIngestaDatos", "_rescued_data", "año", "mes", "dia"]

print(f"[LsdpPlataClientesSaldos] Columnas mapeo_cmstfl : {len(mapeo_cmstfl)} entradas")
print(f"[LsdpPlataClientesSaldos] Columnas mapeo_blncfl : {len(mapeo_blncfl)} entradas")
print(f"[LsdpPlataClientesSaldos] Columnas a excluir    : {columnas_excluir}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Vista Materializada: clientes_saldos_consolidados
# MAGIC
# MAGIC Aplica Dimension Tipo 1 sobre cmstfl y blncfl por separado (solo el registro
# MAGIC mas reciente por CUSTID), ejecuta LEFT JOIN, renombra todas las columnas a
# MAGIC espanol snake_case y agrega 4 campos calculados.

# COMMAND ----------


@dp.expect("fecha_nacimiento_valida",       "fecha_nacimiento < '2009-01-01'")
@dp.expect("fecha_apertura_cuenta_valida",  "fecha_apertura_cuenta > '2020-12-31'")
@dp.expect("limite_credito_no_nulo",        "limite_credito IS NOT NULL")
@dp.expect("identificador_cliente_no_nulo", "identificador_cliente IS NOT NULL")
@dp.expect("limite_credito_positivo",       "limite_credito > 0")
@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados",
    comment="Vista materializada que consolida Maestro de Clientes y Saldos de bronce. "
            "Dimension Tipo 1 por CUSTID. Sin columnas duplicadas. 4 campos calculados. "
            "5 expectativas de calidad de datos.",
    table_properties={
        "delta.enableChangeDataFeed":          "true",
        "delta.autoOptimize.autoCompact":      "true",
        "delta.autoOptimize.optimizeWrite":    "true",
        "delta.deletedFileRetentionDuration":  "interval 30 days",
        "delta.logRetentionDuration":          "interval 60 days",
    },
    cluster_by=["huella_identificacion_cliente", "identificador_cliente"],
)
def clientes_saldos_consolidados() -> DataFrame:
    """
    Vista materializada que consolida las streaming tables de bronce cmstfl y blncfl.

    Aplica:
    - Dimension Tipo 1: registro mas reciente por CUSTID (Window + ROW_NUMBER)
    - Exclusion de columnas de control de bronce (_rescued_data, año, mes, dia, FechaIngestaDatos)
    - LEFT JOIN por CUSTID (cmstfl como tabla base)
    - Renombrado de todas las columnas AS400 a espanol snake_case
    - 4 campos calculados: clasificacion_riesgo_cliente, categoria_saldo_disponible,
      perfil_actividad_bancaria, huella_identificacion_cliente
    - Reordenamiento del liquid cluster (huella_identificacion_cliente, identificador_cliente)

    Retorna:
        DataFrame con 173 columnas listo para la vista materializada de plata
    """

    # -------------------------------------------------------------------------
    # PASO 1: Dimension Tipo 1 sobre cmstfl
    # -------------------------------------------------------------------------
    print("[LsdpPlataClientesSaldos] Iniciando transformacion clientes_saldos_consolidados...")
    print("[LsdpPlataClientesSaldos] PASO 1: Dimension Tipo 1 sobre cmstfl...")

    df_cmstfl = spark.read.table("cmstfl")
    print(f"[LsdpPlataClientesSaldos]   cmstfl leido: {df_cmstfl.columns.__len__()} columnas")

    ventana_cmstfl = Window.partitionBy("CUSTID").orderBy(
        F.col("FechaIngestaDatos").desc(),
        F.col("CUSTID").desc(),
    )
    df_cmstfl_numerado = df_cmstfl.withColumn("numero_fila", F.row_number().over(ventana_cmstfl))
    df_cmstfl_reciente = df_cmstfl_numerado.filter(F.col("numero_fila") == 1).drop("numero_fila")

    # Eliminar columnas de control con manejo gracioso (pueden no existir)
    columnas_excluir_cmstfl = [c for c in columnas_excluir if c in df_cmstfl_reciente.columns]
    df_cmstfl_limpio = df_cmstfl_reciente.drop(*columnas_excluir_cmstfl)

    print(f"[LsdpPlataClientesSaldos]   cmstfl Dim Tipo 1: {df_cmstfl_limpio.columns.__len__()} columnas (control excluidas: {columnas_excluir_cmstfl})")

    # -------------------------------------------------------------------------
    # PASO 2: Dimension Tipo 1 sobre blncfl
    # -------------------------------------------------------------------------
    print("[LsdpPlataClientesSaldos] PASO 2: Dimension Tipo 1 sobre blncfl...")

    df_blncfl = spark.read.table("blncfl")
    print(f"[LsdpPlataClientesSaldos]   blncfl leido: {df_blncfl.columns.__len__()} columnas")

    ventana_blncfl = Window.partitionBy("CUSTID").orderBy(
        F.col("FechaIngestaDatos").desc(),
        F.col("CUSTID").desc(),
    )
    df_blncfl_numerado = df_blncfl.withColumn("numero_fila", F.row_number().over(ventana_blncfl))
    df_blncfl_reciente = df_blncfl_numerado.filter(F.col("numero_fila") == 1).drop("numero_fila")

    # Eliminar columnas de control con manejo gracioso
    columnas_excluir_blncfl = [c for c in columnas_excluir if c in df_blncfl_reciente.columns]
    df_blncfl_limpio = df_blncfl_reciente.drop(*columnas_excluir_blncfl)

    print(f"[LsdpPlataClientesSaldos]   blncfl Dim Tipo 1: {df_blncfl_limpio.columns.__len__()} columnas (control excluidas: {columnas_excluir_blncfl})")

    # -------------------------------------------------------------------------
    # PASO 3: LEFT JOIN por CUSTID (cmstfl como tabla base)
    # on="CUSTID" deduplica automaticamente la columna de union
    # -------------------------------------------------------------------------
    print("[LsdpPlataClientesSaldos] PASO 3: LEFT JOIN cmstfl + blncfl por CUSTID...")

    df_joined = df_cmstfl_limpio.join(df_blncfl_limpio, on="CUSTID", how="left")

    print(f"[LsdpPlataClientesSaldos]   DataFrame unificado: {df_joined.columns.__len__()} columnas (esperado: ~169)")

    # -------------------------------------------------------------------------
    # PASO 4: Seleccion y renombrado de columnas via diccionarios de mapeo
    # -------------------------------------------------------------------------
    print("[LsdpPlataClientesSaldos] PASO 4: Renombrado de columnas a espanol snake_case...")

    mapeo_unificado = {**mapeo_cmstfl, **mapeo_blncfl}
    columnas_seleccion = [
        F.col(col_as400).alias(col_plata)
        for col_as400, col_plata in mapeo_unificado.items()
        if col_as400 in df_joined.columns
    ]
    df_renombrado = df_joined.select(columnas_seleccion)

    print(f"[LsdpPlataClientesSaldos]   Columnas renombradas: {df_renombrado.columns.__len__()} (esperado: 169)")

    # -------------------------------------------------------------------------
    # PASO 5: Campos calculados
    # Proteccion de nulos con F.coalesce() para compatibilidad con ANSI mode
    # -------------------------------------------------------------------------
    print("[LsdpPlataClientesSaldos] PASO 5: Agregando 4 campos calculados...")

    # Campo 1: clasificacion_riesgo_cliente
    # Basado en: nivel_riesgo (CUSRK), calificacion_crediticia (CUSCR), score_cliente (CUSSC)
    df_con_campos = df_renombrado.withColumn(
        "clasificacion_riesgo_cliente",
        F.when(
            (F.coalesce(F.col("score_cliente"), F.lit(0)) >= 750)
            & (F.col("nivel_riesgo").isin("L", "LOW"))
            & (F.col("calificacion_crediticia").isin("A", "AA", "AAA")),
            F.lit("RIESGO_BAJO"),
        ).when(
            (F.coalesce(F.col("score_cliente"), F.lit(0)) >= 500)
            & (F.col("nivel_riesgo").isin("M", "MED")),
            F.lit("RIESGO_MEDIO"),
        ).when(
            (F.coalesce(F.col("score_cliente"), F.lit(0)) < 500)
            | (F.col("nivel_riesgo").isin("H", "HIGH")),
            F.lit("RIESGO_ALTO"),
        ).otherwise(F.lit("SIN_CLASIFICAR")),
    )

    # Campo 2: categoria_saldo_disponible
    # Basado en: saldo_disponible (BLAV), limite_credito (BLCR), saldo_total (BLTB)
    df_con_campos = df_con_campos.withColumn(
        "categoria_saldo_disponible",
        F.when(
            (F.coalesce(F.col("saldo_disponible"), F.lit(0.0)) >= 100000.0)
            & (F.coalesce(F.col("limite_credito"),   F.lit(0.0)) >= 50000.0)
            & (F.coalesce(F.col("saldo_total"),       F.lit(0.0)) >= 150000.0),
            F.lit("PREMIUM"),
        ).when(
            (F.coalesce(F.col("saldo_disponible"), F.lit(0.0)) >= 25000.0)
            & (F.coalesce(F.col("limite_credito"),   F.lit(0.0)) >= 10000.0),
            F.lit("ESTANDAR"),
        ).when(
            F.coalesce(F.col("saldo_disponible"), F.lit(0.0)) > 0.0,
            F.lit("BASICO"),
        ).otherwise(F.lit("SIN_SALDO")),
    )

    # Campo 3: perfil_actividad_bancaria
    # Basado en: cantidad_transacciones (CUSTX), cantidad_cuentas (CUSAC2), ranking_prestamos (CUSLR)
    df_con_campos = df_con_campos.withColumn(
        "perfil_actividad_bancaria",
        F.when(
            (F.coalesce(F.col("cantidad_transacciones"), F.lit(0)) >= 100)
            & (F.coalesce(F.col("cantidad_cuentas"),      F.lit(0)) >= 3)
            & (F.coalesce(F.col("ranking_prestamos"),     F.lit(0)) >= 5),
            F.lit("MUY_ACTIVO"),
        ).when(
            (F.coalesce(F.col("cantidad_transacciones"), F.lit(0)) >= 30)
            & (F.coalesce(F.col("cantidad_cuentas"),      F.lit(0)) >= 2),
            F.lit("ACTIVO"),
        ).when(
            F.coalesce(F.col("cantidad_transacciones"), F.lit(0)) >= 1,
            F.lit("MODERADO"),
        ).otherwise(F.lit("INACTIVO")),
    )

    # Campo 4: huella_identificacion_cliente
    # Hash SHA2-256 del identificador_cliente para anonimizacion y liquid cluster
    df_con_campos = df_con_campos.withColumn(
        "huella_identificacion_cliente",
        F.sha2(F.col("identificador_cliente").cast("string"), 256),
    )

    print(f"[LsdpPlataClientesSaldos]   Columnas tras campos calculados: {df_con_campos.columns.__len__()} (esperado: 173)")

    # -------------------------------------------------------------------------
    # PASO 6: Reordenamiento para liquid cluster
    # huella_identificacion_cliente e identificador_cliente en posiciones 1 y 2
    # -------------------------------------------------------------------------
    print("[LsdpPlataClientesSaldos] PASO 6: Reordenando columnas para liquid cluster...")

    df_final = reordenar_columnas_liquid_cluster(
        df_con_campos,
        ["huella_identificacion_cliente", "identificador_cliente"],
    )

    print(f"[LsdpPlataClientesSaldos] Transformacion completada.")
    print(f"[LsdpPlataClientesSaldos]   Total columnas finales : {df_final.columns.__len__()} (esperado: 173)")
    print(f"[LsdpPlataClientesSaldos]   Liquid cluster [0]     : {df_final.columns[0]}")
    print(f"[LsdpPlataClientesSaldos]   Liquid cluster [1]     : {df_final.columns[1]}")
    print(f"[LsdpPlataClientesSaldos]   Deduplicacion columnas : CUSTID tomado de cmstfl, columnas BL* de blncfl")
    print(f"[LsdpPlataClientesSaldos]   Vista creada           : {catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados")

    return df_final
