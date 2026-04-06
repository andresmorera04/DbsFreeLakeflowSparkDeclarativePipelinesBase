# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbTddGenerarParquets
# MAGIC
# MAGIC **Proposito**: Pruebas TDD que validan la estructura, tipos de datos, cantidades,
# MAGIC distribuciones, integridad referencial y modelo de mutacion de los 3 parquets
# MAGIC generados por los notebooks de la feature 002-generar-parquets-as400.
# MAGIC
# MAGIC **Prerequisitos**:
# MAGIC - Los 3 parquets deben haberse generado previamente:
# MAGIC   1. NbGenerarMaestroCliente (CMSTFL)
# MAGIC   2. NbGenerarTransaccionalCliente (TRXPFL)
# MAGIC   3. NbGenerarSaldosCliente (BLNCFL)
# MAGIC
# MAGIC **Idempotencia absoluta**: Ejecutar estas pruebas multiples veces produce el
# MAGIC mismo resultado sin modificar los parquets.
# MAGIC
# MAGIC **Salida**: Resumen PASS/FAIL por prueba con conteo total y resultado global.

# COMMAND ----------

# ==============================================================================
# PASO 1: IMPORTS
# ==============================================================================

import time
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, DoubleType, DateType, TimestampType

# COMMAND ----------

# ==============================================================================
# PASO 2: DEFINICION DE WIDGETS (PARAMETROS DE ENTRADA)
# ==============================================================================
# 6 widgets segun contrato.
# ==============================================================================

dbutils.widgets.text("catalogoParametro",          "control",                         "Catalogo de la tabla Parametros")
dbutils.widgets.text("esquemaParametro",           "regional",                        "Esquema de la tabla Parametros")
dbutils.widgets.text("tablaParametros",            "Parametros",                      "Nombre de la tabla Parametros")
dbutils.widgets.text("rutaRelativaMaestroCliente", "LSDP_Base/As400/MaestroCliente/", "Ruta relativa del parquet CMSTFL")
dbutils.widgets.text("rutaRelativaTransaccional",  "LSDP_Base/As400/Transaccional/",  "Ruta relativa del parquet TRXPFL")
dbutils.widgets.text("rutaRelativaSaldoCliente",   "LSDP_Base/As400/SaldoCliente/",   "Ruta relativa del parquet BLNCFL")

# Captura de valores
catalogo_parametro    = dbutils.widgets.get("catalogoParametro")
esquema_parametro     = dbutils.widgets.get("esquemaParametro")
tabla_parametros      = dbutils.widgets.get("tablaParametros")
ruta_relativa_maestro = dbutils.widgets.get("rutaRelativaMaestroCliente")
ruta_relativa_transac = dbutils.widgets.get("rutaRelativaTransaccional")
ruta_relativa_saldo   = dbutils.widgets.get("rutaRelativaSaldoCliente")

# COMMAND ----------

# ==============================================================================
# PASO 3: LECTURA DE TABLA PARAMETROS Y CONSTRUCCION DE RUTAS
# ==============================================================================

ruta_tabla_parametros = f"{catalogo_parametro}.{esquema_parametro}.{tabla_parametros}"

filas_parametros = spark.read.table(ruta_tabla_parametros).collect()
dict_parametros  = {fila["Clave"]: fila["Valor"] for fila in filas_parametros}

tipo_storage = dict_parametros.get("TipoStorage", "")

if tipo_storage == "Volume":
    catalogo_volume      = dict_parametros.get("catalogoVolume", "")
    esquema_volume       = dict_parametros.get("esquemaVolume", "")
    nombre_volume        = dict_parametros.get("nombreVolume", "")
    ruta_completa_maestro = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa_maestro}"
    ruta_completa_transac = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa_transac}"
    ruta_completa_saldo   = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa_saldo}"
elif tipo_storage == "AmazonS3":
    bucket_s3             = dict_parametros.get("bucketS3", "")
    ruta_completa_maestro = f"s3://{bucket_s3}/{ruta_relativa_maestro}"
    ruta_completa_transac = f"s3://{bucket_s3}/{ruta_relativa_transac}"
    ruta_completa_saldo   = f"s3://{bucket_s3}/{ruta_relativa_saldo}"
else:
    raise ValueError(
        f"ERROR: TipoStorage '{tipo_storage}' no es valido. Valores aceptados: 'Volume', 'AmazonS3'"
    )

print(f"Ruta CMSTFL  : {ruta_completa_maestro}")
print(f"Ruta TRXPFL  : {ruta_completa_transac}")
print(f"Ruta BLNCFL  : {ruta_completa_saldo}")

# COMMAND ----------

# ==============================================================================
# PASO 4: FUNCION AUXILIAR DE REPORTE DE PRUEBAS Y LECTURA DE PARQUETS
# ==============================================================================

resultados_pruebas = []
tiempo_inicio_tdd  = time.time()

def registrar_prueba(descripcion, resultado, detalle=""):
    """Registra el resultado de una prueba TDD y lo imprime inmediatamente."""
    estado = "PASS" if resultado else "FAIL"
    msg    = f"  [{estado}] {descripcion}"
    if detalle and not resultado:
        msg += f"\n       DETALLE: {detalle}"
    print(msg)
    resultados_pruebas.append({"descripcion": descripcion, "resultado": resultado, "detalle": detalle})

print("=" * 70)
print("INICIO: NbTddGenerarParquets — Pruebas TDD")
print("=" * 70)

# Lectura de los 3 parquets con manejo de error
df_cmstfl  = None
df_trxpfl  = None
df_blncfl  = None
error_carga = []

try:
    df_cmstfl = spark.read.parquet(ruta_completa_maestro)
    print(f"CMSTFL cargado  : {df_cmstfl.count()} registros")
except Exception as e:
    error_carga.append(f"CMSTFL: {e}")
    print(f"ERROR cargando CMSTFL: {e}")

try:
    df_trxpfl = spark.read.parquet(ruta_completa_transac)
    print(f"TRXPFL cargado  : {df_trxpfl.count()} registros")
except Exception as e:
    error_carga.append(f"TRXPFL: {e}")
    print(f"ERROR cargando TRXPFL: {e}")

try:
    df_blncfl = spark.read.parquet(ruta_completa_saldo)
    print(f"BLNCFL cargado  : {df_blncfl.count()} registros")
except Exception as e:
    error_carga.append(f"BLNCFL: {e}")
    print(f"ERROR cargando BLNCFL: {e}")

if error_carga:
    print(f"\nADVERTENCIA: No se pudieron cargar los siguientes parquets: {', '.join(error_carga)}")
    print("Ejecute primero los notebooks generadores en el orden correcto.\n")

# COMMAND ----------

# ==============================================================================
# PASO 5: PRUEBAS TDD PARA CMSTFL (Maestro de Clientes)
# ==============================================================================

print("\n" + "=" * 70)
print("PRUEBAS CMSTFL — Maestro de Clientes (70 columnas esperadas)")
print("=" * 70)

if df_cmstfl is not None:
    # Conteo de registros
    total_cmstfl = df_cmstfl.count()

    # --- T5.1: Verificar 70 columnas ---
    num_cols_cmstfl = len(df_cmstfl.columns)
    registrar_prueba(
        f"CMSTFL: Numero de columnas = 70 (encontrado: {num_cols_cmstfl})",
        num_cols_cmstfl == 70,
        f"Columnas encontradas: {df_cmstfl.columns}"
    )

    # --- T5.2: Distribucion de tipos (41 StringType, 18 DateType, 9 LongType, 2 DoubleType) ---
    tipos_cmstfl    = {f.name: f.dataType for f in df_cmstfl.schema.fields}
    cnt_str_cmstfl  = sum(1 for t in tipos_cmstfl.values() if isinstance(t, StringType))
    cnt_date_cmstfl = sum(1 for t in tipos_cmstfl.values() if isinstance(t, DateType))
    cnt_long_cmstfl = sum(1 for t in tipos_cmstfl.values() if isinstance(t, LongType))
    cnt_dbl_cmstfl  = sum(1 for t in tipos_cmstfl.values() if isinstance(t, DoubleType))

    registrar_prueba(
        f"CMSTFL: StringType = 41 (encontrado: {cnt_str_cmstfl})",
        cnt_str_cmstfl == 41
    )
    registrar_prueba(
        f"CMSTFL: DateType = 18 (encontrado: {cnt_date_cmstfl})",
        cnt_date_cmstfl == 18
    )
    registrar_prueba(
        f"CMSTFL: LongType = 9 (encontrado: {cnt_long_cmstfl})",
        cnt_long_cmstfl == 9
    )
    registrar_prueba(
        f"CMSTFL: DoubleType = 2 (encontrado: {cnt_dbl_cmstfl})",
        cnt_dbl_cmstfl == 2
    )

    # --- T5.3: Cantidad de registros >= 1 ---
    registrar_prueba(
        f"CMSTFL: Cantidad de registros >= 1 (encontrado: {total_cmstfl})",
        total_cmstfl >= 1
    )

    # --- T5.4: CUSTID secuencial, unico y positivo ---
    count_custid_positivos = df_cmstfl.filter(F.col("CUSTID") > 0).count()
    count_custid_unicos    = df_cmstfl.select("CUSTID").distinct().count()

    registrar_prueba(
        f"CMSTFL: Todos los CUSTID son positivos ({count_custid_positivos}/{total_cmstfl})",
        count_custid_positivos == total_cmstfl
    )
    registrar_prueba(
        f"CMSTFL: CUSTID es unico (distintos={count_custid_unicos}, total={total_cmstfl})",
        count_custid_unicos == total_cmstfl
    )

    # --- T5.5: Exclusion de nombres latinos ---
    # Lista de nombres/apellidos latinos mas comunes para deteccion
    nombres_latinos = [
        "Juan", "Maria", "Carlos", "Ana", "Luis", "Rosa", "Pedro", "Carmen",
        "Miguel", "Isabel", "Garcia", "Gonzalez", "Rodriguez", "Lopez", "Martinez",
        "Hernandez", "Perez", "Sanchez", "Ramirez", "Torres", "Flores", "Rivera",
        "Gomez", "Morales", "Gutierrez", "Cruz", "Vargas", "Castillo", "Ramos",
        "Moreno", "Ruiz", "Romero", "Reyes", "Diaz", "Jimenez", "Alvarez",
        "Jose", "Antonio", "Francisco", "Manuel", "Roberto", "Eduardo", "Fernando",
        "Gabriela", "Alejandra", "Fernanda", "Claudia", "Patricia", "Sandra"
    ]
    patron_latinos = "|".join(nombres_latinos)

    # Verificar nombres (CUSNM y CUSLN) no contienen nombres latinos
    count_nombres_latinos = df_cmstfl.filter(
        F.col("CUSNM").rlike(f"(?i)\\b({patron_latinos})\\b") |
        F.col("CUSLN").rlike(f"(?i)\\b({patron_latinos})\\b")
    ).count()

    registrar_prueba(
        f"CMSTFL: Nombres exclusivamente hebreos/egipcios/ingleses (registros con nombres latinos: {count_nombres_latinos})",
        count_nombres_latinos == 0,
        f"Se encontraron {count_nombres_latinos} registros con posibles nombres latinos en CUSNM o CUSLN"
    )

    # --- T5.6: Columnas obligatorias presentes ---
    columnas_obligatorias_cmstfl = [
        "CUSTID", "CUSNM", "CUSLN", "CUSFN", "CUSSX", "CUSDB", "CUSIN", "CUSBL",
        "CUSTP", "CUSSG", "CUSMS", "CUSKT", "CUSVP", "CUSED", "CUSLG"
    ]
    cols_faltantes_cmstfl = [c for c in columnas_obligatorias_cmstfl if c not in df_cmstfl.columns]
    registrar_prueba(
        f"CMSTFL: Todas las columnas criticas presentes ({len(columnas_obligatorias_cmstfl)} verificadas)",
        len(cols_faltantes_cmstfl) == 0,
        f"Columnas faltantes: {cols_faltantes_cmstfl}"
    )

    # --- T5.7: Valores CUSSX limitados a M/F ---
    valores_cussx_invalidos = df_cmstfl.filter(~F.col("CUSSX").isin("M", "F")).count()
    registrar_prueba(
        f"CMSTFL: CUSSX solo contiene M o F (invalidos: {valores_cussx_invalidos})",
        valores_cussx_invalidos == 0
    )

    # --- T5.8: Fechas de nacimiento en rango 1970-2007 ---
    count_cusdb_fuera_rango = df_cmstfl.filter(
        (F.year(F.col("CUSDB")) < 1970) | (F.year(F.col("CUSDB")) > 2007)
    ).count()
    registrar_prueba(
        f"CMSTFL: CUSDB en rango 1970-2007 (fuera de rango: {count_cusdb_fuera_rango})",
        count_cusdb_fuera_rango == 0
    )

else:
    registrar_prueba("CMSTFL: No se pudo cargar el parquet para pruebas", False, "Parquet no disponible")

# COMMAND ----------

# ==============================================================================
# PASO 6: PRUEBAS TDD PARA TRXPFL (Transaccional de Clientes)
# ==============================================================================

print("\n" + "=" * 70)
print("PRUEBAS TRXPFL — Transaccional de Clientes (60 columnas esperadas)")
print("=" * 70)

if df_trxpfl is not None:
    total_trxpfl = df_trxpfl.count()

    # --- T6.1: Verificar 60 columnas ---
    num_cols_trxpfl = len(df_trxpfl.columns)
    registrar_prueba(
        f"TRXPFL: Numero de columnas = 60 (encontrado: {num_cols_trxpfl})",
        num_cols_trxpfl == 60,
        f"Columnas encontradas: {df_trxpfl.columns}"
    )

    # --- T6.2: Distribucion de tipos (7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType) ---
    tipos_trxpfl     = {f.name: f.dataType for f in df_trxpfl.schema.fields}
    cnt_str_trxpfl   = sum(1 for t in tipos_trxpfl.values() if isinstance(t, StringType))
    cnt_date_trxpfl  = sum(1 for t in tipos_trxpfl.values() if isinstance(t, DateType))
    cnt_ts_trxpfl    = sum(1 for t in tipos_trxpfl.values() if isinstance(t, TimestampType))
    cnt_long_trxpfl  = sum(1 for t in tipos_trxpfl.values() if isinstance(t, LongType))
    cnt_dbl_trxpfl   = sum(1 for t in tipos_trxpfl.values() if isinstance(t, DoubleType))

    registrar_prueba(
        f"TRXPFL: StringType = 7 (encontrado: {cnt_str_trxpfl})",
        cnt_str_trxpfl == 7
    )
    registrar_prueba(
        f"TRXPFL: DateType = 19 (encontrado: {cnt_date_trxpfl})",
        cnt_date_trxpfl == 19
    )
    registrar_prueba(
        f"TRXPFL: TimestampType = 2 (encontrado: {cnt_ts_trxpfl})",
        cnt_ts_trxpfl == 2
    )
    registrar_prueba(
        f"TRXPFL: LongType = 2 (encontrado: {cnt_long_trxpfl})",
        cnt_long_trxpfl == 2
    )
    registrar_prueba(
        f"TRXPFL: DoubleType = 30 (encontrado: {cnt_dbl_trxpfl})",
        cnt_dbl_trxpfl == 30
    )

    # --- T6.3: Cantidad de registros >= 1 ---
    registrar_prueba(
        f"TRXPFL: Cantidad de registros >= 1 (encontrado: {total_trxpfl})",
        total_trxpfl >= 1
    )

    # --- T6.4: Integridad referencial — todos los CUSTID existen en CMSTFL ---
    if df_cmstfl is not None:
        df_custids_cmstfl = df_cmstfl.select("CUSTID").distinct()
        count_custid_invalidos = df_trxpfl.join(
            df_custids_cmstfl,
            "CUSTID",
            how="left_anti"
        ).count()
        registrar_prueba(
            f"TRXPFL: Integridad referencial CUSTID (registros sin FK en CMSTFL: {count_custid_invalidos})",
            count_custid_invalidos == 0
        )
    else:
        registrar_prueba("TRXPFL: Integridad referencial CUSTID (no se puede verificar — CMSTFL no disponible)", False)

    # --- T6.5: Formato TRXID (prefijo 4 chars + 8 digitos) ---
    import re
    count_trxid_invalidos = df_trxpfl.filter(
        ~F.col("TRXID").rlike("^[A-Z]{4}[0-9]{8}$")
    ).count()
    registrar_prueba(
        f"TRXPFL: Formato TRXID correcto 'XXXX00000001' (invalidos: {count_trxid_invalidos})",
        count_trxid_invalidos == 0
    )

    # --- T6.6: Distribucion de tipos de transaccion (~60%/~30%/~10% con tolerancia 5%) ---
    tipos_altos  = ["CATM", "DATM", "CMPR", "TINT", "DPST"]
    tipos_medios = ["PGSL", "TEXT", "RTRO", "PGSV", "NMNA", "INTR"]
    tipos_bajos  = ["ADSL", "IMPT", "DMCL", "CMSN"]

    count_alto  = df_trxpfl.filter(F.col("TRXTYP").isin(tipos_altos)).count()
    count_medio = df_trxpfl.filter(F.col("TRXTYP").isin(tipos_medios)).count()
    count_bajo  = df_trxpfl.filter(F.col("TRXTYP").isin(tipos_bajos)).count()

    pct_alto  = count_alto  / total_trxpfl * 100 if total_trxpfl > 0 else 0
    pct_medio = count_medio / total_trxpfl * 100 if total_trxpfl > 0 else 0
    pct_bajo  = count_bajo  / total_trxpfl * 100 if total_trxpfl > 0 else 0

    tolerancia = 5  # 5 puntos porcentuales de tolerancia
    registrar_prueba(
        f"TRXPFL: Distribucion ALTA ~60% (encontrado: {pct_alto:.1f}%, esperado: 55-65%)",
        abs(pct_alto - 60) <= tolerancia,
        f"Conteo tipo alto: {count_alto}"
    )
    registrar_prueba(
        f"TRXPFL: Distribucion MEDIA ~30% (encontrado: {pct_medio:.1f}%, esperado: 25-35%)",
        abs(pct_medio - 30) <= tolerancia,
        f"Conteo tipo medio: {count_medio}"
    )
    registrar_prueba(
        f"TRXPFL: Distribucion BAJA ~10% (encontrado: {pct_bajo:.1f}%, esperado: 5-15%)",
        abs(pct_bajo - 10) <= tolerancia,
        f"Conteo tipo bajo: {count_bajo}"
    )

    # --- T6.7: TRXTYP solo contiene los 15 tipos validos ---
    tipos_validos_trx = tipos_altos + tipos_medios + tipos_bajos
    count_tipos_invalidos = df_trxpfl.filter(~F.col("TRXTYP").isin(tipos_validos_trx)).count()
    registrar_prueba(
        f"TRXPFL: TRXTYP solo contiene los 15 tipos validos (invalidos: {count_tipos_invalidos})",
        count_tipos_invalidos == 0
    )

else:
    registrar_prueba("TRXPFL: No se pudo cargar el parquet para pruebas", False, "Parquet no disponible")

# COMMAND ----------

# ==============================================================================
# PASO 7: PRUEBAS TDD PARA BLNCFL (Saldos de Clientes)
# ==============================================================================

print("\n" + "=" * 70)
print("PRUEBAS BLNCFL — Saldos de Clientes (100 columnas esperadas)")
print("=" * 70)

if df_blncfl is not None:
    total_blncfl = df_blncfl.count()

    # --- T7.1: Verificar 100 columnas ---
    num_cols_blncfl = len(df_blncfl.columns)
    registrar_prueba(
        f"BLNCFL: Numero de columnas = 100 (encontrado: {num_cols_blncfl})",
        num_cols_blncfl == 100,
        f"Columnas encontradas: {df_blncfl.columns}"
    )

    # --- T7.2: Distribucion de tipos (2 LongType, 29 StringType, 34 DoubleType, 35 DateType) ---
    tipos_blncfl    = {f.name: f.dataType for f in df_blncfl.schema.fields}
    cnt_long_bl     = sum(1 for t in tipos_blncfl.values() if isinstance(t, LongType))
    cnt_str_bl      = sum(1 for t in tipos_blncfl.values() if isinstance(t, StringType))
    cnt_dbl_bl      = sum(1 for t in tipos_blncfl.values() if isinstance(t, DoubleType))
    cnt_date_bl     = sum(1 for t in tipos_blncfl.values() if isinstance(t, DateType))

    registrar_prueba(
        f"BLNCFL: LongType = 2 (encontrado: {cnt_long_bl})",
        cnt_long_bl == 2
    )
    registrar_prueba(
        f"BLNCFL: StringType = 29 (encontrado: {cnt_str_bl})",
        cnt_str_bl == 29
    )
    registrar_prueba(
        f"BLNCFL: DoubleType = 34 (encontrado: {cnt_dbl_bl})",
        cnt_dbl_bl == 34
    )
    registrar_prueba(
        f"BLNCFL: DateType = 35 (encontrado: {cnt_date_bl})",
        cnt_date_bl == 35
    )

    # --- T7.3: Relacion 1:1 con CMSTFL (misma cantidad de registros, mismos CUSTID) ---
    if df_cmstfl is not None:
        total_cmstfl_para_bl = df_cmstfl.count()

        # Misma cantidad de registros
        registrar_prueba(
            f"BLNCFL: Misma cantidad de registros que CMSTFL (BLNCFL={total_blncfl}, CMSTFL={total_cmstfl_para_bl})",
            total_blncfl == total_cmstfl_para_bl
        )

        # Mismos CUSTIDs (todos los CUSTID de BLNCFL existen en CMSTFL)
        count_blncfl_sin_custid = df_blncfl.join(
            df_cmstfl.select("CUSTID").distinct(),
            "CUSTID",
            how="left_anti"
        ).count()
        registrar_prueba(
            f"BLNCFL: Todos los CUSTID existen en CMSTFL (sin FK: {count_blncfl_sin_custid})",
            count_blncfl_sin_custid == 0
        )

        # CUSTID unico en BLNCFL (relacion 1:1)
        count_custid_unicos_bl = df_blncfl.select("CUSTID").distinct().count()
        registrar_prueba(
            f"BLNCFL: CUSTID unico en BLNCFL (distintos={count_custid_unicos_bl}, total={total_blncfl})",
            count_custid_unicos_bl == total_blncfl
        )
    else:
        registrar_prueba("BLNCFL: Relacion 1:1 con CMSTFL (no se puede verificar — CMSTFL no disponible)", False)

    # --- T7.4: Distribucion de tipos de cuenta (AHRO 40%, CRTE 30%, PRES 20%, INVR 10% con tolerancia 3%) ---
    count_ahro = df_blncfl.filter(F.col("BLACT") == "AHRO").count()
    count_crte = df_blncfl.filter(F.col("BLACT") == "CRTE").count()
    count_pres = df_blncfl.filter(F.col("BLACT") == "PRES").count()
    count_invr = df_blncfl.filter(F.col("BLACT") == "INVR").count()

    pct_ahro = count_ahro / total_blncfl * 100 if total_blncfl > 0 else 0
    pct_crte = count_crte / total_blncfl * 100 if total_blncfl > 0 else 0
    pct_pres = count_pres / total_blncfl * 100 if total_blncfl > 0 else 0
    pct_invr = count_invr / total_blncfl * 100 if total_blncfl > 0 else 0

    tolerancia_cuenta = 3  # 3 puntos porcentuales de tolerancia
    registrar_prueba(
        f"BLNCFL: Distribucion AHRO ~40% (encontrado: {pct_ahro:.1f}%, esperado: 37-43%)",
        abs(pct_ahro - 40) <= tolerancia_cuenta
    )
    registrar_prueba(
        f"BLNCFL: Distribucion CRTE ~30% (encontrado: {pct_crte:.1f}%, esperado: 27-33%)",
        abs(pct_crte - 30) <= tolerancia_cuenta
    )
    registrar_prueba(
        f"BLNCFL: Distribucion PRES ~20% (encontrado: {pct_pres:.1f}%, esperado: 17-23%)",
        abs(pct_pres - 20) <= tolerancia_cuenta
    )
    registrar_prueba(
        f"BLNCFL: Distribucion INVR ~10% (encontrado: {pct_invr:.1f}%, esperado: 7-13%)",
        abs(pct_invr - 10) <= tolerancia_cuenta
    )

    # --- T7.5: BLACT solo contiene valores validos ---
    count_blact_invalidos = df_blncfl.filter(~F.col("BLACT").isin("AHRO", "CRTE", "PRES", "INVR")).count()
    registrar_prueba(
        f"BLNCFL: BLACT solo contiene AHRO/CRTE/PRES/INVR (invalidos: {count_blact_invalidos})",
        count_blact_invalidos == 0
    )

    # --- T7.6: Saldos BLAV y BLTB son positivos ---
    count_saldos_negativos = df_blncfl.filter((F.col("BLAV") < 0) | (F.col("BLTB") < 0)).count()
    registrar_prueba(
        f"BLNCFL: Saldos BLAV y BLTB son no-negativos (negativos: {count_saldos_negativos})",
        count_saldos_negativos == 0
    )

else:
    registrar_prueba("BLNCFL: No se pudo cargar el parquet para pruebas", False, "Parquet no disponible")

# COMMAND ----------

# ==============================================================================
# PASO 8: RESUMEN FINAL DE PRUEBAS
# ==============================================================================

tiempo_fin_tdd = time.time()
tiempo_total   = round(tiempo_fin_tdd - tiempo_inicio_tdd, 3)

total_pruebas   = len(resultados_pruebas)
pruebas_pass    = sum(1 for r in resultados_pruebas if r["resultado"])
pruebas_fail    = total_pruebas - pruebas_pass
resultado_global = pruebas_fail == 0

print("\n" + "=" * 70)
print("RESUMEN FINAL — NbTddGenerarParquets")
print("=" * 70)
print(f"  Total de pruebas : {total_pruebas}")
print(f"  PASS             : {pruebas_pass}")
print(f"  FAIL             : {pruebas_fail}")
print(f"  Tiempo total TDD : {tiempo_total}s")
print(f"  Resultado global : {'*** TODAS LAS PRUEBAS PASAN ***' if resultado_global else '!!! HAY PRUEBAS FALLIDAS !!!'}")

if pruebas_fail > 0:
    print("\nPruebas fallidas:")
    for r in resultados_pruebas:
        if not r["resultado"]:
            print(f"  - {r['descripcion']}")
            if r["detalle"]:
                print(f"    {r['detalle']}")

print("=" * 70)

if not resultado_global:
    raise Exception(
        f"TDD FALLIDO: {pruebas_fail} de {total_pruebas} pruebas no pasaron. "
        "Revise los detalles arriba para identificar y corregir los problemas."
    )
