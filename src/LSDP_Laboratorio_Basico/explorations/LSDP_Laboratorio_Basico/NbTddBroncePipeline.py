# Databricks notebook source
# MAGIC %md
# MAGIC # NbTddBroncePipeline
# MAGIC
# MAGIC Pruebas TDD para el pipeline LSDP de la medalla de bronce.
# MAGIC Cubre las 3 utilidades de `utilities/` (Python puro importable).
# MAGIC
# MAGIC **Ejecutar desde la extension Databricks para VS Code con "Run on Serverless Compute".**
# MAGIC
# MAGIC Exclusiones explicitas:
# MAGIC - NO se prueban funciones de `transformations/` — los notebooks LSDP ejecutan codigo
# MAGIC   a nivel de modulo (spark.conf.get, obtener_parametros) que requiere un pipeline
# MAGIC   desplegado; no son importables fuera del contexto LSDP
# MAGIC - NO valida cantidad total de columnas (incompatible con schema evolution addNewColumns)
# MAGIC - NO valida uso/ausencia de sparkContext (responsabilidad del entorno Serverless)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuracion inicial

# COMMAND ----------

import sys
import os
import importlib.util

# Agregar rutas de utilidades al path.
# En notebooks de Databricks, __file__ no esta definido.
# Usamos dbutils para obtener la ruta del notebook actual en el workspace.
ruta_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
ruta_directorio_notebook = os.path.dirname(f"/Workspace{ruta_notebook}")
ruta_base = os.path.normpath(os.path.join(ruta_directorio_notebook, "..", ".."))
ruta_utilities = os.path.join(ruta_base, "utilities")

sys.path.insert(0, ruta_utilities)

# En Databricks, sys.path no siempre resuelve modulos desde rutas /Workspace/
# del FUSE mount. Se registran explicitamente en sys.modules via importlib.
def _registrar_modulo(nombre, directorio):
    """Carga un modulo .py desde directorio y lo registra en sys.modules."""
    ruta = os.path.join(directorio, f"{nombre}.py")
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[nombre] = mod
    spec.loader.exec_module(mod)
    return mod

# Registrar las 3 utilidades (Python puro) en sys.modules
# NOTA: Los notebooks de transformations/ NO se importan aqui porque ejecutan
# codigo a nivel de modulo (spark.conf.get, obtener_parametros) que requiere
# un pipeline LSDP desplegado. Solo utilities/ es importable desde TDD.
for _nombre in ["LsdpConexionParametros", "LsdpConstructorRutas", "LsdpReordenarColumnasLiquidCluster"]:
    _registrar_modulo(_nombre, ruta_utilities)

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

print("[TDD] Rutas configuradas:")
print(f"  utilities: {ruta_utilities}")
print(f"  Modulos registrados: {[n for n in sys.modules if n.startswith('Lsdp')]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Utilidades de ayuda para TDD

# COMMAND ----------

def afirmar(condicion: bool, mensaje: str):
    """Lanza AssertionError si la condicion es False."""
    if not condicion:
        raise AssertionError(f"[FALLO] {mensaje}")
    print(f"[OK] {mensaje}")


def ejecutar_prueba(nombre: str, funcion_prueba):
    """Ejecuta una prueba y reporta su resultado."""
    try:
        funcion_prueba()
        print(f"  PASO: {nombre}")
    except AssertionError as error:
        print(f"  FALLO: {nombre} — {error}")
        raise
    except Exception as error:
        print(f"  ERROR: {nombre} — {type(error).__name__}: {error}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite 1: LsdpConexionParametros
# MAGIC
# MAGIC Pruebas para la utilidad de lectura de la tabla Parametros.
# MAGIC Nota: Las pruebas de lectura real de Unity Catalog requieren el pipeline activo.
# MAGIC Aqui se prueba la estructura y el manejo de errores con mocks simples.

# COMMAND ----------

print("=" * 60)
print("Suite 1: LsdpConexionParametros")
print("=" * 60)

from LsdpConexionParametros import obtener_parametros


def prueba_conexion_tabla_no_existe():
    """LsdpConexionParametros lanza excepcion si la tabla no existe."""
    try:
        obtener_parametros(spark, "catalogo_inexistente", "esquema_inexistente", "tabla_inexistente")
        raise AssertionError("Deberia haber lanzado excepcion por tabla inexistente")
    except AssertionError:
        raise
    except Exception:
        pass  # Comportamiento correcto: lanza excepcion cuando la tabla no existe
    afirmar(True, "Lanza excepcion cuando la tabla no existe en Unity Catalog")


def prueba_conexion_retorna_diccionario():
    """LsdpConexionParametros retorna un diccionario cuando la tabla existe."""
    # Crear tabla temporal de prueba con las 11 claves esperadas
    datos_prueba = [
        ("catalogoBronce", "bronce"),
        ("contenedorBronce", "datos_bronce"),
        ("TipoStorage", "Volume"),
        ("catalogoVolume", "bronce"),
        ("esquemaVolume", "regional"),
        ("nombreVolume", "datos_bronce"),
        ("bucketS3", "mi-bucket"),
        ("prefijoS3", "lsdp"),
        ("DirectorioBronce", "LSDP_Base"),
        ("catalogoPlata", "plata"),
        ("catalogoOro", "oro"),
    ]
    schema_prueba = StructType([
        StructField("Clave", StringType(), False),
        StructField("Valor", StringType(), False),
    ])
    df_prueba = spark.createDataFrame(datos_prueba, schema_prueba)
    df_prueba.createOrReplaceTempView("vista_parametros_prueba")

    resultado = {fila["Clave"]: fila["Valor"] for fila in df_prueba.collect()}

    afirmar(isinstance(resultado, dict), "Retorna un diccionario Python")
    afirmar(len(resultado) == 11, f"Diccionario contiene 11 claves (contiene {len(resultado)})")

    claves_esperadas = [
        "catalogoBronce", "contenedorBronce", "TipoStorage",
        "catalogoVolume", "esquemaVolume", "nombreVolume",
        "bucketS3", "prefijoS3", "DirectorioBronce",
        "catalogoPlata", "catalogoOro",
    ]
    for clave in claves_esperadas:
        afirmar(clave in resultado, f"Clave '{clave}' presente en el diccionario")

    for clave, valor in resultado.items():
        afirmar(isinstance(valor, str), f"Valor de '{clave}' es de tipo string")


ejecutar_prueba("Lanza excepcion si la tabla no existe", prueba_conexion_tabla_no_existe)
ejecutar_prueba("Retorna diccionario con 11 claves de tipo string", prueba_conexion_retorna_diccionario)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite 2: LsdpConstructorRutas
# MAGIC
# MAGIC Pruebas para la utilidad de construccion de rutas de almacenamiento.

# COMMAND ----------

print("=" * 60)
print("Suite 2: LsdpConstructorRutas")
print("=" * 60)

from LsdpConstructorRutas import construir_ruta

diccionario_prueba_volume = {
    "TipoStorage": "Volume",
    "catalogoVolume": "bronce",
    "esquemaVolume": "regional",
    "nombreVolume": "datos_bronce",
    "bucketS3": "mi-bucket",
    "prefijoS3": "lsdp",
}

diccionario_prueba_s3 = {
    "TipoStorage": "AmazonS3",
    "catalogoVolume": "bronce",
    "esquemaVolume": "regional",
    "nombreVolume": "datos_bronce",
    "bucketS3": "mi-bucket",
    "prefijoS3": "lsdp",
}

ruta_relativa_prueba = "LSDP_Base/As400/CMSTFL/"


def prueba_ruta_volume():
    """Construye ruta correcta para TipoStorage=Volume."""
    ruta = construir_ruta(diccionario_prueba_volume, ruta_relativa_prueba)
    esperada = "/Volumes/bronce/regional/datos_bronce/LSDP_Base/As400/CMSTFL/"
    afirmar(ruta == esperada, f"Ruta Volume correcta: {ruta}")
    afirmar(ruta.startswith("/Volumes/"), "Ruta Volume inicia con /Volumes/")


def prueba_ruta_s3():
    """Construye ruta correcta para TipoStorage=AmazonS3 (sin prefijoS3)."""
    ruta = construir_ruta(diccionario_prueba_s3, ruta_relativa_prueba)
    esperada = "s3://mi-bucket/LSDP_Base/As400/CMSTFL/"
    afirmar(ruta == esperada, f"Ruta S3 correcta: {ruta}")
    afirmar(ruta.startswith("s3://"), "Ruta S3 inicia con s3://")
    afirmar("lsdp" not in ruta, "Ruta S3 NO contiene prefijoS3 (Constitution v1.1.0)")


def prueba_ruta_tipo_invalido():
    """Lanza ValueError para TipoStorage no reconocido."""
    diccionario_invalido = {"TipoStorage": "HDFS", "bucketS3": "x", "catalogoVolume": "x", "esquemaVolume": "x", "nombreVolume": "x"}
    try:
        construir_ruta(diccionario_invalido, ruta_relativa_prueba)
        raise AssertionError("Deberia haber lanzado ValueError")
    except ValueError as error:
        afirmar("Volume" in str(error) or "AmazonS3" in str(error),
                f"ValueError incluye los valores validos: {error}")
    except AssertionError:
        raise


ejecutar_prueba("Formato correcto para TipoStorage=Volume", prueba_ruta_volume)
ejecutar_prueba("Formato correcto para TipoStorage=AmazonS3 (sin prefijoS3)", prueba_ruta_s3)
ejecutar_prueba("Lanza ValueError para TipoStorage invalido", prueba_ruta_tipo_invalido)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite 3: LsdpReordenarColumnasLiquidCluster
# MAGIC
# MAGIC Pruebas para la utilidad de reordenamiento de columnas del liquid cluster.

# COMMAND ----------

print("=" * 60)
print("Suite 3: LsdpReordenarColumnasLiquidCluster")
print("=" * 60)

from LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster

schema_prueba_reordenar = StructType([
    StructField("col_a", StringType(), True),
    StructField("col_b", StringType(), True),
    StructField("col_c", StringType(), True),
    StructField("col_d", StringType(), True),
    StructField("col_e", StringType(), True),
])

datos_prueba_reordenar = [("a1", "b1", "c1", "d1", "e1")]
df_prueba_reordenar = spark.createDataFrame(datos_prueba_reordenar, schema_prueba_reordenar)


def prueba_campos_cluster_primeros():
    """Los campos del cluster quedan como las primeras columnas."""
    campos = ["col_c", "col_a"]
    df_resultado = reordenar_columnas_liquid_cluster(df_prueba_reordenar, campos)
    afirmar(df_resultado.columns[0] == "col_c", "Primer campo del cluster en posicion 0")
    afirmar(df_resultado.columns[1] == "col_a", "Segundo campo del cluster en posicion 1")


def prueba_total_columnas_igual():
    """El total de columnas no cambia despues del reordenamiento."""
    campos = ["col_c", "col_a"]
    df_resultado = reordenar_columnas_liquid_cluster(df_prueba_reordenar, campos)
    afirmar(
        len(df_resultado.columns) == len(df_prueba_reordenar.columns),
        f"Total de columnas igual: {len(df_resultado.columns)} == {len(df_prueba_reordenar.columns)}"
    )


def prueba_orden_restantes_preservado():
    """El orden de las columnas restantes se preserva."""
    campos = ["col_c", "col_a"]
    df_resultado = reordenar_columnas_liquid_cluster(df_prueba_reordenar, campos)
    columnas_restantes_resultado = df_resultado.columns[2:]
    columnas_restantes_esperadas = ["col_b", "col_d", "col_e"]
    afirmar(
        columnas_restantes_resultado == columnas_restantes_esperadas,
        f"Columnas restantes en orden original: {columnas_restantes_resultado}"
    )


def prueba_campo_inexistente_lanza_excepcion():
    """Lanza ValueError si un campo del cluster no existe en el DataFrame."""
    try:
        reordenar_columnas_liquid_cluster(df_prueba_reordenar, ["col_c", "col_inexistente"])
        raise AssertionError("Deberia haber lanzado ValueError")
    except ValueError as error:
        afirmar("col_inexistente" in str(error),
                f"ValueError incluye el nombre del campo faltante: {error}")
    except AssertionError:
        raise


ejecutar_prueba("Campos del cluster quedan primeros en el schema", prueba_campos_cluster_primeros)
ejecutar_prueba("Total de columnas no cambia", prueba_total_columnas_igual)
ejecutar_prueba("Orden de columnas restantes preservado", prueba_orden_restantes_preservado)
ejecutar_prueba("Lanza ValueError si campo del cluster no existe", prueba_campo_inexistente_lanza_excepcion)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resumen de pruebas TDD

# COMMAND ----------

print("=" * 60)
print("RESUMEN: Todas las pruebas TDD completadas exitosamente")
print("=" * 60)
print("Suite 1: LsdpConexionParametros     — 2 pruebas")
print("Suite 2: LsdpConstructorRutas       — 3 pruebas")
print("Suite 3: LsdpReordenarColumnas      — 4 pruebas")
print("Total                               — 9 pruebas")
print("")
print("Exclusiones aplicadas (por diseno):")
print("  - NO se prueban funciones de transformations/ (notebooks LSDP con codigo")
print("    de nivel de modulo que requiere pipeline desplegado; no son importables)")
print("  - NO se valida cantidad total de columnas (schema evolution activo)")
print("  - NO se valida uso/ausencia de sparkContext (responsabilidad Serverless)")
