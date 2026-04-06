# Databricks notebook source
# MAGIC %md
# MAGIC # NbTddOroPipeline
# MAGIC
# MAGIC Pruebas TDD para el pipeline LSDP de la medalla de oro.
# MAGIC Cubre la utilidad `LsdpInsertarTiposTransaccion` de `utilities/`.
# MAGIC
# MAGIC **Ejecutar desde la extension Databricks para VS Code con "Run on Serverless Compute".**
# MAGIC
# MAGIC Exclusiones explicitas:
# MAGIC - NO se prueban funciones de `transformations/` — los notebooks LSDP ejecutan codigo
# MAGIC   a nivel de modulo (spark.conf.get, obtener_parametros) que requiere un pipeline
# MAGIC   desplegado; no son importables fuera del contexto LSDP
# MAGIC - NO se prueba la vista materializada `comportamiento_atm_cliente` ni `resumen_integral_cliente`
# MAGIC   directamente (requieren pipeline LSDP activo)

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

# Registrar la utilidad de oro en sys.modules via importlib
def _registrar_modulo(nombre, directorio):
    """Carga un modulo .py desde directorio y lo registra en sys.modules."""
    ruta = os.path.join(directorio, f"{nombre}.py")
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[nombre] = mod
    spec.loader.exec_module(mod)
    return mod

_registrar_modulo("LsdpInsertarTiposTransaccion", ruta_utilities)

from LsdpInsertarTiposTransaccion import insertar_tipos_transaccion

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
# MAGIC ## Configuracion de la tabla de prueba
# MAGIC
# MAGIC Se crea una tabla Delta temporal en el catalogo y esquema configurados.
# MAGIC La tabla se elimina al finalizar todas las pruebas (cleanup).

# COMMAND ----------

# Catalogo y esquema para la tabla de prueba.
# Se leen de los parametros del pipeline si estan disponibles; de lo contrario
# se usan valores por defecto de control.lab1 (donde reside la tabla Parametros).
try:
    catalogo_prueba = spark.conf.get("pipelines.parameters.catalogoParametro")
    esquema_prueba  = spark.conf.get("pipelines.parameters.esquemaParametro")
except Exception:
    catalogo_prueba = "control"
    esquema_prueba  = "lab1"

tabla_prueba = "TddOroParametrosTiposTransaccion"
nombre_completo_tabla_prueba = f"{catalogo_prueba}.{esquema_prueba}.{tabla_prueba}"

print(f"[TDD] Tabla de prueba: {nombre_completo_tabla_prueba}")

# Crear tabla Delta de prueba (DROP IF EXISTS + CREATE para estado limpio)
spark.sql(f"DROP TABLE IF EXISTS {nombre_completo_tabla_prueba}")
spark.sql(f"""
    CREATE TABLE {nombre_completo_tabla_prueba} (
        Clave STRING NOT NULL,
        Valor STRING NOT NULL
    )
""")
print(f"[TDD] Tabla de prueba creada: {nombre_completo_tabla_prueba}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite: LsdpInsertarTiposTransaccion
# MAGIC
# MAGIC Pruebas para la utilidad de insercion idempotente de tipos de transaccion.

# COMMAND ----------

print("=" * 60)
print("Suite: LsdpInsertarTiposTransaccion")
print("=" * 60)


def prueba_insercion_cuando_clave_no_existe():
    """Inserta la clave TiposTransaccionesLabBase cuando no existe."""
    # Verificar que la tabla esta vacia antes de la prueba
    conteo_inicial = spark.sql(
        f"SELECT COUNT(*) AS total FROM {nombre_completo_tabla_prueba} WHERE Clave = 'TiposTransaccionesLabBase'"
    ).collect()[0]["total"]
    afirmar(conteo_inicial == 0, f"Tabla vacia antes de la prueba (conteo = {conteo_inicial})")

    # Ejecutar la funcion bajo prueba
    insertar_tipos_transaccion(spark, catalogo_prueba, esquema_prueba, tabla_prueba)

    # Verificar que la clave fue insertada con el valor correcto
    df_resultado = spark.sql(
        f"SELECT Valor FROM {nombre_completo_tabla_prueba} WHERE Clave = 'TiposTransaccionesLabBase'"
    )
    conteo_post = df_resultado.count()
    afirmar(conteo_post == 1, f"Se inserto exactamente 1 fila (conteo = {conteo_post})")

    valor_insertado = df_resultado.collect()[0]["Valor"]
    afirmar(
        valor_insertado == "DATM,CATM,PGSL",
        f"Valor insertado correcto: '{valor_insertado}' == 'DATM,CATM,PGSL'"
    )


def prueba_idempotencia_cuando_clave_ya_existe():
    """No realiza ninguna operacion y no duplica filas cuando la clave ya existe."""
    # La clave fue insertada por la prueba anterior
    conteo_pre = spark.sql(
        f"SELECT COUNT(*) AS total FROM {nombre_completo_tabla_prueba} WHERE Clave = 'TiposTransaccionesLabBase'"
    ).collect()[0]["total"]
    afirmar(conteo_pre == 1, f"La clave ya existe antes de la segunda invocacion (conteo = {conteo_pre})")

    valor_pre = spark.sql(
        f"SELECT Valor FROM {nombre_completo_tabla_prueba} WHERE Clave = 'TiposTransaccionesLabBase'"
    ).collect()[0]["Valor"]

    # Invocar la funcion de nuevo (debe ser idempotente)
    insertar_tipos_transaccion(spark, catalogo_prueba, esquema_prueba, tabla_prueba)

    # Verificar que el valor no cambio
    valor_post = spark.sql(
        f"SELECT Valor FROM {nombre_completo_tabla_prueba} WHERE Clave = 'TiposTransaccionesLabBase'"
    ).collect()[0]["Valor"]
    afirmar(valor_pre == valor_post, f"Valor no cambio: '{valor_post}' == '{valor_pre}'")

    # Verificar que no se duplicaron filas
    conteo_post = spark.sql(
        f"SELECT COUNT(*) AS total FROM {nombre_completo_tabla_prueba} WHERE Clave = 'TiposTransaccionesLabBase'"
    ).collect()[0]["total"]
    afirmar(conteo_post == 1, f"Sin filas duplicadas — sigue habiendo exactamente 1 fila (conteo = {conteo_post})")


ejecutar_prueba("Inserta la clave cuando no existe", prueba_insercion_cuando_clave_no_existe)
ejecutar_prueba("Idempotente cuando la clave ya existe", prueba_idempotencia_cuando_clave_ya_existe)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup — Eliminar tabla de prueba

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {nombre_completo_tabla_prueba}")
print(f"[TDD] Tabla de prueba eliminada: {nombre_completo_tabla_prueba}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resultado Final

# COMMAND ----------

print("=" * 60)
print("RESULTADO: Todas las pruebas de LsdpInsertarTiposTransaccion PASARON")
print("=" * 60)
