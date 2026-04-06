# Databricks notebook source
# MAGIC %md
# MAGIC # LsdpInsertarTiposTransaccion
# MAGIC
# MAGIC Utilidad LSDP que inserta de forma idempotente la clave `TiposTransaccionesLabBase`
# MAGIC en la tabla Parametros de Unity Catalog.
# MAGIC
# MAGIC Invocada desde el notebook que orquesta el pipeline LSDP antes de ejecutarlo,
# MAGIC para garantizar que los tipos de transaccion esten disponibles en la tabla
# MAGIC Parametros cuando el script de oro (`LsdpOroClientes.py`) los consulta.
# MAGIC
# MAGIC Modulo Python puro (NO notebook de Databricks). Reutilizable por TDD.

# COMMAND ----------


def insertar_tipos_transaccion(spark, catalogo: str, esquema: str, tabla_parametros: str) -> None:
    """
    Inserta de forma idempotente la clave TiposTransaccionesLabBase en la tabla Parametros.

    Parametros:
        spark: SparkSession activa
        catalogo: Catalogo UC donde reside la tabla Parametros
        esquema: Esquema UC donde reside la tabla Parametros
        tabla_parametros: Nombre de la tabla Parametros

    Comportamiento:
        - Si la clave no existe: inserta ('TiposTransaccionesLabBase', 'DATM,CATM,PGSL')
        - Si la clave ya existe: no realiza ninguna operacion (idempotente)
        - Imprime el resultado de la operacion para observabilidad

    Compatibilidad:
        100% compatible con Databricks Serverless Compute.
        Sin sparkContext, sin RDD, sin .cache(), sin .persist().
    """
    nombre_completo_tabla = f"{catalogo}.{esquema}.{tabla_parametros}"

    print(f"[LsdpInsertarTiposTransaccion] Verificando clave 'TiposTransaccionesLabBase' en: {nombre_completo_tabla}")

    df_existente = spark.sql(
        f"SELECT Valor FROM {nombre_completo_tabla} WHERE Clave = 'TiposTransaccionesLabBase'"
    )

    if df_existente.count() == 0:
        spark.sql(
            f"INSERT INTO {nombre_completo_tabla} VALUES ('TiposTransaccionesLabBase', 'DATM,CATM,PGSL')"
        )
        print("[LsdpInsertarTiposTransaccion] Clave 'TiposTransaccionesLabBase' insertada con valor: DATM,CATM,PGSL")
    else:
        valor_actual = df_existente.collect()[0]["Valor"]
        print(f"[LsdpInsertarTiposTransaccion] Clave 'TiposTransaccionesLabBase' ya existe con valor: {valor_actual}. No se realiza ninguna operacion.")

    df_final = spark.sql(
        f"SELECT Valor FROM {nombre_completo_tabla} WHERE Clave = 'TiposTransaccionesLabBase'"
    )
    valor_final = df_final.collect()[0]["Valor"]
    print(f"[LsdpInsertarTiposTransaccion] Valor final de 'TiposTransaccionesLabBase': {valor_final}")
