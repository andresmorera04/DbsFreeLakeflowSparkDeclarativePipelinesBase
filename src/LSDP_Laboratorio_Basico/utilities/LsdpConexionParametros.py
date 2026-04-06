"""
Utilidad LSDP: LsdpConexionParametros
Lee la tabla Parametros de Unity Catalog y retorna un diccionario Python {Clave: Valor}.
Modulo Python puro (NO notebook de Databricks). Reutilizable por todos los scripts del LSDP.
"""


def obtener_parametros(spark, catalogo_parametro: str, esquema_parametro: str, tabla_parametros: str) -> dict:
    """
    Lee la tabla Parametros de Unity Catalog y retorna un diccionario Python {Clave: Valor}.

    Parametros:
        spark: SparkSession activa
        catalogo_parametro: Nombre del catalogo de Unity Catalog donde reside la tabla
        esquema_parametro: Nombre del esquema dentro del catalogo
        tabla_parametros: Nombre de la tabla Parametros

    Retorna:
        dict[str, str] — Diccionario con todas las claves y valores de la tabla

    Errores:
        Exception — Si la tabla no existe o no es accesible en Unity Catalog

    Efectos secundarios:
        Imprime en pantalla los parametros leidos
    """
    nombre_completo = f"{catalogo_parametro}.{esquema_parametro}.{tabla_parametros}"
    print(f"[LsdpConexionParametros] Leyendo tabla de parametros: {nombre_completo}")

    df_parametros = spark.read.table(nombre_completo)
    diccionario = {fila["Clave"]: fila["Valor"] for fila in df_parametros.collect()}

    print(f"[LsdpConexionParametros] Parametros leidos ({len(diccionario)} claves):")
    for clave, valor in diccionario.items():
        print(f"  {clave} = {valor}")

    return diccionario
