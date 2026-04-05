"""
Utilidad LSDP: LsdpConstructorRutas
Construye la ruta completa de almacenamiento segun el TipoStorage configurado.
Modulo Python puro (NO notebook de Databricks). Reutilizable por todos los scripts del LSDP.
"""


def construir_ruta(diccionario_parametros: dict, ruta_relativa: str) -> str:
    """
    Construye la ruta completa de almacenamiento segun el TipoStorage.

    Parametros:
        diccionario_parametros: Diccionario {Clave: Valor} de la tabla Parametros
        ruta_relativa: Ruta relativa dentro del storage (ej: "LSDP_Base/As400/CMSTFL/")

    Retorna:
        str — Ruta completa construida

    Errores:
        ValueError — Si TipoStorage no es "Volume" ni "AmazonS3"

    Efectos secundarios:
        Imprime en pantalla la ruta construida
    """
    tipo_storage = diccionario_parametros.get("TipoStorage", "")

    if tipo_storage == "Volume":
        catalogo_volume = diccionario_parametros["catalogoVolume"]
        esquema_volume = diccionario_parametros["esquemaVolume"]
        nombre_volume = diccionario_parametros["nombreVolume"]
        ruta_completa = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa}"

    elif tipo_storage == "AmazonS3":
        # El parametro prefijoS3 existe en la tabla Parametros por herencia del Incremento 1
        # pero NO se utiliza en la construccion de rutas S3, segun Constitution v1.1.0.
        bucket_s3 = diccionario_parametros["bucketS3"]
        ruta_completa = f"s3://{bucket_s3}/{ruta_relativa}"

    else:
        raise ValueError(
            f"[LsdpConstructorRutas] TipoStorage no reconocido: '{tipo_storage}'. "
            f"Los valores validos son: 'Volume' o 'AmazonS3'."
        )

    print(f"[LsdpConstructorRutas] Ruta construida ({tipo_storage}): {ruta_completa}")
    return ruta_completa
