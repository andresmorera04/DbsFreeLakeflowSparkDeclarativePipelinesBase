"""
Utilidad LSDP: LsdpReordenarColumnasLiquidCluster
Reordena las columnas de un DataFrame colocando los campos del liquid cluster primero.
Modulo Python puro (NO notebook de Databricks). Reutilizable por todos los scripts del LSDP.
"""


def reordenar_columnas_liquid_cluster(df, campos_cluster: list):
    """
    Reordena las columnas del DataFrame colocando los campos del liquid cluster primero.

    Parametros:
        df: DataFrame de PySpark
        campos_cluster: Lista de nombres de columnas del liquid cluster, en el orden deseado

    Retorna:
        DataFrame — Con las columnas reordenadas: campos_cluster primero, resto en orden original

    Errores:
        ValueError — Si algun campo de campos_cluster no existe en el DataFrame
    """
    columnas_df = df.columns

    for campo in campos_cluster:
        if campo not in columnas_df:
            raise ValueError(
                f"[LsdpReordenarColumnasLiquidCluster] El campo '{campo}' del liquid cluster "
                f"no existe en el DataFrame. Columnas disponibles: {columnas_df}"
            )

    columnas_restantes = [col for col in columnas_df if col not in campos_cluster]
    orden_final = campos_cluster + columnas_restantes

    return df.select(orden_final)
