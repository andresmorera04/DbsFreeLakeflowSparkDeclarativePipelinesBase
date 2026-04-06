# Contratos: Incremento 3 - LSDP Medalla de Bronce

**Feature**: 003-lsdp-bronce-pipeline
**Fecha**: 2026-04-04

## 1. Contrato de Utilidad: LsdpConexionParametros

**Archivo**: `src/LSDP_Laboratorio_Basico/utilities/LsdpConexionParametros.py`
**Formato**: Python puro (NO notebook). Modulo importable sin celdas Databricks.

### Firma de la Funcion

```python
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
```

### Comportamiento

- Ejecuta `spark.read.table(f"{catalogo_parametro}.{esquema_parametro}.{tabla_parametros}")`
- Convierte el DataFrame a diccionario Python `{row["Clave"]: row["Valor"] for row in df.collect()}`
- Imprime cada clave-valor leida para observabilidad
- NO valida que claves especificas esten presentes (responsabilidad del consumidor)
- NO usa JDBC, Secret Scopes ni Azure Key Vault

---

## 2. Contrato de Utilidad: LsdpConstructorRutas

**Archivo**: `src/LSDP_Laboratorio_Basico/utilities/LsdpConstructorRutas.py`
**Formato**: Python puro (NO notebook). Modulo importable sin celdas Databricks.

### Firma de la Funcion

```python
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
```

### Comportamiento

- Lee `TipoStorage` del diccionario
- Si `TipoStorage == "Volume"`:
  - Lee `catalogoVolume`, `esquemaVolume`, `nombreVolume` del diccionario
  - Retorna `/Volumes/{catalogoVolume}/{esquemaVolume}/{nombreVolume}/{ruta_relativa}`
- Si `TipoStorage == "AmazonS3"`:
  - Lee `bucketS3` del diccionario
  - Retorna `s3://{bucketS3}/{ruta_relativa}`
  - Nota: El parametro `prefijoS3` existe en la tabla Parametros por herencia del Incremento 1 pero NO se utiliza en la construccion de rutas S3 (Constitution v1.1.0)
- Si `TipoStorage` tiene otro valor: lanza `ValueError` con mensaje indicando los valores validos
- Imprime la ruta construida via `print()`

---

## 3. Contrato de Utilidad: LsdpReordenarColumnasLiquidCluster

**Archivo**: `src/LSDP_Laboratorio_Basico/utilities/LsdpReordenarColumnasLiquidCluster.py`
**Formato**: Python puro (NO notebook). Modulo importable sin celdas Databricks.

### Firma de la Funcion

```python
def reordenar_columnas_liquid_cluster(df, campos_cluster: list) -> "DataFrame":
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
```

### Comportamiento

- Valida que cada campo de `campos_cluster` exista en `df.columns`
- Si algun campo no existe, lanza `ValueError` con el nombre del campo faltante
- Calcula `columnas_restantes` = columnas del DataFrame que no estan en `campos_cluster`, preservando orden original
- Retorna `df.select(campos_cluster + columnas_restantes)`
- No genera efectos secundarios (funcion pura excepto por la lectura de `df.columns`)

---

## 4. Contrato de Transformacion: transformar_cmstfl

**Archivo**: `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceCmstfl.py`

### Firma de la Funcion

```python
def transformar_cmstfl(df) -> "DataFrame":
    """
    Agrega FechaIngestaDatos y reordena columnas para liquid cluster de cmstfl.

    Parametros:
        df: DataFrame de PySpark (streaming o batch)

    Retorna:
        DataFrame — Con FechaIngestaDatos como primera columna, CUSTID como segunda,
                    y el resto en orden original
    """
```

### Comportamiento

- Agrega columna `FechaIngestaDatos` de tipo `TimestampType` con `F.current_timestamp()`
- Invoca `reordenar_columnas_liquid_cluster(df, ["FechaIngestaDatos", "CUSTID"])`
- Retorna DataFrame reordenado
- No accede a sparkContext, no usa cache/persist

---

## 5. Contrato de Transformacion: transformar_trxpfl

**Archivo**: `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceTrxpfl.py`

### Firma de la Funcion

```python
def transformar_trxpfl(df) -> "DataFrame":
    """
    Agrega FechaIngestaDatos y reordena columnas para liquid cluster de trxpfl.

    Parametros:
        df: DataFrame de PySpark (streaming o batch)

    Retorna:
        DataFrame — Con TRXDT como primera columna, CUSTID como segunda, TRXTYP como tercera,
                    y el resto en orden original
    """
```

### Comportamiento

- Agrega columna `FechaIngestaDatos` de tipo `TimestampType` con `F.current_timestamp()`
- Invoca `reordenar_columnas_liquid_cluster(df, ["TRXDT", "CUSTID", "TRXTYP"])`
- `FechaIngestaDatos` queda despues de los 3 campos del cluster (posicion 4)
- Retorna DataFrame reordenado

---

## 6. Contrato de Transformacion: transformar_blncfl

**Archivo**: `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceBlncfl.py`

### Firma de la Funcion

```python
def transformar_blncfl(df) -> "DataFrame":
    """
    Agrega FechaIngestaDatos y reordena columnas para liquid cluster de blncfl.

    Parametros:
        df: DataFrame de PySpark (streaming o batch)

    Retorna:
        DataFrame — Con FechaIngestaDatos como primera columna, CUSTID como segunda,
                    y el resto en orden original
    """
```

### Comportamiento

- Agrega columna `FechaIngestaDatos` de tipo `TimestampType` con `F.current_timestamp()`
- Invoca `reordenar_columnas_liquid_cluster(df, ["FechaIngestaDatos", "CUSTID"])`
- Retorna DataFrame reordenado
- Identico patron que `transformar_cmstfl`

---

## 7. Contrato del Decorador: @dp.table para Streaming Tables

**Archivos**: `LsdpBronceCmstfl.py`, `LsdpBronceTrxpfl.py`, `LsdpBronceBlncfl.py`

### Configuracion Comun del Decorador

```python
@dp.table(
    name="<nombre_tabla>",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days"
    },
    cluster_by=["<campo1>", "<campo2>", ...]
)
def tabla_bronce_<nombre>():
    df_streaming = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", ruta_schema_location)
        .load(ruta_completa))
    return transformar_<nombre>(df_streaming)
```

### Configuracion por Tabla

| Tabla | name | cluster_by |
|-------|------|------------|
| cmstfl | `"cmstfl"` | `["FechaIngestaDatos", "CUSTID"]` |
| trxpfl | `"trxpfl"` | `["TRXDT", "CUSTID", "TRXTYP"]` |
| blncfl | `"blncfl"` | `["FechaIngestaDatos", "CUSTID"]` |

---

## 8. Contrato de Parametros del Pipeline

### Parametros de Pipeline (leidos con `spark.conf.get()`)

| Parametro | Formato | Requerido | Ejemplo |
|-----------|---------|-----------|---------|
| catalogoParametro | string | Si | `"control"` |
| esquemaParametro | string | Si | `"lab1"` |
| tablaParametros | string | Si | `"Parametros"` |
| rutaCompletaMaestroCliente | string | Si | `"LSDP_Base/As400/MaestroCliente/"` |
| rutaCompletaTransaccional | string | Si | `"LSDP_Base/As400/Transaccional/"` |
| rutaCompletaSaldoCliente | string | Si | `"LSDP_Base/As400/SaldoCliente/"` |
| rutaSchemaLocationCmstfl | string | Si | `"LSDP_Base/SchemaLocation/Bronce/cmstfl/"` |
| rutaSchemaLocationTrxpfl | string | Si | `"LSDP_Base/SchemaLocation/Bronce/trxpfl/"` |
| rutaSchemaLocationBlncfl | string | Si | `"LSDP_Base/SchemaLocation/Bronce/blncfl/"` |

### Claves de la Tabla Parametros (leidas por `LsdpConexionParametros`)

| Clave | Consumidor | Uso |
|-------|------------|-----|
| TipoStorage | LsdpConstructorRutas | Determina formato de ruta (Volume o S3) |
| catalogoVolume | LsdpConstructorRutas | Catalogo para ruta Volume |
| esquemaVolume | LsdpConstructorRutas | Esquema para ruta Volume |
| nombreVolume | LsdpConstructorRutas | Nombre del Volume para ruta Volume |
| bucketS3 | LsdpConstructorRutas | Bucket para ruta S3 |
| prefijoS3 | (No utilizado en rutas S3 — herencia Inc.1, Constitution v1.1.0) | Prefijo para ruta S3 |
| catalogoBronce | (Disponible para futuros incrementos) | Catalogo de bronce |
| contenedorBronce | (Disponible para futuros incrementos) | Directorio contenedor |
| DirectorioBronce | (Disponible para futuros incrementos) | Subdirectorio |
| catalogoPlata | (Disponible para futuros incrementos) | Catalogo de plata |
| catalogoOro | (Disponible para futuros incrementos) | Catalogo de oro |
