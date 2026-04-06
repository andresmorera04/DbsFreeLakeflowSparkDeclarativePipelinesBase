# Modelo de Datos: Incremento 3 - LSDP Medalla de Bronce

**Feature**: 003-lsdp-bronce-pipeline
**Fecha**: 2026-04-04

## Entidades

### 1. Streaming Table cmstfl (`bronce.lab1.cmstfl`)

**Descripcion**: Tabla streaming de bronce que acumula historicamente todos los registros del Maestro de Clientes AS400 ingresados via AutoLoader.
**Cantidad de columnas**: 72 (70 originales del parquet CMSTFL + `FechaIngestaDatos` + `_rescued_data`)
**Origen**: Parquet CMSTFL generado en el Incremento 2, ruta dinamica via `LsdpConstructorRutas`
**Acumulacion**: Append-only historica (cada ejecucion agrega registros nuevos sin sobrescribir los anteriores)
**Liquid Cluster**: `FechaIngestaDatos`, `CUSTID`
**Schema Evolution**: `cloudFiles.schemaEvolutionMode=addNewColumns`

| # | Campo | Tipo | Posicion | Descripcion |
|---|-------|------|----------|-------------|
| 1 | FechaIngestaDatos | TimestampType | 1a (liquid cluster) | Marca de tiempo de ingesta, generada con `F.current_timestamp()` |
| 2 | CUSTID | LongType | 2a (liquid cluster) | Identificador unico del cliente (PK del parquet fuente) |
| 3-71 | (70 columnas originales restantes del parquet CMSTFL) | Varios | 3a-71a | Schema heredado del parquet AS400, en orden original excluyendo CUSTID |
| 72 | _rescued_data | StringType | Ultima | Columna automatica de AutoLoader para datos que no encajan con el schema vigente |

**Propiedades Delta** (via `table_properties` del decorador `@dp.table`):

| Propiedad | Valor |
|-----------|-------|
| delta.enableChangeDataFeed | true |
| delta.autoOptimize.autoCompact | true |
| delta.autoOptimize.optimizeWrite | true |
| delta.deletedFileRetentionDuration | interval 30 days |
| delta.logRetentionDuration | interval 60 days |

**Reglas**:
- Los campos del liquid cluster (`FechaIngestaDatos`, `CUSTID`) DEBEN ser las dos primeras columnas del schema.
- El reordenamiento se realiza via `LsdpReordenarColumnasLiquidCluster` antes de retornar el DataFrame.
- No se aplican Data Quality Checks (`@dp.expect`). El dato crudo se persiste tal cual.

---

### 2. Streaming Table trxpfl (`bronce.lab1.trxpfl`)

**Descripcion**: Tabla streaming de bronce que acumula historicamente todas las transacciones AS400 ingresadas via AutoLoader.
**Cantidad de columnas**: 62 (60 originales del parquet TRXPFL + `FechaIngestaDatos` + `_rescued_data`)
**Origen**: Parquet TRXPFL generado en el Incremento 2, ruta dinamica via `LsdpConstructorRutas`
**Acumulacion**: Append-only historica
**Liquid Cluster**: `TRXDT`, `CUSTID`, `TRXTYP`
**Schema Evolution**: `cloudFiles.schemaEvolutionMode=addNewColumns`

| # | Campo | Tipo | Posicion | Descripcion |
|---|-------|------|----------|-------------|
| 1 | TRXDT | DateType | 1a (liquid cluster) | Fecha de la transaccion |
| 2 | CUSTID | LongType | 2a (liquid cluster) | Identificador del cliente (FK a CMSTFL) |
| 3 | TRXTYP | StringType | 3a (liquid cluster) | Tipo de transaccion (15 tipos) |
| 4 | FechaIngestaDatos | TimestampType | 4a | Marca de tiempo de ingesta |
| 5-61 | (57 columnas originales restantes del parquet TRXPFL) | Varios | 5a-61a | Schema heredado del parquet AS400, en orden original excluyendo TRXDT, CUSTID y TRXTYP |
| 62 | _rescued_data | StringType | Ultima | Columna automatica de AutoLoader |

**Propiedades Delta**: Identicas a cmstfl (5 propiedades).

**Reglas**:
- Los campos del liquid cluster (`TRXDT`, `CUSTID`, `TRXTYP`) DEBEN ser las tres primeras columnas del schema.
- `FechaIngestaDatos` se agrega como cuarta columna (despues del liquid cluster).
- El reordenamiento incluye tanto los campos del cluster como `FechaIngestaDatos`.

---

### 3. Streaming Table blncfl (`bronce.lab1.blncfl`)

**Descripcion**: Tabla streaming de bronce que acumula historicamente todos los registros de saldos AS400 ingresados via AutoLoader.
**Cantidad de columnas**: 102 (100 originales del parquet BLNCFL + `FechaIngestaDatos` + `_rescued_data`)
**Origen**: Parquet BLNCFL generado en el Incremento 2, ruta dinamica via `LsdpConstructorRutas`
**Acumulacion**: Append-only historica
**Liquid Cluster**: `FechaIngestaDatos`, `CUSTID`
**Schema Evolution**: `cloudFiles.schemaEvolutionMode=addNewColumns`

| # | Campo | Tipo | Posicion | Descripcion |
|---|-------|------|----------|-------------|
| 1 | FechaIngestaDatos | TimestampType | 1a (liquid cluster) | Marca de tiempo de ingesta |
| 2 | CUSTID | LongType | 2a (liquid cluster) | Identificador del cliente (FK a CMSTFL) |
| 3-101 | (98 columnas originales restantes del parquet BLNCFL) | Varios | 3a-101a | Schema heredado del parquet AS400, en orden original excluyendo CUSTID |
| 102 | _rescued_data | StringType | Ultima | Columna automatica de AutoLoader |

**Propiedades Delta**: Identicas a cmstfl (5 propiedades).

**Reglas**:
- Los campos del liquid cluster (`FechaIngestaDatos`, `CUSTID`) DEBEN ser las dos primeras columnas del schema.
- Identico patron de reordenamiento que cmstfl.

---

### 4. Utilidad LsdpConexionParametros

**Descripcion**: Funcion que lee la tabla Parametros de Unity Catalog y retorna un diccionario Python.
**Ubicacion**: `src/LSDP_Laboratorio_Basico/utilities/LsdpConexionParametros.py`
**Formato**: Python puro (NO notebook de Databricks). Modulo importable.
**Reutilizable**: Si — por todos los scripts del LSDP (bronce, plata, oro).

**Interfaz**:
- **Entrada**: `spark` (SparkSession), `catalogo_parametro` (str), `esquema_parametro` (str), `tabla_parametros` (str)
- **Salida**: `dict[str, str]` — diccionario `{Clave: Valor}` con todos los registros de la tabla
- **Efectos secundarios**: Imprime en pantalla los parametros leidos (observabilidad)
- **Errores**: Lanza excepcion si la tabla no existe o no es accesible

**Claves esperadas en la tabla Parametros** (11 registros del Incremento 1):

| Clave | Tipo | Descripcion |
|-------|------|-------------|
| catalogoBronce | str | Catalogo Unity Catalog de bronce |
| contenedorBronce | str | Directorio contenedor de bronce |
| TipoStorage | str | "Volume" o "AmazonS3" |
| catalogoVolume | str | Catalogo del Volume |
| esquemaVolume | str | Esquema del Volume |
| nombreVolume | str | Nombre del Volume |
| bucketS3 | str | Bucket S3 |
| prefijoS3 | str | Prefijo S3 |
| DirectorioBronce | str | Subdirectorio dentro del contenedor |
| catalogoPlata | str | Catalogo Unity Catalog de plata |
| catalogoOro | str | Catalogo Unity Catalog de oro |

---

### 5. Utilidad LsdpConstructorRutas

**Descripcion**: Funcion que recibe el diccionario de parametros y una ruta relativa, y construye la ruta completa de almacenamiento.
**Ubicacion**: `src/LSDP_Laboratorio_Basico/utilities/LsdpConstructorRutas.py`
**Formato**: Python puro (NO notebook de Databricks). Modulo importable.
**Reutilizable**: Si — por todos los scripts del LSDP.

**Interfaz**:
- **Entrada**: `diccionario_parametros` (dict), `ruta_relativa` (str)
- **Salida**: `str` — ruta completa construida
- **Efectos secundarios**: Imprime en pantalla la ruta construida
- **Errores**: Lanza excepcion si `TipoStorage` no es "Volume" ni "AmazonS3"

**Logica de construccion**:
- Si `TipoStorage=Volume`: `/Volumes/<catalogoVolume>/<esquemaVolume>/<nombreVolume>/<ruta_relativa>`
- Si `TipoStorage=AmazonS3`: `s3://<bucketS3>/<ruta_relativa>` (el parametro `prefijoS3` existe en la tabla Parametros por herencia del Incremento 1 pero NO se utiliza en la construccion de rutas S3, segun Constitution v1.1.0)

---

### 6. Utilidad LsdpReordenarColumnasLiquidCluster

**Descripcion**: Funcion que recibe un DataFrame y una lista de campos del liquid cluster, y retorna el DataFrame con los campos del cluster como las primeras columnas.
**Ubicacion**: `src/LSDP_Laboratorio_Basico/utilities/LsdpReordenarColumnasLiquidCluster.py`
**Formato**: Python puro (NO notebook de Databricks). Modulo importable.
**Reutilizable**: Si — por todas las streaming tables y vistas materializadas del LSDP.

**Interfaz**:
- **Entrada**: `df` (DataFrame), `campos_cluster` (list[str])
- **Salida**: `DataFrame` — con columnas reordenadas
- **Errores**: Lanza excepcion si algun campo del cluster no existe en el DataFrame

**Algoritmo**:
1. Validar que todos los campos de `campos_cluster` existan en `df.columns`.
2. Calcular `columnas_restantes` = columnas de `df` que no estan en `campos_cluster`, preservando orden original.
3. Retornar `df.select(campos_cluster + columnas_restantes)`.

---

### 7. Tabla Parametros (existente — Incremento 1)

**Descripcion**: Tabla Delta en Unity Catalog con la configuracion dinamica del proyecto.
**Ubicacion**: `<catalogoParametro>.<esquemaParametro>.<tablaParametros>` (por defecto: `control.lab1.Parametros`)
**Columnas**: `Clave` (StringType), `Valor` (StringType)
**Registros**: 11 claves de configuracion insertadas por `conf/NbConfiguracionInicial.py`
**Relacion con Incremento 3**: Leida por `LsdpConexionParametros` al inicializar cada script de bronce.

---

## Diagrama de Relaciones

```text
+---------------------+       +---------------------+       +---------------------+
|  Parquet CMSTFL     |       |  Parquet TRXPFL     |       |  Parquet BLNCFL     |
|  (70 cols, Volume   |       |  (60 cols, Volume   |       |  (100 cols, Volume  |
|   o S3)             |       |   o S3)             |       |   o S3)             |
+---------------------+       +---------------------+       +---------------------+
          |                             |                             |
          | AutoLoader                  | AutoLoader                  | AutoLoader
          | schemaEvolution             | schemaEvolution             | schemaEvolution
          v                             v                             v
+---------------------+       +---------------------+       +---------------------+
|  bronce.lab1    |       |  bronce.lab1    |       |  bronce.lab1    |
|  .cmstfl            |       |  .trxpfl            |       |  .blncfl            |
|  (72 cols)          |       |  (62 cols)          |       |  (102 cols)         |
|  LC: FechaIngesta,  |       |  LC: TRXDT, CUSTID, |       |  LC: FechaIngesta,  |
|      CUSTID         |       |      TRXTYP         |       |      CUSTID         |
|  + FechaIngestaDatos|       |  + FechaIngestaDatos|       |  + FechaIngestaDatos|
|  + _rescued_data    |       |  + _rescued_data    |       |  + _rescued_data    |
+---------------------+       +---------------------+       +---------------------+

+---------------------+       +---------------------+
|  Tabla Parametros   |       |  LsdpConstructor    |
|  (control.lab1) |------>|  Rutas              |
|  11 claves          |       |  (Volume o S3)      |
+---------------------+       +---------------------+
          ^
          |
+---------------------+
|  LsdpConexion       |
|  Parametros         |
|  (spark.read.table) |
+---------------------+
```

## Resumen de Entidades

| # | Entidad | Tipo | Columnas | Liquid Cluster | Ubicacion |
|---|---------|------|----------|----------------|-----------|
| 1 | cmstfl | Streaming Table | 72 | FechaIngestaDatos, CUSTID | bronce.lab1 |
| 2 | trxpfl | Streaming Table | 62 | TRXDT, CUSTID, TRXTYP | bronce.lab1 |
| 3 | blncfl | Streaming Table | 102 | FechaIngestaDatos, CUSTID | bronce.lab1 |
| 4 | LsdpConexionParametros | Utilidad Python | N/A | N/A | utilities/ |
| 5 | LsdpConstructorRutas | Utilidad Python | N/A | N/A | utilities/ |
| 6 | LsdpReordenarColumnasLiquidCluster | Utilidad Python | N/A | N/A | utilities/ |
| 7 | Tabla Parametros | Tabla Delta (existente) | 2 | N/A | control.lab1 |
