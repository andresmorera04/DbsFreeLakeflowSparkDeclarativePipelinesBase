# Manual Tecnico: LSDP Laboratorio Basico

**Proyecto**: DbsFreeLakeflowSparkDeclarativePipelinesBase
**Plataforma**: Databricks Free Edition con Unity Catalog
**Fecha**: 2026-04-06
**Version**: Incremento 5 (pipeline completo: bronce, plata, oro)

---

## Tabla de Contenidos

1. [Decoradores LSDP](#1-decoradores-lsdp)
2. [Paradigma Declarativo de LSDP](#2-paradigma-declarativo-de-lsdp)
3. [Propiedades de Tablas Delta y Vistas Materializadas](#3-propiedades-de-tablas-delta-y-vistas-materializadas)
4. [Operaciones con la API de DataFrames de Spark](#4-operaciones-con-la-api-de-dataframes-de-spark)
5. [Parametros del Pipeline y Notebooks](#5-parametros-del-pipeline-y-notebooks)
6. [Tabla Parametros](#6-tabla-parametros)
7. [Dependencias](#7-dependencias)

---

## 1. Decoradores LSDP

### Importacion

Todos los scripts de transformacion del pipeline importan el modulo `pipelines` de PySpark:

```python
from pyspark import pipelines as dp
```

Este modulo es nativo del Databricks Runtime. No debe confundirse con `databricks-sdk`
(paquete PyPI para la API REST), que no contiene decoradores `@dp.table` ni
`@dp.materialized_view`.

### @dp.table — Streaming Tables (Capa Bronce)

Declara una streaming table. Se usa para la capa Bronce, donde los datos se ingestan
incrementalmente via AutoLoader desde parquets AS400.

**Parametros del decorador**:

| Parametro | Tipo | Descripcion |
|-----------|------|-------------|
| `name` | string | Nombre de la tabla (sin esquema — LSDP la ubica en el pipeline) |
| `comment` | string | Descripcion de la tabla (opcional) |
| `table_properties` | dict | Propiedades Delta en formato `{"clave": "valor"}` |
| `cluster_by` | list | Lista de columnas para Liquid Clustering |

**Ejemplo real del proyecto** (`LsdpBronceCmstfl.py`):

```python
@dp.table(
    name="cmstfl",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
    },
    cluster_by=["FechaIngestaDatos", "CUSTID"],
)
def tabla_bronce_cmstfl():
    df_streaming = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", ruta_schema_location)
        .load(ruta_parquet)
    )
    return transformar_cmstfl(df_streaming)
```

### @dp.materialized_view — Vistas Materializadas (Capas Plata y Oro)

Declara una vista materializada. Se usa en las capas Plata y Oro para transformaciones
y agregaciones sobre datos ya ingestados.

**Parametros del decorador**:

| Parametro | Tipo | Descripcion |
|-----------|------|-------------|
| `name` | string | Nombre de 3 partes: `catalogo.esquema.nombre_vista` |
| `comment` | string | Descripcion de la vista (opcional) |
| `table_properties` | dict | Propiedades Delta en formato `{"clave": "valor"}` |
| `cluster_by` | list | Lista de columnas para Liquid Clustering |

**CRITICO**: El decorador `@dp.materialized_view` NO acepta los parametros `catalog=` ni
`schema=` como kwargs separados. Genera el error
`materialized_view() got an unexpected keyword argument 'catalog'`. El nombre completo
de 3 partes DEBE ir en el parametro `name`:

```python
# CORRECTO
@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente",
    table_properties=propiedades_delta_oro,
    cluster_by=["identificador_cliente"],
)

# INCORRECTO (genera error en runtime)
@dp.materialized_view(
    catalog=catalogo_oro,
    schema=esquema_oro,
    name="comportamiento_atm_cliente",
)
```

**Ejemplo real del proyecto — con @dp.expect** (`LsdpPlataClientesSaldos.py`):

```python
@dp.expect("fecha_nacimiento_valida",       "fecha_nacimiento < '2009-01-01'")
@dp.expect("fecha_apertura_cuenta_valida",  "fecha_apertura_cuenta > '2020-12-31'")
@dp.expect("limite_credito_no_nulo",        "limite_credito IS NOT NULL")
@dp.expect("identificador_cliente_no_nulo", "identificador_cliente IS NOT NULL")
@dp.expect("limite_credito_positivo",       "limite_credito > 0")
@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados",
    comment="Vista materializada que consolida Maestro de Clientes y Saldos de bronce.",
    table_properties={
        "delta.enableChangeDataFeed":         "true",
        "delta.autoOptimize.autoCompact":     "true",
        "delta.autoOptimize.optimizeWrite":   "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration":         "interval 60 days",
    },
    cluster_by=["huella_identificacion_cliente", "identificador_cliente"],
)
def clientes_saldos_consolidados() -> DataFrame:
    ...
```

**Ejemplo real del proyecto — Oro con agregacion** (`LsdpOroClientes.py`):

```python
@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.resumen_integral_cliente",
    table_properties=propiedades_delta_oro,
    cluster_by=["huella_identificacion_cliente", "identificador_cliente"],
)
def resumen_integral_cliente() -> DataFrame:
    df_plata = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados"
    )
    df_oro = spark.read.table(
        f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente"
    )
    df_resultado = df_plata.join(df_oro, on="identificador_cliente", how="inner")
    return df_resultado.select(...)
```

### @dp.expect — Expectativas de Calidad de Datos

Declara una expectativa de calidad sobre una vista materializada. Se apila sobre el
decorador `@dp.materialized_view`. En modo observacional (sin `_on_violation`), registra
metricas de calidad sin bloquear la escritura.

```python
@dp.expect("nombre_expectativa", "expresion_sql")
```

El proyecto define 9 expectativas en total: 5 en `clientes_saldos_consolidados` y 4 en
`transacciones_enriquecidas`.

---

## 2. Paradigma Declarativo de LSDP

### Motor Declarativo

LSDP (Lakeflow Spark Declarative Pipelines) es el motor de Databricks para pipelines de
datos declarativos. El usuario declara que tablas y vistas existen mediante decoradores
Python; LSDP resuelve el orden de ejecucion, las dependencias entre entidades y el
procesamiento incremental automaticamente.

### Patron Closure

Todos los scripts de transformacion del proyecto siguen el patron Closure:

1. **Nivel de modulo**: Los parametros del pipeline se leen con `spark.conf.get` y los
   valores de la tabla Parametros con `obtener_parametros()`. Esto ocurre una sola vez
   al importar el script.
2. **Closure**: Las variables calculadas a nivel de modulo quedan capturadas
   automaticamente por las funciones decoradas con `@dp.table` o
   `@dp.materialized_view`.
3. **Serializacion**: cloudpickle serializa el closure completo (funcion + variables
   capturadas) para enviarlo al motor de ejecucion distribuida de Spark.

```python
# Nivel de modulo — se ejecuta una vez al importar el script
catalogo_plata = parametros.get("catalogoPlata", "plata")
esquema_plata  = parametros.get("esquemaPlata",  "lab1")   # capturado por closure

# Funcion decorada — usa las variables capturadas por closure
@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas",
    ...
)
def transacciones_enriquecidas() -> DataFrame:
    # catalogo_plata y esquema_plata son accesibles a traves del closure
    df = spark.read.table(f"{catalogo_plata}.{esquema_plata}.trxpfl")
    ...
```

### Resolucion Automatica de Imports

LSDP resuelve automaticamente los imports entre carpetas del mismo pipeline. Los scripts
de `transformations/` pueden importar modulos de `utilities/` directamente:

```python
# Correcto en transformations/ — LSDP resuelve el import
from utilities.LsdpConexionParametros import obtener_parametros
from utilities.LsdpConstructorRutas import construir_ruta
from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster
```

Los notebooks de exploracion/TDD (fuera del pipeline) SI requieren manipulacion manual
de `sys.path` o uso de `importlib` para resolver estos imports.

### Compatibilidad con Serverless Compute

El proyecto es 100% compatible con Databricks Free Edition Serverless Compute. Las
restricciones que aplican:

- Prohibido: `.cache()`, `.persist()`, `spark.sparkContext`, `.rdd`, `sc.broadcast()`
- Permitido: `spark.conf.set("spark.sql.shuffle.partitions", n)` y `spark.sql.adaptive.*`
- ANSI mode activado por defecto: `F.hash().cast("long")` antes de `F.abs()` obligatorio

### Tiempo de Ejecucion de Notebooks

Los notebooks de transformaciones ejecutan codigo a nivel de modulo (patron Closure)
al ser importados. Esto significa que el codigo fuera de funciones se ejecuta
inmediatamente al importar el script — incluyendo las llamadas a
`spark.conf.get("pipelines.parameters.*")` y `obtener_parametros()`. Por este motivo,
los scripts de `transformations/` NO son testeables via TDD desde notebooks de
exploracion: requieren un pipeline LSDP desplegado y activo para poder ejecutarse.

---

## 3. Propiedades de Tablas Delta y Vistas Materializadas

### Change Data Feed (CDF)

Habilitado en todas las tablas y vistas del proyecto. Permite consultar los cambios
incrementales de una tabla Delta directamente.

```python
"delta.enableChangeDataFeed": "true"
```

### Optimizacion Automatica

Ambas optimizaciones estan habilitadas en todas las entidades:

| Propiedad | Valor | Descripcion |
|-----------|-------|-------------|
| `delta.autoOptimize.autoCompact` | `"true"` | Compacta automaticamente archivos pequenos al escribir |
| `delta.autoOptimize.optimizeWrite` | `"true"` | Optimiza el tamano de archivos al escribir |

### Liquid Clustering

Liquid Clustering reemplaza a las particiones tradicionales. Los campos de cluster
se definen por entidad segun su patron de acceso:

| Entidad | Campos de Cluster | Justificacion |
|---------|------------------|---------------|
| `bronce.lab1.cmstfl` | `FechaIngestaDatos`, `CUSTID` | Consultas por fecha de ingesta y cliente |
| `bronce.lab1.trxpfl` | `FechaIngestaDatos`, `CUSTID` | Consultas por fecha de ingesta y cliente |
| `bronce.lab1.blncfl` | `FechaIngestaDatos`, `CUSTID` | Consultas por fecha de ingesta y cliente |
| `plata.lab1.clientes_saldos_consolidados` | `huella_identificacion_cliente`, `identificador_cliente` | Lookups por cliente |
| `plata.lab1.transacciones_enriquecidas` | `identificador_cliente`, `tipo_transaccion` | Consultas por cliente y tipo |
| `oro.lab1.comportamiento_atm_cliente` | `identificador_cliente` | Aggregaciones por cliente |
| `oro.lab1.resumen_integral_cliente` | `huella_identificacion_cliente`, `identificador_cliente` | Lookups por cliente |

### Retencion de Archivos y Logs

Configuracion uniforme en todas las entidades del proyecto:

| Propiedad | Valor | Descripcion |
|-----------|-------|-------------|
| `delta.deletedFileRetentionDuration` | `"interval 30 days"` | Retiene archivos eliminados 30 dias antes de vacuumar |
| `delta.logRetentionDuration` | `"interval 60 days"` | Retiene el log de transacciones 60 dias |

### Expectativas de Calidad de Datos

Las vistas materializadas de plata incluyen expectativas de calidad en modo
observacional (sin bloquear escritura):

**plata.lab1.clientes_saldos_consolidados** (5 expectativas):

| Nombre | Expresion SQL | Descripcion |
|--------|---------------|-------------|
| `limite_credito_positivo` | `limite_credito > 0` | Limite de credito debe ser positivo |
| `identificador_cliente_no_nulo` | `identificador_cliente IS NOT NULL` | Clave primaria no nula |
| `limite_credito_no_nulo` | `limite_credito IS NOT NULL` | Limite de credito obligatorio |
| `fecha_apertura_cuenta_valida` | `fecha_apertura_cuenta > '2020-12-31'` | Cuentas abiertas desde 2021 |
| `fecha_nacimiento_valida` | `fecha_nacimiento < '2009-01-01'` | Clientes mayores de edad |

**plata.lab1.transacciones_enriquecidas** (4 expectativas):

| Nombre | Expresion SQL | Descripcion |
|--------|---------------|-------------|
| `moneda_no_nula` | `moneda_transaccion IS NOT NULL` | Moneda de transaccion obligatoria |
| `monto_neto_no_nulo` | `monto_neto IS NOT NULL` | Monto neto calculado obligatorio |
| `monto_neto_positivo` | `monto_neto > 0` | Monto neto debe ser positivo |
| `identificador_cliente_no_nulo` | `identificador_cliente IS NOT NULL` | Clave foranea no nula |

---

## 4. Operaciones con la API de DataFrames de Spark

### Lecturas de Datos

**AutoLoader con cloudFiles (streaming, capa bronce)**:

```python
df_streaming = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", ruta_schema_location)
    .load(ruta_parquet)
)
```

- `cloudFiles.schemaEvolutionMode = addNewColumns`: Agrega columnas nuevas al schema
  automaticamente al detectar campos nuevos en el parquet fuente.
- `cloudFiles.schemaLocation`: Ruta donde AutoLoader persiste el schema inferido entre
  ejecuciones. Ruta construida dinamicamente por `LsdpConstructorRutas`.

**Lectura de tabla Unity Catalog (batch, capas plata y oro)**:

```python
df = spark.read.table(f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")
```

### Joins entre Tablas

**LEFT JOIN — plata.clientes_saldos_consolidados** (CMSTFL + BLNCFL):

```python
# Dimension Tipo 1: registro mas reciente por CUSTID usando Window
ventana_cmstfl = Window.partitionBy("CUSTID").orderBy(F.col("FechaIngestaDatos").desc())
df_cmstfl_dedup = (
    df_cmstfl
    .withColumn("rn", F.row_number().over(ventana_cmstfl))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
df_resultado = df_cmstfl_dedup.join(df_blncfl_dedup, on="CUSTID", how="left")
```

**INNER JOIN — oro.resumen_integral_cliente** (plata + oro):

```python
df_resultado = df_plata.join(df_oro, on="identificador_cliente", how="inner")
```

### Agregaciones con groupBy y F.when

La vista `comportamiento_atm_cliente` usa agregacion condicional sin filtros previos.
Calcula 5 metricas ATM en una sola pasada:

```python
df_agrupado = df_transacciones.groupBy("identificador_cliente").agg(
    F.count(F.when(F.col("tipo_transaccion") == TIPO_DEPOSITO_ATM, F.col("monto_principal"))).alias("cantidad_depositos_atm"),
    F.count(F.when(F.col("tipo_transaccion") == TIPO_RETIRO_ATM,   F.col("monto_principal"))).alias("cantidad_retiros_atm"),
    F.avg(F.when(F.col("tipo_transaccion") == TIPO_DEPOSITO_ATM,   F.col("monto_principal"))).alias("promedio_monto_depositos_atm"),
    F.avg(F.when(F.col("tipo_transaccion") == TIPO_RETIRO_ATM,     F.col("monto_principal"))).alias("promedio_monto_retiros_atm"),
    F.sum(F.when(F.col("tipo_transaccion") == TIPO_PAGO_SALDO,     F.col("monto_principal"))).alias("total_pagos_saldo_cliente"),
)
```

### Transformaciones Comunes

| Funcion | Uso en el Proyecto |
|---------|-------------------|
| `F.current_timestamp()` | Marca de tiempo de ingesta (`FechaIngestaDatos`) en bronce |
| `F.coalesce(col, lit(0))` | Reemplaza NULL por 0 en metricas de agregacion |
| `F.when(cond, val).otherwise(val)` | Campos calculados de clasificacion y categorización |
| `F.sha2(F.concat(...), 256)` | Huella de identificacion del cliente (hash SHA2-256) |
| `F.concat(col1, col2)` | Concatenacion de columnas para el hash |
| `F.row_number().over(window)` | Deduplicacion Dimension Tipo 1 |
| `F.col(...).cast("long")` | Cast obligatorio antes de `F.abs(F.hash(...))` en ANSI mode |

### Protecciones de ANSI Mode

`spark.sql.ansi.enabled = true` viene activado por defecto en Databricks Serverless.

**F.hash() con F.abs() — patron obligatorio**:

```python
# CORRECTO — cast a long ANTES de abs para evitar ARITHMETIC_OVERFLOW
F.abs(F.hash(F.col("columna")).cast("long"))

# INCORRECTO — F.hash() retorna IntegerType (32 bits); Integer.MIN_VALUE causa overflow
F.abs(F.hash(F.col("columna")))  # ARITHMETIC_OVERFLOW en ANSI mode
```

**F.hash() con multiples columnas**:

```python
# CORRECTO — argumentos separados
F.hash(F.col("col1"), F.col("col2"))

# INCORRECTO — el operador + entre columnas es suma aritmetica, no concatenacion
F.hash(F.col("col1") + F.col("col2"))  # DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE
```

---

## 5. Parametros del Pipeline y Notebooks

### Parametros del Pipeline LSDP

Se configuran en la definicion del pipeline LSDP en la interfaz de Databricks o via
archivo JSON de configuracion. Se leen en tiempo de ejecucion con
`spark.conf.get("pipelines.parameters.nombreParametro")`.

| Parametro | Tipo | Ejemplo | Descripcion |
|-----------|------|---------|-------------|
| `catalogoParametro` | string | `control` | Catalogo UC donde reside la tabla Parametros |
| `esquemaParametro` | string | `lab1` | Esquema donde reside la tabla Parametros |
| `tablaParametros` | string | `Parametros` | Nombre de la tabla Parametros |
| `rutaCompletaMaestroCliente` | string | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa del parquet CMSTFL (relativa al almacenamiento base) |
| `rutaCompletaTransaccional` | string | `LSDP_Base/As400/Transaccional/` | Ruta relativa del parquet TRXPFL |
| `rutaCompletaSaldoCliente` | string | `LSDP_Base/As400/SaldoCliente/` | Ruta relativa del parquet BLNCFL |
| `rutaSchemaLocationCmstfl` | string | `LSDP_Base/SchemaLocations/cmstfl/` | Ruta donde AutoLoader persiste el schema de cmstfl |
| `rutaSchemaLocationTrxpfl` | string | `LSDP_Base/SchemaLocations/trxpfl/` | Ruta donde AutoLoader persiste el schema de trxpfl |
| `rutaSchemaLocationBlncfl` | string | `LSDP_Base/SchemaLocations/blncfl/` | Ruta donde AutoLoader persiste el schema de blncfl |

Las rutas relativas se combinan con el almacenamiento base (Volume o S3) mediante
`LsdpConstructorRutas` para producir rutas absolutas en tiempo de ejecucion.

### Widgets de Notebooks de Exploracion

**NbConfiguracionInicial.py** (conf/):

| Widget | Valor por Defecto | Descripcion |
|--------|---------------------|-------------|
| `catalogoParametro` | `control` | Catalogo de la tabla Parametros |
| `esquemaParametro` | `lab1` | Esquema de la tabla Parametros |
| `tablaParametros` | `Parametros` | Nombre de la tabla Parametros |

**NbGenerarMaestroCliente.py, NbGenerarTransaccionalCliente.py, NbGenerarSaldosCliente.py** (src/.../GenerarParquets/):

| Widget | Valor por Defecto | Descripcion |
|--------|---------------------|-------------|
| `catalogoParametro` | `control` | Catalogo UC de la tabla Parametros |
| `esquemaParametro` | `lab1` | Esquema UC de la tabla Parametros |
| `tablaParametros` | `Parametros` | Nombre de la tabla Parametros |
| `numeroRegistros` / `numeroClientes` | `50000` / `150000` | Cantidad de registros a generar |
| `semilla` | `42` | Semilla aleatoria para reproducibilidad |

---

## 6. Tabla Parametros

### Ubicacion y Estructura

La tabla Parametros es la fuente centralizada de configuracion del pipeline.

| Atributo | Valor |
|----------|-------|
| Ubicacion | `control.lab1.Parametros` |
| Tipo | Delta Table (Unity Catalog Managed) |
| Columnas | `Clave STRING NOT NULL`, `Valor STRING NOT NULL` |
| Registros | 15 claves de configuracion + 1 registro especial |

### Registros de Configuracion (15 claves)

| Clave | Valor por Defecto | Descripcion |
|-------|------------------|-------------|
| `catalogoBronce` | `bronce` | Catalogo UC de la capa Bronce |
| `esquemaBronce` | `lab1` | Esquema dentro del catalogo de bronce |
| `contenedorBronce` | `bronce` | Nombre del contenedor/directorio base |
| `TipoStorage` | `Volume` | Tipo de almacenamiento: `Volume` o `AmazonS3` |
| `catalogoVolume` | `bronce` | Catalogo del Volume UC (solo para TipoStorage=Volume) |
| `esquemaVolume` | `lab1` | Esquema del Volume UC (solo para TipoStorage=Volume) |
| `nombreVolume` | `datos_bronce` | Nombre del Volume UC |
| `bucketS3` | `` (vacio) | Nombre del bucket S3 (solo para TipoStorage=AmazonS3) |
| `prefijoS3` | `` (vacio) | Prefijo de rutas en S3 (solo para TipoStorage=AmazonS3) |
| `DirectorioBronce` | `archivos` | Subdirectorio base dentro del almacenamiento |
| `catalogoPlata` | `plata` | Catalogo UC de la capa Plata |
| `esquemaPlata` | `lab1` | Esquema dentro del catalogo de plata |
| `catalogoOro` | `oro` | Catalogo UC de la capa Oro |
| `esquemaOro` | `lab1` | Esquema dentro del catalogo de oro |
| `esquemaControl` | `lab1` | Esquema dentro del catalogo de control |

### Registro Especial

| Clave | Valor por Defecto | Descripcion |
|-------|------------------|-------------|
| `TiposTransaccionesLabBase` | `DATM,CATM,PGSL` | Tipos de transaccion ATM separados por coma. Usados en `LsdpOroClientes.py` para calcular metricas de comportamiento ATM. DATM=Deposito ATM, CATM=Retiro ATM, PGSL=Pago al Saldo. |

### Uso en el Pipeline

La tabla Parametros es leida al inicio de cada script de transformacion mediante la
utilidad `LsdpConexionParametros.obtener_parametros()`:

```python
from utilities.LsdpConexionParametros import obtener_parametros

diccionario_parametros = obtener_parametros(
    spark, catalogo_parametro, esquema_parametro, tabla_parametros
)

# Extraccion de valores individuales con valor por defecto
catalogo_plata = diccionario_parametros.get("catalogoPlata", "plata")
esquema_plata  = diccionario_parametros.get("esquemaPlata",  "lab1")
```

El resultado es un diccionario Python `{Clave: Valor}` que permite acceder a cualquier
parametro con `.get()` y un valor por defecto de seguridad.

---

## 7. Dependencias

### Plataforma

| Componente | Requerimiento |
|------------|---------------|
| Databricks | Free Edition (Serverless Compute obligatorio) |
| Unity Catalog | Habilitado en el workspace de Databricks |
| Databricks Runtime | 14.x o superior (soporte de `pyspark.pipelines`) |
| Compute | Serverless (sin Classic ni Job Cluster para el pipeline LSDP) |

### Unity Catalog — Catalogos y Esquemas

El proyecto usa 4 catalogos con un esquema `lab1` en cada uno:

| Catalogo | Esquema | Contenido |
|----------|---------|-----------|
| `control` | `lab1` | Tabla Parametros (`control.lab1.Parametros`) |
| `bronce` | `lab1` | Streaming tables: `cmstfl`, `trxpfl`, `blncfl` |
| `plata` | `lab1` | Vistas materializadas: `clientes_saldos_consolidados`, `transacciones_enriquecidas` |
| `oro` | `lab1` | Vistas materializadas: `comportamiento_atm_cliente`, `resumen_integral_cliente` |

Los catalogos y esquemas se crean automaticamente ejecutando `conf/NbConfiguracionInicial.py`.

### Almacenamiento (TipoStorage)

El proyecto soporta dos modos de almacenamiento, configurados via la clave `TipoStorage`
en la tabla Parametros:

**Mode Volume (por defecto)**:

| Componente | Valor |
|------------|-------|
| Tipo | Unity Catalog Managed Volume |
| Ruta base | `/Volumes/bronce/lab1/datos_bronce/` |
| Catalogo | `bronce` |
| Esquema | `lab1` |
| Nombre | `datos_bronce` |

El Volume se crea automaticamente al ejecutar `conf/NbConfiguracionInicial.py`.

**Mode AmazonS3 (opcional)**:

| Componente | Descripcion |
|------------|-------------|
| `bucketS3` | Nombre del bucket S3 (sin `s3://`) |
| `prefijoS3` | Subfijo base dentro del bucket |
| Ruta base generada | `s3://{bucketS3}/{prefijoS3}/` |

Para usar Amazon S3, actualizar `TipoStorage=AmazonS3`, `bucketS3` y `prefijoS3` en la
tabla Parametros. `LsdpConstructorRutas` detecta el modo y construye la ruta adecuada
automaticamente.

### Utilidades del Proyecto

| Modulo | Ubicacion | Funcion |
|--------|-----------|---------|
| `LsdpConexionParametros` | `utilities/` | Lee la tabla Parametros y retorna un diccionario `{Clave: Valor}` |
| `LsdpConstructorRutas` | `utilities/` | Construye rutas absolutas (Volume o S3) a partir de rutas relativas y la tabla Parametros |
| `LsdpReordenarColumnasLiquidCluster` | `utilities/` | Reordena columnas del DataFrame para colocar los campos del Liquid Cluster en las primeras posiciones del schema |
| `LsdpInsertarTiposTransaccion` | `utilities/` | Inserta los tipos de transaccion en la tabla Parametros al crear los datos de prueba |

### Extensiones VS Code Recomendadas

| Extension | Uso |
|-----------|-----|
| Databricks Extension | Conexion al workspace, ejecucion de notebooks, despliegue de pipelines |
| Driver for SQLTools (Databricks) | Consultas SQL interactivas sobre las tablas Unity Catalog |
