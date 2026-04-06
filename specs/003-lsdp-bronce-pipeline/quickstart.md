# Guia Rapida: Incremento 3 - LSDP Medalla de Bronce

**Feature**: 003-lsdp-bronce-pipeline
**Fecha**: 2026-04-04

## Prerequisitos

1. **Incremento 1 completado**: Tabla `Parametros` creada en Unity Catalog con los 11 registros de configuracion. Catalogos `bronce`, `control`, `plata`, `oro` y esquemas `regional` existentes. Volume `datos_bronce` creado.

2. **Incremento 2 completado**: Parquets AS400 (CMSTFL, TRXPFL, BLNCFL) generados y disponibles en la ruta configurada (Volume o S3).

3. **Extensiones VS Code instaladas**: Databricks Extension for Visual Studio Code y Databricks Driver for SQLTools configuradas y conectadas al workspace de Databricks Free Edition.

4. **Workspace Databricks Free Edition activo**: Unity Catalog habilitado, Serverless Compute disponible.

## Estructura de Archivos del Incremento

```text
src/LSDP_Laboratorio_Basico/
├── utilities/                              # Python puro (NO notebooks)
│   ├── LsdpConexionParametros.py
│   ├── LsdpConstructorRutas.py
│   └── LsdpReordenarColumnasLiquidCluster.py
├── transformations/
│   ├── LsdpBronceCmstfl.py
│   ├── LsdpBronceTrxpfl.py
│   └── LsdpBronceBlncfl.py
└── explorations/
    └── LSDP_Laboratorio_Basico/
        └── NbTddBroncePipeline.py
```

## Paso 1: Ejecutar las Pruebas TDD

Antes de desplegar el pipeline, ejecutar las pruebas TDD desde VS Code:

1. Abrir el archivo `src/LSDP_Laboratorio_Basico/explorations/LSDP_Laboratorio_Basico/NbTddBroncePipeline.py`
2. Click derecho > **Run on Serverless Compute**
3. Verificar que todas las pruebas pasen sin errores

Las pruebas TDD validan EXCLUSIVAMENTE las utilidades de `utilities/` (Python puro importable):
- `LsdpConexionParametros`: lectura de la tabla Parametros y retorno del diccionario
- `LsdpConstructorRutas`: construccion de rutas para Volume y AmazonS3, excepcion para tipo invalido
- `LsdpReordenarColumnasLiquidCluster`: reordenamiento de columnas, preservacion del orden del resto, excepcion para campo inexistente

**NOTA**: Los notebooks de `transformations/` (`transformar_cmstfl`, `transformar_trxpfl`, `transformar_blncfl`) NO se incluyen en el TDD porque ejecutan codigo a nivel de modulo (patron Closure) que requiere un pipeline LSDP desplegado. Su validacion se realiza mediante el despliegue del pipeline.

## Paso 2: Configurar el Pipeline LSDP en Databricks

1. Ir a la UI de Databricks > **Workflows** > **Pipelines** > **Create pipeline**
2. Configurar:
   - **Pipeline name**: `LSDP_Laboratorio_Basico_Bronce` (o nombre deseado)
   - **Product Edition**: `Advanced`
   - **Pipeline mode**: `Triggered`
   - **Compute**: Serverless
   - **Catalog**: `bronce`
   - **Target schema**: `regional`
3. Agregar los archivos fuente:
   - `src/LSDP_Laboratorio_Basico/utilities/LsdpConexionParametros.py`
   - `src/LSDP_Laboratorio_Basico/utilities/LsdpConstructorRutas.py`
   - `src/LSDP_Laboratorio_Basico/utilities/LsdpReordenarColumnasLiquidCluster.py`
   - `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceCmstfl.py`
   - `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceTrxpfl.py`
   - `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceBlncfl.py`
4. Configurar los 9 parametros del pipeline:

| Parametro | Valor ejemplo |
|-----------|---------------|
| catalogoParametro | `control` |
| esquemaParametro | `regional` |
| tablaParametros | `Parametros` |
| rutaCompletaMaestroCliente | `LSDP_Base/As400/MaestroCliente/` |
| rutaCompletaTransaccional | `LSDP_Base/As400/Transaccional/` |
| rutaCompletaSaldoCliente | `LSDP_Base/As400/SaldoCliente/` |
| rutaSchemaLocationCmstfl | `LSDP_Base/SchemaLocation/Bronce/cmstfl/` |
| rutaSchemaLocationTrxpfl | `LSDP_Base/SchemaLocation/Bronce/trxpfl/` |
| rutaSchemaLocationBlncfl | `LSDP_Base/SchemaLocation/Bronce/blncfl/` |

## Paso 3: Ejecutar el Pipeline

1. Click en **Start** en la UI del pipeline
2. Monitorear la ejecucion en la vista de grafo del pipeline
3. Verificar en los logs que se impriman:
   - Parametros del pipeline leidos
   - Valores de la tabla Parametros
   - Rutas construidas para cada parquet
   - Confirmacion de reordenamiento exitoso por cada tabla

## Paso 4: Verificar los Resultados

### Verificar streaming tables creadas

```sql
-- Verificar existencia de las tres streaming tables
SHOW TABLES IN bronce.regional;

-- Verificar conteo de registros
SELECT COUNT(*) FROM bronce.regional.cmstfl;
SELECT COUNT(*) FROM bronce.regional.trxpfl;
SELECT COUNT(*) FROM bronce.regional.blncfl;
```

### Verificar esquema y orden de columnas

```sql
-- Verificar que FechaIngestaDatos y CUSTID son las primeras columnas de cmstfl
DESCRIBE bronce.regional.cmstfl;

-- Verificar que TRXDT, CUSTID, TRXTYP son las primeras columnas de trxpfl
DESCRIBE bronce.regional.trxpfl;

-- Verificar que FechaIngestaDatos y CUSTID son las primeras columnas de blncfl
DESCRIBE bronce.regional.blncfl;
```

### Verificar propiedades Delta

```sql
-- Verificar propiedades de cmstfl
SHOW TBLPROPERTIES bronce.regional.cmstfl;

-- Deben aparecer:
-- delta.enableChangeDataFeed = true
-- delta.autoOptimize.autoCompact = true
-- delta.autoOptimize.optimizeWrite = true
-- delta.deletedFileRetentionDuration = interval 30 days
-- delta.logRetentionDuration = interval 60 days
```

### Verificar idempotencia

1. Ejecutar el pipeline una segunda vez con los mismos parquets
2. Verificar que el conteo de registros sea identico al de la primera ejecucion (sin duplicados)

## Solucion de Problemas

| Problema | Causa probable | Solucion |
|----------|---------------|----------|
| `Table not found` al leer Parametros | Tabla Parametros no existe o parametros de catalogo/esquema incorrectos | Verificar Incremento 1 completo y parametros del pipeline |
| `ValueError: TipoStorage` | Valor de TipoStorage en tabla Parametros no es "Volume" ni "AmazonS3" | Verificar registros en tabla Parametros |
| `FileNotFoundException` en AutoLoader | Ruta de parquets no existe o Volume no accesible | Verificar Incremento 2 completo y rutas en parametros del pipeline |
| `AnalysisException: Column not found` | Campo del liquid cluster no existe en el parquet fuente | Verificar schema de los parquets AS400 generados |
| `NOT_SUPPORTED_WITH_SERVERLESS` | Uso de sparkContext, cache o persist | Revisar codigo — estas operaciones estan prohibidas |
