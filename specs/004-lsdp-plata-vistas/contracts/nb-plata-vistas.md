# Contrato: Scripts de Plata — Vistas Materializadas LSDP

**Feature**: 004-lsdp-plata-vistas
**Fecha**: 2026-04-05
**Tipo**: Notebooks Databricks (scripts de transformacion LSDP)

## Descripcion General

Este contrato define la interfaz publica de los dos scripts de transformacion de plata que crean vistas materializadas en el pipeline LSDP. Ambos scripts siguen el patron Closure autocontenido y son ejecutados exclusivamente por el motor LSDP (no se importan ni invocan directamente).

---

## 1. LsdpPlataClientesSaldos.py

**Ubicacion**: `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py`
**Formato**: Notebook Databricks (`.py` con `# COMMAND ----------` y `# MAGIC %md`)
**Ejecutado por**: Motor LSDP (pipeline declarativo)

### Parametros del Pipeline Requeridos

| Parametro | Tipo | Fuente | Ejemplo |
|-----------|------|--------|---------|
| catalogoParametro | string | `spark.conf.get("pipelines.parameters.catalogoParametro")` | control |
| esquemaParametro | string | `spark.conf.get("pipelines.parameters.esquemaParametro")` | regional |
| tablaParametros | string | `spark.conf.get("pipelines.parameters.tablaParametros")` | Parametros |

### Claves de Tabla Parametros Requeridas

| Clave | Tipo | Ejemplo | Uso |
|-------|------|---------|-----|
| catalogoPlata | string | plata | Catalogo UC destino para las vistas materializadas de plata |
| esquemaPlata | string | regional | Esquema UC destino para las vistas materializadas de plata |

### Dependencias (imports)

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import DataFrame
from utilities.LsdpConexionParametros import obtener_parametros
from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster
```

### Artefacto Producido

| Atributo | Valor |
|----------|-------|
| Tipo | Vista materializada (`@dp.materialized_view`) |
| Nombre | `clientes_saldos_consolidados` |
| Catalogo/Esquema | `plata.regional` (gestionado por LSDP via configuracion del pipeline) |
| Columnas | 173 (ver data-model.md para mapeo completo) |
| Liquid Cluster | `huella_identificacion_cliente`, `identificador_cliente` |
| Propiedades Delta | `delta.enableChangeDataFeed=true`, `delta.autoOptimize.autoCompact=true`, `delta.autoOptimize.optimizeWrite=true`, `delta.deletedFileRetentionDuration=interval 30 days`, `delta.logRetentionDuration=interval 60 days` |

### Expectativas de Calidad (`@dp.expect`)

| Nombre | Condicion |
|--------|-----------|
| limite_credito_positivo | `limite_credito > 0` |
| identificador_cliente_no_nulo | `identificador_cliente IS NOT NULL` |
| limite_credito_no_nulo | `limite_credito IS NOT NULL` |
| fecha_apertura_cuenta_valida | `fecha_apertura_cuenta > '2020-12-31'` |
| fecha_nacimiento_valida | `fecha_nacimiento < '2009-01-01'` |

### Flujo de Ejecucion

```text
1. [Nivel de modulo] Leer parametros del pipeline via spark.conf.get()
2. [Nivel de modulo] Invocar obtener_parametros() para leer tabla Parametros
3. [Nivel de modulo] Extraer catalogoPlata y esquemaPlata de la tabla Parametros
4. [Nivel de modulo] Definir diccionarios de mapeo de columnas (cmstfl y blncfl)
5. [Nivel de modulo] Imprimir todos los parametros y configuracion
6. [Funcion decorada] Leer cmstfl via spark.read.table("cmstfl")
7. [Funcion decorada] Aplicar Dimension Tipo 1 a cmstfl (Window + ROW_NUMBER)
8. [Funcion decorada] Leer blncfl via spark.read.table("blncfl")
9. [Funcion decorada] Aplicar Dimension Tipo 1 a blncfl (Window + ROW_NUMBER)
10. [Funcion decorada] LEFT JOIN por CUSTID
11. [Funcion decorada] Seleccionar y renombrar columnas (excluyendo FechaIngestaDatos, _rescued_data, año, mes, dia)
12. [Funcion decorada] Agregar 4 campos calculados
13. [Funcion decorada] Reordenar columnas del liquid cluster a posiciones iniciales
14. [Funcion decorada] Imprimir resumen y retornar DataFrame
```

### Observabilidad (prints esperados)

- Inicio: parametros del pipeline, catalogo/esquema plata destino (de Parametros)
- Procesamiento: cantidad columnas cmstfl, cantidad columnas blncfl, columnas comunes detectadas
- Campos calculados: nombres de los 4 campos, columnas del liquid cluster
- Fin: cantidad total de columnas en la vista, confirmacion de creacion exitosa

---

## 2. LsdpPlataTransacciones.py

**Ubicacion**: `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py`
**Formato**: Notebook Databricks (`.py` con `# COMMAND ----------` y `# MAGIC %md`)
**Ejecutado por**: Motor LSDP (pipeline declarativo)

### Parametros del Pipeline Requeridos

Identicos a `LsdpPlataClientesSaldos.py` (3 parametros del pipeline + 2 claves de tabla Parametros).

### Claves de Tabla Parametros Requeridas

Identicas a `LsdpPlataClientesSaldos.py`: `catalogoPlata` y `esquemaPlata`.

### Dependencias (imports)

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from utilities.LsdpConexionParametros import obtener_parametros
from utilities.LsdpReordenarColumnasLiquidCluster import reordenar_columnas_liquid_cluster
```

> Nota: NO importa `Window` porque no aplica Dimension Tipo 1.

### Artefacto Producido

| Atributo | Valor |
|----------|-------|
| Tipo | Vista materializada (`@dp.materialized_view`) |
| Nombre | `transacciones_enriquecidas` |
| Catalogo/Esquema | `plata.regional` (gestionado por LSDP via configuracion del pipeline) |
| Columnas | 64 (ver data-model.md para mapeo completo) |
| Liquid Cluster | `fecha_transaccion`, `identificador_cliente`, `tipo_transaccion` |
| Propiedades Delta | Identicas a `clientes_saldos_consolidados` |

### Expectativas de Calidad (`@dp.expect`)

| Nombre | Condicion |
|--------|-----------|
| moneda_transaccion_no_nula | `moneda_transaccion IS NOT NULL` |
| monto_neto_no_nulo | `monto_neto IS NOT NULL` |
| monto_neto_positivo | `monto_neto > 0` |
| identificador_cliente_no_nulo | `identificador_cliente IS NOT NULL` |

### Flujo de Ejecucion

```text
1. [Nivel de modulo] Leer parametros del pipeline via spark.conf.get()
2. [Nivel de modulo] Invocar obtener_parametros() para leer tabla Parametros
3. [Nivel de modulo] Extraer catalogoPlata y esquemaPlata de la tabla Parametros
4. [Nivel de modulo] Definir diccionario de mapeo de columnas (trxpfl)
5. [Nivel de modulo] Imprimir todos los parametros y configuracion
6. [Funcion decorada] Leer trxpfl via spark.read.table("trxpfl") SIN FILTROS
7. [Funcion decorada] Seleccionar y renombrar columnas (excluyendo FechaIngestaDatos, _rescued_data, año, mes, dia)
8. [Funcion decorada] Agregar 4 campos calculados numericos
9. [Funcion decorada] Reordenar columnas del liquid cluster a posiciones iniciales
10. [Funcion decorada] Imprimir resumen y retornar DataFrame
```

### Restriccion Critica — Carga Incremental

El script NO DEBE usar:
- `.filter()`, `.where()` sobre el DataFrame fuente
- `F.current_timestamp()`, `F.current_date()` en columnas calculadas
- `groupBy()`, `agg()`, `distinct()` sobre el DataFrame fuente
- JOINs con tablas externas al pipeline

Estas operaciones forzarian refrescamiento completo, anulando la carga incremental automatica de LSDP.

### Observabilidad (prints esperados)

- Inicio: parametros del pipeline, catalogo/esquema plata destino (de Parametros)
- Procesamiento: cantidad columnas trxpfl, confirmacion de 0 filtros aplicados
- Campos calculados: nombres de los 4 campos numericos, columnas del liquid cluster
- Fin: cantidad total de columnas en la vista, confirmacion de creacion exitosa

---

## Contrato Comun — Patron Closure

Ambos scripts siguen el patron Closure autocontenido:

```python
# --- Nivel de modulo (se ejecuta al importar el script) ---
catalogo_parametro = spark.conf.get("pipelines.parameters.catalogoParametro")
esquema_parametro = spark.conf.get("pipelines.parameters.esquemaParametro")
tabla_parametros = spark.conf.get("pipelines.parameters.tablaParametros")

diccionario_parametros = obtener_parametros(spark, catalogo_parametro, esquema_parametro, tabla_parametros)
catalogo_plata = diccionario_parametros["catalogoPlata"]
esquema_plata = diccionario_parametros["esquemaPlata"]

# --- Funcion decorada (captura variables de modulo por closure) ---
@dp.materialized_view(name=f"{catalogo_plata}.{esquema_plata}.nombre_vista", table_properties={...}, cluster_by=[...])
@dp.expect("...", "...")
def vista_plata():
    df = spark.read.table("tabla_bronce")  # nombre simple, resuelto por pipeline default
    # ... transformaciones ...
    return df_transformado
```

**Invariante**: Las variables de modulo (`catalogo_parametro`, `diccionario_parametros`, `catalogo_plata`, `esquema_plata`, diccionarios de mapeo) se calculan una sola vez y quedan capturadas por closure en la funcion decorada. `cloudpickle` serializa automaticamente.
