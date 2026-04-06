# Contrato: Script de Oro — Vistas Materializadas LSDP

**Feature**: 005-lsdp-oro-vistas
**Fecha**: 2026-04-05
**Tipo**: Notebook Databricks (script de transformacion LSDP)

## Descripcion General

Este contrato define la interfaz publica del script de transformacion de oro que crea dos vistas materializadas en el pipeline LSDP. El script sigue el patron Closure autocontenido y es ejecutado exclusivamente por el motor LSDP (no se importa ni invoca directamente).

---

## 1. LsdpOroClientes.py

**Ubicacion**: `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py`
**Formato**: Notebook Databricks (`.py` con `# COMMAND ----------` y `# MAGIC %md`)
**Ejecutado por**: Motor LSDP (pipeline declarativo)

### Parametros del Pipeline Requeridos

| Parametro | Tipo | Fuente | Ejemplo |
|-----------|------|--------|---------|
| catalogoParametro | string | `spark.conf.get("pipelines.parameters.catalogoParametro")` | control |
| esquemaParametro | string | `spark.conf.get("pipelines.parameters.esquemaParametro")` | lab1 |
| tablaParametros | string | `spark.conf.get("pipelines.parameters.tablaParametros")` | Parametros |

### Claves de Tabla Parametros Requeridas

| Clave | Tipo | Ejemplo | Valor por defecto | Uso |
|-------|------|---------|-------------------|-----|
| catalogoOro | string | oro | oro | Catalogo UC destino para las vistas materializadas de oro |
| esquemaOro | string | lab1 | lab1 | Esquema UC destino para las vistas materializadas de oro |
| catalogoPlata | string | plata | plata | Catalogo UC para leer vistas materializadas de plata |
| esquemaPlata | string | lab1 | lab1 | Esquema UC para leer vistas materializadas de plata |
| TiposTransaccionesLabBase | string | DATM,CATM,PGSL | DATM,CATM,PGSL | Tipos de transaccion separados por coma para metricas ATM |

### Dependencias (imports)

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from utilities.LsdpConexionParametros import obtener_parametros
```

### Tipos de Transaccion (desde Tabla Parametros)

Los tipos de transaccion se leen de la tabla Parametros con la clave `TiposTransaccionesLabBase`. El valor es una cadena separada por comas (ej: `DATM,CATM,PGSL`) que se parsea con `.split(",")` para obtener la lista de tipos.

```python
tipos_transaccion_str = parametros.get("TiposTransaccionesLabBase", "DATM,CATM,PGSL")
tipos_transaccion = tipos_transaccion_str.split(",")
TIPO_DEPOSITO_ATM = tipos_transaccion[0]   # DATM
TIPO_RETIRO_ATM = tipos_transaccion[1]     # CATM
TIPO_PAGO_SALDO = tipos_transaccion[2]     # PGSL
```

La clave `TiposTransaccionesLabBase` es insertada de forma idempotente por la utilidad `LsdpInsertarTiposTransaccion` desde el notebook que orquesta el pipeline LSDP.

### Vista Materializada 1: comportamiento_atm_cliente

**Decorador**:
```python
@dp.materialized_view(
    name="comportamiento_atm_cliente",
    catalog=catalogo_oro,       # variable de closure
    schema=esquema_oro,         # variable de closure
    table_properties=propiedades_delta_oro,
    cluster_by=["identificador_cliente"],
)
```

**Funcion**: `def comportamiento_atm_cliente() -> DataFrame`

**Entrada**: `spark.read.table(f"{catalogo_plata}.{esquema_plata}.transacciones_enriquecidas")`

**Salida**: DataFrame con 6 columnas:

| Columna | Tipo |
|---------|------|
| identificador_cliente | LongType |
| cantidad_depositos_atm | LongType |
| cantidad_retiros_atm | LongType |
| promedio_monto_depositos_atm | DoubleType |
| promedio_monto_retiros_atm | DoubleType |
| total_pagos_saldo_cliente | DoubleType |

**Logica**:
1. Lee `transacciones_enriquecidas` con nombre completo de 3 partes (cross-catalog).
2. `groupBy("identificador_cliente")` con agregaciones condicionales:
   - `F.count(F.when(tipo == DATM, monto_principal))` para cantidad depositos.
   - `F.count(F.when(tipo == CATM, monto_principal))` para cantidad retiros.
   - `F.avg(F.when(tipo == DATM, monto_principal))` para promedio depositos.
   - `F.avg(F.when(tipo == CATM, monto_principal))` para promedio retiros.
   - `F.sum(F.when(tipo == PGSL, monto_principal))` para total pagos saldo.
3. Aplica `F.coalesce` con defaults (0 para conteos, 0.0 para promedios/sumas).
4. Retorna DataFrame final.

---

### Vista Materializada 2: resumen_integral_cliente

**Decorador**:
```python
@dp.materialized_view(
    name="resumen_integral_cliente",
    catalog=catalogo_oro,       # variable de closure
    schema=esquema_oro,         # variable de closure
    table_properties=propiedades_delta_oro,
    cluster_by=["huella_identificacion_cliente", "identificador_cliente"],
)
```

**Funcion**: `def resumen_integral_cliente() -> DataFrame`

**Entradas**:
- `spark.read.table(f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados")` — cross-catalog, nombre completo de 3 partes
- `spark.read.table(f"{catalogo_oro}.{esquema_oro}.comportamiento_atm_cliente")` — nombre completo de 3 partes (sin excepciones)

**Salida**: DataFrame con 22 columnas (ver data-model.md para detalle)

**Logica**:
1. Lee `clientes_saldos_consolidados` con nombre completo de 3 partes.
2. Lee `comportamiento_atm_cliente` con nombre completo de 3 partes.
3. INNER JOIN por `identificador_cliente`. Solo los clientes presentes en ambas fuentes se incluyen.
4. Selecciona 17 columnas de plata + 5 metricas de oro.
5. Retorna DataFrame final con 22 columnas.

---

---

## 2. LsdpInsertarTiposTransaccion.py

**Ubicacion**: `src/LSDP_Laboratorio_Basico/utilities/LsdpInsertarTiposTransaccion.py`
**Formato**: Notebook Databricks (`.py` con `# COMMAND ----------` y `# MAGIC %md`)
**Invocado desde**: Notebook que orquesta el pipeline LSDP (no desde el script de transformacion)

### Funcion Publica

```python
def insertar_tipos_transaccion(spark, catalogo: str, esquema: str, tabla_parametros: str) -> None
```

**Parametros**:
| Parametro | Tipo | Descripcion |
|-----------|------|-------------|
| spark | SparkSession | Sesion de Spark activa |
| catalogo | str | Catalogo UC donde reside la tabla Parametros |
| esquema | str | Esquema UC donde reside la tabla Parametros |
| tabla_parametros | str | Nombre de la tabla Parametros |

**Comportamiento**:
1. Consulta si la clave `TiposTransaccionesLabBase` ya existe en la tabla Parametros.
2. Si NO existe, ejecuta INSERT con el valor `DATM,CATM,PGSL`.
3. Si YA existe, no realiza ninguna operacion (idempotente).
4. Imprime el resultado de la operacion para observabilidad.

**Requiere TDD**: Si — cobertura para caso de clave inexistente (inserta) y clave existente (no opera).

---

### Observabilidad

El script imprime al inicio del modulo:
- Parametros del pipeline: `catalogoParametro`, `esquemaParametro`, `tablaParametros`.
- Valores de tabla Parametros: `catalogoOro`, `esquemaOro`, `catalogoPlata`, `esquemaPlata`.
- Nombres de las vistas materializadas a crear.
- Campos del liquid cluster de cada vista.
- Tipos de transaccion leidos de la tabla Parametros (clave `TiposTransaccionesLabBase`).
- Nombres de las 5 metricas calculadas.
- Nombres de las 22 columnas del resumen integral.

### Compatibilidad

- 100% compatible con Databricks Serverless Compute.
- Sin `sparkContext`, sin RDD, sin `.cache()`, sin `.persist()`.
- Patron Closure: parametros calculados a nivel de modulo, capturados por closure en funciones decoradas.
- Sin `import dlt`. Solo `from pyspark import pipelines as dp`.
- Sin ZOrder ni PartitionBy. Solo Liquid Cluster via `cluster_by`.
- Sin filtros previos sobre DataFrame en `comportamiento_atm_cliente`.
