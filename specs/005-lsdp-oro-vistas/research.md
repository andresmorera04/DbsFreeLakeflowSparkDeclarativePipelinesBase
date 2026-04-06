# Research: Incremento 5 - LSDP Medalla de Oro - Vistas Materializadas

**Feature**: 005-lsdp-oro-vistas
**Fecha**: 2026-04-05

## Tareas de Investigacion

### 1. Agregacion Condicional con `groupBy` en Vistas Materializadas LSDP

**Estado**: APROBADA POR EL USUARIO (2026-04-05)

**Pregunta**: Como implementar metricas de agregacion condicional (count/avg/sum por tipo de transaccion) dentro de una vista materializada LSDP sin aplicar filtros previos sobre el DataFrame?

**Hallazgos**:
- PySpark permite combinar `F.when()` dentro de funciones de agregacion: `F.count(F.when(condicion, valor))`, `F.sum(F.when(condicion, valor))`, `F.avg(F.when(condicion, valor))`.
- `F.count(F.when(...))` cuenta solo las filas que cumplen la condicion. Las que no cumplen retornan `None` y `count` las ignora (solo cuenta no-nulos).
- `F.avg(F.when(...))` calcula el promedio solo sobre los valores no-nulos que cumplen la condicion.
- `F.sum(F.when(...))` suma solo los valores que cumplen la condicion; las filas sin match aportan `None` (ignorado por `sum`).
- Al usar `groupBy("identificador_cliente")` con multiples agregaciones condicionales, todos los clientes con transacciones aparecen en el resultado, incluso aquellos sin transacciones ATM (sus metricas seran `None` hasta aplicar `coalesce`).
- Este patron es compatible con Serverless Compute y no requiere `sparkContext`.
- El patron NO filtra el DataFrame antes de agrupar, lo que permite a LSDP optimizar la estrategia de carga automaticamente (carga incremental vs completa).

**Decision**: Usar `groupBy("identificador_cliente")` con `F.count(F.when(...))`, `F.avg(F.when(...))` y `F.sum(F.when(...))` para calcular las 5 metricas en una sola operacion de agregacion.
**Justificacion**: Patron nativo PySpark, eficiente (un solo paso de agregacion), compatible con Serverless, no aplica filtros previos (respeta RF-027) y produce resultados para todos los clientes con transacciones.
**Alternativas Consideradas**: Filtrar DataFrame por tipo y luego agrupar — rechazado porque aplicaria filtros previos que impiden carga incremental. Multiples DataFrames separados con join posterior — rechazado por ineficiencia (multiples scans de la misma tabla).

---

### 2. Manejo de Valores Nulos con `F.coalesce` en Metricas

**Estado**: APROBADA POR EL USUARIO (2026-04-05)

**Pregunta**: Como garantizar que las metricas ATM sean 0 o 0.0 en lugar de NULL cuando un cliente no tiene transacciones de un tipo especifico?

**Hallazgos**:
- `F.coalesce(columna, F.lit(valor_defecto))` retorna el primer valor no-nulo entre sus argumentos.
- Despues de la agregacion condicional, si un cliente no tiene transacciones DATM, la metrica `cantidad_depositos_atm` sera `None`. Con `F.coalesce(F.col("cantidad_depositos_atm"), F.lit(0))` se convierte a `0`.
- Para cantidades (count): valor por defecto `F.lit(0)` con tipo `LongType`.
- Para promedios (avg) y totales (sum): valor por defecto `F.lit(0.0)` con tipo `DoubleType`.
- Este patron se aplica en dos puntos:
  1. En `comportamiento_atm_cliente`: despues del `groupBy().agg()`, aplicar `F.coalesce` a cada metrica.
  2. En `resumen_integral_cliente`: despues del LEFT JOIN, aplicar `F.coalesce` a las 5 columnas de metricas ATM para clientes sin contraparte en `comportamiento_atm_cliente`.
- `F.coalesce` es una funcion nativa PySpark 100% compatible con Serverless.

**Decision**: Aplicar `F.coalesce` con literales por defecto (0 para conteos, 0.0 para promedios y sumas) en ambas vistas materializadas.
**Justificacion**: Garantiza que el area de negocio nunca vea NULL en las metricas, facilitando el analisis de correlacion ATM vs pagos al saldo. Es la practica estandar en PySpark.
**Alternativas Consideradas**: `F.fillna()` despues del agg — funcional pero menos preciso porque aplica a todas las columnas o requiere especificar un diccionario. `F.when(F.col(...).isNull(), 0).otherwise(F.col(...))` — mas verboso, equivalente funcional a `coalesce`.

---

### 3. INNER JOIN entre Vistas Materializadas de Diferentes Capas (Plata + Oro)

**Estado**: MODIFICADA POR EL USUARIO (2026-04-05) — Se cambia LEFT JOIN a INNER JOIN. Todas las lecturas de tablas/vistas usan nombre de 3 partes sin excepcion.

**Pregunta**: Como implementar un INNER JOIN entre una vista materializada de plata y otra de oro dentro del mismo pipeline LSDP?

**Hallazgos**:
- La vista `clientes_saldos_consolidados` reside en `plata.lab1` (catalogo diferente al de oro). Para leerla desde un script de oro se usa `spark.read.table("catalogo_plata.esquema_plata.clientes_saldos_consolidados")` con nombre completo de tres partes.
- La vista `comportamiento_atm_cliente` reside en `oro.lab1`. Se lee con nombre completo de tres partes: `spark.read.table("catalogo_oro.esquema_oro.comportamiento_atm_cliente")`.
- LSDP detecta automaticamente las dependencias entre tablas/vistas del pipeline y ejecuta en el orden correcto (`comportamiento_atm_cliente` se crea antes que `resumen_integral_cliente`).
- El INNER JOIN en PySpark: `df_plata.join(df_oro, on="identificador_cliente", how="inner")`.
- Con `how="inner"`, solo los clientes que existen en ambas fuentes se incluyen. Los clientes de plata sin ninguna transaccion no aparecen en el resumen.

**Decision**: Leer todas las tablas y vistas con nombre completo de 3 partes sin excepcion. Usar `df_plata.join(df_oro, on="identificador_cliente", how="inner")`.
**Justificacion**: Decision del usuario: INNER JOIN asegura que solo los clientes con actividad transaccional aparezcan en el resumen integral. El nombre de 3 partes garantiza claridad y consistencia en todas las lecturas cross-catalog e intra-catalog.
**Alternativas Consideradas**: LEFT JOIN — propuesta original rechazada por el usuario. FULL OUTER JOIN — rechazado porque incluiria clientes que solo existen en oro.

---

### 4. Parametros `catalog` y `schema` en el Decorador `@dp.materialized_view`

**Estado**: APROBADA POR EL USUARIO (2026-04-05)

**Pregunta**: Como hacer que las vistas de oro se creen en `oro.lab1` cuando el catalogo por defecto del pipeline es `bronce.lab1`?

**Hallazgos**:
- El decorador `@dp.materialized_view` acepta los parametros `catalog` y `schema` para especificar un catalogo y esquema diferente al por defecto del pipeline.
- Uso: `@dp.materialized_view(name="nombre", catalog="oro", schema="lab1", table_properties={...}, cluster_by=[...])`.
- Este patron ya fue usado exitosamente en los scripts de plata del Incremento 4 para crear vistas en `plata.lab1`.
- Los valores de `catalog` y `schema` se obtienen de la tabla Parametros (claves `catalogoOro` y `esquemaOro`) y se capturan por closure.
- El pipeline LSDP debe tener permisos para crear tablas en el catalogo destino.

**Decision**: Usar `catalog=catalogo_oro` y `schema=esquema_oro` en el decorador `@dp.materialized_view`, donde `catalogo_oro` y `esquema_oro` son variables de closure leidas de la tabla Parametros.
**Justificacion**: Patron identico al usado en plata (Inc. 4), probado y funcional. Completamente dinamico via tabla Parametros.
**Alternativas Consideradas**: Especificar el catalogo de oro como default del pipeline — rechazado porque el pipeline ya tiene `bronce` como default y se necesitan crear tablas en bronce, plata y oro.

---

### 5. Propiedades Delta y Liquid Cluster en Vistas Materializadas de Oro

**Estado**: APROBADA POR EL USUARIO (2026-04-05)

**Pregunta**: Que propiedades Delta y configuracion de liquid cluster aplicar a las vistas materializadas de oro?

**Hallazgos**:
- Las mismas propiedades Delta usadas en bronce y plata aplican a oro (requisito de constitution):
  ```python
  propiedades_delta_oro = {
      "delta.enableChangeDataFeed": "true",
      "delta.autoOptimize.autoCompact": "true",
      "delta.autoOptimize.optimizeWrite": "true",
      "delta.deletedFileRetentionDuration": "interval 30 days",
      "delta.logRetentionDuration": "interval 60 days",
  }
  ```
- Liquid cluster para `comportamiento_atm_cliente`: `cluster_by=["identificador_cliente"]` — esta es la unica dimension de agrupacion y la columna de consulta mas frecuente.
- Liquid cluster para `resumen_integral_cliente`: `cluster_by=["huella_identificacion_cliente", "identificador_cliente"]` — consistente con la vista de plata `clientes_saldos_consolidados` y optimiza consultas por ambos criterios.
- Las propiedades se pasan como diccionario en `table_properties` del decorador.

**Decision**: Usar el mismo diccionario de propiedades Delta para ambas vistas. Liquid cluster segun los campos definidos en la spec (RF-009 y RF-010).
**Justificacion**: Consistencia con los estandares del proyecto establecidos en incrementos anteriores. Los campos de liquid cluster optimizan las consultas mas frecuentes del area de negocio.
**Alternativas Consideradas**: Propiedades Delta diferentes por vista — rechazado por falta de justificacion tecnica. Liquid cluster solo en `identificador_cliente` para ambas — rechazado porque `resumen_integral_cliente` se consulta tambien por `huella_identificacion_cliente`.

---

### 6. Tipos de Transaccion desde Tabla Parametros con Utilidad Idempotente

**Estado**: MODIFICADA POR EL USUARIO (2026-04-05) — Los tipos de transaccion se almacenan en la tabla Parametros, no como constantes de modulo.

**Pregunta**: Como manejar los tipos de transaccion (DATM, CATM, PGSL) en el script de oro de forma dinamica y centralizada?

**Hallazgos**:
- El usuario decide que los tipos de transaccion se almacenen en la tabla Parametros con la clave `TiposTransaccionesLabBase` y valor separado por comas: `DATM,CATM,PGSL`.
- Se requiere una nueva utilidad en `utilities/` llamada `LsdpInsertarTiposTransaccion` que:
  - Recibe como parametros: sesion de spark, catalogo, esquema y nombre de la tabla Parametros.
  - Valida si la clave `TiposTransaccionesLabBase` ya existe en la tabla.
  - Si NO existe, realiza el INSERT con el valor `DATM,CATM,PGSL`.
  - Si YA existe, no hace nada (idempotente).
  - Imprime el resultado de la operacion para observabilidad.
- La utilidad se invoca desde el notebook que ejecuta el pipeline LSDP (no desde el script de transformacion).
- El script de oro lee la clave `TiposTransaccionesLabBase` del diccionario de parametros y la parsea con `.split(",")` para obtener la lista de tipos.
- Esta utilidad requiere cobertura TDD.

**Decision**: Almacenar los tipos de transaccion en la tabla Parametros con clave `TiposTransaccionesLabBase`. Crear utilidad `LsdpInsertarTiposTransaccion` idempotente en `utilities/`. Invocar desde el notebook LSDP. Parsear con `split(",")` en el script de oro.
**Justificacion**: Decision del usuario. Centraliza la configuracion en la tabla Parametros (principio V de la constitution). Permite modificar los tipos de transaccion sin tocar codigo. La idempotencia garantiza que multiples ejecuciones no dupliquen el registro.
**Alternativas Consideradas**: Constantes de modulo al inicio del script — propuesta original rechazada por el usuario. Diccionario de mapeo hardcodeado — rechazado igualmente.
