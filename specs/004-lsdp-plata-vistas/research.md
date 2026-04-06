# Research: Incremento 4 - LSDP Medalla de Plata - Vistas Materializadas

**Feature**: 004-lsdp-plata-vistas
**Fecha**: 2026-04-05

## Tareas de Investigacion

### 1. Decorador `@dp.materialized_view` — API y Parametros

**Pregunta**: Como se define una vista materializada con la API `pyspark.pipelines` y que parametros acepta el decorador?

**Hallazgos**:
- El decorador `@dp.materialized_view` funciona de manera analoga a `@dp.table`, pero crea una vista materializada en lugar de una streaming table.
- Acepta los mismos parametros clave que `@dp.table`: `name` (nombre de la vista), `table_properties` (diccionario de propiedades Delta), `cluster_by` (lista de columnas para liquid cluster).
- A diferencia de `@dp.table` (que usa `spark.readStream`), la funcion decorada con `@dp.materialized_view` retorna un DataFrame batch (usando `spark.read.table()` en lugar de `spark.readStream.table()`).
- Las vistas materializadas se recalculan completamente en cada ejecucion del pipeline, a menos que LSDP determine que puede optimizar con carga incremental (cuando no hay filtros ni funciones que fuercen refrescamiento completo).
- Se importa desde el mismo modulo: `from pyspark import pipelines as dp`.

**Decision**: Usar `@dp.materialized_view(name="<nombre>", table_properties={...}, cluster_by=[...])` con `spark.read.table()` para leer streaming tables de bronce.
**Justificacion**: Es la API oficial de LSDP para vistas materializadas. Sigue el mismo patron que `@dp.table` del Incremento 3, minimizando la curva de aprendizaje.
**Alternativas Consideradas**: No hay alternativas — `@dp.materialized_view` es el unico mecanismo declarativo de LSDP para vistas materializadas. La API legacy `dlt.create_table()` esta prohibida por constitution.

---

### 2. Expectativas de Calidad de Datos (`@dp.expect`)

**Pregunta**: Como se configuran expectativas de calidad de datos en modo observacional usando la API `pyspark.pipelines`?

**Hallazgos** (fuente: https://docs.databricks.com/aws/en/ldp/expectations.html):
- El decorador `@dp.expect("nombre", "condicion SQL")` registra violaciones en metricas pero conserva todos los registros (modo `warn` por defecto).
- `@dp.expect_or_drop("nombre", "condicion")` descartaria registros invalidos.
- `@dp.expect_or_fail("nombre", "condicion")` detendria el pipeline al detectar violaciones.
- Las condiciones usan sintaxis SQL estandar: comparaciones, IS NOT NULL, BETWEEN, funciones SQL como `year()`, `current_date()`.
- Se pueden apilar multiples decoradores `@dp.expect` sobre una misma funcion (uno por expectativa).
- Alternativa: `@dp.expect_all(dict)` acepta un diccionario `{"nombre": "condicion", ...}` para definir multiples expectativas en un solo decorador.
- Las expectativas funcionan en streaming tables, materialized views y temporary views.
- Las metricas se visualizan en la pestana "Data quality" de la UI del pipeline LSDP.
- Los nombres de expectativas deben ser unicos por dataset pero se pueden reutilizar entre datasets.

**Decision**: Usar multiples decoradores `@dp.expect("nombre", "condicion SQL")` apilados sobre cada funcion decorada con `@dp.materialized_view`. Modo observacional (warn) para todas las expectativas, segun decision de clarificacion.
**Justificacion**: El stacking de `@dp.expect` individuales es mas legible que `@dp.expect_all(dict)` cuando hay pocas expectativas (5 para consolidada, 4 para transaccional). Cada expectativa tiene su propio nombre descriptivo para la UI de metricas.
**Alternativas Consideradas**: `@dp.expect_all(dict)` — rechazado por menor legibilidad. `@dp.expect_or_drop` — rechazado en clarificacion: se requiere conservar todos los registros. `@dp.expect_or_fail` — rechazado: no se debe detener el pipeline por violaciones de calidad.

---

### 3. Lectura de Streaming Tables desde Vistas Materializadas

**Pregunta**: Como se leen las streaming tables de bronce desde scripts de plata en el paradigma declarativo de LSDP?

**Hallazgos**:
- Dentro de un pipeline LSDP, las tablas internas del pipeline se pueden referenciar directamente por nombre usando `spark.read.table("nombre_tabla")`.
- Para tablas en otros catalogos/esquemas, se usa el nombre completo de tres partes: `spark.read.table("catalogo.esquema.tabla")`.
- Las streaming tables de bronce (`cmstfl`, `blncfl`, `trxpfl`) se declaran dentro del mismo pipeline LSDP, por lo que desde los scripts de plata se leen por nombre simple: `spark.read.table("cmstfl")`. El pipeline resuelve automaticamente el catalogo y esquema por defecto asignado al pipeline.
- No se necesita construir nombres de tres partes para las tablas de bronce — el acceso por nombre simple es suficiente y evita hardcodear esquemas.
- `spark.read.table()` retorna un DataFrame batch, que es lo correcto para `@dp.materialized_view`.
- LSDP gestiona automaticamente las dependencias entre tablas del pipeline cuando se referencia por nombre.

**Decision**: Usar `spark.read.table("nombre_tabla")` (nombre simple) para leer cada streaming table de bronce. El pipeline resuelve el catalogo y esquema por defecto. Los catalogos y esquemas de plata para la vista destino se obtienen de la tabla Parametros (`catalogoPlata`, `esquemaPlata`).
**Justificacion**: Patron mas limpio y desacoplado — no requiere conocer ni construir el catalogo/esquema de bronce. LSDP detecta automaticamente las dependencias entre tablas del pipeline.
**Alternativas Consideradas**: `dp.read("nombre_tabla")` — investigado pero no confirmado como API estable. `spark.readStream.table()` — rechazado porque `@dp.materialized_view` trabaja con DataFrames batch, no streaming.

---

### 4. Dimension Tipo 1 con Window Functions en PySpark

**Pregunta**: Como implementar Dimension Tipo 1 (solo el registro mas reciente por cada CUSTID) usando funciones nativas de PySpark compatibles con Serverless?

**Hallazgos**:
- El patron estandar usa `Window` con `ROW_NUMBER()`:
  ```python
  from pyspark.sql import Window
  from pyspark.sql import functions as F
  
  ventana = Window.partitionBy("CUSTID").orderBy(F.col("FechaIngestaDatos").desc())
  df_con_numero = df.withColumn("numero_fila", F.row_number().over(ventana))
  df_reciente = df_con_numero.filter(F.col("numero_fila") == 1).drop("numero_fila")
  ```
- `Window`, `F.row_number()`, `F.col()` son funciones nativas de PySpark 100% compatibles con Serverless (no requieren `sparkContext`).
- Para desempate determinista (cuando multiples registros tienen el mismo `FechaIngestaDatos`), se agrega un criterio secundario de ordenamiento: CUSTID descendente.
- El filtro `.filter(F.col("numero_fila") == 1)` se aplica dentro de la funcion de transformacion, no como filtro sobre la fuente.

**Decision**: Usar `Window.partitionBy("CUSTID").orderBy(F.col("FechaIngestaDatos").desc(), F.col("CUSTID").desc())` con `ROW_NUMBER` para ambas tablas de bronce (cmstfl y blncfl) antes del LEFT JOIN. El desempate se resuelve con CUSTID descendente (determinista y estable).
**Justificacion**: Patron estandar, 100% nativo PySpark, compatible con Serverless, sin dependencias externas. El desempate por CUSTID es determinista porque CUSTID es unico por cliente.
**Alternativas Consideradas**: `F.max("FechaIngestaDatos").over(Window)` con self-join — rechazado por complejidad y doble scan. `LAST_VALUE` — rechazado por menor claridad semantica. Group by + max + join — rechazado por rendimiento inferior a Window + filter.

---

### 5. Mapeo de Columnas AS400 a Espanol Snake_Case

**Pregunta**: Como mapear las ~170 columnas AS400 (CMSTFL+BLNCFL) y ~60 columnas (TRXPFL) a nombres en espanol snake_case de forma mantenible?

**Hallazgos**:
- PySpark ofrece `.withColumnRenamed(old, new)` para renombrar columnas individuales.
- Para renombrar multiples columnas eficientemente, se puede usar `.select([F.col(old).alias(new) for old, new in mapeo.items()])`.
- Los diccionarios de mapeo se definen a nivel de modulo (capturados por closure) para evitar recreacion en cada ejecucion.
- El patron `.select()` con lista de alias es mas eficiente que encadenar 170+ `.withColumnRenamed()` porque genera un solo plan de ejecucion.
- Para las columnas a excluir (`_rescued_data`, `año`, `mes`, `dia`, `FechaIngestaDatos`), simplemente se omiten del mapeo/select.
- Para columnas opcionales (`año`, `mes`, `dia` que podrian no existir en bronce), se filtra el mapeo contra las columnas reales del DataFrame antes de aplicar el select.

**Decision**: Definir diccionarios de mapeo constantes a nivel de modulo para cada tabla de bronce. Usar `.select([F.col(old).alias(new) for old, new in mapeo.items()])` para renombrar todas las columnas en una sola operacion. Las columnas a excluir simplemente no se incluyen en el diccionario.
**Justificacion**: Patron eficiente (un solo select), mantenible (diccionario legible), compatible con Serverless. Los diccionarios quedan capturados por closure.
**Alternativas Consideradas**: `.withColumnRenamed()` encadenado — rechazado por ineficiencia con 170+ columnas. `.toDF(*new_names)` — rechazado porque requiere conocer el orden exacto de columnas y es fragil ante schema evolution.

---

### 6. Campos Calculados — Logica CASE y Aritmetica en PySpark

**Pregunta**: Como implementar los 8 campos calculados (4 CASE + 4 aritmeticos) de forma compatible con Serverless y ANSI mode?

**Hallazgos**:
- **CASE con F.when/F.otherwise**: `F.when(condicion1, valor1).when(condicion2, valor2).otherwise(valor_defecto)` — nativo PySpark, compatible con Serverless.
- **SHA2_256**: `F.sha2(F.col("CUSTID").cast("string"), 256)` — funcion nativa, retorna string hexadecimal de 64 caracteres.
- **Aritmetica con ANSI mode**: `spark.sql.ansi.enabled = true` por defecto en Serverless. Division por cero lanza error. Se debe proteger con `F.when(denominador != 0, numerador / denominador).otherwise(F.lit(0.0))`.
- **Manejo de nulos**: `F.coalesce(col, F.lit(valor_defecto))` para proteger contra NullPointerException en operaciones aritmeticas.
- **Regla de abs + hash**: Si se usa `F.abs(F.hash(...))`, se debe hacer `.cast("long")` antes de `F.abs()` para evitar overflow en ANSI mode. Para SHA2_256 se usa `F.sha2()` que retorna string directamente — no aplica esta regla.

**Decision**: Usar `F.when().when().otherwise()` para los 4 campos CASE. Usar operaciones aritmeticas con proteccion de division por cero y coalesce de nulos para los 4 campos numericos. Usar `F.sha2(F.col("CUSTID").cast("string"), 256)` para `huella_identificacion_cliente`.
**Justificacion**: Funciones 100% nativas PySpark, compatibles con Serverless y ANSI mode. Manejo explicito de nulos segun RF-026.
**Alternativas Consideradas**: UDFs Python — rechazadas por incompatibilidad con Serverless y rendimiento inferior. SQL expressions via `F.expr()` — viable pero menos legible que la API de columnas.

---

### 7. Carga Incremental Automatica en Vistas Materializadas

**Pregunta**: Como configurar la vista `transacciones_enriquecidas` para que LSDP realice cargas incrementales automaticamente?

**Hallazgos** (fuente: documentacion oficial LSDP):
- LSDP decide automaticamente si una vista materializada puede actualizarse de forma incremental basandose en el plan de ejecucion.
- Condiciones que FUERZAN refrescamiento completo (y deben evitarse):
  - Uso de `.filter()`, `.where()` o cualquier condicion sobre la fuente de datos.
  - Uso de funciones de agregacion (`groupBy`, `agg`).
  - Uso de `current_timestamp()`, `current_date()` u otras funciones no deterministas en la transformacion.
  - Uso de JOINs con tablas externas al pipeline.
- Condiciones que PERMITEN carga incremental:
  - Lectura directa de una streaming table sin filtros.
  - Transformaciones columna-a-columna (select, withColumn, alias).
  - Funciones deterministas en columnas existentes (aritmetica, CASE WHEN, cast, sha2).
- Los campos calculados aritmeticos y CASE WHEN son deterministas y no impiden la carga incremental.
- La adicion de columnas via `.withColumn()` o `.select()` no fuerza refrescamiento completo.

**Decision**: La vista `transacciones_enriquecidas` lee de `trxpfl` SIN filtros, aplica exclusion de columnas y campos calculados via `.select()` con alias y expresiones. No se usan funciones no deterministas. LSDP deberia poder optimizar con carga incremental.
**Justificacion**: Cumple RF-006 (sin filtros) y RF-007 (configuracion para carga incremental). Las transformaciones son columna-a-columna con funciones deterministas.
**Alternativas Consideradas**: No hay alternativas — las restricciones son claras. Cualquier filtro forzaria refrescamiento completo.

---

### 8. Patron Closure para Scripts de Plata

**Pregunta**: Como adaptar el patron Closure del Incremento 3 (bronce) para los scripts de plata que no necesitan rutas de archivos?

**Hallazgos**:
- En bronce, el patron Closure lee parametros del pipeline + tabla Parametros + construye rutas de archivos con `LsdpConstructorRutas`.
- En plata, las vistas materializadas leen directamente de streaming tables por nombre. No se necesitan rutas de archivos.
- Los parametros requeridos para plata son:
  - `catalogoParametro`, `esquemaParametro`, `tablaParametros` — para leer la tabla Parametros.
  - De la tabla Parametros: `catalogoPlata` y `esquemaPlata` — para el nombre completo de la vista materializada destino (e.g., `f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados"`).
- Las streaming tables de bronce se leen por nombre simple (`spark.read.table("cmstfl")`) — no se necesita `catalogoBronce` en los scripts de plata.
- No se necesitan: `rutaCompletaMaestroCliente`, `rutaSchemaLocation*`, `TipoStorage`, `catalogoVolume`, `esquemaVolume`, `nombreVolume`, `bucketS3`.
- El patron Closure sigue siendo: lectura de parametros a nivel de modulo -> captura por closure en funcion decorada.

**Decision**: Simplificar el patron Closure para plata: leer solo `catalogoParametro/esquemaParametro/tablaParametros` del pipeline y `catalogoPlata`/`esquemaPlata` de la tabla Parametros para el nombre de la vista destino. Las streaming tables de bronce se leen por nombre simple (catalogo/esquema por defecto del pipeline). No invocar `LsdpConstructorRutas`.
**Justificacion**: Principio de minimo privilegio — los scripts de plata solo necesitan saber donde escribir la vista destino. El acceso a bronce es por nombre simple de tabla, no por ruta de archivos ni nombre de tres partes.
**Alternativas Consideradas**: Reutilizar el patron completo de bronce (con rutas) — rechazado por innecesario y confuso.

---

## Resumen de Decisiones

| # | Aspecto | Decision | Impacto |
|---|---------|----------|---------|
| 1 | Decorador vistas | `@dp.materialized_view` con `table_properties` y `cluster_by` | Define la estructura de ambas vistas |
| 2 | Calidad de datos | `@dp.expect` stacked (modo observacional) | 5+4 expectativas, conserva todos los registros |
| 3 | Lectura de bronce | `spark.read.table("nombre_tabla")` (nombre simple, pipeline default) | Acceso batch a streaming tables |
| 4 | Dimension Tipo 1 | `Window + ROW_NUMBER + filter` con desempate por CUSTID | Registro mas reciente por cliente |
| 5 | Renombrado columnas | `.select([F.col(old).alias(new)])` con diccionario | Eficiente para 170+ columnas |
| 6 | Campos calculados | `F.when/otherwise` + aritmetica con proteccion de nulos/cero | 4 CASE + 4 numericos |
| 7 | Carga incremental | Sin filtros, sin funciones no deterministas | LSDP optimiza automaticamente |
| 8 | Patron Closure plata | Simplificado: catalogo/esquema plata de Parametros, bronce por nombre simple, sin rutas | Menor acoplamiento |
