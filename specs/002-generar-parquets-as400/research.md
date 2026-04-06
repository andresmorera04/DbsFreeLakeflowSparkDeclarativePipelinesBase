# Investigacion: Generacion de Parquets Simulando Data AS400

**Feature**: 002-generar-parquets-as400
**Fecha**: 2026-04-03
**Estado**: Todas las decisiones APROBADAS por el usuario (2026-04-03)

## Tema 1: Disponibilidad de Faker en Databricks Serverless

**Decision**: Faker con fallback a listas estaticas embebidas.
**Estado**: APROBADA por el usuario (2026-04-03)

**Razonamiento**: Faker **no es una libreria pre-instalada** en el entorno Serverless de Databricks. Las versiones de entorno Serverless (v14.3 a v18.0) incluyen paquetes del Databricks Runtime base pero no incluyen Faker. Para instalar dependencias personalizadas en Serverless se usa el panel Environment del notebook o `%pip install faker` como primera celda. Sin embargo, en Databricks Free Edition esto puede fallar por restricciones de cuotas o de red. La estrategia aprobada es:

1. Intentar `import faker` dentro de un bloque `try/except ImportError`.
2. Si Faker esta disponible, usarlo con locales `he_IL` (hebreo), `ar_EG` (egipcio/arabe) y `en_US`/`en_GB` (ingles) para nombres.
3. Si Faker no esta disponible, caer a listas estaticas de nombres hebreos, egipcios e ingleses embebidas directamente en el notebook.
4. Las listas estaticas deben contener al menos 100 nombres y 80 apellidos por cada origen etnico para garantizar variabilidad.

**Alternativas consideradas**:
- Instalar Faker obligatoriamente con `%pip install faker` al inicio: Descartado porque puede fallar en Free Edition.
- Usar solo random con listas estaticas sin intentar Faker: Descartado porque Faker genera datos mas realistas cuando esta disponible.
- Usar mimesis como alternativa a Faker: Descartado por menor soporte de locales para hebreo y egipcio.

**Fuente**: https://docs.databricks.com/aws/en/compute/serverless/dependencies.html — Las dependencias del notebook Serverless se agregan via panel Environment; Faker no figura en las bibliotecas pre-instaladas del entorno base.

---

## Tema 2: Escritura de Parquet con PySpark a Unity Catalog Volumes

**Decision**: Usar `df.write.mode("overwrite").parquet(ruta)` con rutas de Volumes.
**Estado**: APROBADA por el usuario (2026-04-03)

**Razonamiento**: Las rutas de Unity Catalog Volumes tienen el formato `/Volumes/<catalogo>/<esquema>/<volume>/<ruta_relativa>` y son totalmente compatibles con operaciones de lectura/escritura de PySpark, incluyendo `spark.read.parquet()` y `df.write.parquet()`. Las rutas de Volumes se tratan como paths del sistema de archivos en DBFS y son accesibles directamente desde Serverless Compute. Para Amazon S3, las rutas `s3://<bucket>/<ruta>` tambien son compatibles con estas operaciones (requiere credenciales IAM configuradas en el workspace).

El patron de escritura sera:
```python
df_resultado.coalesce(numero_particiones).write.mode("overwrite").parquet(ruta_completa)
```

**Notas clave**:
- `coalesce(int(numeroParticiones))` antes de escribir, donde `numeroParticiones` se recibe via widget (defecto 8), para reducir el numero de archivos parquet generados.
- `mode("overwrite")` reemplaza el contenido del directorio destino.
- No se necesita `format("delta")` ni tablas Delta; los archivos son parquets planos.
- La lectura del parquet existente (para mutacion en CMSTFL) tambien usa `spark.read.parquet(ruta)`.

**Alternativas consideradas**:
- Escribir como tablas Delta en lugar de parquet plano: Descartado porque el proposito es simular archivos de un sistema AS400 externo.
- Usar `dbutils.fs.cp()` para copiar archivos: Descartado porque PySpark ya maneja las rutas de Volumes nativamente.

**Fuente**: https://docs.databricks.com/aws/en/connect/unity-catalog/volumes.html — Los Volumes de Unity Catalog son accesibles como paths del sistema de archivos.

---

## Tema 3: Generacion de Datos con PySpark sin sparkContext

**Decision**: Usar `spark.range()` + transformaciones con funciones de PySpark y closures de Python.
**Estado**: APROBADA por el usuario (2026-04-03)

**Razonamiento**: En Databricks Serverless esta **prohibido** el uso de `spark.sparkContext` (error `JVM_ATTRIBUTE_NOT_SUPPORTED`). Esto significa que `spark.sparkContext.parallelize()` no se puede usar para crear DataFrames desde listas Python. Las alternativas compatibles con Serverless son:

1. **`spark.range(n)`**: Crea un DataFrame con una columna `id` de 0 a n-1. Es el punto de partida para generacion de datos.
2. **Funciones de columna PySpark**: `F.rand()`, `F.randn()`, `F.lit()`, `F.concat()`, `F.lpad()`, `F.when()`, `F.array()`, `F.element_at()`, `F.expr()` para transformar las columnas.
3. **UDFs de Python**: `@F.udf` o `F.pandas_udf` para logica compleja que no se puede expresar con funciones nativas.
4. **`spark.createDataFrame(lista_python)`**: Compatible con Serverless para DataFrames pequenos (listas estaticas de referencia).

El patron general para generar un DataFrame con datos simulados:
```python
df = (spark.range(cantidad_registros)
    .withColumn("CUSTID", F.col("id") + 1)
    .withColumn("indice_nombre", (F.rand() * F.lit(len(lista_nombres))).cast("int"))
    .withColumn("CUSNM", F.element_at(F.array([F.lit(n) for n in lista_nombres]), F.col("indice_nombre") + 1))
    # ... mas columnas
    .drop("id", "indice_nombre"))
```

**Alternativas consideradas**:
- `spark.sparkContext.parallelize()`: Prohibido en Serverless.
- Generar todo en Python puro y luego `spark.createDataFrame()`: Descartado para volumetrias grandes (50,000+ registros) porque serializa todo en el driver.
- Usar Pandas UDFs para toda la generacion: Descartado por complejidad excesiva; `spark.range()` + funciones nativas es mas eficiente.

**Fuente**: Decisiones Tecnicas Aprobadas en SYSTEM.md (Decision 5) y restriccion de compatibilidad Serverless en la constitution.

---

## Tema 4: Lectura de la Tabla Parametros desde Unity Catalog

**Decision**: Usar `spark.read.table()` directamente y convertir a diccionario Python.
**Estado**: APROBADA por el usuario (2026-04-03)

**Razonamiento**: La tabla Parametros fue creada en el Incremento 1 en `control.lab1.Parametros` con columnas `Clave` (string) y `Valor` (string). Contiene 15 registros de configuracion. La lectura se realiza con:

```python
nombre_completo_tabla = f"{catalogo_parametro}.{esquema_parametro}.{tabla_parametros}"
df_parametros = spark.read.table(nombre_completo_tabla)
dict_parametros = {fila["Clave"]: fila["Valor"] for fila in df_parametros.collect()}
```

Este patron es:
- 100% compatible con Serverless Compute.
- No usa JDBC, ni Secret Scopes, ni Azure Key Vault.
- El `collect()` es seguro porque la tabla tiene solo 15 registros (caben en memoria del driver).
- El diccionario resultante permite acceder a cualquier parametro con `dict_parametros["TipoStorage"]`.

**Parametros necesarios de la tabla Parametros para este incremento:**
| Clave | Uso |
|-------|-----|
| TipoStorage | Determinar si usar Volume o S3 |
| catalogoVolume | Construir ruta de Volume |
| esquemaVolume | Construir ruta de Volume |
| nombreVolume | Construir ruta de Volume |
| bucketS3 | Construir ruta S3 |

**Alternativas consideradas**:
- `spark.sql(f"SELECT * FROM {tabla}")`: Funciona pero `spark.read.table()` es mas pythonico.
- Implementar `LsdpConexionParametros.py` en este incremento: Descartado porque las utilidades LSDP se crean en el Incremento 3.

**Fuente**: Decision 3 y Decision 5 de SYSTEM.md, confirmado en la constitution.

---

## Tema 5: Modelo de Mutacion Diferenciada del Maestro de Clientes

**Decision**: Leer parquet existente desde `rutaMaestroClienteExistente`, mutar `porcentajeMutacion`% en los campos de `camposMutacion`, agregar `porcentajeNuevos`% nuevos, union, overwrite.
**Estado**: APROBADA por el usuario (2026-04-03)

**Razonamiento**: El modelo de mutacion diferenciada del CMSTFL simula el comportamiento real de un sistema AS400 donde los datos de clientes cambian entre extracciones. El patron implementado sera:

**Primera ejecucion** (`rutaMaestroClienteExistente` vacio):
1. Generar `cantidadClientes` registros con CUSTID secuencial (1, 2, ..., N).
2. Escribir en modo overwrite.

**Re-ejecuciones** (`rutaMaestroClienteExistente` con ruta del parquet existente):
1. Leer el parquet existente con `spark.read.parquet(ruta_maestro_existente)` donde la ruta se construye dinamicamente a partir de `rutaMaestroClienteExistente`.
2. Calcular cantidad de registros a mutar: `int(conteo_actual * porcentaje_mutacion)` (por defecto 0.20 = 20%, escala 0-1).
3. Calcular cantidad de nuevos registros: `int(conteo_actual * porcentaje_nuevos)` (por defecto 0.006 = 0.6%, escala 0-1).
4. Parsear `camposMutacion` (separados por coma) para obtener la lista de campos a mutar. Seleccionar aleatoriamente `porcentajeMutacion` de registros existentes y mutar esos campos: por defecto CUSNM (nombre), CUSLN (apellido), CUSMD (segundo nombre), CUSFN (nombre completo), CUSAD (direccion), CUSA2 (direccion 2), CUSCT (ciudad), CUSST (estado), CUSZP (codigo postal), CUSPH (telefono), CUSMB (movil), CUSEM (email), CUSMS (estado civil), CUSOC (ocupacion), CUSED (nivel educativo).
5. Generar nuevos registros con CUSTID continuando la secuencia (max_custid + 1, ..., max_custid + N).
6. Union de: registros no mutados + registros mutados + registros nuevos.
7. Escribir en modo overwrite en la ruta destino `rutaRelativaMaestroCliente` (reemplaza el parquet anterior con el consolidado).

**Notas de implementacion**:
- La seleccion aleatoria se hace con `F.rand()` y `F.row_number()` para seleccionar el `porcentajeMutacion`%.
- La mutacion usa `F.when()` para aplicar cambios solo a los registros seleccionados.
- Los campos a mutar se reciben via el widget `camposMutacion` (separados por coma), con los 15 campos demograficos como valor por defecto.

**Alternativas consideradas**:
- Usar Delta Lake MERGE para la mutacion: Descartado porque los parquets simulan archivos AS400 planos.
- Mutar in-place sin re-leer: No posible en Spark (inmutabilidad de DataFrames).

**Fuente**: Politica de Idempotencia Diferenciada en la constitution.

---

## Tema 6: Construccion Dinamica de Rutas de Almacenamiento

**Decision**: Logica inline en cada notebook (sin utilidades LSDP en este incremento).
**Estado**: APROBADA por el usuario (2026-04-03)

**Razonamiento**: Las utilidades `LsdpConexionParametros.py` y `LsdpConstructorRutas.py` son exclusivas del LSDP y se crearan en el Incremento 3. En el Incremento 2, la logica de construccion de rutas se implementa directamente dentro de cada notebook:

```python
tipo_storage = dict_parametros["TipoStorage"]

if tipo_storage == "Volume":
    ruta_base = f"/Volumes/{dict_parametros['catalogoVolume']}/{dict_parametros['esquemaVolume']}/{dict_parametros['nombreVolume']}"
elif tipo_storage == "AmazonS3":
    ruta_base = f"s3://{dict_parametros['bucketS3']}"
else:
    raise ValueError(f"TipoStorage no reconocido: '{tipo_storage}'. Valores validos: 'Volume', 'AmazonS3'")

ruta_completa = f"{ruta_base}/{ruta_relativa}"
```

**Valores actuales de la tabla Parametros (Incremento 1)**:
- TipoStorage = "Volume"
- catalogoVolume = "bronce"
- esquemaVolume = "lab1"
- nombreVolume = "datos_bronce"
- bucketS3 = "" (vacio)

Con Volume, la ruta base resultante es: `/Volumes/bronce/lab1/datos_bronce`

**Alternativas consideradas**:
- Crear las utilidades LSDP anticipadamente: Descartado por decision del usuario para mantener los incrementos separados.
- Usar solo rutas hardcodeadas: Prohibido por la constitution.

**Fuente**: Clarificacion Q2 del spec, politica de Parametrizacion Total de la constitution.

---

## Tema 7: Optimizacion para Databricks Free Edition

**Decision**: Particiones reducidas, shuffle bajo, volumetria por defecto controlada.
**Estado**: APROBADA por el usuario (2026-04-03)

**Razonamiento**: Databricks Free Edition opera en un entorno serverless con cuotas limitadas. Las configuraciones de optimizacion son:

| Configuracion | Valor | Justificacion |
|---------------|-------|---------------|
| Particiones de escritura (coalesce) | Parametro `numeroParticiones` (defecto 8) | Menos archivos parquet, menos overhead I/O |
| shuffle.partitions | Parametro `shufflePartitions` (defecto 8) | Reducir tareas en operaciones de shuffle |
| cantidadClientes (defecto) | 50,000 | Dimensionado para cuotas de Free Edition |
| cantidadTransacciones (defecto) | 150,000 | Ratio 1:3 con clientes |
| Listas estaticas de Faker (fallback) | 100+ nombres por etnia | Suficiente variabilidad |

**Nota sobre spark.sql.shuffle.partitions**: Este es un parametro Spark soportado en Serverless. Se configura con `spark.conf.set("spark.sql.shuffle.partitions", str(shufflePartitions))` al inicio de cada notebook, donde `shufflePartitions` es el valor recibido via widget (por defecto "8").

**Alternativas consideradas**:
- No limitar particiones: Descartado porque genera demasiados archivos pequenos.
- Volumetria mayor (500,000+ clientes): Descartado por defecto para Free Edition; configurable via widget si el usuario tiene mas recursos.

**Fuente**: Politica de Optimizacion para Databricks Free Edition en la constitution.
