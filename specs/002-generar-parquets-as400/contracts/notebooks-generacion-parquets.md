# Contrato: NbGenerarMaestroCliente.py

**Notebook**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`
**Entidad**: CMSTFL — Maestro de Clientes
**Tipo**: Notebook generador de parquets (.py formato Databricks)

## Parametros de Entrada (dbutils.widgets)

| Widget | Tipo | Defecto | Obligatorio | Descripcion |
|--------|------|---------|-------------|-------------|
| catalogoParametro | text | "control" | Si | Catalogo UC de la tabla Parametros |
| esquemaParametro | text | "lab1" | Si | Esquema UC de la tabla Parametros |
| tablaParametros | text | "Parametros" | Si | Nombre de la tabla de parametros |
| cantidadClientes | text | "50000" | Si | Cantidad base de registros a generar |
| rutaRelativaMaestroCliente | text | "LSDP_Base/As400/MaestroCliente/" | Si | Ruta relativa del parquet dentro del storage |
| rutaMaestroClienteExistente | text | (vacio) | No | Ruta relativa del maestro de clientes existente para re-ejecuciones; vacio en primera ejecucion |
| porcentajeMutacion | text | "0.20" | Si | Porcentaje de registros existentes a mutar en re-ejecuciones (escala 0-1, donde 0.20 = 20%) |
| porcentajeNuevos | text | "0.006" | Si | Porcentaje de registros nuevos a agregar en re-ejecuciones (escala 0-1, donde 0.006 = 0.6%) |
| camposMutacion | text | "CUSNM,CUSLN,CUSMD,CUSFN,CUSAD,CUSA2,CUSCT,CUSST,CUSZP,CUSPH,CUSMB,CUSEM,CUSMS,CUSOC,CUSED" | Si | Lista de campos demograficos a mutar, separados por coma |
| montoMinimo | text | "10" | Si | Valor minimo para columnas de tipo monto (DoubleType) |
| montoMaximo | text | "100000" | Si | Valor maximo para columnas de tipo monto (DoubleType) |
| numeroParticiones | text | "8" | Si | Numero de particiones para coalesce al escribir parquet |
| shufflePartitions | text | "8" | Si | Valor de spark.sql.shuffle.partitions |

## Parametros Leidos de Tabla Parametros

| Clave | Uso |
|-------|-----|
| TipoStorage | Determinar tipo de almacenamiento (Volume o AmazonS3) |
| catalogoVolume | Construir ruta de Volume |
| esquemaVolume | Construir ruta de Volume |
| nombreVolume | Construir ruta de Volume |
| bucketS3 | Construir ruta S3 |

## Salida

| Elemento | Detalle |
|----------|---------|
| Tipo | Archivo Parquet |
| Modo escritura | overwrite |
| Columnas | 70 (41 StringType, 18 DateType, 9 LongType, 2 DoubleType) |
| Ruta (Volume) | `/Volumes/<catalogoVolume>/<esquemaVolume>/<nombreVolume>/<rutaRelativaMaestroCliente>` |
| Ruta (S3) | `s3://<bucketS3>/<rutaRelativaMaestroCliente>` |

## Comportamiento

- **Primera ejecucion** (`rutaMaestroClienteExistente` vacio): Genera `cantidadClientes` registros con CUSTID secuencial (1..N).
- **Re-ejecucion** (`rutaMaestroClienteExistente` con ruta): Lee parquet existente desde `rutaMaestroClienteExistente`, conserva registros, muta `porcentajeMutacion` (por defecto 0.20 = 20%) en los campos listados en `camposMutacion`, agrega `porcentajeNuevos` (por defecto 0.006 = 0.6%) nuevos, y escribe el resultado consolidado en `rutaRelativaMaestroCliente`.
- **Observabilidad**: Imprime resumen de parametros al inicio y metricas al final (conteos, tiempos).
- **Dependencia con Faker**: Intenta usar Faker; si no disponible, usa listas estaticas.

## Validaciones

- Parametros widgets no vacios (excepto `rutaMaestroClienteExistente` que puede estar vacio).
- `cantidadClientes` debe ser entero positivo.
- `porcentajeMutacion` y `porcentajeNuevos` deben ser numericos positivos en rango (0, 1.0] (escala 0-1).
- `montoMinimo` debe ser menor que `montoMaximo`; ambos deben ser numericos positivos.
- `TipoStorage` debe ser "Volume" o "AmazonS3".
- Si `rutaMaestroClienteExistente` tiene valor, el parquet debe ser legible.

---

# Contrato: NbGenerarTransaccionalCliente.py

**Notebook**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`
**Entidad**: TRXPFL — Transaccional de Clientes
**Tipo**: Notebook generador de parquets (.py formato Databricks)

## Parametros de Entrada (dbutils.widgets)

| Widget | Tipo | Defecto | Obligatorio | Descripcion |
|--------|------|---------|-------------|-------------|
| catalogoParametro | text | "control" | Si | Catalogo UC de la tabla Parametros |
| esquemaParametro | text | "lab1" | Si | Esquema UC de la tabla Parametros |
| tablaParametros | text | "Parametros" | Si | Nombre de la tabla de parametros |
| cantidadTransacciones | text | "150000" | Si | Cantidad de transacciones a generar |
| fechaTransaccion | text | (sin defecto) | Si | Fecha de las transacciones formato YYYY-MM-DD |
| rutaRelativaTransaccional | text | "LSDP_Base/As400/Transaccional/" | Si | Ruta relativa del parquet transaccional |
| rutaRelativaMaestroCliente | text | "LSDP_Base/As400/MaestroCliente/" | Si | Ruta para leer el Maestro de Clientes existente |
| montoMinimo | text | "10" | Si | Valor minimo para columnas de tipo monto (DoubleType) |
| montoMaximo | text | "100000" | Si | Valor maximo para columnas de tipo monto (DoubleType) |
| numeroParticiones | text | "8" | Si | Numero de particiones para coalesce al escribir parquet |
| shufflePartitions | text | "8" | Si | Valor de spark.sql.shuffle.partitions |

## Parametros Leidos de Tabla Parametros

| Clave | Uso |
|-------|-----|
| TipoStorage | Determinar tipo de almacenamiento |
| catalogoVolume | Construir ruta de Volume |
| esquemaVolume | Construir ruta de Volume |
| nombreVolume | Construir ruta de Volume |
| bucketS3 | Construir ruta S3 |

## Salida

| Elemento | Detalle |
|----------|---------|
| Tipo | Archivo Parquet |
| Modo escritura | overwrite |
| Columnas | 60 (7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType) |
| Ruta (Volume) | `/Volumes/<catalogoVolume>/<esquemaVolume>/<nombreVolume>/<rutaRelativaTransaccional>` |
| Ruta (S3) | `s3://<bucketS3>/<rutaRelativaTransaccional>` |

## Dependencias

- **Requiere**: Maestro de Clientes (CMSTFL) generado previamente en la ruta indicada.
- Lee el Maestro para obtener los CUSTID existentes y la cantidad de clientes.
- **Deduplicacion**: Aplica `.select("CUSTID").distinct()` al leer el maestro. Esto permite que `rutaRelativaMaestroCliente` apunte a una carpeta padre (ej: `archivos/LSDP_Base/As400/CMSTFL`) que contenga multiples particiones por dia (año=2026/mes=04/dia=01/, dia=02/, etc.), y Spark leera recursivamente todos los parquets sin duplicar CUSTIDs.

## Comportamiento

- **Cada ejecucion**: Genera un lote completamente nuevo de transacciones (no acumulativo).
- **Lectura del maestro**: Lee todos los parquets bajo la ruta indicada y extrae CUSTIDs unicos via `.distinct()`. La cantidad de clientes (`total_clientes`) se calcula sobre el conjunto deduplicado.
- **Distribucion de tipos**: Alta ~60% (CATM, DATM, CMPR, TINT, DPST), Media ~30% (PGSL, TEXT, RTRO, PGSV, NMNA, INTR), Baja ~10% (ADSL, IMPT, DMCL, CMSN).
- **TRXID**: Formato alfanumerico con prefijo del tipo + secuencial (ej: "CATM00000001").
- **Observabilidad**: Imprime resumen de parametros al inicio y metricas al final.

## Validaciones

- Parametros widgets no vacios.
- `cantidadTransacciones` debe ser entero positivo.
- `fechaTransaccion` debe tener formato valido YYYY-MM-DD.
- `montoMinimo` debe ser menor que `montoMaximo`; ambos deben ser numericos positivos.
- El Maestro de Clientes debe existir en la ruta indicada.
- `TipoStorage` debe ser "Volume" o "AmazonS3".

---

# Contrato: NbGenerarSaldosCliente.py

**Notebook**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarSaldosCliente.py`
**Entidad**: BLNCFL — Saldos de Clientes
**Tipo**: Notebook generador de parquets (.py formato Databricks)

## Parametros de Entrada (dbutils.widgets)

| Widget | Tipo | Defecto | Obligatorio | Descripcion |
|--------|------|---------|-------------|-------------|
| catalogoParametro | text | "control" | Si | Catalogo UC de la tabla Parametros |
| esquemaParametro | text | "lab1" | Si | Esquema UC de la tabla Parametros |
| tablaParametros | text | "Parametros" | Si | Nombre de la tabla de parametros |
| rutaRelativaSaldoCliente | text | "LSDP_Base/As400/SaldoCliente/" | Si | Ruta relativa del parquet de saldos |
| rutaRelativaMaestroCliente | text | "LSDP_Base/As400/MaestroCliente/" | Si | Ruta para leer el Maestro de Clientes existente |
| montoMinimo | text | "10" | Si | Valor minimo para columnas de tipo monto (DoubleType) |
| montoMaximo | text | "100000" | Si | Valor maximo para columnas de tipo monto (DoubleType) |
| numeroParticiones | text | "8" | Si | Numero de particiones para coalesce al escribir parquet |
| shufflePartitions | text | "8" | Si | Valor de spark.sql.shuffle.partitions |

## Parametros Leidos de Tabla Parametros

| Clave | Uso |
|-------|-----|
| TipoStorage | Determinar tipo de almacenamiento |
| catalogoVolume | Construir ruta de Volume |
| esquemaVolume | Construir ruta de Volume |
| nombreVolume | Construir ruta de Volume |
| bucketS3 | Construir ruta S3 |

## Salida

| Elemento | Detalle |
|----------|---------|
| Tipo | Archivo Parquet |
| Modo escritura | overwrite |
| Columnas | 100 (2 LongType, 29 StringType, 34 DoubleType, 35 DateType) |
| Ruta (Volume) | `/Volumes/<catalogoVolume>/<esquemaVolume>/<nombreVolume>/<rutaRelativaSaldoCliente>` |
| Ruta (S3) | `s3://<bucketS3>/<rutaRelativaSaldoCliente>` |

## Dependencias

- **Requiere**: Maestro de Clientes (CMSTFL) generado previamente en la ruta indicada.
- La cantidad de registros se determina automaticamente (1 saldo por cada cliente unico).
- **Deduplicacion**: Aplica `.select("CUSTID").distinct().orderBy("CUSTID")` al leer el maestro. Esto permite que `rutaRelativaMaestroCliente` apunte a una carpeta padre (ej: `archivos/LSDP_Base/As400/CMSTFL`) que contenga multiples particiones por dia, y Spark leera recursivamente todos los parquets sin duplicar CUSTIDs.

## Comportamiento

- **Cada ejecucion**: Regenera completamente el archivo de saldos (no acumulativo).
- **Lectura del maestro**: Lee todos los parquets bajo la ruta indicada y extrae CUSTIDs unicos via `.distinct()`. La relacion 1:1 se establece sobre el conjunto deduplicado.
- **Relacion 1:1**: Genera exactamente 1 registro por cada CUSTID unico del Maestro de Clientes.
- **Distribucion de tipos de cuenta**: AHRO (40%), CRTE (30%), PRES (20%), INVR (10%).
- **Observabilidad**: Imprime resumen de parametros al inicio y metricas al final.

## Validaciones

- Parametros widgets no vacios.
- `montoMinimo` debe ser menor que `montoMaximo`; ambos deben ser numericos positivos.
- El Maestro de Clientes debe existir en la ruta indicada.
- `TipoStorage` debe ser "Volume" o "AmazonS3".

---

# Contrato: NbTddGenerarParquets.py

**Notebook**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py`
**Tipo**: Notebook de pruebas TDD (.py formato Databricks)

## Parametros de Entrada (dbutils.widgets)

| Widget | Tipo | Defecto | Obligatorio | Descripcion |
|--------|------|---------|-------------|-------------|
| catalogoParametro | text | "control" | Si | Catalogo UC de la tabla Parametros |
| esquemaParametro | text | "lab1" | Si | Esquema UC de la tabla Parametros |
| tablaParametros | text | "Parametros" | Si | Nombre de la tabla de parametros |
| rutaRelativaMaestroCliente | text | "LSDP_Base/As400/MaestroCliente/" | Si | Ruta del parquet CMSTFL |
| rutaRelativaTransaccional | text | "LSDP_Base/As400/Transaccional/" | Si | Ruta del parquet TRXPFL |
| rutaRelativaSaldoCliente | text | "LSDP_Base/As400/SaldoCliente/" | Si | Ruta del parquet BLNCFL |

## Salida

| Elemento | Detalle |
|----------|---------|
| Tipo | Resultados de pruebas impresos en consola |
| Formato | PASS/FAIL por cada prueba con descripcion |

## Pruebas Incluidas

### CMSTFL
- Verificar 70 columnas con distribucion de tipos correcta (41/18/9/2).
- Verificar cantidad de registros.
- Verificar exclusion de nombres latinos.
- Verificar CUSTID secuencial y unico.
- Verificar modelo de mutacion diferenciada (0.6% nuevos con porcentajeNuevos=0.006, 20% mutados con porcentajeMutacion=0.20).

### TRXPFL
- Verificar 60 columnas con distribucion de tipos correcta (7/19/2/2/30).
- Verificar cantidad de registros.
- Verificar integridad referencial (todos los CUSTID existen en CMSTFL).
- Verificar formato TRXID (prefijo tipo + secuencial).
- Verificar distribucion de tipos de transaccion (~60%/~30%/~10%).

### BLNCFL
- Verificar 100 columnas con distribucion de tipos correcta (2/29/34/35).
- Verificar relacion 1:1 con CMSTFL (misma cantidad de registros, mismos CUSTID).
- Verificar distribucion de tipos de cuenta (40%/30%/20%/10%).

## Comportamiento

- **Idempotencia absoluta**: Ejecutar las pruebas multiples veces produce el mismo resultado.
- Requiere que los 3 parquets hayan sido generados previamente.
- Imprime resumen con conteo de pruebas pasadas/fallidas.
