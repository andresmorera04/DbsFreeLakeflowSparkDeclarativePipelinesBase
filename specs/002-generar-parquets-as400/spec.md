# Especificacion de Feature: Generacion de Parquets Simulando Data AS400

**Branch de Feature**: `002-generar-parquets-as400`
**Creado**: 2026-04-03
**Estado**: Aprobado
**Entrada**: Incremento 2 del proyecto DbsFreeLakeflowSparkDeclarativePipelinesBase — Crear notebooks Python que generen 3 archivos parquet simulando la data del sistema AS400 de una entidad bancaria, adaptados al almacenamiento dinamico (Volume o Amazon S3).

## Clarifications

### Session 2026-04-03

- Q: Cual es la estrategia de escritura de parquets (overwrite vs append vs mixto)? -> A: Overwrite — Cada ejecucion reemplaza el parquet anterior en la misma ruta para los 3 notebooks.
- Q: Se incluyen las utilidades LsdpConexionParametros.py y LsdpConstructorRutas.py en el Incremento 2? -> A: No. Son exclusivas del LSDP y se crearan en el Incremento 3. La logica de lectura de parametros y construccion de rutas se implementa directamente dentro de cada notebook.
- Q: Cual es el mecanismo de generacion de datos (nombres, transacciones, saldos) sin dependencias externas garantizadas? -> A: Faker con fallback. Se intenta usar Faker para datos realistas; si no esta disponible en el entorno Serverless, se cae automaticamente a listas estaticas embebidas en el codigo como fallback.
- Q: Cual es el formato del identificador unico de cliente (CUSTID)? -> A: Numerico secuencial (LongType) comenzando en 1 (ej: 1, 2, 3, ... 50000).
- Q: Cual es el formato del identificador unico de transaccion (TRXID)? -> A: Alfanumerico estilo AS400 (StringType) con prefijo del tipo de transaccion + secuencial (ej: "CATM00000001", "DPST00000002").

## Escenarios de Usuario y Pruebas

### Historia de Usuario 1 - Generacion del Maestro de Clientes (Prioridad: P1)

El ingeniero de datos ejecuta el notebook de generacion del Maestro de Clientes (CMSTFL) para producir un archivo parquet que simula la tabla maestra de clientes del sistema AS400 de una entidad bancaria. En la primera ejecucion (cuando el parametro `rutaMaestroClienteExistente` esta vacio), el notebook genera la cantidad base de registros configurada en `cantidadClientes`. En re-ejecuciones posteriores, el ingeniero proporciona la ruta del maestro de clientes mas reciente en el parametro `rutaMaestroClienteExistente`, y el notebook lee ese parquet, aplica el modelo de mutacion diferenciada: conserva los clientes existentes, muta el porcentaje configurado en `porcentajeMutacion` (por defecto 0.20, equivalente a 20%) de los registros en los campos definidos en `camposMutacion` (por defecto 15 campos demograficos separados por coma) y agrega el porcentaje configurado en `porcentajeNuevos` (por defecto 0.006, equivalente a 0.6%) de clientes nuevos. Los nombres y apellidos de los clientes deben ser exclusivamente hebreos, egipcios e ingleses; queda prohibido el uso de nombres y apellidos latinos.

**Por que esta prioridad**: El Maestro de Clientes es la entidad base del proyecto. Tanto el archivo de Saldos (BLNCFL) como el Transaccional (TRXPFL) dependen de los identificadores de cliente (CUSTID) generados por este notebook. Sin este archivo, los demas no pueden generarse correctamente.

**Prueba Independiente**: Se ejecuta el notebook con los parametros por defecto (incluyendo `rutaMaestroClienteExistente` vacio), se verifica que el parquet resultante tenga 70 columnas con la distribucion de tipos correcta (41 StringType, 18 DateType, 9 LongType, 2 DoubleType), que la cantidad de registros coincida con `cantidadClientes`, y que los nombres sean exclusivamente hebreos, egipcios e ingleses. En una segunda ejecucion (proporcionando la ruta del parquet anterior en `rutaMaestroClienteExistente`), se verifica que la cantidad de registros aumente segun `porcentajeNuevos` (por defecto 0.006, equivalente a 0.6%) y que el porcentaje definido en `porcentajeMutacion` (por defecto 0.20, equivalente a 20%) presente cambios en los campos configurados en `camposMutacion`.

**Escenarios de Aceptacion**:

1. **Dado** que el parametro `rutaMaestroClienteExistente` esta vacio (primera ejecucion), **Cuando** el ingeniero ejecuta el notebook con `cantidadClientes=50000`, **Entonces** se genera un archivo parquet con exactamente 50,000 registros, 70 columnas, y la distribucion de tipos de datos correcta.
2. **Dado** que el parametro `rutaMaestroClienteExistente` apunta a un parquet de 50,000 clientes, **Cuando** el ingeniero ejecuta el notebook con los porcentajes por defecto (`porcentajeMutacion=0.20`, `porcentajeNuevos=0.006`), **Entonces** el parquet resultante contiene 50,300 registros (50,000 existentes + 0.6% nuevos), con el 20% de los registros previos mutados en los campos configurados en `camposMutacion`.
3. **Dado** que se ejecuta el notebook, **Cuando** se inspeccionan los campos de nombres y apellidos del parquet resultante, **Entonces** todos los nombres y apellidos son exclusivamente hebreos, egipcios o ingleses; ningun nombre es de origen latino.
4. **Dado** que el parametro `TipoStorage` en la tabla Parametros es `"Volume"`, **Cuando** se ejecuta el notebook, **Entonces** el parquet se escribe en una ruta de Unity Catalog Volumes construida dinamicamente.
5. **Dado** que el parametro `TipoStorage` en la tabla Parametros es `"AmazonS3"`, **Cuando** se ejecuta el notebook, **Entonces** el parquet se escribe en una ruta S3 construida dinamicamente.

---

### Historia de Usuario 2 - Generacion del Transaccional de Clientes (Prioridad: P1)

El ingeniero de datos ejecuta el notebook de generacion del Transaccional (TRXPFL) para producir un archivo parquet que simula las transacciones bancarias del sistema AS400. Cada ejecucion genera un lote completamente nuevo de transacciones asociadas a los clientes existentes en el Maestro de Clientes. El parametro `rutaRelativaMaestroCliente` puede apuntar a una carpeta padre (ej: `archivos/LSDP_Base/As400/CMSTFL`) que contenga multiples particiones por dia; el notebook lee recursivamente todos los parquets y extrae CUSTIDs unicos via `.distinct()` para evitar duplicacion. Las transacciones se distribuyen en 15 tipos con frecuencias diferenciadas: alta frecuencia (~60%), media frecuencia (~30%) y baja frecuencia (~10%). La fecha de la transaccion se recibe como parametro de entrada.

**Por que esta prioridad**: El Transaccional es fundamental para el producto de datos final solicitado por el area de negocio (comportamiento ATM por cliente). Sin transacciones, las medallas de plata y oro no pueden procesarse.

**Prueba Independiente**: Se ejecuta el notebook con los parametros por defecto, se verifica que el parquet resultante tenga 60 columnas con la distribucion de tipos correcta (7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType), la cantidad de registros configurada, y que la distribucion de tipos de transaccion respete las proporciones esperadas.

**Escenarios de Aceptacion**:

1. **Dado** que existe un Maestro de Clientes generado previamente, **Cuando** el ingeniero ejecuta el notebook con `cantidadTransacciones=150000` y `fechaTransaccion=2026-04-03`, **Entonces** se genera un parquet con 150,000 registros, 60 columnas, y todos los CUSTID referenciados existen en el Maestro de Clientes.
2. **Dado** que se generaron 150,000 transacciones, **Cuando** se analiza la distribucion de tipos de transaccion, **Entonces** CATM, DATM, CMPR, TINT y DPST representan aproximadamente el 60% del total; PGSL, TEXT, RTRO, PGSV, NMNA e INTR representan aproximadamente el 30%; y ADSL, IMPT, DMCL y CMSN representan aproximadamente el 10%.
3. **Dado** que el ingeniero ejecuta el notebook dos veces consecutivas con la misma fecha, **Cuando** se comparan ambos parquets, **Entonces** son lotes completamente diferentes (IDs de transaccion distintos) simulando nuevas extracciones del AS400.
4. **Dado** que el parametro `TipoStorage` en la tabla Parametros es `"Volume"`, **Cuando** se ejecuta el notebook, **Entonces** el parquet se escribe en una ruta de Unity Catalog Volumes construida dinamicamente.

---

### Historia de Usuario 3 - Generacion de Saldos de Clientes (Prioridad: P1)

El ingeniero de datos ejecuta el notebook de generacion de Saldos (BLNCFL) para producir un archivo parquet que simula los saldos bancarios del sistema AS400. Cada ejecucion regenera completamente el archivo, manteniendo una relacion 1:1 con el Maestro de Clientes (un registro de saldo por cada cliente unico existente). El parametro `rutaRelativaMaestroCliente` puede apuntar a una carpeta padre (ej: `archivos/LSDP_Base/As400/CMSTFL`) que contenga multiples particiones por dia; el notebook lee recursivamente todos los parquets y extrae CUSTIDs unicos via `.distinct()` para evitar duplicacion. Los saldos se distribuyen en 4 tipos de cuenta con proporciones definidas.

**Por que esta prioridad**: Los Saldos son parte integral de la vista materializada de plata `clientes_saldos_consolidados` y del producto de datos final del area de negocio. La relacion 1:1 con el Maestro de Clientes es critica para la integridad de los datos.

**Prueba Independiente**: Se ejecuta el notebook despues de generar el Maestro de Clientes, se verifica que el parquet tenga 100 columnas con la distribucion de tipos correcta (2 LongType, 29 StringType, 34 DoubleType, 35 DateType), exactamente un registro por cliente, y la distribucion esperada de tipos de cuenta.

**Escenarios de Aceptacion**:

1. **Dado** que existe un Maestro de Clientes con 50,000 registros, **Cuando** el ingeniero ejecuta el notebook de Saldos, **Entonces** se genera un parquet con exactamente 50,000 registros (uno por cliente), 100 columnas, y la distribucion de tipos de datos correcta.
2. **Dado** que se generaron 50,000 registros de saldos, **Cuando** se analiza la distribucion de tipos de cuenta, **Entonces** AHRO representa aproximadamente el 40%, CRTE el 30%, PRES el 20% e INVR el 10%.
3. **Dado** que se re-ejecuta el notebook de Saldos, **Cuando** se compara con el parquet anterior, **Entonces** el parquet es completamente nuevo (regenerado) pero mantiene la relacion 1:1 con todos los CUSTID del Maestro de Clientes actual.
4. **Dado** que el parametro `TipoStorage` en la tabla Parametros es `"AmazonS3"`, **Cuando** se ejecuta el notebook, **Entonces** el parquet se escribe en una ruta S3 construida dinamicamente.

---

### Historia de Usuario 4 - Observabilidad Completa en la Generacion (Prioridad: P2)

El ingeniero de datos necesita visibilidad total sobre la ejecucion de cada notebook de generacion de parquets. Al iniciar un notebook, se muestra un bloque de resumen con todos los parametros recibidos, el tipo de almacenamiento seleccionado y las rutas construidas. Durante la ejecucion, se imprimen los conteos de registros procesados, el tiempo de cada operacion critica y cualquier condicion excepcional. Al finalizar, se muestra un resumen completo con metricas de ejecucion.

**Por que esta prioridad**: La observabilidad es una politica fundamental del proyecto pero no impacta la funcionalidad de generacion de datos directamente. Es un requisito transversal que complementa la funcionalidad principal.

**Prueba Independiente**: Se ejecuta cualquier notebook de generacion y se verifica que las impresiones en pantalla incluyan: parametros recibidos, tipo de storage, ruta construida, conteos de registros y tiempo de ejecucion.

**Escenarios de Aceptacion**:

1. **Dado** que el ingeniero ejecuta cualquiera de los 3 notebooks de generacion, **Cuando** inicia la ejecucion, **Entonces** se imprime en pantalla un bloque de resumen con todos los parametros recibidos via widget y los valores leidos de la tabla Parametros.
2. **Dado** que se ejecuta un notebook de generacion, **Cuando** se escribe el archivo parquet, **Entonces** se imprime en pantalla la ruta completa donde se almaceno, el numero de registros escritos y el tiempo de ejecucion de la operacion de escritura.
3. **Dado** que un notebook detecta una condicion excepcional (parquet previo corrupto, ruta inaccesible), **Cuando** ocurre la condicion, **Entonces** se imprime un mensaje explicativo en pantalla antes de detener la ejecucion.

---

### Historia de Usuario 5 - Compatibilidad con Almacenamiento Dinamico (Prioridad: P2)

El ingeniero de datos debe poder alternar entre almacenamiento en Unity Catalog Volumes y Amazon S3 sin modificar el codigo de los notebooks. La seleccion del tipo de almacenamiento se determina automaticamente a partir del parametro `TipoStorage` almacenado en la tabla Parametros de Unity Catalog. Los notebooks construyen las rutas completas dinamicamente segun el tipo seleccionado.

**Por que esta prioridad**: El almacenamiento dinamico es un requisito transversal del proyecto. Los notebooks deben funcionar con ambos tipos de storage, pero el caso principal (Volume) es suficiente para validar la generacion de datos.

**Prueba Independiente**: Se ejecuta un notebook de generacion con `TipoStorage=Volume` y se verifica la ruta resultante. Luego se cambia a `TipoStorage=AmazonS3` en la tabla Parametros y se verifica que la ruta se construya con formato S3.

**Escenarios de Aceptacion**:

1. **Dado** que `TipoStorage=Volume` en la tabla Parametros, **Cuando** se ejecuta un notebook de generacion, **Entonces** la ruta del parquet sigue el formato `/Volumes/<catalogo>/<esquema>/<volume>/<ruta_relativa>`.
2. **Dado** que `TipoStorage=AmazonS3` en la tabla Parametros, **Cuando** se ejecuta un notebook de generacion, **Entonces** la ruta del parquet sigue el formato `s3://<bucket>/<ruta_relativa>`.
3. **Dado** que el parametro `TipoStorage` contiene un valor no reconocido, **Cuando** se ejecuta un notebook de generacion, **Entonces** la ejecucion se detiene con un mensaje explicativo indicando los valores validos.

---

### Historia de Usuario 6 - Pruebas TDD para la Generacion de Parquets (Prioridad: P2)

El ingeniero de datos necesita un conjunto de pruebas basadas en Test-Driven Development que validen la correcta generacion de cada uno de los 3 archivos parquet. Las pruebas deben verificar la estructura de columnas, los tipos de datos, las cantidades de registros, las distribuciones esperadas, la integridad referencial entre entidades, el modelo de mutacion diferenciada del Maestro de Clientes, y la compatibilidad con el almacenamiento dinamico.

**Por que esta prioridad**: El TDD es un requisito obligatorio a partir del Incremento 2 segun las politicas del proyecto. Las pruebas garantizan la calidad de los datos generados.

**Prueba Independiente**: Se ejecuta el conjunto de pruebas TDD y se valida que cubran todos los escenarios criticos de los 3 generadores de parquets.

**Escenarios de Aceptacion**:

1. **Dado** que existen pruebas TDD para CMSTFL, **Cuando** se ejecutan, **Entonces** validan: 70 columnas, distribucion de tipos (41 StringType, 18 DateType, 9 LongType, 2 DoubleType), cantidad de registros, exclusion de nombres latinos, y modelo de mutacion diferenciada.
2. **Dado** que existen pruebas TDD para TRXPFL, **Cuando** se ejecutan, **Entonces** validan: 60 columnas, distribucion de tipos (7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType), integridad referencial con CMSTFL, y distribucion de tipos de transaccion (~60%/~30%/~10%).
3. **Dado** que existen pruebas TDD para BLNCFL, **Cuando** se ejecutan, **Entonces** validan: 100 columnas, distribucion de tipos (2 LongType, 29 StringType, 34 DoubleType, 35 DateType), relacion 1:1 con CMSTFL, y distribucion de tipos de cuenta (40%/30%/20%/10%).

---

### Casos Borde

- Que sucede cuando se ejecuta el notebook de Saldos (BLNCFL) o Transaccional (TRXPFL) sin haber generado previamente el Maestro de Clientes (CMSTFL)? El notebook debe detectarlo e informar al usuario.
- Que sucede cuando la cantidad de registros configurada es cero o negativa? El notebook debe validar y rechazar el valor.
- Que sucede cuando el parametro `fechaTransaccion` no tiene formato valido YYYY-MM-DD? El notebook debe validar el formato antes de procesar.
- Que sucede cuando la ruta de destino no es accesible (Volume no existe o bucket S3 sin permisos)? El notebook debe reportar el error con un mensaje claro.
- Que sucede cuando `rutaMaestroClienteExistente` contiene una ruta pero el parquet esta corrupto o incompleto (es decir, `spark.read.parquet(ruta)` lanza una excepcion)? El notebook debe capturar la excepcion, informar al usuario con un mensaje descriptivo (incluyendo la excepcion original), y proceder como si fuera la primera ejecucion (generacion base sin mutacion).
- Que sucede cuando los recursos de Databricks Free Edition son insuficientes para la volumetria configurada? El notebook debe detectar y reportar la situacion antes de fallar silenciosamente.

## Requisitos

### Requisitos Funcionales

> *Nota*: RF-023 y RF-024 fueron incorporados como adiciones posteriores al ciclo de clarificacion. Se preserva su numeracion original para mantener trazabilidad con tasks.md y contratos.

- **RF-001**: Los notebooks deben recibir los parametros de conexion a la tabla Parametros (`catalogoParametro`, `esquemaParametro`, `tablaParametros`) via `dbutils.widgets.text()` con valores por defecto recomendados.
- **RF-002**: Los notebooks deben leer de la tabla Parametros los siguientes valores de configuracion: `TipoStorage`, `catalogoVolume`, `esquemaVolume`, `nombreVolume`, `bucketS3`, para construir las rutas de almacenamiento dinamicamente.
- **RF-003**: El notebook de Maestro de Clientes (NbGenerarMaestroCliente.py) debe recibir via `dbutils.widgets.text()` los parametros: `cantidadClientes` (por defecto "50000"), `rutaRelativaMaestroCliente` (ruta relativa dentro del storage donde se almacenara el parquet), `rutaMaestroClienteExistente` (por defecto vacio — ruta relativa del maestro de clientes existente para re-ejecuciones; vacio en la primera ejecucion), `porcentajeMutacion` (por defecto "0.20"), `porcentajeNuevos` (por defecto "0.006"), `camposMutacion` (por defecto "CUSNM,CUSLN,CUSMD,CUSFN,CUSAD,CUSA2,CUSCT,CUSST,CUSZP,CUSPH,CUSMB,CUSEM,CUSMS,CUSOC,CUSED"), `montoMinimo` (por defecto "10"), `montoMaximo` (por defecto "100000"), `numeroParticiones` (por defecto "8") y `shufflePartitions` (por defecto "8").
- **RF-004**: El notebook de Transaccional (NbGenerarTransaccionalCliente.py) debe recibir via `dbutils.widgets.text()` los parametros: `cantidadTransacciones` (por defecto "150000"), `fechaTransaccion` (formato YYYY-MM-DD, sin valor por defecto), `rutaRelativaTransaccional` (ruta relativa dentro del storage), `rutaRelativaMaestroCliente` (por defecto "LSDP_Base/As400/MaestroCliente/", ruta para leer el Maestro de Clientes existente), `montoMinimo` (por defecto "10"), `montoMaximo` (por defecto "100000"), `numeroParticiones` (por defecto "8") y `shufflePartitions` (por defecto "8").
- **RF-005**: El notebook de Saldos (NbGenerarSaldosCliente.py) debe recibir via `dbutils.widgets.text()` los parametros: `rutaRelativaSaldoCliente` (ruta relativa dentro del storage), `rutaRelativaMaestroCliente` (por defecto "LSDP_Base/As400/MaestroCliente/", ruta para leer el Maestro de Clientes existente), `montoMinimo` (por defecto "10"), `montoMaximo` (por defecto "100000"), `numeroParticiones` (por defecto "8") y `shufflePartitions` (por defecto "8"). La cantidad de registros se determina automaticamente a partir del Maestro de Clientes existente.
- **RF-006**: El notebook CMSTFL debe generar un parquet con exactamente 70 columnas distribuidas en: 41 StringType, 18 DateType, 9 LongType y 2 DoubleType. Los nombres de campos deben ser lo mas similares posible a los del sistema AS400.
- **RF-007**: El notebook TRXPFL debe generar un parquet con exactamente 60 columnas distribuidas en: 7 StringType, 19 DateType, 2 TimestampType, 2 LongType y 30 DoubleType. Cada transaccion debe tener un CUSTID valido que exista en el Maestro de Clientes.
- **RF-008**: El notebook BLNCFL debe generar un parquet con exactamente 100 columnas distribuidas en: 2 LongType, 29 StringType, 34 DoubleType y 35 DateType. Debe existir exactamente un registro de saldo por cada cliente del Maestro.
- **RF-009**: El notebook CMSTFL debe implementar el modelo de mutacion diferenciada: si el parametro `rutaMaestroClienteExistente` esta vacio (primera ejecucion), genera registros base segun `cantidadClientes`; si `rutaMaestroClienteExistente` contiene una ruta (re-ejecuciones), lee el parquet existente desde esa ruta, conserva los clientes, muta el porcentaje definido en `porcentajeMutacion` (por defecto 0.20, equivalente a 20%) en los campos listados en `camposMutacion` (por defecto 15 campos demograficos), agrega el porcentaje definido en `porcentajeNuevos` (por defecto 0.006, equivalente a 0.6%) de clientes nuevos, y sobreescribe (overwrite) el parquet en la ruta destino `rutaRelativaMaestroCliente` con el resultado consolidado.
- **RF-010**: El notebook TRXPFL debe generar un lote completamente nuevo de transacciones por cada ejecucion, distribuidas en 15 tipos con proporciones: alta frecuencia ~60% (CATM, DATM, CMPR, TINT, DPST), media frecuencia ~30% (PGSL, TEXT, RTRO, PGSV, NMNA, INTR) y baja frecuencia ~10% (ADSL, IMPT, DMCL, CMSN).
- **RF-011**: El notebook BLNCFL debe regenerarse completamente en cada ejecucion sobreescribiendo (overwrite) el parquet anterior, con 4 tipos de cuenta distribuidos: AHRO (40%), CRTE (30%), PRES (20%) e INVR (10%).
- **RF-023**: Los 3 notebooks deben usar modo overwrite al escribir los parquets: cada ejecucion reemplaza el archivo parquet anterior en la misma ruta destino. *(Consolida el modo de escritura overwrite ya implicito en RF-009, RF-010 y RF-011.)*
- **RF-024**: El notebook CMSTFL debe intentar usar la biblioteca `Faker` para generar datos realistas (nombres hebreos, egipcios e ingleses, direcciones, etc.) con restriccion etnica obligatoria. Los notebooks TRXPFL y BLNCFL pueden usar `Faker` opcionalmente para campos descriptivos (nombres de gerente, descripciones de transaccion) sin restriccion etnica. Si `Faker` no esta disponible en el entorno Serverless, los notebooks deben caer automaticamente a listas estaticas de datos embebidas en el codigo como mecanismo de fallback, sin interrumpir la ejecucion.
- **RF-012**: Los nombres y apellidos generados para el Maestro de Clientes deben ser exclusivamente hebreos, egipcios e ingleses. Queda prohibido el uso de nombres y apellidos de origen latino.
- **RF-013**: Las rutas de almacenamiento deben construirse dinamicamente segun el valor de `TipoStorage` leido de la tabla Parametros: para `"Volume"` usar formato `/Volumes/<catalogo>/<esquema>/<volume>/<ruta_relativa>` y para `"AmazonS3"` usar formato `s3://<bucket>/<ruta_relativa>`.
- **RF-014**: Cada notebook debe imprimir al inicio un bloque de resumen con todos los parametros recibidos (widgets y tabla Parametros), el tipo de storage seleccionado y las rutas construidas. Al finalizar, debe imprimir un resumen con conteos de registros y tiempos de ejecucion.
- **RF-015**: Cada notebook debe validar que los parametros obligatorios no esten vacios y que los valores numericos sean positivos antes de iniciar cualquier operacion de generacion. Adicionalmente, `porcentajeMutacion` y `porcentajeNuevos` deben estar en el rango (0, 1.0] (escala 0-1).
- **RF-016**: Los notebooks deben ser optimizados para Databricks Free Edition: usar el numero de particiones configurado en el parametro `numeroParticiones` (por defecto 8) para coalesce, y configurar `spark.sql.shuffle.partitions` al valor del parametro `shufflePartitions` (por defecto 8). La deteccion de recursos insuficientes se implementa con un bloque try/except alrededor de la operacion de escritura del parquet: si la escritura falla, se imprime un mensaje explicativo sugiriendo reducir la volumetria via los parametros del widget.
- **RF-017**: Los notebooks deben ser 100% compatibles con Serverless Compute: queda prohibido el uso de `spark.sparkContext` y cualquier acceso directo al JVM del driver.
- **RF-018**: Todo el codigo debe ser dinamico via parametros; queda prohibido el uso de valores constantes hardcodeadas.
- **RF-019**: Los notebooks de generacion de parquets deben ubicarse en el directorio `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/`.
- **RF-020**: Debe existir un conjunto de pruebas TDD ubicadas en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/` que validen la estructura, tipos de datos, cantidades, distribuciones e integridad referencial de los 3 parquets generados.
- **RF-021**: Los notebooks deben ser archivos `.py` con formato de notebook compatible con Databricks, con todo el codigo y comentarios en idioma espanol. Los nombres de archivo deben usar formato PascalCase con prefijo "Nb".
- **RF-022**: El notebook TRXPFL debe recibir la fecha de la transaccion como parametro en formato YYYY-MM-DD y validar que el formato sea correcto antes de procesar.

### Entidades Clave

- **CMSTFL (Maestro de Clientes)**: Representa la tabla maestra de clientes del sistema AS400 de una entidad bancaria. Contiene 70 columnas con datos demograficos, identificacion y atributos del cliente. Clave primaria: CUSTID (LongType, numerico secuencial comenzando en 1). Relacion: origen de las claves foraneas en TRXPFL y BLNCFL.
- **TRXPFL (Transaccional)**: Representa las transacciones bancarias del sistema AS400. Contiene 60 columnas con datos de transacciones, montos, fechas y tipos. Clave primaria: TRXID (StringType, alfanumerico con prefijo del tipo de transaccion + secuencial, ej: "CATM00000001"). Relacion: CUSTID como clave foranea referenciando a CMSTFL.
- **BLNCFL (Saldos de Clientes)**: Representa los saldos y cuentas de los clientes del sistema AS400. Contiene 100 columnas con datos de saldos, tipos de cuenta y fechas. Relacion 1:1 con CMSTFL a traves de CUSTID.
- **Tabla Parametros**: Tabla Delta existente en Unity Catalog (creada en el Incremento 1) con columnas `Clave` y `Valor`. Provee los valores de configuracion dinamicos del proyecto (TipoStorage, datos de Volume, datos de S3, etc.).

### Parametros de los Notebooks — Origen de cada Valor

| Parametro | Origen | Notebook(s) | Descripcion |
| --------- | ------ | ----------- | ----------- |
| catalogoParametro | dbutils.widgets (defecto: "control") | Los 3 | Catalogo de la tabla Parametros |
| esquemaParametro | dbutils.widgets (defecto: "regional") | Los 3 | Esquema de la tabla Parametros |
| tablaParametros | dbutils.widgets (defecto: "Parametros") | Los 3 | Nombre de la tabla Parametros |
| cantidadClientes | dbutils.widgets (defecto: "50000") | CMSTFL | Cantidad base de registros de clientes |
| cantidadTransacciones | dbutils.widgets (defecto: "150000") | TRXPFL | Cantidad de transacciones a generar |
| fechaTransaccion | dbutils.widgets (sin defecto) | TRXPFL | Fecha de las transacciones (YYYY-MM-DD) |
| rutaRelativaMaestroCliente | dbutils.widgets (defecto: "LSDP_Base/As400/MaestroCliente/") | CMSTFL | Ruta relativa donde se almacena el parquet CMSTFL |
| rutaRelativaTransaccional | dbutils.widgets (defecto: "LSDP_Base/As400/Transaccional/") | TRXPFL | Ruta relativa donde se almacena el parquet TRXPFL |
| rutaRelativaSaldoCliente | dbutils.widgets (defecto: "LSDP_Base/As400/SaldoCliente/") | BLNCFL | Ruta relativa donde se almacena el parquet BLNCFL |
| rutaRelativaMaestroCliente (lectura) | dbutils.widgets (defecto: "LSDP_Base/As400/MaestroCliente/") | TRXPFL, BLNCFL | Ruta para leer el Maestro de Clientes existente |
| rutaMaestroClienteExistente | dbutils.widgets (defecto: vacio) | CMSTFL | Ruta relativa del maestro de clientes existente para re-ejecuciones; vacio en primera ejecucion |
| porcentajeMutacion | dbutils.widgets (defecto: "0.20") | CMSTFL | Porcentaje de registros existentes a mutar en re-ejecuciones (escala 0-1, donde 0.20 = 20%) |
| porcentajeNuevos | dbutils.widgets (defecto: "0.006") | CMSTFL | Porcentaje de registros nuevos a agregar en re-ejecuciones (escala 0-1, donde 0.006 = 0.6%) |
| camposMutacion | dbutils.widgets (defecto: "CUSNM,CUSLN,CUSMD,CUSFN,CUSAD,CUSA2,CUSCT,CUSST,CUSZP,CUSPH,CUSMB,CUSEM,CUSMS,CUSOC,CUSED") | CMSTFL | Lista de campos demograficos a mutar, separados por coma |
| montoMinimo | dbutils.widgets (defecto: "10") | Los 3 | Valor minimo para columnas de tipo monto (DoubleType) |
| montoMaximo | dbutils.widgets (defecto: "100000") | Los 3 | Valor maximo para columnas de tipo monto (DoubleType) |
| numeroParticiones | dbutils.widgets (defecto: "8") | Los 3 | Numero de particiones para coalesce al escribir parquets |
| shufflePartitions | dbutils.widgets (defecto: "8") | Los 3 | Valor de spark.sql.shuffle.partitions |
| TipoStorage | Tabla Parametros | Los 3 | Tipo de almacenamiento: "Volume" o "AmazonS3" |
| catalogoVolume | Tabla Parametros | Los 3 | Catalogo del Volume (si TipoStorage=Volume) |
| esquemaVolume | Tabla Parametros | Los 3 | Esquema del Volume (si TipoStorage=Volume) |
| nombreVolume | Tabla Parametros | Los 3 | Nombre del Volume (si TipoStorage=Volume) |
| bucketS3 | Tabla Parametros | Los 3 | Bucket S3 (si TipoStorage=AmazonS3) |

## Criterios de Exito

### Resultados Medibles

- **CE-001**: Los 3 notebooks generan exitosamente sus respectivos archivos parquet en una sola ejecucion sin errores, con las cantidades de registros configuradas.
- **CE-002**: El parquet CMSTFL cumple con exactamente 70 columnas en la distribucion de tipos especificada (41/18/9/2) y la cantidad de registros configurada.
- **CE-003**: El parquet TRXPFL cumple con exactamente 60 columnas en la distribucion de tipos especificada (7/19/2/2/30) y la distribucion de tipos de transaccion respeta las proporciones (~60%/~30%/~10%) con una tolerancia del 5%.
- **CE-004**: El parquet BLNCFL cumple con exactamente 100 columnas en la distribucion de tipos especificada (2/29/34/35) y la relacion 1:1 con CMSTFL es exacta.
- **CE-005**: El modelo de mutacion diferenciada del CMSTFL produce resultados verificables: el porcentaje definido en `porcentajeNuevos` (por defecto 0.006, equivalente a 0.6%) de registros nuevos y el porcentaje definido en `porcentajeMutacion` (por defecto 0.20, equivalente a 20%) de registros mutados en los campos definidos en `camposMutacion` durante re-ejecuciones.
- **CE-006**: El 100% de los registros del TRXPFL tienen un CUSTID que existe en el CMSTFL (integridad referencial completa).
- **CE-007**: La distribucion de tipos de cuenta en BLNCFL respeta las proporciones (40%/30%/20%/10%) con una tolerancia del 3%.
- **CE-008**: Los notebooks generan parquets exitosamente tanto con `TipoStorage=Volume` como con `TipoStorage=AmazonS3`, construyendo rutas en el formato correcto para cada tipo.
- **CE-009**: Cada notebook imprime en pantalla al menos: parametros recibidos, tipo de storage, ruta construida, conteo de registros escritos y tiempo de ejecucion total.
- **CE-010**: El conjunto de pruebas TDD ejecuta exitosamente y cubre todos los escenarios criticos de estructura, tipos de datos, cantidades, distribuciones e integridad referencial.
- **CE-011**: Los notebooks se ejecutan exitosamente en Databricks Free Edition con la volumetria por defecto (50,000 clientes, 150,000 transacciones) sin agotar los recursos del entorno serverless.
- **CE-012**: Ningun notebook utiliza `spark.sparkContext` ni accede directamente al JVM, garantizando compatibilidad total con Serverless Compute.

## Supuestos

- La tabla Parametros en Unity Catalog ya fue creada exitosamente por el Incremento 1 (`conf/NbConfiguracionInicial.py`) y contiene los 15 registros de configuracion definidos.
- Los catalogos (`control`, `bronce`, `plata`, `oro`) y esquemas (`regional`) ya fueron creados por el Incremento 1.
- El Volume gestionado (`datos_bronce`) ya fue creado por el Incremento 1 y esta accesible.
- El workspace de Databricks Free Edition esta activo con Unity Catalog habilitado y Serverless Compute disponible.
- Las extensiones Databricks Extension for Visual Studio Code y Databricks Driver for SQLTools estan correctamente instaladas y configuradas.
- Para el caso de `TipoStorage=AmazonS3`, se asume que las credenciales IAM ya estan configuradas en el workspace de Databricks.
- Los nombres de campos del AS400 seguiran convenciones tipicas del sistema (nombres cortos, alfanumericos, mayusculas) para mantener similitud con el sistema real.
- Los valores por defecto de volumetria (50,000 clientes, 150,000 transacciones) estan dimensionados para ejecutarse dentro de las cuotas de Databricks Free Edition.
- Las utilidades `LsdpConexionParametros.py` y `LsdpConstructorRutas.py` son exclusivas del LSDP y NO se crean en este incremento. Se crearan en el Incremento 3. En el Incremento 2, la logica de lectura de la tabla Parametros y la construccion de rutas dinamicas se implementa directamente dentro de cada notebook de generacion de parquets.
