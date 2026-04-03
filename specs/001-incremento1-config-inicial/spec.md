# Especificacion de Feature: Incremento 1 - Research Inicial y Configuracion Base

**Rama de Feature**: `001-incremento1-config-inicial`
**Creado**: 2026-04-03
**Estado**: Borrador
**Entrada**: Descripcion del usuario: "Incremento 1: Research inicial, configuracion inicial y tabla Parametros en Unity Catalog, incluyendo creacion de Volume"

## Clarificaciones

### Sesion 2026-04-03

- P: Cual es el mecanismo de idempotencia para la tabla Parametros? → R: `CREATE OR REPLACE TABLE` + INSERT de todos los registros (recrea la tabla completa cada vez).
- P: El notebook debe crear automaticamente los catalogos y esquemas o se asume que ya existen? → R: El notebook crea automaticamente todos los catalogos y esquemas necesarios con `IF NOT EXISTS` antes de crear la tabla y el Volume.
- P: Donde se documentan los hallazgos y decisiones de la investigacion inicial? → R: Directamente en SYSTEM.md, enriqueciendo las secciones existentes o agregando una seccion de decisiones, manteniendo la base de verdad centralizada.
- P: Como recibe el notebook de configuracion inicial sus parametros de entrada? → R: Mediante `dbutils.widgets` con valores por defecto, mecanismo nativo y estandar de Databricks compatible con UI interactiva y la extension de VS Code.
- P: Todos los catalogos comparten el mismo esquema o cada uno tiene esquema independiente? → R: Cada catalogo tiene su propio parametro de esquema (`esquemaBronce`, `esquemaPlata`, `esquemaOro`, `esquemaControl`), todos con valor por defecto `regional`, permitiendo flexibilidad futura.

## Escenarios de Usuario y Pruebas *(obligatorio)*

### Historia de Usuario 1 - Investigacion Inicial del Proyecto (Prioridad: P1)

Como Ingeniero de Datos, necesito realizar una investigacion exhaustiva de Lakeflow Spark Declarative Pipelines (LSDP) y de las extensiones de Databricks para Visual Studio Code, para comprender las capacidades, limitaciones y mejores practicas que guiaran las decisiones de arquitectura de todo el proyecto.

**Por que esta prioridad**: Sin esta investigacion fundamental, todas las decisiones tecnicas posteriores (constitution, specify, plan) carecerian de base solida. Es el prerequisito para que los incrementos 2 al 6 se ejecuten correctamente.

**Prueba Independiente**: Se puede validar verificando que existe un documento de decisiones con hallazgos concretos sobre LSDP (decoradores `@dp.table`, `@dp.materialized_view`, compatibilidad serverless, AutoLoader) y sobre las extensiones de Databricks para VS Code (conectividad al workspace, ejecucion de notebooks, computo disponible).

**Escenarios de Aceptacion**:

1. **Dado** que el proyecto parte de cero en Databricks Free Edition, **Cuando** se complete la investigacion, **Entonces** se habran documentado las capacidades y restricciones de LSDP relevantes para el proyecto (API nueva vs. legacy, decoradores disponibles, propiedades de tablas soportadas, comportamiento de AutoLoader).
2. **Dado** que las extensiones de Databricks para VS Code son un requisito del proyecto, **Cuando** se complete la investigacion, **Entonces** se habra verificado que la extension Databricks Extension for Visual Studio Code y Databricks Driver for SQLTools estan instaladas y configuradas, permitiendo acceder al workspace de Databricks Free Edition y listar los computos disponibles.
3. **Dado** que el proyecto requiere decisiones de arquitectura informadas, **Cuando** se finalice la investigacion, **Entonces** se habran tomado las decisiones clave sobre: estructura de catalogos Unity Catalog, esquema de parametrizacion, patron de storage dinamico (Volume vs. S3) y estrategia de observabilidad, y estas decisiones quedaran documentadas directamente en SYSTEM.md como base de verdad centralizada.

---

### Historia de Usuario 2 - Creacion de la Tabla de Parametros (Prioridad: P1)

Como Ingeniero de Datos, necesito un mecanismo centralizado de configuracion donde todos los parametros del proyecto esten almacenados como tabla Delta en Unity Catalog, para que todos los notebooks y el pipeline LSDP puedan leer la configuracion de forma consistente y sin valores hardcodeados.

**Por que esta prioridad**: La tabla de parametros es la columna vertebral de configuracion de todo el proyecto. Sin ella, ningun otro componente (generadores de parquets, utilidades, transformaciones LSDP) puede funcionar correctamente. Tiene la misma prioridad critica que la investigacion.

**Prueba Independiente**: Se puede validar ejecutando el notebook de configuracion inicial y verificando que la tabla se crea correctamente con todas las columnas y registros esperados, y que la re-ejecucion no genera errores ni duplicados.

**Escenarios de Aceptacion**:

1. **Dado** un workspace de Databricks Free Edition con Unity Catalog habilitado, **Cuando** se ejecute el notebook de configuracion inicial proporcionando el catalogo, esquema y nombre de tabla como parametros, **Entonces** se creara una tabla Delta con exactamente 2 columnas (`Clave` de tipo string y `Valor` de tipo string).
2. **Dado** que la tabla de parametros fue creada exitosamente, **Cuando** se consulte su contenido, **Entonces** contendra todos los 15 registros de parametros definidos para el proyecto (catalogoBronce, esquemaBronce, contenedorBronce, TipoStorage, catalogoVolume, esquemaVolume, nombreVolume, bucketS3, prefijoS3, DirectorioBronce, catalogoPlata, esquemaPlata, catalogoOro, esquemaOro, esquemaControl) con sus valores correspondientes.
3. **Dado** que la tabla de parametros ya existe con datos previos, **Cuando** se re-ejecute el notebook de configuracion inicial con los mismos parametros, **Entonces** la tabla se recreara completamente mediante `CREATE OR REPLACE TABLE` seguido de INSERT de todos los registros, garantizando un estado completamente conocido sin duplicados ni errores.
4. **Dado** que el notebook se ejecuto exitosamente, **Cuando** se revise la salida en pantalla, **Entonces** se habra impreso: los parametros recibidos, la ruta completa de la tabla creada y todos los registros insertados con sus valores para verificacion visual.

---

### Historia de Usuario 3 - Creacion del Volume en Unity Catalog (Prioridad: P1)

Como Ingeniero de Datos, necesito que durante la configuracion inicial se cree automaticamente el Volume de Unity Catalog que servira como almacenamiento de archivos parquets, para que el proyecto tenga disponible el destino de almacenamiento desde el primer momento sin intervencion manual adicional.

**Por que esta prioridad**: El Volume es el destino fisico donde se almacenaran los parquets generados en el Incremento 2. Su creacion es parte integral de la configuracion base y debe existir antes de que cualquier otro componente intente escribir datos. Se incluye en el mismo notebook de configuracion inicial.

**Prueba Independiente**: Se puede validar ejecutando el notebook de configuracion inicial y verificando que el Volume se crea correctamente en el catalogo y esquema especificados en la tabla de Parametros, usando el nombre establecido en el registro `nombreVolume`.

**Escenarios de Aceptacion**:

1. **Dado** que la tabla de Parametros fue creada exitosamente con el registro `nombreVolume`, **Cuando** el notebook de configuracion inicial proceda a crear el Volume, **Entonces** se creara un Volume gestionado en Unity Catalog usando el catalogo del registro `catalogoVolume`, el esquema del registro `esquemaVolume` y el nombre del registro `nombreVolume` de la tabla de Parametros.
2. **Dado** que el Volume ya existe de una ejecucion previa, **Cuando** se re-ejecute el notebook de configuracion inicial, **Entonces** no se produciran errores (el proceso es idempotente) y se reportara en pantalla que el Volume ya existia.
3. **Dado** que el notebook se ejecuto correctamente, **Cuando** se revise la salida en pantalla, **Entonces** se habra impreso el nombre completo del Volume creado (catalogo.esquema.volumen) y su estado (creado o ya existente).

---

### Casos Borde

- Que ocurre si la creacion automatica de un catalogo o esquema falla por permisos insuficientes o errores del servidor? El notebook debe capturar el error y reportarlo claramente en pantalla, indicando cual catalogo o esquema no pudo crearse y la causa del fallo.
- Que ocurre si los parametros del notebook (catalogo, esquema, tabla) se proporcionan vacios o nulos? El notebook debe validar los parametros al inicio y detener la ejecucion con un mensaje explicativo.
- Que ocurre si la tabla de Parametros existe pero con una estructura de columnas diferente? La estrategia `CREATE OR REPLACE TABLE` resuelve este caso implicitamente: la tabla se recrea completa con la estructura correcta en cada ejecucion, sin necesidad de deteccion explicita de inconsistencias.
- Que ocurre si no se tienen permisos suficientes para crear tablas o Volumes en el catalogo especificado? Las excepciones de `spark.sql()` se propagan automaticamente con stack trace visible, cumpliendo el Principio IV de la constitution (Observabilidad). No se requiere captura explicita adicional.
- Que ocurre si la cuota de Databricks Free Edition se ha agotado al momento de ejecutar la configuracion? Las excepciones del runtime Serverless se propagan automaticamente con mensajes descriptivos. No se requiere captura explicita adicional.

## Requisitos *(obligatorio)*

### Requisitos Funcionales

- **RF-001**: El sistema DEBE proporcionar un notebook de configuracion inicial que reciba como parametros el nombre del catalogo, esquema y tabla para la tabla de parametros, utilizando `dbutils.widgets` con valores por defecto como mecanismo de entrada (compatible con la UI de Databricks y la extension de VS Code).
- **RF-002**: El sistema DEBE crear una tabla Delta con exactamente dos columnas: `Clave` (string) y `Valor` (string) en la ubicacion especificada por los parametros.
- **RF-003**: El sistema DEBE insertar todos los registros de parametros del proyecto: `catalogoBronce`, `esquemaBronce`, `contenedorBronce`, `TipoStorage`, `catalogoVolume`, `esquemaVolume`, `nombreVolume`, `bucketS3`, `prefijoS3`, `DirectorioBronce`, `catalogoPlata`, `esquemaPlata`, `catalogoOro`, `esquemaOro`, `esquemaControl`.
- **RF-004**: El sistema DEBE ser idempotente mediante `CREATE OR REPLACE TABLE` seguido de INSERT de todos los registros, recreando la tabla completa en cada ejecucion para garantizar un estado conocido sin duplicados ni datos corruptos.
- **RF-005**: El sistema DEBE imprimir en pantalla la ubicacion completa de la tabla creada y cada registro insertado con su clave y valor durante la insercion.
- **RF-006**: El sistema DEBE crear un Volume gestionado en Unity Catalog utilizando los valores de `catalogoVolume`, `esquemaVolume` y `nombreVolume` leidos de la tabla de Parametros recientemente creada.
- **RF-007**: La creacion del Volume DEBE ser idempotente: si el Volume ya existe, no debe generar error y debe reportar su existencia en pantalla.
- **RF-008**: El sistema DEBE validar que los parametros de entrada no esten vacios antes de proceder con la creacion de la tabla y el Volume.
- **RF-009**: El sistema DEBE reportar tiempos de ejecucion de todas las operaciones DDL: creacion de catalogos, creacion de esquema de parametros, creacion de tabla, insercion de registros, creacion de esquemas medallion y creacion de Volume.
- **RF-010**: El sistema DEBE imprimir un bloque de resumen al inicio con todos los parametros recibidos y un bloque de resumen al final con metricas de ejecucion.
- **RF-011**: El sistema NO DEBE contener valores hardcodeados. Todos los valores de configuracion deben provenir de los parametros de entrada o de la tabla de Parametros.
- **RF-012**: El notebook DEBE documentarse completamente con comentarios en espanol siguiendo los estandares del proyecto.
- **RF-013**: El sistema DEBE crear automaticamente todos los catalogos (`control`, `bronce`, `plata`, `oro`) con `IF NOT EXISTS` y el esquema de la tabla Parametros ANTES de crear la tabla. Posteriormente, tras insertar los registros, DEBE leer los valores de esquema (`esquemaControl`, `esquemaBronce`, `esquemaPlata`, `esquemaOro`) de la tabla y crear los esquemas restantes con `IF NOT EXISTS`, garantizando que el entorno queda completamente preparado en una sola ejecucion.
- **RF-014**: Los hallazgos y decisiones de la investigacion inicial DEBEN documentarse directamente en el archivo SYSTEM.md existente, enriqueciendo las secciones actuales o agregando una seccion dedicada de decisiones, para mantener la base de verdad centralizada del proyecto.

### Entidades Clave

- **Tabla Parametros**: Tabla Delta centralizada en Unity Catalog que almacena todos los pares clave-valor de configuracion del proyecto. Columnas: `Clave` (identificador unico del parametro) y `Valor` (valor de configuracion asociado). Contiene 15 registros que definen: catalogos y esquemas de cada medalla (diferenciados por catalogo), configuracion de almacenamiento (Volume y S3), y directorios de datos.
- **Volume Unity Catalog**: Recurso gestionado de Unity Catalog que sirve como almacenamiento de archivos parquets. Se identifica por tres componentes: catalogo, esquema y nombre. Su nombre se obtiene dinamicamente del registro `nombreVolume` de la tabla de Parametros.

## Criterios de Exito *(obligatorio)*

### Resultados Medibles

- **CE-001**: El notebook de configuracion inicial se ejecuta completamente en menos de 2 minutos en Databricks Free Edition.
- **CE-002**: La tabla de configuracion contiene exactamente 15 registros de parametros despues de la primera ejecucion.
- **CE-003**: Re-ejecutar el notebook 3 veces consecutivas produce el mismo resultado final: 15 registros en la tabla, sin duplicados y sin errores.
- **CE-004**: El 100% de las operaciones criticas (creacion de tabla, inserciones, creacion de Volume) quedan registradas en la salida de pantalla con mensajes claros.
- **CE-005**: El Volume queda creado y accesible para escritura despues de la ejecucion del notebook.
- **CE-006**: La investigacion inicial produce decisiones documentadas que cubren al menos: capacidades de LSDP, compatibilidad serverless, patron de storage dinamico, esquema de catalogos y estrategia de observabilidad.
- **CE-007**: Las extensiones Databricks Extension for Visual Studio Code y Databricks Driver for SQLTools quedan verificadas como funcionales (conexion al workspace y listado de computos).
- **CE-008**: Ningun parametro del notebook esta hardcodeado; todos los valores de configuracion son proporcionados dinamicamente por el usuario o leidos de la tabla.

## Supuestos

- El usuario ya tiene un workspace de Databricks Free Edition creado y accesible.
- Unity Catalog esta habilitado en el workspace de Databricks Free Edition.
- Los catalogos (`control`, `bronce`, `plata`, `oro`) y sus respectivos esquemas (parametrizados como `esquemaControl`, `esquemaBronce`, `esquemaPlata`, `esquemaOro`, todos con valor por defecto `regional`) seran creados automaticamente por el notebook de configuracion inicial con `IF NOT EXISTS`. No se requiere intervencion manual previa del usuario para crear estos recursos.
- El computo Serverless esta disponible en el workspace de Databricks Free Edition.
- Visual Studio Code esta instalado en la maquina del usuario con acceso a internet para la instalacion de extensiones.
- Este incremento esta excluido del requisito de TDD (pruebas Test-Driven Development), segun lo establecido en las politicas del proyecto.
- El notebook de configuracion inicial se ejecuta una sola vez como paso de setup del proyecto, aunque puede re-ejecutarse de forma segura por su naturaleza idempotente.
- Los valores por defecto de los parametros de la tabla (`TipoStorage = "Volume"`, `catalogoBronce = "bronce"`, etc.) representan la configuracion recomendada y pueden ser modificados por el usuario antes de la ejecucion.
