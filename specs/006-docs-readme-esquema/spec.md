# Especificacion de Feature: Incremento 6 - Documentacion, README y Sustitucion de Esquemas

**Feature Branch**: `006-docs-readme-esquema`
**Creado**: 2026-04-06
**Estado**: Borrador
**Entrada**: Descripcion del usuario: Basarse en el archivo SYSTEM.md seccion Specify, Incremento 6.

## Clarificaciones

### Sesion 2026-04-06

- P: Cual debe ser la granularidad de documentacion de campos en el ModeladoDatos.md? -> R: Documentar TODOS los campos de las 10 entidades (diccionario completo, ~500 filas de tablas)
- P: Deben sustituirse tambien las ocurrencias de "regional" dentro de nombres de entidad de 3 partes en specs anteriores? -> R: Si, sustituir en TODAS las ocurrencias de esquema (valores de parametro, nombres de entidad de 3 partes, ejemplos de configuracion)

## Escenarios de Usuario y Pruebas

### Historia de Usuario 1 - Manual Tecnico Completo (Prioridad: P1)

Como Ingeniero de Datos que se incorpora al proyecto o necesita consultar detalles tecnicos, necesito un documento `ManualTecnico.md` en la carpeta `docs/` que me explique en detalle todos los aspectos tecnicos de la solucion: los decoradores `@dp.table` y `@dp.materialized_view`, el paradigma declarativo de LSDP, las propiedades de las tablas delta y vistas materializadas, las operaciones con la API de DataFrames de Spark, todos los parametros del pipeline y notebooks, la tabla Parametros con detalle de claves y valores, y las dependencias que el usuario debe tener configuradas (Databricks Free Edition con Unity Catalog, catalogos y esquemas creados, y opcionalmente acceso a Amazon S3).

**Por que esta prioridad**: Es el documento fundacional que centraliza todo el conocimiento tecnico del proyecto. Sin el, cualquier persona nueva no podra entender la solucion y sus componentes.

**Prueba Independiente**: Se puede verificar que el documento existe en `docs/ManualTecnico.md`, que cubre las 7 secciones requeridas (decoradores, paradigma declarativo, propiedades Delta, operaciones DataFrame, parametros pipeline/notebooks, tabla Parametros, dependencias) y que toda la informacion es consistente con el codigo implementado en los incrementos 1-5.

**Escenarios de Aceptacion**:

1. **Dado** que el proyecto tiene 5 incrementos implementados, **Cuando** un ingeniero consulta `docs/ManualTecnico.md`, **Entonces** encuentra documentacion de los decoradores `@dp.table` y `@dp.materialized_view` con ejemplos concretos del proyecto.
2. **Dado** que el manual tecnico existe, **Cuando** un ingeniero busca como se configuran los parametros del pipeline LSDP, **Entonces** encuentra la lista completa de parametros con tipo, ejemplo y descripcion.
3. **Dado** que el manual tecnico existe, **Cuando** un ingeniero nuevo necesita saber que dependencias configurar, **Entonces** encuentra una seccion de dependencias que detalla Databricks Free Edition, Unity Catalog, catalogos, esquemas y opcionalmente Amazon S3.

---

### Historia de Usuario 2 - Modelado de Datos Completo (Prioridad: P1)

Como Ingeniero de Datos o Analista de Datos, necesito un documento `ModeladoDatos.md` en la carpeta `docs/` que contenga el diccionario de datos completo con las 10 entidades del proyecto (3 Parquets AS400, 3 Streaming Tables de Bronce, 2 Vistas Materializadas de Plata, 2 Vistas Materializadas de Oro), incluyendo campos, tipos de datos, descripciones, logica de campos calculados y diagrama de linaje de datos.

**Por que esta prioridad**: El diccionario de datos es esencial para comprender la estructura de datos del proyecto y es referencia obligatoria para cualquier consulta, extension o modificacion futura.

**Prueba Independiente**: Se puede verificar que el documento existe en `docs/ModeladoDatos.md`, que documenta las 10 entidades con sus campos, tipos de datos, descripciones y logica de campos calculados, y que incluye un diagrama de linaje de datos.

**Escenarios de Aceptacion**:

1. **Dado** que las 10 entidades del proyecto estan implementadas, **Cuando** un ingeniero consulta `docs/ModeladoDatos.md`, **Entonces** encuentra las 10 entidades documentadas con campos, tipos, descripciones y campos calculados.
2. **Dado** que el documento de modelado existe, **Cuando** un ingeniero necesita entender el flujo de datos desde los parquets AS400 hasta las vistas de oro, **Entonces** encuentra un diagrama de linaje que muestra las transformaciones entre las capas del medallion.
3. **Dado** que el documento de modelado existe, **Cuando** un ingeniero busca la logica de un campo calculado como `clasificacion_riesgo_cliente`, **Entonces** encuentra la documentacion de la formula o logica utilizada para calcular ese campo.

---

### Historia de Usuario 3 - Actualizacion del README Profesional (Prioridad: P1)

Como visitante del repositorio (Ingeniero de Datos, Arquitecto, Evaluador tecnico), necesito que el archivo `README.md` tenga un formato profesional, al nivel de las grandes empresas de tecnologia (Netflix, Spotify, Uber, Microsoft, Amazon), que resalte todos los elementos relevantes del proyecto, haga referencia al `ManualTecnico.md`, al `ModeladoDatos.md` y al `SYSTEM.md`, y destaque que el desarrollo de este laboratorio es con IA asistida usando GitHub Copilot y el framework spec-kit.

**Por que esta prioridad**: El README es la primera impresion del repositorio. Un README profesional comunica seriedad, calidad y facilita la adopcion del proyecto.

**Prueba Independiente**: Se puede verificar que el `README.md` tiene formato profesional, contiene secciones claras (descripcion, arquitectura, stack tecnologico, estructura del proyecto, guia de inicio, referencias a documentacion), menciona GitHub Copilot y spec-kit, y enlaza a los documentos de `docs/` y al `SYSTEM.md`.

**Escenarios de Aceptacion**:

1. **Dado** que el repositorio tiene README basico, **Cuando** se actualiza el README, **Entonces** tiene formato profesional con secciones de descripcion, arquitectura medallion, stack tecnologico, estructura de carpetas, guia rapida y enlaces a documentacion.
2. **Dado** que el README esta actualizado, **Cuando** un visitante lo lee, **Entonces** encuentra una mencion clara de que el desarrollo usa IA asistida con GitHub Copilot y el framework spec-kit para Spec-Driven Development.
3. **Dado** que el README esta actualizado, **Cuando** un visitante busca mas detalles, **Entonces** encuentra enlaces directos a `docs/ManualTecnico.md`, `docs/ModeladoDatos.md` y `SYSTEM.md`.

---

### Historia de Usuario 4 - Sustitucion de Esquema "regional" por "lab1" (Prioridad: P1)

Como Ingeniero de Datos responsable del proyecto, necesito que todas las referencias al esquema de Unity Catalog que usan el valor por defecto "regional" sean sustituidas por "lab1" en todo el codigo del proyecto (archivos `.py`, notebooks) y en toda la documentacion de especificaciones del proyecto. Esta sustitucion debe ser coherente y completa, sin dejar referencias huerfanas al valor anterior.

**Por que esta prioridad**: Es un cambio transversal que afecta a todo el proyecto. Si se genera documentacion nueva con el valor antiguo "regional", seria inconsistente y generaria confusion. Debe hacerse como una de las primeras acciones del incremento.

**Prueba Independiente**: Se puede verificar buscando en todo el repositorio la cadena "regional" usada como valor de esquema de Unity Catalog y confirmando que no quedan ocurrencias (exceptuando el `SYSTEM.md` original que documenta la decision y el historial).

**Escenarios de Aceptacion**:

1. **Dado** que multiples archivos `.py` y notebooks usan "regional" como valor por defecto de esquema, **Cuando** se ejecuta la sustitucion, **Entonces** todos los valores por defecto de esquema cambian a "lab1".
2. **Dado** que la tabla Parametros tiene registros con el valor "regional" para esquemas, **Cuando** se actualiza `conf/NbConfiguracionInicial.py`, **Entonces** los valores por defecto de esquema son "lab1" en los INSERTs correspondientes.
3. **Dado** que las especificaciones de incrementos anteriores documentan "regional", **Cuando** se actualizan las specs, **Entonces** los valores por defecto de esquema reflejan "lab1" de manera consistente.

---

### Historia de Usuario 5 - Template de Configuracion Inicial para Databricks (Prioridad: P2)

Como usuario que quiere replicar este laboratorio en su propio workspace de Databricks Free Edition, necesito una guia paso a paso en `demo/ConfiguracionInicial.md` que me explique como importar el repositorio, configurar el workspace, crear los catalogos y esquemas necesarios, y ejecutar la configuracion inicial.

**Por que esta prioridad**: Es un complemento valioso para la adopcion del proyecto, pero depende de que la documentacion tecnica y el README ya existan. No bloquea funcionalidad del pipeline.

**Prueba Independiente**: Se puede verificar que el documento existe en `demo/ConfiguracionInicial.md`, que contiene pasos secuenciales claros y que referencia los catalogos, esquemas y parametros correctos del proyecto (ya con "lab1" en lugar de "regional").

**Escenarios de Aceptacion**:

1. **Dado** que un usuario tiene acceso a Databricks Free Edition, **Cuando** sigue los pasos de `demo/ConfiguracionInicial.md`, **Entonces** puede configurar el workspace con los catalogos, esquemas y tabla Parametros necesarios para ejecutar el pipeline.
2. **Dado** que la guia existe, **Cuando** un usuario la consulta, **Entonces** encuentra pasos numerados desde la importacion del repositorio hasta la primera ejecucion del pipeline.

---

### Casos Borde

- Que sucede si un archivo `.py` contiene la cadena "regional" en un contexto que NO es valor de esquema (por ejemplo, un comentario descriptivo o un nombre de variable)? Se debe distinguir entre usos como valor de esquema y usos en contextos no relacionados.
- Que sucede si el `SYSTEM.md` contiene "regional" en la documentacion historica o de decisiones? El `SYSTEM.md` es el documento fuente de verdad del proyecto y NO debe modificarse como parte de la sustitucion automatica; el usuario lo actualiza manualmente.
- Que sucede si un archivo de especificacion de un incremento anterior referencia "regional" en contexto narrativo vs. como valor de parametro? Se deben actualizar solo los valores de parametro y configuracion, no las narrativas historicas que documenten la decision del cambio.

## Requisitos Funcionales

### Requisitos Funcionales

- **RF-001**: El proyecto DEBE tener un archivo `docs/ManualTecnico.md` que documente exhaustivamente los decoradores `@dp.table` y `@dp.materialized_view` con ejemplos reales del proyecto.
- **RF-002**: El manual tecnico DEBE documentar el paradigma declarativo de LSDP, incluyendo el patron Closure, la resolucion automatica de imports y la compatibilidad con Serverless Compute.
- **RF-003**: El manual tecnico DEBE detallar las propiedades de las tablas delta (streaming tables) y vistas materializadas: Change Data Feed, autoOptimize, Liquid Cluster, expectativas de calidad de datos, retencion de archivos y logs.
- **RF-004**: El manual tecnico DEBE documentar las operaciones con la API de DataFrames de Spark utilizadas en el proyecto: lecturas, joins, groupBy, agregaciones condicionales, coalesce, withColumn, select, alias.
- **RF-005**: El manual tecnico DEBE listar todos los parametros del pipeline LSDP y de los notebooks (widgets), con tipo, ejemplo y descripcion de cada uno.
- **RF-006**: El manual tecnico DEBE documentar la tabla Parametros con el detalle de todas las claves y sus valores, incluyendo la descripcion del uso de cada clave.
- **RF-007**: El manual tecnico DEBE incluir una seccion de dependencias que indique: Databricks Free Edition configurado con Unity Catalog, catalogos y esquemas creados, y opcionalmente acceso a Amazon S3.
- **RF-008**: El proyecto DEBE tener un archivo `docs/ModeladoDatos.md` que contenga el diccionario de datos completo con las 10 entidades del proyecto.
- **RF-009**: El modelado de datos DEBE documentar para cada entidad: nombre, capa del medallion, cantidad de columnas, y TODOS los campos individuales con tipo de dato y descripcion (diccionario completo, ~740 filas de documentacion entre las 10 entidades), asi como la logica de campos calculados donde aplique.
- **RF-010**: El modelado de datos DEBE incluir un diagrama de linaje de datos que muestre el flujo desde los parquets AS400 hasta las vistas de oro, identificando las transformaciones entre capas.
- **RF-011**: El archivo `README.md` DEBE ser actualizado con formato profesional, al nivel de empresas como Netflix, Spotify, Uber, Microsoft o Amazon.
- **RF-012**: El README DEBE contener secciones claras para: descripcion del proyecto, arquitectura medallion, stack tecnologico, estructura del proyecto, guia rapida de inicio y enlaces a documentacion.
- **RF-013**: El README DEBE mencionar explicitamente que el desarrollo del laboratorio es con IA asistida usando GitHub Copilot y el framework spec-kit para Spec-Driven Development.
- **RF-014**: El README DEBE enlazar a `docs/ManualTecnico.md`, `docs/ModeladoDatos.md` y `SYSTEM.md`.
- **RF-015**: Todas las referencias al valor por defecto "regional" usadas como esquema de Unity Catalog en archivos `.py` y notebooks del directorio `src/` y `conf/` DEBEN ser sustituidas por "lab1".
- **RF-016**: Todas las referencias al valor por defecto "regional" usadas como esquema de Unity Catalog en los archivos de especificaciones del directorio `specs/` DEBEN ser sustituidas por "lab1", incluyendo valores de parametro, nombres de entidad de 3 partes (ej: `bronce.regional.cmstfl` -> `bronce.lab1.cmstfl`) y ejemplos de configuracion.
- **RF-017**: La sustitucion DEBE aplicarse unicamente a valores de esquema de Unity Catalog, NO a usos de la palabra "regional" en contextos no relacionados (comentarios genericos, narrativas historicas).
- **RF-018**: Los archivos `SYSTEM.md` y `.specify/memory/constitution.md` NO DEBEN ser modificados como parte de la sustitucion automatica. El usuario los actualiza manualmente.
- **RF-019**: El proyecto DEBE tener un directorio `demo/` en la raiz del repositorio con un archivo `ConfiguracionInicial.md`.
- **RF-020**: El archivo `demo/ConfiguracionInicial.md` DEBE contener un template paso a paso de como importar y configurar el laboratorio en Databricks Free Edition, incluyendo: importacion del repositorio, creacion de catalogos y esquemas, creacion de la tabla Parametros, ejecucion del pipeline.
- **RF-021**: Toda la documentacion generada (ManualTecnico.md, ModeladoDatos.md, README.md, ConfiguracionInicial.md) DEBE usar "lab1" como valor de esquema en lugar de "regional".
- **RF-022**: El README DEBE estar en idioma espanol, al igual que toda la documentacion generada.

### Entidades Clave

- **ManualTecnico.md**: Documento markdown en `docs/` que centraliza todo el conocimiento tecnico del proyecto: decoradores LSDP, paradigma declarativo, propiedades Delta, operaciones DataFrame, parametros y dependencias.
- **ModeladoDatos.md**: Documento markdown en `docs/` que contiene el diccionario de datos de las 10 entidades del proyecto con campos, tipos, descripciones, logica calculada y linaje.
- **README.md**: Archivo raiz del repositorio actualizado con formato profesional, enlaces a documentacion y mencion de IA asistida.
- **ConfiguracionInicial.md**: Documento markdown en `demo/` con template paso a paso para configurar el laboratorio en Databricks Free Edition.

## Criterios de Exito

### Resultados Medibles

- **CE-001**: Un ingeniero nuevo puede entender la solucion completa leyendo unicamente el ManualTecnico.md y el ModeladoDatos.md, sin necesidad de revisar el codigo fuente.
- **CE-002**: Las 10 entidades de datos del proyecto estan documentadas con nombre, capa, campos, tipos, descripciones y logica de campos calculados en el ModeladoDatos.md.
- **CE-003**: El README.md tiene formato profesional comparable al de repositorios de codigo abierto de empresas de referencia, con al menos 6 secciones diferenciadas.
- **CE-004**: La busqueda de "regional" como valor de esquema en archivos `.py`, notebooks y especificaciones del proyecto retorna 0 ocurrencias tras la sustitucion (excluyendo SYSTEM.md).
- **CE-005**: El archivo `demo/ConfiguracionInicial.md` contiene al menos 5 pasos secuenciales y verificables para configurar el laboratorio desde cero.
- **CE-006**: Toda la documentacion generada esta en idioma espanol y usa "lab1" como valor de esquema de Unity Catalog.

## Supuestos

- El `SYSTEM.md` es el documento fuente de verdad y NO se modifica automaticamente. El usuario lo actualiza manualmente por separado.
- Los 5 incrementos anteriores estan completamente implementados y funcionando. La documentacion se basa en el codigo existente.
- Databricks Free Edition es la plataforma objetivo. La documentacion no cubre Azure Databricks ni otras plataformas.
- La sustitucion de "regional" por "lab1" es un requisito del usuario que aplica a valores de esquema en todo el proyecto (excepto SYSTEM.md).
- El formato profesional del README se inspira en repositorios de empresas como Netflix, Spotify y Microsoft, pero se adapta al contexto academico/laboratorio del proyecto.
- El TDD no aplica en este incremento dado que las entregas son exclusivamente documentos markdown y modificaciones de valores de configuracion en archivos existentes. Sin embargo, la sustitucion de "regional" por "lab1" debe verificarse exhaustivamente para no romper la funcionalidad existente.

> *Nota*: La sustitucion de "regional" por "lab1" en archivos `.py` de `src/` y `conf/` puede afectar la ejecucion del pipeline si los catalogos y esquemas correspondientes en Unity Catalog no se renombran. La guia de `demo/ConfiguracionInicial.md` debe reflejar los nombres de esquema actualizados.
