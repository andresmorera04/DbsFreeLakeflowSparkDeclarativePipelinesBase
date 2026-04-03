# Investigacion Fase 0: Incremento 1 - Research Inicial y Configuracion Base

**Fecha**: 2026-04-03
**Branch**: `001-incremento1-config-inicial`

## Tema 1: API de Lakeflow Spark Declarative Pipelines (LSDP)

**Fuente oficial**: https://docs.databricks.com/aws/en/ldp/

### Hallazgos Clave

- El modulo `databricks.sdk.pipelines` proporciona decoradores `@dp.table` y `@dp.materialized_view` para definir tablas y vistas materializadas de manera declarativa.
- La API legacy `dlt.*` (`import dlt`) esta deprecada y NO es recomendada para nuevos proyectos.
- Propiedades de tablas soportadas en LSDP:
  - Change Data Feed (CDF): soportado con configuracion en propiedades de tabla.
  - autoOptimize: automaticamente habilitado en Databricks Runtime para pipelines.
  - Liquid Clustering: soportado con `cluster_by` en metadatos de tabla.
  - Expectativas (Data Quality Checks): soporte nativo integrado en decoradores.
- Compatibilidad con Serverless Compute: TOTALMENTE COMPATIBLE. LSDP esta disenado para serverless.
- AutoLoader se usa mediante `@dp.table` con `cloud_files` como fuente, con gestion automatica de checkpoints.
- Para crear tablas/vistas en catalogos y esquemas diferentes al default del pipeline, se usan los parametros `catalog` y `schema` en los decoradores.

### Decision

Adoptar completamente LSDP con decoradores `@dp.table` y `@dp.materialized_view`, prohibiendo la API legacy `dlt.*`.

### Alternativas Consideradas

- API Legacy DLT: Descartada (sintaxis deprecada, limitaciones en serverless).
- Structured Streaming directo: Posible pero mas complejo, sin las ventajas declarativas de LSDP.

### Razon

LSDP es el estandar moderno de Databricks optimizado para Serverless Compute. Ofrece codigo mas conciso, gestion automatica de estado y checkpoints, integracion con Data Quality, y monitoreo integrado.

---

## Tema 2: AutoLoader en Databricks Free Edition

**Fuente oficial**: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/

### Hallazgos Clave

- AutoLoader es TOTALMENTE COMPATIBLE con Serverless Compute y esta optimizado para ello.
- Formato para lectura de Parquets: `spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(ruta)`.
- En LSDP, los checkpoints son TOTALMENTE AUTOMATICOS (no requieren configuracion manual).
- AutoLoader con Unity Catalog Volumes: ruta `/Volumes/catalogo/esquema/volumen/datos/` totalmente soportada.
- AutoLoader con S3: ruta `s3://bucket/ruta/` soportada, requiere credenciales IAM.
- Formatos soportados: JSON, CSV, XML, Parquet, Avro, ORC, Text, BinaryFile.

### Decision

Usar AutoLoader en LSDP para toda la ingestion incremental en bronce, con rutas dinamicas segun TipoStorage (Volume o S3).

### Alternativas Consideradas

- Structured Streaming directo: Requiere mas configuracion manual.
- COPY INTO: Solo para carga batch, no incremental.

### Razon

AutoLoader ofrece exactly-once semantics, escalabilidad probada, manejo automatico de checkpoints en serverless, y backfill sin conflictos.

---

## Tema 3: Unity Catalog DDL (Catalogos, Esquemas, Volumes, Tablas)

**Fuente oficial**: https://docs.databricks.com/aws/en/sql/language-manual/

### Hallazgos Clave

- `CREATE CATALOG IF NOT EXISTS nombre_catalogo`: crea catalogo solo si no existe. Parametros opcionales: COMMENT.
- `CREATE SCHEMA IF NOT EXISTS catalogo.esquema`: crea esquema. Requiere que el catalogo padre exista.
- `CREATE VOLUME IF NOT EXISTS catalogo.esquema.volumen`: crea volumen gestionado (managed). Ruta de acceso: `/Volumes/catalogo/esquema/volumen/`.
- `CREATE OR REPLACE TABLE catalogo.esquema.tabla (col1 tipo, col2 tipo)`: recrea la tabla completamente.
- Todos los comandos ejecutables via `spark.sql()` en notebooks.
- TOTALMENTE COMPATIBLE con Databricks Free Edition y Serverless.

### Decision

Implementar la creacion automatica de catalogos, esquemas, tabla de parametros y volume usando DDL de Unity Catalog via `spark.sql()`.

### Alternativas Consideradas

- Hive Metastore Legacy: Incompatible con Serverless.
- DBFS mounts: Legacy, prohibido en el proyecto.

### Razon

Unity Catalog es requisito obligatorio para Serverless Compute. Ofrece control granular de permisos, auditoria integrada, y es el estandar recomendado por Databricks.

---

## Tema 4: Extensiones de Databricks para Visual Studio Code

**Fuente oficial**: https://docs.databricks.com/aws/en/dev-tools/vscode-ext/

### Hallazgos Clave

- **Databricks Extension for Visual Studio Code**: permite crear/abrir proyectos Databricks, ejecutar codigo Python en serverless remoto, ejecutar notebooks como jobs, debugging interactivo con Databricks Connect, sincronizacion de workspace.
- **Databricks Driver for SQLTools**: extension complementaria para queries SQL nativos al workspace.
- Conexion al workspace: via logo Databricks en sidebar, seleccionar host, metodo de autenticacion (Personal token u OAuth), proporcionar workspace URL.
- Computo disponible: Para Serverless se selecciona "Run on Serverless". Para clusters, dropdown lista todos los disponibles.
- Ejecucion de notebooks: click derecho en archivo, opcion "Run on serverless compute" o "Run on cluster".

### Decision

Verificar que ambas extensiones esten instaladas y configuradas. Usar "Run on Serverless" para todas las ejecuciones.

### Alternativas Consideradas

- Notebook UI de Databricks web: Limita debugging remoto.
- Sin extension (herramientas Python estandar): Requiere configuracion manual compleja.

### Razon

Reduce el ciclo de desarrollo-testing, ofrece debugging nativo en VS Code, y permite ejecucion local con compute remota serverless.

---

## Tema 5: dbutils.widgets en Databricks

**Fuente oficial**: https://docs.databricks.com/aws/en/dev-tools/databricks-utils.html

### Hallazgos Clave

- `dbutils.widgets.text(name, defaultValue, label)`: crea widget de texto con valor por defecto y etiqueta visible en la UI.
- `dbutils.widgets.get(name)`: recupera el valor actual del widget como STRING.
- TOTALMENTE COMPATIBLE con Serverless Compute y con notebooks en jobs serverless.
- Los parametros pueden pasarse via job parameters cuando se ejecuta como job.
- Comandos adicionales: `dbutils.widgets.dropdown()`, `dbutils.widgets.combobox()`, `dbutils.widgets.multiselect()`, `dbutils.widgets.remove()`, `dbutils.widgets.removeAll()`.
- Mejores practicas: definir widgets al inicio del notebook, capturar valores inmediatamente, validar que no esten vacios.

### Decision

Usar `dbutils.widgets.text()` para todos los parametros del notebook de configuracion inicial, con valores por defecto que representen la configuracion recomendada.

### Alternativas Consideradas

- Job task parameters: Menos flexible para ejecucion interactiva.
- Variables de ambiente: No disponibles en Serverless.
- Valores hardcodeados: Prohibido por el constitution del proyecto.

### Razon

Es el mecanismo nativo y estandar de Databricks para notebooks interactivos, compatible con la UI, con VS Code, y con jobs automatizados. Permite cambiar configuracion sin modificar codigo.

---

## Resumen de Decisiones

| Tema | Decision | Estado |
|------|----------|--------|
| API LSDP | Adoptar `@dp.table` y `@dp.materialized_view`. Prohibir `dlt.*` legacy. | Aprobada por el usuario (2026-04-03) |
| AutoLoader | Usar en LSDP para toda la ingestion en bronce. Checkpoints automaticos. | Aprobada por el usuario (2026-04-03) |
| Unity Catalog DDL | Creacion automatica de catalogos, esquemas, tabla y volume via `spark.sql()`. | Aprobada por el usuario (2026-04-03) |
| Extensiones VS Code | Verificar instalacion y configuracion. Usar "Run on Serverless". | Aprobada por el usuario (2026-04-03) |
| dbutils.widgets | Usar `text()` para parametros con valores por defecto. Validar al inicio. | Aprobada por el usuario (2026-04-03) |
