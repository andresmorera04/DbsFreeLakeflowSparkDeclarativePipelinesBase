# Research: Incremento 6 - Documentacion, README y Sustitucion de Esquemas

**Branch**: `006-docs-readme-esquema`
**Fecha**: 2026-04-06

---

## R-001: Estructura de README Profesional para Repositorios de Datos

### Tarea de Investigacion

Determinar la estructura y secciones que debe tener el README.md para alcanzar el nivel profesional de empresas de referencia (Netflix, Spotify, Uber, Microsoft, Amazon).

### Hallazgos

Los repositorios de referencia de estas empresas comparten un patron comun:

1. **Header visual**: Titulo del proyecto con descripcion concisa (1-2 lineas).
2. **Badges/Indicadores**: Tecnologias, estado del proyecto, licencia.
3. **Descripcion extendida**: Parrafo explicativo del proposito y alcance del proyecto.
4. **Arquitectura**: Diagrama o descripcion de la arquitectura (en este caso, medallion).
5. **Stack tecnologico**: Tabla con tecnologias utilizadas.
6. **Estructura del proyecto**: Arbol de directorios con descripciones.
7. **Guia rapida de inicio**: Pasos minimos para ejecutar el proyecto.
8. **Documentacion**: Enlaces a documentos detallados (ManualTecnico, ModeladoDatos, SYSTEM).
9. **Metodologia de desarrollo**: Mencion del proceso SDD con IA asistida.
10. **Licencia y creditos**: Informacion legal y reconocimientos.

### Decision

Adoptar estructura de 8-10 secciones: Header + Descripcion, Arquitectura Medallion, Stack Tecnologico, Estructura del Proyecto, Guia Rapida, Documentacion, Desarrollo con IA Asistida, Licencia. Todo en espanol.

### Alternativas Consideradas

- README minimalista (solo descripcion + instrucciones): Descartado por no cumplir el requisito de formato profesional.
- README con screenshots/GIFs: Descartado porque el proyecto no tiene interfaz grafica, es un pipeline de datos.

---

## R-002: Estructura del Manual Tecnico para Pipelines LSDP

### Tarea de Investigacion

Definir las 7 secciones del ManualTecnico.md segun RF-001 a RF-007 del spec.

### Hallazgos

Las 7 secciones requeridas por el spec son:

1. **Decoradores LSDP** (`@dp.table` y `@dp.materialized_view`):
   - Documentar importacion: `from pyspark import pipelines as dp`
   - Parametros de los decoradores: `name`, `comment`, `table_properties`, `cluster_by`
   - CRITICO: `@dp.materialized_view` NO acepta `catalog`/`schema` como kwargs separados — se usa nombre de 3 partes en `name=f"{catalogo}.{esquema}.nombre_vista"`
   - Ejemplos concretos del proyecto (bronce con `@dp.table`, plata/oro con `@dp.materialized_view`)

2. **Paradigma Declarativo de LSDP**:
   - Patron Closure: parametros leidos a nivel de modulo, capturados por closure en funciones decoradas
   - cloudpickle serializa automaticamente las variables capturadas
   - Resolucion automatica de imports entre carpetas del pipeline
   - El motor LSDP gestiona dependencias entre tablas y vistas

3. **Propiedades Delta**:
   - Change Data Feed (`delta.enableChangeDataFeed = true`)
   - autoOptimize.autoCompact y optimizeWrite
   - Liquid Cluster (campos por entidad documentados en data-model)
   - Retencion: `deletedFileRetentionDuration = 30 days`, `logRetentionDuration = 60 days`

4. **Operaciones API DataFrames Spark**:
   - Lecturas: `spark.readStream.format("cloudFiles")`, `spark.read.table()`
   - Joins: LEFT JOIN (plata clientes+saldos), INNER JOIN (oro resumen)
   - Agregaciones: `groupBy`, `F.count`, `F.avg`, `F.sum` con `F.when` condicional
   - Transformaciones: `F.coalesce`, `F.when`, `F.sha2`, `F.concat`, `F.current_timestamp`
   - Protecciones ANSI: `F.hash().cast("long")` antes de `F.abs()`

5. **Parametros del Pipeline y Notebooks**:
   - 12 parametros del pipeline LSDP (via `spark.conf.get("pipelines.parameters.*")`)
   - 3 parametros de widgets para NbConfiguracionInicial
   - Parametros de widgets para notebooks de generacion de parquets

6. **Tabla Parametros**:
   - 15 registros de configuracion + 1 registro especial (TiposTransaccionesLabBase)
   - Ubicacion: `control.lab1.Parametros` (despues de sustitucion)
   - Estructura: Clave (string), Valor (string)

7. **Dependencias**:
   - Databricks Free Edition con Unity Catalog habilitado
   - 4 catalogos: bronce, plata, oro, control
   - 4 esquemas: lab1 en cada catalogo
   - Volume: `bronce.lab1.datos_bronce`
   - Opcionalmente: acceso a Amazon S3

### Decision

Seguir las 7 secciones del spec con la informacion real del proyecto. Incluir ejemplos de codigo extraidos directamente de los archivos implementados.

### Alternativas Consideradas

- Generar documentacion automatica desde docstrings: Descartado porque los archivos .py no son modulos Python estandar (son notebooks Databricks).

---

## R-003: Diccionario de Datos Completo (~500 Campos)

### Tarea de Investigacion

Determinar la estructura optima para documentar las 10 entidades con ~500 campos en ModeladoDatos.md.

### Hallazgos

El inventario exhaustivo del proyecto arroja:

| Capa | Entidad | Columnas |
|------|---------|----------|
| Fuente | CMSTFL (Maestro Clientes) | 70 |
| Fuente | TRXPFL (Transaccional) | 60 |
| Fuente | BLNCFL (Saldos) | 100 |
| Bronce | bronce.lab1.cmstfl | 75 (70 + 5 control) |
| Bronce | bronce.lab1.trxpfl | 65 (60 + 5 control) |
| Bronce | bronce.lab1.blncfl | 105 (100 + 5 control) |
| Plata | plata.lab1.clientes_saldos_consolidados | 173 (70 + 99 + 4 calc) |
| Plata | plata.lab1.transacciones_enriquecidas | 64 (60 + 4 calc) |
| Oro | oro.lab1.comportamiento_atm_cliente | 6 (1 clave + 5 metricas) |
| Oro | oro.lab1.resumen_integral_cliente | 22 |

Total: ~580 campos (con duplicaciones por propagacion entre capas).

La clarificacion Q1 del spec exige documentar TODOS los campos de las 10 entidades.

### Decision

Documentar por capa del medallion con tablas detalladas por entidad. Para las entidades fuente (parquets) documentar las 230 columnas originales. Para bronce agregar las 5 columnas de control. Para plata y oro documentar columnas finales con mapeo desde bronce. Incluir diagrama de linaje en formato texto (compatible con markdown puro).

### Alternativas Consideradas

- Documentar solo las 10 entidades sin detalle de campos: Descartado por clarificacion Q1 del spec.
- Usar formato de tabla unica con 500+ filas: Descartado por legibilidad. Mejor agrupar por entidad.

---

## R-004: Estrategia de Sustitucion "regional" por "lab1"

### Tarea de Investigacion

Definir la estrategia segura para sustituir "regional" por "lab1" en todo el repositorio (excepto SYSTEM.md).

### Hallazgos

Inventario de ocurrencias de "regional" en archivos .py:

| Archivo | Ocurrencias | Contexto |
|---------|-------------|----------|
| conf/NbConfiguracionInicial.py | 6 | Widget default, INSERT values |
| NbGenerarMaestroCliente.py | 1 | Widget default |
| NbGenerarSaldosCliente.py | 1 | Widget default |
| NbGenerarTransaccionalCliente.py | 1 | Widget default |
| NbTddGenerarParquets.py | 1 | Widget default |
| NbTddBroncePipeline.py | 4 | Datos esperados en asserts |
| NbTddOroPipeline.py | 2 | Valor de esquema en pruebas |
| LsdpBronceBlncfl.py | 2 | Comentarios magic %md |
| LsdpBronceCmstfl.py | 2 | Comentarios magic %md |
| LsdpBronceTrxpfl.py | 2 | Comentarios magic %md |
| LsdpOroClientes.py | 2 | Defaults en `.get()` |
| LsdpPlataClientesSaldos.py | 1 | Default en `.get()` |
| LsdpPlataTransacciones.py | 1 | Default en `.get()` |
| **Total .py** | **~26** | |

Inventario en specs (archivos .md en specs/):
- specs/001-incremento1-config-inicial/: ~14 ocurrencias (data-model, contracts, quickstart, spec, tasks)
- specs/002-generar-parquets-as400/: ~12 ocurrencias (contracts, quickstart)
- specs/003-lsdp-bronce-pipeline/: ~15+ ocurrencias (data-model, contracts, quickstart, spec, plan)
- specs/004-lsdp-plata-vistas/: ~15+ ocurrencias
- specs/005-lsdp-oro-vistas/: ~15+ ocurrencias
- specs/006-docs-readme-esquema/: referenciado en spec como valor a sustituir
- **Total specs**: ~80+ ocurrencias

**Archivos EXCLUIDOS de sustitucion** (RF-018):
- SYSTEM.md: Documento fuente de verdad del proyecto, usuario lo actualiza manualmente
- constitution.md: Documento de governanza (contiene "regional" como referencia historica)

### Decision

Sustitucion en dos fases:

1. **Fase Codigo (.py)**: Sustitucion directa de "regional" por "lab1" en todos los archivos .py de `src/` y `conf/`. Son valores de esquema sin ambiguedad.
2. **Fase Specs (.md en specs/)**: Sustitucion de "regional" por "lab1" en todos los archivos de especificaciones de incrementos 001 a 005. Aplica a valores de parametro, nombres de entidad de 3 partes y ejemplos de configuracion. NO aplica a narrativas historicas sobre la decision del cambio.

Verificacion: buscar "regional" post-sustitucion, validar 0 ocurrencias en .py y specs (excluyendo SYSTEM.md y constitution.md).

### Alternativas Consideradas

- Sustitucion con sed automatizado: Viable pero riesgoso sin revision. Mejor hacerlo archivo por archivo con validacion.
- Dejar specs historicos sin cambiar: Descartado por clarificacion Q2 del spec.

---

## R-005: Guia de Configuracion Inicial para Databricks Free Edition

### Tarea de Investigacion

Definir los pasos minimos para que un usuario replique el laboratorio en su workspace.

### Hallazgos

Pasos secuenciales requeridos:

1. **Crear cuenta Databricks Free Edition** (community.cloud.databricks.com)
2. **Importar repositorio Git** en el workspace de Databricks
3. **Configurar extensiones VS Code** (Databricks Extension + Driver for SQLTools)
4. **Ejecutar NbConfiguracionInicial.py**: Crea catalogos, esquemas, tabla Parametros, Volume
5. **Ejecutar notebooks de generacion de parquets**: Genera datos de prueba (CMSTFL, TRXPFL, BLNCFL)
6. **Crear pipeline LSDP**: Configurar pipeline con los 12 parametros y los archivos de transformations/
7. **Ejecutar pipeline**: Primera ejecucion del pipeline completo (bronce -> plata -> oro)
8. **Verificar resultados**: Consultar vistas de oro para validar el producto de datos

Los valores de esquema deben usar "lab1" (no "regional") en toda la guia.

### Decision

Crear guia con 8 pasos secuenciales, cada uno con descripcion, comando/accion y verificacion. Incluir tabla de parametros recomendados con valores por defecto actualizados a "lab1".

### Alternativas Consideradas

- Video tutorial: Fuera de alcance (incremento de documentacion markdown).
- Script automatizado de setup: Fuera de alcance (el NbConfiguracionInicial.py ya cumple esa funcion parcialmente).
