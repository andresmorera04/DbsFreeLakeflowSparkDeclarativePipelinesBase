<div align="center">

# LSDP Laboratorio Basico

### Pipeline Declarativo en Arquitectura Medallion sobre Databricks Free Edition

[![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/product/databricks-free)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Governance-00A972?style=for-the-badge)](https://www.databricks.com/product/unity-catalog)
[![License](https://img.shields.io/badge/License-Educational-blue?style=for-the-badge)](#licencia)

<br/>

**Pipeline de datos end-to-end** que ingesta datos bancarios simulados (AS400), los procesa
en tres capas del medallion y genera productos de datos analiticos — todo sobre
**Databricks Free Edition** con **Serverless Compute**, sin infraestructura de nube adicional.

[Guia de Inicio](#-inicio-rapido) · [Manual Tecnico](docs/ManualTecnico.md) · [Modelo de Datos](docs/ModeladoDatos.md) · [Configuracion Inicial](demo/ConfiguracionInicial.md)

</div>

<br/>

---

## Tabla de Contenidos

- [Acerca del Proyecto](#acerca-del-proyecto)
- [Arquitectura](#arquitectura)
- [Stack Tecnologico](#stack-tecnologico)
- [Inicio Rapido](#-inicio-rapido)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Documentacion](#-documentacion)
- [Datos del Pipeline](#datos-del-pipeline)
- [Desarrollo con IA Asistida](#-desarrollo-con-ia-asistida)
- [Licencia](#licencia)

---

## Acerca del Proyecto

Este repositorio es un **laboratorio de referencia** para el paradigma declarativo de procesamiento de datos a escala. Implementa un pipeline de datos completo que:

- **Ingesta** datos bancarios simulados del sistema mainframe AS400 (3 archivos parquet: clientes, transacciones, saldos).
- **Transforma** los datos a traves de la arquitectura medallion (Bronce, Plata, Oro) usando decoradores declarativos nativos de PySpark.
- **Genera** productos de datos analiticos con calidad verificada y linaje completo en Unity Catalog.

### Por que este proyecto

| Desafio | Solucion en este laboratorio |
|---------|------------------------------|
| Los tutoriales de data pipelines son triviales y no reflejan el mundo real | Pipeline con **230+ columnas**, mapeos AS400 reales, campos calculados y expectativas de calidad |
| Databricks requiere infraestructura de nube costosa para experimentar | Corre 100% sobre **Databricks Free Edition** con **Serverless Compute** — costo cero |
| Declarative Pipelines carece de documentacion practica extendida | Manual tecnico de 7 secciones con ejemplos extraidos directamente del codigo |
| Los modelos de datos no se documentan adecuadamente | Diccionario de datos con **~580 campos** documentados, tipos PySpark, linaje y logica de campos calculados |

### Numeros clave

```
   3   Archivos fuente AS400 (CMSTFL, TRXPFL, BLNCFL)
  10   Entidades en el modelo de datos
 580+  Campos documentados en el diccionario de datos
   9   Expectativas de calidad de datos
   9   Parametros de configuracion del pipeline
   4   Catalogos Unity Catalog (control, bronce, plata, oro)
   6   Scripts de transformacion LSDP
```

---

## Arquitectura

El pipeline sigue la **arquitectura medallion** (Bronze / Silver / Gold), un patron de diseno de datos popularizado por Databricks que organiza los datos en capas de calidad creciente.

```
                    ┌──────────────────────────────────────────────────────────────────┐
                    │                    DATABRICKS FREE EDITION                       │
                    │                    Unity Catalog + Serverless Compute            │
                    │                                                                  │
  ┌──────────┐     │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐        │
  │          │     │  │             │     │             │     │             │        │
  │  AS400   │     │  │   BRONCE    │     │    PLATA    │     │     ORO     │        │
  │ Parquets │────>│  │             │────>│             │────>│             │        │
  │          │     │  │ Streaming   │     │  Materialized│     │ Materialized│        │
  │ CMSTFL   │     │  │ Tables     │     │  Views      │     │ Views      │        │
  │ TRXPFL   │     │  │             │     │             │     │             │        │
  │ BLNCFL   │     │  │ @dp.table   │     │ @dp.mat_view│     │ @dp.mat_view│        │
  │          │     │  │             │     │ @dp.expect  │     │             │        │
  └──────────┘     │  └─────────────┘     └─────────────┘     └─────────────┘        │
                    │                                                                  │
  70+60+100 cols    │  75+65+105 cols       173+64 cols         6+22 cols              │
                    │  AutoLoader            LEFT JOIN           groupBy               │
                    │  cloudFiles            Enriquecimiento     INNER JOIN            │
                    │  Schema Evolution      9 Expectativas      Metricas ATM          │
                    │                                                                  │
                    └──────────────────────────────────────────────────────────────────┘
```

### Flujo detallado por capa

<table>
<tr>
<th width="25%">Capa</th>
<th width="25%">Tipo</th>
<th width="25%">Operacion</th>
<th width="25%">Tablas</th>
</tr>
<tr>
<td><strong>Bronce</strong></td>
<td>Streaming Tables</td>
<td>AutoLoader (<code>cloudFiles</code>) con schema evolution <code>addNewColumns</code>. Agrega 5 columnas de control: <code>FechaIngestaDatos</code>, <code>_rescued_data</code>, <code>anio</code>, <code>mes</code>, <code>dia</code>.</td>
<td><code>bronce.lab1.cmstfl</code><br/><code>bronce.lab1.trxpfl</code><br/><code>bronce.lab1.blncfl</code></td>
</tr>
<tr>
<td><strong>Plata</strong></td>
<td>Vistas Materializadas</td>
<td>LEFT JOIN entre dimensiones (CMSTFL + BLNCFL), renombramiento de 230+ columnas AS400 a nombres en espanol, 8 campos calculados, 9 expectativas de calidad.</td>
<td><code>plata.lab1.clientes_saldos_consolidados</code><br/><code>plata.lab1.transacciones_enriquecidas</code></td>
</tr>
<tr>
<td><strong>Oro</strong></td>
<td>Vistas Materializadas</td>
<td>INNER JOIN + <code>groupBy</code> con agregaciones condicionales (<code>F.count</code>, <code>F.avg</code>, <code>F.sum</code> con <code>F.when</code>) para generar metricas ATM y perfil integral de cliente.</td>
<td><code>oro.lab1.comportamiento_atm_cliente</code><br/><code>oro.lab1.resumen_integral_cliente</code></td>
</tr>
</table>

> Para el diagrama de linaje completo y el detalle de cada transformacion, consultar el [Modelo de Datos](docs/ModeladoDatos.md).

---

## Stack Tecnologico

| Capa | Tecnologia | Detalle |
|------|-----------|---------|
| **Plataforma** | Databricks Free Edition | Serverless Compute, sin infraestructura de nube adicional |
| **Pipeline Engine** | Lakeflow Spark Declarative Pipelines | `from pyspark import pipelines as dp` — decoradores `@dp.table`, `@dp.materialized_view` |
| **Gobierno** | Unity Catalog | 4 catalogos (`control`, `bronce`, `plata`, `oro`), esquema `lab1` |
| **Almacenamiento** | Delta Lake | Change Data Feed, autoOptimize, Liquid Clustering |
| **Ingesta** | AutoLoader | Formato `cloudFiles` con schema evolution `addNewColumns` |
| **Calidad** | `@dp.expect` | 9 expectativas observacionales en capa Plata |
| **Storage** | Volume / Amazon S3 | Seleccion dinamica via parametro `TipoStorage` |
| **Lenguaje** | Python 3.x / PySpark | Funciones nativas `F.*` exclusivamente — sin UDFs |
| **IDE** | VS Code | Databricks Extension + Driver for SQLTools |

---

## <img src="https://img.icons8.com/color/28/000000/rocket.png" width="24"/> Inicio Rapido

> **Prerequisitos**: Cuenta [Databricks Free Edition](https://www.databricks.com/product/databricks-free) activa con Unity Catalog habilitado + VS Code con [Databricks Extension](https://marketplace.visualstudio.com/items?itemName=databricks.databricks).

### 1. Clonar el repositorio

```bash
git clone https://github.com/andresmorera04/DbsFreeLakeflowSparkDeclarativePipelinesBase.git 
```

### 2. Importar en Databricks

Desde la interfaz web: **Workspace** > **Repos** > **Add Repo** > pegar la URL del repositorio.

### 3. Ejecutar la configuracion inicial

Abrir y ejecutar `conf/NbConfiguracionInicial.py` — crea los 4 catalogos, esquemas, tabla Parametros (15 registros) y el Volume de almacenamiento:

```
catalogoParametro = control
esquemaParametro  = lab1
tablaParametros   = Parametros
```

### 4. Generar datos de prueba

Ejecutar en orden los notebooks de simulacion de datos AS400:

| # | Notebook | Registros | Columnas |
|---|----------|-----------|----------|
| 1 | `src/.../GenerarParquets/NbGenerarMaestroCliente.py` | 50,000 | 70 |
| 2 | `src/.../GenerarParquets/NbGenerarTransaccionalCliente.py` | 150,000 | 60 |
| 3 | `src/.../GenerarParquets/NbGenerarSaldosCliente.py` | 50,000 | 100 |

### 5. Crear y ejecutar el pipeline

En **Workflows** > **Pipelines** > **Create Pipeline**, agregar los 6 scripts de `transformations/` y configurar los 9 parametros del pipeline.

> La guia detallada paso a paso con capturas y parametros exactos esta en **[demo/ConfiguracionInicial.md](demo/ConfiguracionInicial.md)**.

### 6. Verificar resultados

```sql
SELECT COUNT(*) FROM oro.lab1.resumen_integral_cliente;        -- ~50,000
SELECT COUNT(*) FROM oro.lab1.comportamiento_atm_cliente;      -- ~50,000
```

---

## Estructura del Proyecto

```
DbsFreeLakeflowSparkDeclarativePipelinesBase/
│
├── conf/                                    # Configuracion
│   └── NbConfiguracionInicial.py            #   Notebook: crea catalogos, esquemas, Parametros, Volume
│
├── src/LSDP_Laboratorio_Basico/
│   ├── explorations/
│   │   ├── GenerarParquets/                 # Generacion de datos de prueba AS400
│   │   │   ├── NbGenerarMaestroCliente.py   #   CMSTFL: 50,000 clientes (70 cols)
│   │   │   ├── NbGenerarTransaccionalCliente.py  #   TRXPFL: 150,000 transacciones (60 cols)
│   │   │   ├── NbGenerarSaldosCliente.py    #   BLNCFL: 50,000 saldos (100 cols)
│   │   │   └── NbTddGenerarParquets.py      #   Tests de generacion de datos
│   │   └── LSDP_Laboratorio_Basico/         # Tests del pipeline
│   │       ├── NbTddBroncePipeline.py       #   TDD capa Bronce
│   │       └── NbTddOroPipeline.py          #   TDD capa Oro
│   │
│   ├── transformations/                     # Pipeline LSDP (scripts declarativos)
│   │   ├── LsdpBronceCmstfl.py              #   @dp.table — Streaming Table clientes
│   │   ├── LsdpBronceTrxpfl.py              #   @dp.table — Streaming Table transacciones
│   │   ├── LsdpBronceBlncfl.py              #   @dp.table — Streaming Table saldos
│   │   ├── LsdpPlataClientesSaldos.py       #   @dp.materialized_view — 173 cols, 5 expectativas
│   │   ├── LsdpPlataTransacciones.py        #   @dp.materialized_view — 64 cols, 4 expectativas
│   │   └── LsdpOroClientes.py               #   @dp.materialized_view — metricas ATM + perfil integral
│   │
│   └── utilities/                           # Modulos Python reutilizables
│       ├── LsdpConexionParametros.py        #   Lectura de tabla Parametros
│       ├── LsdpConstructorRutas.py          #   Construccion de rutas (Volume / S3)
│       ├── LsdpInsertarTiposTransaccion.py  #   Tipos de transaccion para generacion
│       └── LsdpReordenarColumnasLiquidCluster.py  #   Reordenamiento de columnas + Liquid Cluster
│
├── docs/                                    # Documentacion tecnica
│   ├── ManualTecnico.md                     #   Manual completo (7 secciones)
│   └── ModeladoDatos.md                     #   Diccionario de datos (10 entidades, ~580 campos)
│
├── demo/                                    # Guias de configuracion
│   └── ConfiguracionInicial.md              #   Guia paso a paso (8 pasos)
│
├── specs/                                   # Especificaciones SDD (spec-kit)
│   ├── 001-incremento1-config-inicial/
│   ├── 002-generar-parquets-as400/
│   ├── 003-lsdp-bronce-pipeline/
│   ├── 004-lsdp-plata-vistas/
│   ├── 005-lsdp-oro-vistas/
│   └── 006-docs-readme-esquema/
│
├── README.md                                # Este archivo
└── SYSTEM.md                                # Documento de referencia del sistema
```

---

## <img src="https://img.icons8.com/color/28/000000/books.png" width="24"/> Documentacion

Este proyecto cuenta con documentacion tecnica extensiva. Cada documento fue generado a partir del codigo fuente real para garantizar consistencia:

### [Manual Tecnico](docs/ManualTecnico.md)

Referencia tecnica completa del pipeline con 7 secciones:

| Seccion | Contenido |
|---------|-----------|
| **Decoradores LSDP** | `@dp.table`, `@dp.materialized_view`, `@dp.expect` con ejemplos reales del codigo |
| **Paradigma Declarativo** | Patron Closure, cloudpickle, imports automaticos, compatibilidad Serverless |
| **Propiedades Delta** | Change Data Feed, autoOptimize, Liquid Clustering por entidad, retencion |
| **Operaciones DataFrame** | readStream, joins, groupBy, transformaciones, protecciones ANSI |
| **Parametros del Pipeline** | 9 parametros via `spark.conf.get("pipelines.parameters.*")` |
| **Tabla Parametros** | 15 registros de `control.lab1.Parametros` con descripcion de cada clave |
| **Dependencias** | Plataforma, catalogos, Volume/S3, modulos utilitarios |

### [Modelo de Datos](docs/ModeladoDatos.md)

Diccionario de datos completo de las 10 entidades del pipeline:

| Capa | Entidades | Campos documentados |
|------|-----------|:-------------------:|
| **Fuente** | CMSTFL, TRXPFL, BLNCFL | 230 |
| **Bronce** | cmstfl, trxpfl, blncfl (+5 cols control c/u) | 245 |
| **Plata** | clientes_saldos_consolidados, transacciones_enriquecidas | 237 |
| **Oro** | comportamiento_atm_cliente, resumen_integral_cliente | 28 |
| **Control** | Parametros (16 registros) | — |

Incluye: diagrama de linaje, mapeo columna-a-columna AS400 a espanol, logica de los 8 campos calculados y las 9 expectativas de calidad.

### [Guia de Configuracion Inicial](demo/ConfiguracionInicial.md)

Guia paso a paso para replicar el laboratorio desde cero en una cuenta nueva de Databricks Free Edition:

1. Prerequisitos de software
2. Importar repositorio en Databricks
3. Configurar extensiones VS Code
4. Ejecutar notebook de configuracion inicial
5. Generar datos de prueba (250,000 registros)
6. Crear pipeline LSDP con 9 parametros
7. Ejecutar pipeline (Bronce > Plata > Oro)
8. Verificar resultados con consultas SQL

---

## Datos del Pipeline

### Parametros de configuracion

El pipeline LSDP se configura con 9 parametros accesibles via `spark.conf.get("pipelines.parameters.*")`:

```
catalogoParametro           = control
esquemaParametro            = lab1
tablaParametros             = Parametros
rutaCompletaMaestroCliente  = LSDP_Base/As400/MaestroCliente/
rutaCompletaTransaccional   = LSDP_Base/As400/Transaccional/
rutaCompletaSaldoCliente    = LSDP_Base/As400/SaldoCliente/
rutaSchemaLocationCmstfl    = LSDP_Base/SchemaLocations/cmstfl/
rutaSchemaLocationTrxpfl    = LSDP_Base/SchemaLocations/trxpfl/
rutaSchemaLocationBlncfl    = LSDP_Base/SchemaLocations/blncfl/
```

### Ejemplo de codigo: Streaming Table (Bronce)

```python
from pyspark import pipelines as dp

@dp.table(
    name="cmstfl",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["FechaIngestaDatos", "CUSTID"],
)
def tabla_bronce_cmstfl():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", ruta_schema_location)
        .load(ruta_parquet)
    )
```

### Ejemplo de codigo: Vista Materializada con Expectativas (Plata)

```python
@dp.expect("id_cliente_valido", "identificador_cliente IS NOT NULL")
@dp.expect("nombre_cliente_valido", "nombre_cliente IS NOT NULL")
@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.clientes_saldos_consolidados",
    table_properties={...},
    cluster_by=["identificador_cliente"],
)
def vista_plata_clientes():
    # LEFT JOIN CMSTFL + BLNCFL con 173 columnas renombradas a espanol
    ...
```

> Para la referencia completa de decoradores y operaciones, ver el **[Manual Tecnico](docs/ManualTecnico.md)**.

---

## <img src="https://img.icons8.com/color/28/000000/artificial-intelligence.png" width="24"/> Desarrollo con IA Asistida

Este proyecto fue desarrollado usando **Spec-Driven Development (SDD)** — un enfoque donde la especificacion formal impulsa toda la implementacion con asistencia de inteligencia artificial.

### Herramientas

| Herramienta | Rol |
|-------------|-----|
| **GitHub Copilot** | Agente de desarrollo — implementacion de codigo, analisis de codebase, generacion de documentacion |
| **spec-kit** | Framework de especificacion — orquesta el flujo SDD desde requisitos hasta implementacion |

### Flujo de trabajo SDD

```
  Requisito              Especificacion          Diseno               Tareas              Codigo
  (lenguaje natural)     (spec.md)               (plan.md)            (tasks.md)          (implementacion)
       │                      │                      │                     │                    │
       ▼                      ▼                      ▼                     ▼                    ▼
  /speckit.specify  →  /speckit.clarify  →  /speckit.plan  →  /speckit.tasks  →  /speckit.implement
```

### Incrementos del proyecto

Cada incremento tiene su especificacion completa en `specs/`, permitiendo **trazabilidad total** desde el requisito hasta el codigo:

| # | Incremento | Alcance |
|---|-----------|---------|
| 001 | Configuracion Inicial | Catalogos, esquemas, tabla Parametros, Volume |
| 002 | Generacion de Parquets | Datos de prueba AS400 (250,000 registros) |
| 003 | Pipeline Bronce | 3 Streaming Tables con AutoLoader |
| 004 | Pipeline Plata | 2 Vistas Materializadas con expectativas de calidad |
| 005 | Pipeline Oro | 2 Vistas Materializadas con metricas analiticas |
| 006 | Documentacion | Manual Tecnico, Modelo de Datos, README, Guia de Configuracion |

---

## Licencia

Repositorio de uso **educativo y de referencia**. Todos los datos son simulados y no contienen
informacion real de clientes.

---

<div align="center">

Desarrollado con **GitHub Copilot** + **spec-kit** sobre **Databricks Free Edition**

[Manual Tecnico](docs/ManualTecnico.md) · [Modelo de Datos](docs/ModeladoDatos.md) · [Configuracion Inicial](demo/ConfiguracionInicial.md)

</div>

