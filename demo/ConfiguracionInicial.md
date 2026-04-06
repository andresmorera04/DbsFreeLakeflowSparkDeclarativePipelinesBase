<div align="center">

# Guia de Configuracion Inicial

### Replica el laboratorio completo desde cero en Databricks Free Edition

[![Volver al README](https://img.shields.io/badge/Volver_al-README-2ea44f?style=for-the-badge)](../README.md)
[![Manual Tecnico](https://img.shields.io/badge/Manual-Tecnico-blue?style=for-the-badge)](../docs/ManualTecnico.md)
[![Modelo de Datos](https://img.shields.io/badge/Modelo_de-Datos-orange?style=for-the-badge)](../docs/ModeladoDatos.md)

</div>

<br/>

> **Contexto**: Esta guia es la expansion detallada de la seccion [Inicio Rapido](../README.md#-inicio-rapido) del README principal. Aqui se documenta cada paso con instrucciones precisas, parametros exactos, verificaciones intermedias y videos demostrativos para que cualquier persona pueda replicar el laboratorio sin ambiguedad alguna.

---

## Tabla de Contenidos

- [Prerequisitos de Software](#paso-1-prerequisitos-de-software)
- [Importar Repositorio en Databricks](#paso-2-importar-repositorio-en-databricks) *(incluye video)*
- [Configurar Extensiones VS Code](#paso-3-configurar-extensiones-vs-code)
- [Ejecutar Notebook de Configuracion Inicial](#paso-4-ejecutar-notebook-de-configuracion-inicial)
- [Generar Datos de Prueba (250,000 registros)](#paso-5-generar-datos-de-prueba-250000-registros)
- [Crear Pipeline LSDP con 9 Parametros](#paso-6-crear-pipeline-lsdp-con-9-parametros) *(incluye video)*
- [Ejecutar Pipeline (Bronce > Plata > Oro)](#paso-7-ejecutar-pipeline-bronce--plata--oro) *(incluye video)*
- [Verificar Resultados con Consultas SQL](#paso-8-verificar-resultados-con-consultas-sql)
- [Solucion de Problemas](#solucion-de-problemas)
- [Referencia Rapida](#referencia-rapida)

---

## Paso 1: Prerequisitos de Software

Antes de comenzar, verificar que se cumplen todos los prerequisitos. Cada uno incluye la forma de verificarlo:

### 1.1 Cuenta Databricks Free Edition

| Requisito | Detalle |
|-----------|---------|
| **Que se necesita** | Cuenta activa en [Databricks Free Edition](https://www.databricks.com/product/databricks-free) con Unity Catalog habilitado |
| **Como obtenerla** | Registrarse en [cloud.databricks.com](https://cloud.databricks.com) con una cuenta de correo |
| **Verificacion** | Iniciar sesion, ir a **Catalog** en el menu lateral y confirmar que el Catalog Explorer esta disponible y muestra al menos el catalogo `main` |

### 1.2 Visual Studio Code

| Requisito | Detalle |
|-----------|---------|
| **Que se necesita** | VS Code version 1.85 o superior |
| **Como obtenerlo** | Descargar desde [code.visualstudio.com](https://code.visualstudio.com/) |
| **Verificacion** | En terminal: `code --version` — debe mostrar `1.85.x` o superior |

### 1.3 Databricks Extension para VS Code

| Requisito | Detalle |
|-----------|---------|
| **Que se necesita** | Extension oficial de Databricks instalada en VS Code |
| **Como obtenerla** | En VS Code: `Ctrl+Shift+X` > buscar **"Databricks"** > instalar la extension publicada por Databricks |
| **Verificacion** | El icono de Databricks aparece en la barra lateral izquierda de VS Code |

### 1.4 Driver for SQLTools (Databricks)

| Requisito | Detalle |
|-----------|---------|
| **Que se necesita** | Extension SQLTools + driver de Databricks para ejecutar consultas SQL de validacion |
| **Como obtenerlo** | En VS Code: `Ctrl+Shift+X` > buscar **"SQLTools"** > instalar. Luego buscar e instalar **"SQLTools Databricks Driver"** |
| **Verificacion** | El icono de SQLTools (base de datos) aparece en la barra lateral de VS Code |

### 1.5 Acceso al Repositorio

| Requisito | Detalle |
|-----------|---------|
| **Que se necesita** | Cuenta GitHub con acceso al repositorio (publico o con permisos de lectura) |
| **Verificacion** | Poder navegar al repositorio en el navegador y ver el contenido |

### Checklist de prerequisitos

```
[ ] Cuenta Databricks Free Edition activa con Unity Catalog
[ ] VS Code 1.85+ instalado
[ ] Databricks Extension para VS Code instalada
[ ] SQLTools + Databricks Driver instalados
[ ] Acceso al repositorio en GitHub
```

---

## Paso 2: Importar Repositorio en Databricks

Este paso conecta el repositorio de GitHub con el workspace de Databricks para que los notebooks y scripts esten disponibles para ejecucion.

### Video demostrativo

<!-- VIDEO: Importar repositorio en Databricks
     Reemplazar el comentario con la referencia al video cuando este disponible.
     Formato sugerido:

     https://github.com/user-attachments/assets/[id-del-video].mp4
-->

> **Video pendiente**: Se incorporara un video .mp4 demostrando el proceso completo de importacion del repositorio en Databricks.

---

### Opcion A: Desde la interfaz web de Databricks (recomendado)

1. Iniciar sesion en el workspace de Databricks Free Edition.
2. En el menu lateral izquierdo, hacer clic en **Workspace**.
3. Navegar hasta la carpeta personal o la carpeta donde se desea importar.
4. Hacer clic en el menu de tres puntos (**...**) > **Import** > **Git Repository**.
5. En el campo **Git repository URL**, ingresar:
   ```
   https://github.com/andresmorera04/DbsFreeLakeflowSparkDeclarativePipelinesBase.git
   ```
6. Seleccionar **Git provider**: `GitHub`.
7. Hacer clic en **Import**.
8. El repositorio quedara disponible en:
   ```
   /Workspace/Repos/[tu-usuario]/DbsFreeLakeflowSparkDeclarativePipelinesBase/
   ```

### Opcion B: Desde VS Code con la Extension de Databricks

1. Abrir VS Code y hacer clic en el icono de **Databricks** en la barra lateral.
2. Si aun no esta conectado, configurar la conexion al workspace (ver [Paso 3](#paso-3-configurar-extensiones-vs-code)).
3. En el panel de Databricks, navegar a **Repos**.
4. Usar la opcion **Clone Repo** e ingresar la URL del repositorio.
5. El repositorio se importa y se sincroniza automaticamente con el workspace.

### Verificacion

Confirmar que el repositorio fue importado correctamente:

| Que verificar | Donde | Resultado esperado |
|--------------|-------|-------------------|
| Estructura de directorios | Workspace de Databricks > Repos | Se ven los directorios `conf/`, `src/`, `docs/`, `demo/`, `specs/` |
| Archivo de configuracion | Abrir `conf/NbConfiguracionInicial.py` | Se muestra el contenido del notebook sin errores |
| Extension VS Code | Panel de Databricks en VS Code | El repositorio aparece en la lista de Repos |

---

## Paso 3: Configurar Extensiones VS Code

La integracion entre VS Code y Databricks permite ejecutar notebooks, explorar catalogos y ejecutar consultas SQL directamente desde el editor.

### 3.1 Configurar Databricks Extension

**Paso a paso**:

1. Abrir VS Code > hacer clic en el icono de **Databricks** en la barra lateral izquierda.
2. Hacer clic en **Configure Databricks** (o en el prompt de conexion que aparece automaticamente).
3. Ingresar la **URL del workspace**:
   - Para Databricks Free Edition, la URL tiene el formato:
     ```
     https://[id-workspace].cloud.databricks.com
     ```
   - Esta URL se encuentra en la barra de direcciones del navegador al iniciar sesion en Databricks.
4. **Autenticacion** — Generar un Personal Access Token (PAT):
   - En la interfaz web de Databricks: **Settings** (esquina superior derecha) > **Developer** > **Access Tokens**.
   - Hacer clic en **Generate new token**.
   - Asignar un nombre descriptivo (ej: `vscode-lsdp-lab`) y una duracion (ej: 90 dias).
   - Copiar el token generado — se usara tambien en SQLTools.
5. Pegar el PAT en VS Code cuando lo solicite.
6. Seleccionar **Serverless** como el compute por defecto para ejecutar notebooks.

**Verificacion**:

| Que verificar | Resultado esperado |
|--------------|-------------------|
| Panel de conexion | Muestra el nombre del workspace y el estado "Connected" |
| Catalogo `main` | Visible en el explorador de Unity Catalog dentro del panel |
| Cluster Serverless | Aparece como la opcion de compute seleccionada |

### 3.2 Configurar Driver for SQLTools

**Paso a paso**:

1. Abrir VS Code > hacer clic en el icono de **SQLTools** (base de datos) en la barra lateral.
2. Hacer clic en **Add new connection**.
3. Seleccionar **Databricks** como tipo de conexion.
4. Completar los campos:

   | Campo | Valor | Nota |
   |-------|-------|------|
   | **Connection name** | `LSDP Laboratorio` | Nombre descriptivo para la conexion |
   | **Server/Host** | `[id-workspace].cloud.databricks.com` | Mismo host del workspace |
   | **Port** | `443` | Puerto por defecto |
   | **HTTP Path** | `/sql/1.0/warehouses/[id-warehouse]` | Se obtiene de Databricks: SQL Warehouses > seleccionar warehouse > Connection Details |
   | **Token** | `[el-PAT-generado-en-3.1]` | El mismo token de la extension de Databricks |

5. Hacer clic en **Test Connection** — debe mostrar "Connection successful".
6. Hacer clic en **Save Connection**.

**Verificacion**:

Ejecutar una consulta de prueba para confirmar la conexion:
```sql
SELECT current_catalog(), current_schema();
```
Debe retornar el catalogo y esquema por defecto del warehouse.

---

## Paso 4: Ejecutar Notebook de Configuracion Inicial

El notebook `conf/NbConfiguracionInicial.py` es el punto de partida de todo el laboratorio. En una sola ejecucion idempotente crea toda la infraestructura necesaria en Unity Catalog.

### Que crea este notebook

| # | Recurso | Detalle |
|---|---------|---------|
| 1 | **4 catalogos** | `control`, `bronce`, `plata`, `oro` (con `CREATE CATALOG IF NOT EXISTS`) |
| 2 | **4 esquemas** | `lab1` en cada catalogo (con `CREATE SCHEMA IF NOT EXISTS`) |
| 3 | **Tabla Parametros** | `control.lab1.Parametros` con 15 registros de configuracion (con `CREATE OR REPLACE TABLE`) |
| 4 | **Volume** | `bronce.lab1.datos_bronce` para almacenamiento de archivos parquet |

### Parametros de ejecucion (widgets)

El notebook define 3 widgets con valores por defecto. Estos valores son los recomendados y no requieren modificacion:

| Widget | Valor por Defecto | Descripcion |
|--------|------------------|-------------|
| `catalogoParametro` | `control` | Catalogo donde se crea la tabla Parametros |
| `esquemaParametro` | `lab1` | Esquema dentro del catalogo de control |
| `tablaParametros` | `Parametros` | Nombre de la tabla de configuracion |

### Ejecucion paso a paso

1. **Abrir el notebook**: Navegar a `conf/NbConfiguracionInicial.py` en el workspace de Databricks o desde VS Code.
2. **Verificar los widgets**: En la parte superior del notebook, confirmar que los 3 widgets muestran los valores por defecto.
3. **Ejecutar**: Hacer clic en **Run All** (interfaz web) o `Ctrl+Shift+Enter` (VS Code).
4. **Monitorear la ejecucion**: El notebook ejecuta en secuencia:
   - Validacion de parametros de entrada (falla si algun parametro esta vacio).
   - Bloque de resumen con los valores recibidos.
   - Creacion de catalogos y esquema de la tabla Parametros.
   - `CREATE OR REPLACE TABLE` de la tabla Parametros con schema `(Clave STRING, Valor STRING)`.
   - Insercion de los 15 registros de configuracion.
   - Lectura de esquemas desde la tabla y creacion de los 4 esquemas medallion.
   - Creacion del Volume gestionado.
5. **Tiempo estimado**: 30-60 segundos.

### Los 15 registros de configuracion

La tabla `control.lab1.Parametros` queda poblada con estos valores:

| Clave | Valor | Proposito |
|-------|-------|-----------|
| `catalogoBronce` | `bronce` | Catalogo de la capa bronce |
| `esquemaBronce` | `lab1` | Esquema de la capa bronce |
| `contenedorBronce` | `bronce` | Nombre del contenedor de almacenamiento |
| `TipoStorage` | `Volume` | Tipo de almacenamiento (`Volume` o `AmazonS3`) |
| `catalogoVolume` | `bronce` | Catalogo donde reside el Volume |
| `esquemaVolume` | `lab1` | Esquema donde reside el Volume |
| `nombreVolume` | `datos_bronce` | Nombre del Volume gestionado |
| `bucketS3` | *(vacio)* | Bucket S3 (solo si TipoStorage = AmazonS3) |
| `prefijoS3` | *(vacio)* | Prefijo S3 (solo si TipoStorage = AmazonS3) |
| `DirectorioBronce` | `archivos` | Directorio base dentro del almacenamiento |
| `catalogoPlata` | `plata` | Catalogo de la capa plata |
| `esquemaPlata` | `lab1` | Esquema de la capa plata |
| `catalogoOro` | `oro` | Catalogo de la capa oro |
| `esquemaOro` | `lab1` | Esquema de la capa oro |
| `esquemaControl` | `lab1` | Esquema de la capa control |

> Para la documentacion completa de la tabla Parametros y su uso en el pipeline, ver la Seccion 6 del [Manual Tecnico](../docs/ManualTecnico.md#6-tabla-parametros).

### Verificacion

Ejecutar las siguientes consultas en SQLTools o en un notebook nuevo:

```sql
-- 1. Verificar que la tabla Parametros tiene los 15 registros
SELECT COUNT(*) AS total_registros FROM control.lab1.Parametros;
-- Resultado esperado: 15

-- 2. Listar todos los registros
SELECT Clave, Valor FROM control.lab1.Parametros ORDER BY Clave;
-- Resultado esperado: 15 filas con los valores listados arriba

-- 3. Verificar que los 4 catalogos existen
SHOW CATALOGS;
-- Resultado esperado: aparecen control, bronce, plata, oro (entre otros)

-- 4. Verificar que los esquemas lab1 existen
SHOW SCHEMAS IN bronce;
SHOW SCHEMAS IN plata;
SHOW SCHEMAS IN oro;
SHOW SCHEMAS IN control;
-- Resultado esperado: lab1 aparece en cada catalogo
```

**Idempotencia**: Este notebook se puede re-ejecutar multiples veces sin errores. Usa `CREATE OR REPLACE TABLE` para la tabla Parametros y `IF NOT EXISTS` para catalogos, esquemas y Volume.

---

## Paso 5: Generar Datos de Prueba (250,000 registros)

Los 3 notebooks de generacion simulan los archivos del sistema mainframe AS400 bancario. Generan un total de **250,000 registros** distribuidos en 3 archivos parquet que el pipeline consumira como fuente de datos.

### Resumen de generacion

| # | Notebook | Archivo AS400 | Registros | Columnas | Destino |
|---|----------|--------------|-----------|----------|---------|
| 1 | `NbGenerarMaestroCliente.py` | CMSTFL | 50,000 | 70 | `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/MaestroCliente/` |
| 2 | `NbGenerarTransaccionalCliente.py` | TRXPFL | 150,000 | 60 | `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/Transaccional/` |
| 3 | `NbGenerarSaldosCliente.py` | BLNCFL | 50,000 | 100 | `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/SaldoCliente/` |

> **Orden de ejecucion**: Los notebooks deben ejecutarse en el orden indicado (1 > 2 > 3) porque el notebook de transacciones y saldos puede referenciar los clientes generados en el primer notebook.

### 5.1 Generar Maestro de Clientes (CMSTFL — 50,000 registros)

**Ubicacion**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`

**Parametros del notebook**:

| Widget | Valor | Descripcion |
|--------|-------|-------------|
| `catalogoParametro` | `control` | Catalogo de la tabla Parametros |
| `esquemaParametro` | `lab1` | Esquema de la tabla Parametros |
| `tablaParametros` | `Parametros` | Nombre de la tabla Parametros |
| `cantidadClientes` | `50000` | Cantidad de clientes a generar |
| `rutaRelativaMaestroCliente` | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa dentro del Volume |
| `rutaMaestroClienteExistente` | *(vacio)* | Dejar vacio para primera ejecucion |

**Ejecucion**: Abrir el notebook > verificar widgets > **Run All**.

**Resultado**: Archivo parquet con 50,000 registros y 70 columnas con datos de clientes bancarios simulados (tipos: 41 StringType, 18 DateType, 9 LongType, 2 DoubleType).

### 5.2 Generar Transacciones (TRXPFL — 150,000 registros)

**Ubicacion**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`

**Parametros del notebook**: Los mismos 3 parametros de conexion (`catalogoParametro`, `esquemaParametro`, `tablaParametros`) + `numeroRegistros = 150000`.

**Ejecucion**: Abrir el notebook > verificar widgets > **Run All**.

**Resultado**: Archivo parquet con 150,000 registros y 60 columnas con transacciones bancarias simuladas (depositos, retiros, transferencias, pagos).

### 5.3 Generar Saldos (BLNCFL — 50,000 registros)

**Ubicacion**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarSaldosCliente.py`

**Parametros del notebook**: Los mismos 3 parametros de conexion + `numeroClientes = 50000`.

**Ejecucion**: Abrir el notebook > verificar widgets > **Run All**.

**Resultado**: Archivo parquet con 50,000 registros y 100 columnas con saldos bancarios simulados.

### Verificacion de generacion

Confirmar que los 3 archivos parquet existen en el Volume:

**Opcion A — Via Catalog Explorer**:

Navegar a `bronce` > `lab1` > `datos_bronce` > `LSDP_Base` > `As400` y verificar que existen las 3 carpetas con archivos parquet:

```
/Volumes/bronce/lab1/datos_bronce/
  └── LSDP_Base/
      └── As400/
          ├── MaestroCliente/     (parquet CMSTFL)
          ├── Transaccional/      (parquet TRXPFL)
          └── SaldoCliente/       (parquet BLNCFL)
```

**Opcion B — Via SQL**:

```sql
-- Contar archivos en cada directorio
SELECT COUNT(*) FROM parquet.`/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/MaestroCliente/`;
-- Resultado esperado: 50,000

SELECT COUNT(*) FROM parquet.`/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/Transaccional/`;
-- Resultado esperado: 150,000

SELECT COUNT(*) FROM parquet.`/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/SaldoCliente/`;
-- Resultado esperado: 50,000
```

> Para el detalle completo de las columnas de cada archivo AS400, ver la Seccion 3 del [Modelo de Datos](../docs/ModeladoDatos.md#3-parquets-as400-fuente).

---

## Paso 6: Crear Pipeline LSDP con 9 Parametros

Este paso crea el pipeline declarativo de Lakeflow Spark que procesa los datos a traves de las 3 capas del medallion. El pipeline se compone de 6 scripts de transformacion y 9 parametros de configuracion.

### Video demostrativo

<!-- VIDEO: Crear pipeline LSDP con 9 parametros
     Reemplazar el comentario con la referencia al video cuando este disponible.
     Formato sugerido:

     https://github.com/user-attachments/assets/[id-del-video].mp4
-->

> **Video pendiente**: Se incorporara un video .mp4 demostrando la creacion del pipeline y la configuracion de los 9 parametros en Databricks.

---

### 6.1 Crear el pipeline en Databricks

1. En la interfaz web de Databricks, ir a **Workflows** en el menu lateral izquierdo.
2. Seleccionar la pestana **Delta Live Tables** (o **Pipelines**, segun la version de la interfaz).
3. Hacer clic en **Create Pipeline**.
4. Configurar los campos generales del pipeline:

   | Campo | Valor | Nota |
   |-------|-------|------|
   | **Pipeline name** | `LSDP_Laboratorio_Basico` | Nombre descriptivo del pipeline |
   | **Product edition** | `Core` | La edicion disponible en Free Edition |
   | **Pipeline mode** | `Triggered` | Se ejecuta manualmente (no en streaming continuo) |
   | **Compute** | `Serverless` | Compute gestionado sin cluster dedicado |

### 6.2 Agregar los scripts de transformacion (Source code)

En la seccion **Source code** del pipeline, agregar los 6 archivos de transformacion. Cada archivo contiene los decoradores `@dp.table` o `@dp.materialized_view` que definen las tablas del pipeline:

| # | Archivo | Capa | Decorador | Entidad que crea |
|---|---------|------|-----------|-----------------|
| 1 | `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceCmstfl.py` | Bronce | `@dp.table` | `bronce.lab1.cmstfl` (75 cols) |
| 2 | `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceTrxpfl.py` | Bronce | `@dp.table` | `bronce.lab1.trxpfl` (65 cols) |
| 3 | `src/LSDP_Laboratorio_Basico/transformations/LsdpBronceBlncfl.py` | Bronce | `@dp.table` | `bronce.lab1.blncfl` (105 cols) |
| 4 | `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataClientesSaldos.py` | Plata | `@dp.materialized_view` | `plata.lab1.clientes_saldos_consolidados` (173 cols) |
| 5 | `src/LSDP_Laboratorio_Basico/transformations/LsdpPlataTransacciones.py` | Plata | `@dp.materialized_view` | `plata.lab1.transacciones_enriquecidas` (64 cols) |
| 6 | `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py` | Oro | `@dp.materialized_view` | `oro.lab1.comportamiento_atm_cliente` (6 cols) + `oro.lab1.resumen_integral_cliente` (22 cols) |

> Los scripts tambien importan automaticamente los modulos de `utilities/` (`LsdpConexionParametros.py`, `LsdpConstructorRutas.py`, etc.) gracias a la resolucion automatica de imports del runtime LSDP.

### 6.3 Configurar los 9 parametros del pipeline

En la seccion **Configuration** > **Add configuration**, agregar cada parametro como un par clave-valor. Estos parametros son accesibles en el codigo via `spark.conf.get("pipelines.parameters.<clave>")`:

| # | Parametro | Valor | Proposito |
|---|-----------|-------|-----------|
| 1 | `catalogoParametro` | `control` | Catalogo de la tabla Parametros |
| 2 | `esquemaParametro` | `lab1` | Esquema de la tabla Parametros |
| 3 | `tablaParametros` | `Parametros` | Nombre de la tabla Parametros |
| 4 | `rutaCompletaMaestroCliente` | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa al parquet CMSTFL |
| 5 | `rutaCompletaTransaccional` | `LSDP_Base/As400/Transaccional/` | Ruta relativa al parquet TRXPFL |
| 6 | `rutaCompletaSaldoCliente` | `LSDP_Base/As400/SaldoCliente/` | Ruta relativa al parquet BLNCFL |
| 7 | `rutaSchemaLocationCmstfl` | `LSDP_Base/SchemaLocations/cmstfl/` | Schema location para AutoLoader CMSTFL |
| 8 | `rutaSchemaLocationTrxpfl` | `LSDP_Base/SchemaLocations/trxpfl/` | Schema location para AutoLoader TRXPFL |
| 9 | `rutaSchemaLocationBlncfl` | `LSDP_Base/SchemaLocations/blncfl/` | Schema location para AutoLoader BLNCFL |

> Los parametros 1-3 permiten al pipeline leer la tabla `control.lab1.Parametros` para obtener el resto de la configuracion (catalogos, esquemas, tipo de almacenamiento). Los parametros 4-9 definen las rutas de los archivos fuente y las ubicaciones de schema evolution de AutoLoader. Para el detalle completo de como el pipeline usa estos parametros, ver la Seccion 5 del [Manual Tecnico](../docs/ManualTecnico.md#5-parametros-del-pipeline-y-notebooks).

### 6.4 Guardar el pipeline

Hacer clic en **Create** (o **Save**). El pipeline quedara en estado **Ready** listo para ejecutarse.

### Verificacion

| Que verificar | Resultado esperado |
|--------------|-------------------|
| Estado del pipeline | **Ready** (o **Idle**) |
| Source code | 6 archivos listados correctamente |
| Configuration | 9 parametros visibles con los valores configurados |
| Grafo de dependencias | Muestra 8 nodos: 3 tablas bronce > 2 vistas plata > 2 vistas oro (+ 1 nodo del script oro que genera 2 vistas) |

---

## Paso 7: Ejecutar Pipeline (Bronce > Plata > Oro)

Con el pipeline creado y configurado, este paso ejecuta todo el flujo de datos desde la ingesta de parquets hasta la generacion de vistas analiticas.

### Video demostrativo

<!-- VIDEO: Ejecutar pipeline (Bronce > Plata > Oro)
     Reemplazar el comentario con la referencia al video cuando este disponible.
     Formato sugerido:

     https://github.com/user-attachments/assets/[id-del-video].mp4
-->

> **Video pendiente**: Se incorporara un video .mp4 demostrando la ejecucion del pipeline con el grafo de dependencias en tiempo real.

---

### 7.1 Iniciar la ejecucion

1. En la interfaz de Databricks, navegar al pipeline `LSDP_Laboratorio_Basico`.
2. Hacer clic en el boton **Start** (o **Run**).
3. El pipeline inicia la ejecucion automaticamente.

### 7.2 Orden de ejecucion (automatico)

El motor LSDP resuelve las dependencias entre tablas y ejecuta en el orden optimo:

```
Etapa 1 (en paralelo):
  ├── LsdpBronceCmstfl   →  bronce.lab1.cmstfl        (AutoLoader, 50,000 registros)
  ├── LsdpBronceTrxpfl   →  bronce.lab1.trxpfl        (AutoLoader, 150,000 registros)
  └── LsdpBronceBlncfl   →  bronce.lab1.blncfl        (AutoLoader, 50,000 registros)
          │
          ▼
Etapa 2 (dependiente del bronce):
  ├── LsdpPlataClientesSaldos  →  plata.lab1.clientes_saldos_consolidados
  │                                 (LEFT JOIN CMSTFL + BLNCFL, 173 cols, 5 expectativas)
  └── LsdpPlataTransacciones   →  plata.lab1.transacciones_enriquecidas
                                    (TRXPFL enriquecido, 64 cols, 4 expectativas)
          │
          ▼
Etapa 3 (dependiente del plata):
  └── LsdpOroClientes  →  oro.lab1.comportamiento_atm_cliente    (groupBy, 6 cols)
                        →  oro.lab1.resumen_integral_cliente      (INNER JOIN, 22 cols)
```

### 7.3 Monitorear la ejecucion

Durante la ejecucion, el **grafo de dependencias** de Databricks muestra en tiempo real:

| Color del nodo | Significado |
|----------------|-------------|
| **Gris** | Pendiente — aun no ha comenzado |
| **Azul** | En ejecucion — procesando datos |
| **Verde** | Completado — datos disponibles |
| **Rojo** | Error — la ejecucion fallo (ver logs del nodo) |

Al hacer clic en cualquier nodo del grafo se puede ver:
- Cantidad de registros procesados.
- Tiempo de ejecucion del paso.
- Resultados de las expectativas de calidad (para nodos de plata).
- Logs detallados en caso de error.

### 7.4 Tiempo estimado

| Etapa | Tiempo aproximado |
|-------|-------------------|
| Etapa 1 (Bronce) | 1-3 minutos |
| Etapa 2 (Plata) | 1-3 minutos |
| Etapa 3 (Oro) | 30-60 segundos |
| **Total** | **3-8 minutos** (primera ejecucion) |

### Verificacion rapida post-ejecucion

Una vez el pipeline muestre todos los nodos en verde:

```sql
-- Verificar conteo en las vistas de oro
SELECT COUNT(*) AS total FROM oro.lab1.resumen_integral_cliente;
-- Resultado esperado: ~50,000

SELECT COUNT(*) AS total FROM oro.lab1.comportamiento_atm_cliente;
-- Resultado esperado: ~50,000
```

---

## Paso 8: Verificar Resultados con Consultas SQL

Una vez el pipeline ha completado exitosamente, se ejecutan las siguientes consultas de validacion para confirmar que los datos fluyen correctamente por las 3 capas del medallion.

### 8.1 Validacion completa de la arquitectura medallion

Esta consulta verifica que todas las capas tienen datos y los conteos son consistentes:

```sql
SELECT 'bronce.cmstfl'                      AS tabla, COUNT(*) AS registros FROM bronce.lab1.cmstfl
UNION ALL
SELECT 'bronce.trxpfl',                               COUNT(*)              FROM bronce.lab1.trxpfl
UNION ALL
SELECT 'bronce.blncfl',                               COUNT(*)              FROM bronce.lab1.blncfl
UNION ALL
SELECT 'plata.clientes_saldos_consolidados',           COUNT(*)              FROM plata.lab1.clientes_saldos_consolidados
UNION ALL
SELECT 'plata.transacciones_enriquecidas',             COUNT(*)              FROM plata.lab1.transacciones_enriquecidas
UNION ALL
SELECT 'oro.comportamiento_atm_cliente',               COUNT(*)              FROM oro.lab1.comportamiento_atm_cliente
UNION ALL
SELECT 'oro.resumen_integral_cliente',                 COUNT(*)              FROM oro.lab1.resumen_integral_cliente
ORDER BY tabla;
```

**Resultado esperado**:

| tabla | registros |
|-------|-----------|
| bronce.blncfl | 50,000 |
| bronce.cmstfl | 50,000 |
| bronce.trxpfl | 150,000 |
| oro.comportamiento_atm_cliente | ~50,000 |
| oro.resumen_integral_cliente | ~50,000 |
| plata.clientes_saldos_consolidados | ~50,000 |
| plata.transacciones_enriquecidas | ~150,000 |

### 8.2 Verificar campos calculados de plata

Confirmamos que la clasificacion de riesgo (campo calculado con `F.when`) distribuye los clientes en categorias:

```sql
SELECT
    clasificacion_riesgo_cliente,
    COUNT(*) AS cantidad_clientes
FROM plata.lab1.clientes_saldos_consolidados
GROUP BY clasificacion_riesgo_cliente
ORDER BY cantidad_clientes DESC;
```

**Resultado esperado**: Varias filas con categorias de riesgo (`Alto`, `Medio`, `Bajo`, etc.) y la distribucion de clientes en cada una.

### 8.3 Verificar metricas ATM de oro

Validamos las metricas de comportamiento ATM generadas por la agregacion condicional con `groupBy` + `F.when`:

```sql
SELECT
    identificador_cliente,
    cantidad_depositos_atm,
    cantidad_retiros_atm,
    promedio_monto_depositos_atm,
    promedio_monto_retiros_atm,
    total_pagos_saldo_cliente
FROM oro.lab1.comportamiento_atm_cliente
WHERE cantidad_depositos_atm > 0
   OR cantidad_retiros_atm > 0
ORDER BY cantidad_depositos_atm DESC
LIMIT 10;
```

**Resultado esperado**: 10 filas con clientes que tienen actividad ATM, mostrando conteos y promedios calculados.

### 8.4 Verificar el resumen integral de cliente (oro)

El resumen integral combina datos de plata (perfil cliente) con metricas ATM via INNER JOIN:

```sql
SELECT
    identificador_cliente,
    nombre_cliente,
    clasificacion_riesgo_cliente,
    cantidad_depositos_atm,
    cantidad_retiros_atm,
    total_pagos_saldo_cliente
FROM oro.lab1.resumen_integral_cliente
LIMIT 5;
```

**Resultado esperado**: 5 filas con el perfil completo del cliente (datos de plata + metricas de oro).

### 8.5 Verificar la tabla Parametros

Confirmar que los 15 registros de configuracion se mantienen intactos:

```sql
SELECT Clave, Valor
FROM control.lab1.Parametros
ORDER BY Clave;
```

**Resultado esperado**: 15 filas con las claves y valores de configuracion (todas con esquemas `lab1`).

### 8.6 Verificar expectativas de calidad de datos

Las expectativas `@dp.expect` en la capa plata son observacionales (no bloquean el pipeline). Para ver si hay registros que violan las expectativas:

```sql
-- Verificar que no hay clientes sin identificador
SELECT COUNT(*) AS clientes_sin_id
FROM plata.lab1.clientes_saldos_consolidados
WHERE identificador_cliente IS NULL;
-- Resultado esperado: 0

-- Verificar que no hay transacciones sin monto
SELECT COUNT(*) AS transacciones_sin_monto
FROM plata.lab1.transacciones_enriquecidas
WHERE monto_transaccion IS NULL;
-- Resultado esperado: 0
```

> Para el detalle completo de las 9 expectativas de calidad y todos los campos calculados, ver el [Modelo de Datos](../docs/ModeladoDatos.md#9-expectativas-de-calidad-de-datos).

---

## Solucion de Problemas

### Errores frecuentes y sus soluciones

| Problema | Causa probable | Solucion |
|---------|---------------|----------|
| `NoSuchTableException: control.lab1.Parametros` | El notebook de configuracion inicial no fue ejecutado o fallo | Ejecutar (o re-ejecutar) `conf/NbConfiguracionInicial.py` — es idempotente |
| `FileNotFoundException` al leer parquets | Los notebooks de generacion de datos no fueron ejecutados | Ejecutar los 3 notebooks de `GenerarParquets/` en orden (5.1 > 5.2 > 5.3) |
| `NOT_SUPPORTED_WITH_SERVERLESS` | El codigo usa `.cache()`, `.persist()` o `sparkContext` | Estas operaciones estan prohibidas en Serverless Compute — revisar que el codigo no las utiliza |
| Pipeline falla con "parameter not found" | Los 9 parametros del pipeline no fueron configurados | Verificar en **Configuration** del pipeline que los 9 parametros estan presentes con los valores del [Paso 6.3](#63-configurar-los-9-parametros-del-pipeline) |
| Vistas de oro con 0 registros | Las vistas de plata no completaron antes del oro | Verificar que las capas bronce y plata tienen datos. El grafo de dependencias debe mostrar todos los nodos previos en verde |
| `PARSE_SYNTAX_ERROR` en un script de transformacion | Error de sintaxis en el codigo PySpark | Verificar que los archivos no fueron modificados accidentalmente. Re-sincronizar el repositorio |
| Volume no encontrado | El Volume `bronce.lab1.datos_bronce` no fue creado | Re-ejecutar `conf/NbConfiguracionInicial.py` |

### Logs del pipeline

Para diagnosticar errores durante la ejecucion del pipeline:

1. En la pagina del pipeline, hacer clic en el nodo con error (rojo).
2. Seleccionar la pestana **Logs**.
3. Los mensajes de error incluyen la linea exacta del codigo y la excepcion completa.
4. Los nodos de plata muestran ademas los resultados de las expectativas de calidad.

---

## Referencia Rapida

### Recursos de Unity Catalog

| Recurso | Ruta completa |
|---------|--------------|
| Tabla Parametros | `control.lab1.Parametros` |
| Volume de datos | `bronce.lab1.datos_bronce` |
| Ruta fisica del Volume | `/Volumes/bronce/lab1/datos_bronce/` |

### Rutas de archivos parquet

| Archivo AS400 | Ruta en el Volume |
|--------------|-------------------|
| CMSTFL (Maestro Clientes) | `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/MaestroCliente/` |
| TRXPFL (Transaccional) | `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/Transaccional/` |
| BLNCFL (Saldos) | `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/SaldoCliente/` |

### Catalogos y esquemas

| Catalogo | Esquema | Contiene |
|----------|---------|---------|
| `control` | `lab1` | Tabla Parametros (15 registros de configuracion) |
| `bronce` | `lab1` | 3 Streaming Tables + Volume `datos_bronce` |
| `plata` | `lab1` | 2 Vistas Materializadas (clientes consolidados + transacciones enriquecidas) |
| `oro` | `lab1` | 2 Vistas Materializadas (metricas ATM + resumen integral) |

### Documentacion relacionada

| Documento | Descripcion | Enlace |
|-----------|-------------|--------|
| **README** | Descripcion general del proyecto, arquitectura y inicio rapido | [README.md](../README.md) |
| **Manual Tecnico** | Decoradores LSDP, paradigma declarativo, propiedades Delta, parametros del pipeline | [docs/ManualTecnico.md](../docs/ManualTecnico.md) |
| **Modelo de Datos** | Diccionario de datos completo de las 10 entidades (~580 campos), linaje y expectativas | [docs/ModeladoDatos.md](../docs/ModeladoDatos.md) |

---

<div align="center">

**[Volver al README](../README.md)**

</div>
