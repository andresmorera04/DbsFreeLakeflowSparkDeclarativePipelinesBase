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
- [Importar Repositorio en Databricks](#paso-2-importar-repositorio-en-databricks) *(Video 1)*
- [Configurar Extensiones VS Code](#paso-3-configurar-extensiones-vs-code)
- [Ejecutar Notebook de Configuracion Inicial](#paso-4-ejecutar-notebook-de-configuracion-inicial) *(Video 1)*
- [Generar Datos de Prueba](#paso-5-generar-datos-de-prueba) *(Video 2)*
- [Crear Pipeline LSDP con 9 Parametros](#paso-6-crear-pipeline-lsdp-con-9-parametros) *(Video 1)*
- [Ejecutar Pipeline (Bronce > Plata > Oro)](#paso-7-ejecutar-pipeline-bronce--plata--oro) *(Video 3)*
- [Ejecucion Incremental del Pipeline](#paso-8-ejecucion-incremental-del-pipeline) *(Video 4)*
- [Verificar Resultados con Consultas SQL](#paso-9-verificar-resultados-con-consultas-sql)
- [Solucion de Problemas](#solucion-de-problemas)
- [Referencia Rapida](#referencia-rapida)

---

## Paso 1: Prerequisitos de Software

Antes de comenzar, verificar que se cumplen todos los prerequisitos. Cada uno incluye la forma de verificarlo:

### 1.1 Cuenta Databricks Free Edition

| Requisito | Detalle |
|-----------|---------|
| **Que se necesita** | Cuenta activa en [Databricks Free Edition](https://login.databricks.com/signup?provider=DB_FREE_TIER) con Unity Catalog habilitado |
| **Como obtenerla** | Registrarse en [Databricks Free Edition](https://login.databricks.com/signup?provider=DB_FREE_TIER) con una cuenta de correo personal (No Laboral) |
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

> **Video**: [1 - Importar Solucion a Databricks Free Edition](videos/1%20-%20Importar_Solución_a_Databricks_Free_Edition_sub.mp4) — Este video cubre los Pasos 2, 4 y 6 (importar repositorio, ejecutar notebook de configuracion y crear el pipeline).

---

### Opcion A: Desde la interfaz web de Databricks (recomendado)

1. Iniciar sesion en el workspace de Databricks Free Edition.
2. En el menu lateral izquierdo, hacer clic en **Workspace**.
3. Navegar hasta la carpeta personal del usuario: **Users** > `tu-usuario@gmail.com`.
4. Hacer clic en el menu de tres puntos (**⋮**) o en el boton **Create** > **Git folder**.
5. En el dialogo **Create Git folder**, ingresar la URL del repositorio:
   ```
   https://github.com/andresmorera04/DbsFreeLakeflowSparkDeclarativePipelinesBase.git
   ```
6. El sistema detecta automaticamente el **Git provider** como `GitHub` y completa el **Git folder name** como `DbsFreeLakeflowSparkDeclarativePipelinesBase`.
7. Hacer clic en **Create Git folder**.
8. Esperar a que se descargue el contenido. El repositorio quedara disponible mostrando la estructura:
   ```
   DbsFreeLakeflowSparkDeclarativePipelinesBase/
   ├── .github/
   ├── conf/
   ├── demo/
   ├── docs/
   ├── specs/
   ├── src/
   ├── .gitignore
   ├── README.md
   └── SYSTEM.md
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

### Video demostrativo

> **Video**: [1 - Importar Solucion a Databricks Free Edition](videos/1%20-%20Importar_Solución_a_Databricks_Free_Edition_sub.mp4) — La ejecucion de este notebook se demuestra a partir del minuto 06:40 del video.

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

1. **Abrir el notebook**: En el workspace de Databricks, navegar a la carpeta del repositorio importado > `conf` > abrir `NbConfiguracionInicial`.
2. **Verificar los widgets**: En la parte superior del notebook aparecen 3 widgets con los valores por defecto: `control` | `lab1` | `Parametros`. No es necesario modificarlos.
3. **Ejecutar**: Hacer clic en **Run All** en la barra superior del notebook.
4. **Monitorear la ejecucion**: El notebook ejecuta en secuencia:
   - Validacion de parametros de entrada (falla si algun parametro esta vacio).
   - Bloque de resumen con los valores recibidos.
   - Creacion de los 4 catalogos y esquema de la tabla Parametros.
   - `CREATE OR REPLACE TABLE` de la tabla Parametros con schema `(Clave STRING, Valor STRING)`.
   - Insercion de los 15 registros de configuracion.
   - Lectura de esquemas desde la tabla y creacion de los 4 esquemas medallion.
   - Creacion del Volume gestionado.
5. **Mensaje de finalizacion**: Al completar, la ultima celda muestra:
   ```
   NbConfiguracionInicial: COMPLETADO EXITOSAMENTE
   ```
6. **Tiempos de referencia** (observados en el video):
   - `CREATE CATALOG control`: ~20 segundos (primer catalogo tarda mas).
   - `CREATE CATALOG bronce`: ~1 segundo.
   - `CREATE CATALOG plata`: ~1 segundo.
   - `CREATE CATALOG oro`: ~0.3 segundos.
   - `CREATE SCHEMA control.lab1`: ~4 segundos.
   - **Tiempo total**: 30-60 segundos.

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

## Paso 5: Generar Datos de Prueba

Los 3 notebooks de generacion simulan los archivos del sistema mainframe AS400 bancario. Generan archivos parquet que el pipeline consumira como fuente de datos. La cantidad de registros es configurable mediante widgets.

### Video demostrativo

> **Video**: [2 - Generar Archivos Parquets](videos/2%20-%20Generar_Archivos_Parquets_sub.mp4) — Demuestra la ejecucion de los 3 notebooks de generacion de datos dentro del editor del pipeline LSDP.

---

### Donde abrir los notebooks

Los notebooks de generacion se encuentran dentro del pipeline LSDP. Para acceder a ellos:

1. Abrir el pipeline `LSDP_Laboratorio_Basico` en Databricks.
2. En el panel de archivos de la izquierda, expandir **explorations** > **GenerarParquets**.
3. Las pestanas de los notebooks aparecen como **Excluded** (excluidos del pipeline), lo cual es correcto — son notebooks auxiliares que se ejecutan manualmente.
4. En las opciones de **Widget Panel Settings**, seleccionar **Run Accessed Commands** para que los widgets se configuren automaticamente al ejecutar.

### Resumen de generacion

| # | Notebook | Archivo AS400 | Columnas | Destino en Volume |
|---|----------|--------------|----------|-------------------|
| 1 | `NbGenerarMaestroCliente` | CMSTFL | 70 (41 StringType, 18 DateType, 9 LongType, 2 DoubleType) | `archivos/LSDP_Base/As400/CMSTFL/` |
| 2 | `NbGenerarSaldosCliente` | BLNCFL | 100 (2 LongType, 29 StringType, 34 DoubleType, 35 DateType) | `archivos/LSDP_Base/As400/BLNCFL/` |
| 3 | `NbGenerarTransaccionalCliente` | TRXPFL | 60 (7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType) | `archivos/LSDP_Base/As400/TRXPFL/` |

> **Orden de ejecucion**: Los notebooks deben ejecutarse en el orden indicado (1 > 2 > 3) porque el notebook de saldos y transacciones referencia los clientes generados en el primer notebook (CUSTID).

### 5.1 Generar Maestro de Clientes (CMSTFL)

**Ubicacion**: `explorations/GenerarParquets/NbGenerarMaestroCliente`

**Parametros del notebook (widgets)**:

| Widget | Valor por Defecto | Descripcion |
|--------|------------------|-------------|
| `catalogoParametro` | `control` | Catalogo de la tabla Parametros |
| `esquemaParametro` | `lab1` | Esquema de la tabla Parametros |
| `tablaParametros` | `Parametros` | Nombre de la tabla Parametros |
| `cantidadClientes` | `50000` | Cantidad de clientes a generar (configurable) |
| `camposMutacion` | `CUSNM,CUSLN,CUSMD,CUSFN` | Campos a mutar en re-ejecuciones |
| `porcentajeMutacion` | `0.20` | Porcentaje de registros a mutar (20%) |
| `porcentajeNuevos` | `0.006` | Porcentaje de registros nuevos (0.6%) |
| `montoMinimo` | `10` | Monto minimo para columnas de monto |
| `montoMaximo` | `100000` | Monto maximo para columnas de monto |
| `rutaRelativaMaestroCliente` | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa dentro del Volume |
| `rutaMaestroClienteExistente` | *(vacio)* | Dejar vacio para primera ejecucion |
| `numeroParticiones` | `8` | Numero de particiones del parquet |
| `shufflePartitions` | `8` | Valor de `spark.sql.shuffle.partitions` |

**Ejecucion**: Abrir el notebook > verificar widgets > **Run All**.

**Salida esperada**:
```
FIN: NbGenerarMaestroCliente (CMSTFL)
======================================
  Registros escritos  : 50000
  Columnas            : 70
  Ruta destino        : /Volumes/bronce/lab1/datos_bronce/archivos/LSDP_Base/As400/CMSTFL/...
  Tiempo escritura    : ~XX.XXXs
  Tiempo total        : ~XX.XXXs
======================================
```

### 5.2 Generar Saldos (BLNCFL)

**Ubicacion**: `explorations/GenerarParquets/NbGenerarSaldosCliente`

**Relacion**: Genera exactamente 1 registro por cada CUSTID del Maestro de Clientes. La cantidad de registros se determina automaticamente.

**Distribucion de tipos de cuenta**:
- AHRO (Ahorro): 40%
- CRTE (Corriente): 30%
- PRES (Prestamo): 20%
- INVR (Inversion): 10%

**Parametros principales**:

| Widget | Valor por Defecto | Descripcion |
|--------|------------------|-------------|
| `catalogoParametro` | `control` | Catalogo de la tabla Parametros |
| `esquemaParametro` | `lab1` | Esquema de la tabla Parametros |
| `tablaParametros` | `Parametros` | Nombre de la tabla Parametros |
| `rutaRelativaMaestroCliente` | Ruta del CMSTFL generado | Referencia al maestro existente |
| `rutaRelativaSaldoCliente` | `LSDP_Base/As400/SaldoCliente/` | Ruta destino en Volume |

**Ejecucion**: Abrir el notebook > verificar widgets > **Run All**.

### 5.3 Generar Transacciones (TRXPFL)

**Ubicacion**: `explorations/GenerarParquets/NbGenerarTransaccionalCliente`

**Dependencia**: Requiere que el Maestro de Clientes (CMSTFL) haya sido generado previamente. Lee los CUSTID validos del parquet CMSTFL para garantizar integridad referencial.

**Distribucion de tipos de transaccion**:
- Alta (~60%): CATM, DATM, CMPR, TINT, DPST
- Media (~30%): transferencias y pagos
- Baja (~10%): otros tipos

**Parametros principales**:

| Widget | Valor por Defecto | Descripcion |
|--------|------------------|-------------|
| `cantidadTransacciones` | `150000` | Cantidad de transacciones a generar |
| `catalogoParametro` | `control` | Catalogo de la tabla Parametros |
| `esquemaParametro` | `lab1` | Esquema de la tabla Parametros |
| `tablaParametros` | `Parametros` | Nombre de la tabla Parametros |
| `fechaTransaccion` | *(vacio — se genera automaticamente)* | Fecha de las transacciones |
| `rutaRelativaMaestroCliente` | `LSDP_Base/As400/MaestroCliente/` | Referencia al CMSTFL |
| `rutaRelativaTransaccional` | `LSDP_Base/As400/Transaccional/` | Ruta destino en Volume |

**Ejecucion**: Abrir el notebook > verificar widgets > **Run All**.

**Salida esperada**:
```
FIN: NbGenerarTransaccionalCliente (TRXPFL)
=============================================
  Registros escritos  : 150000
  Columnas            : 60
  Ruta destino        : /Volumes/bronce/lab1/datos_bronce/archivos/LSDP_Base/As400/TRXPFL/...
  Tiempo escritura    : ~XX.XXXs
  Tiempo total        : ~XX.XXXs
=============================================
```

### Verificacion de generacion

Confirmar que los 3 archivos parquet existen en el Volume:

**Opcion A — Via Catalog Explorer**:

Navegar a `bronce` > `lab1` > `datos_bronce` > `archivos` > `LSDP_Base` > `As400` y verificar que existen las 3 carpetas con archivos parquet particionados por fecha:

```
/Volumes/bronce/lab1/datos_bronce/
  └── archivos/
      └── LSDP_Base/
          └── As400/
              ├── CMSTFL/año=YYYY/mes=MM/dia=DD/     (parquet Maestro Clientes)
              ├── BLNCFL/año=YYYY/mes=MM/dia=DD/     (parquet Saldos)
              └── TRXPFL/año=YYYY/mes=MM/dia=DD/     (parquet Transacciones)
```

Dentro de cada carpeta de dia se veran archivos `_SUCCESS`, `_committed_*`, `_started_*` y multiples archivos `part-XXXXX-*.parquet`.

**Opcion B — Via SQL**:

```sql
-- Contar registros en cada archivo generado
SELECT COUNT(*) FROM parquet.`/Volumes/bronce/lab1/datos_bronce/archivos/LSDP_Base/As400/CMSTFL/`;
-- Resultado esperado: la cantidad configurada en cantidadClientes

SELECT COUNT(*) FROM parquet.`/Volumes/bronce/lab1/datos_bronce/archivos/LSDP_Base/As400/TRXPFL/`;
-- Resultado esperado: la cantidad configurada en cantidadTransacciones

SELECT COUNT(*) FROM parquet.`/Volumes/bronce/lab1/datos_bronce/archivos/LSDP_Base/As400/BLNCFL/`;
-- Resultado esperado: igual a la cantidad de clientes (relacion 1:1)
```

> Para el detalle completo de las columnas de cada archivo AS400, ver la Seccion 3 del [Modelo de Datos](../docs/ModeladoDatos.md#3-parquets-as400-fuente).

---

## Paso 6: Crear Pipeline LSDP con 9 Parametros

Este paso crea el pipeline declarativo de Lakeflow Spark que procesa los datos a traves de las 3 capas del medallion. El pipeline se compone de 6 scripts de transformacion y 9 parametros de configuracion.

### Video demostrativo

> **Video**: [1 - Importar Solucion a Databricks Free Edition](videos/1%20-%20Importar_Solución_a_Databricks_Free_Edition_sub.mp4) — La creacion del pipeline se demuestra a partir del minuto 11:00 del video.

---

### 6.1 Crear el pipeline en Databricks

1. En la interfaz web de Databricks, navegar a la opción Jobs & Pipelines.
2. Hacer clic en **Create** > **ETL Pipeline** (o desde el botón directo de **ETL Pipeline** en la parte superior).
3. Configurar los campos generales:

   | Campo | Valor | Nota |
   |-------|-------|------|
   | **Pipeline name** | `LSDP_Laboratorio_Basico` | Nombre descriptivo del pipeline |
   | **Default catalog** | `bronce` | Catalogo por defecto para las tablas del pipeline |
   | **Default schema** | `lab1` | Esquema por defecto |
   | **Compute** | `Serverless` | Compute gestionado sin cluster dedicado |

4. En la opcion avanzada **Add existing assets**.
5. Se abre un dialogo para seleccionar la carpeta raiz del pipeline. Navegar hasta:
   ```
   src/LSDP_Laboratorio_Basico
   ```
6. Luego se debe seleccionar la carpeta del **source code** del proyecto, la cual es la llamada **transformations**
   ```
   src/LSDP_Laboratorio_Basico/transformations
   ```
7. Luego el editor del pipeline carga automaticamente toda la estructura de archivos:
   ```
   LSDP_Laboratorio_Basico/
   ├── explorations/
   │   └── GenerarParquets/
   │       ├── NbGenerarMaestroCliente        (Excluded)
   │       ├── NbGenerarSaldosCliente         (Excluded)
   │       ├── NbGenerarTransaccionalCliente  (Excluded)
   │       └── NbTddGenerarParquets           (Excluded)
   ├── transformations/
   │   ├── LsdpBronceBlncfl
   │   ├── LsdpBronceCmstfl
   │   ├── LsdpBronceTrxpfl
   │   ├── LsdpOroClientes
   │   ├── LsdpPlataClientesSaldos
   │   └── LsdpPlataTransacciones
   └── utilities/
       ├── LsdpConexionParametros.py
       ├── LsdpConstructorRutas.py
       ├── LsdpInsertarTiposTransaccion
       └── LsdpReordenarColumnasLiquidCluster.py
   ```

> Los notebooks bajo `explorations/GenerarParquets/` aparecen con la marca **Excluded** porque no forman parte del flujo declarativo del pipeline — se ejecutan manualmente para generar datos de prueba (ver [Paso 5](#paso-5-generar-datos-de-prueba)).

### 6.2 Scripts de transformacion (cargados automaticamente)

Al configurar la carpeta raiz como `src/LSDP_Laboratorio_Basico` y luego la carpeta con el source code `src/LSDP_Laboratorio_Basico/transformations`, los 6 scripts de transformacion se cargan automaticamente. No es necesario agregarlos individualmente. El pipeline detecta los decoradores `@dp.table` y `@dp.materialized_view` en los archivos:

| # | Archivo | Capa | Decorador | Entidad que crea |
|---|---------|------|-----------|-----------------|
| 1 | `LsdpBronceCmstfl` | Bronce | `@dp.table` | `bronce.lab1.cmstfl` |
| 2 | `LsdpBronceTrxpfl` | Bronce | `@dp.table` | `bronce.lab1.trxpfl` |
| 3 | `LsdpBronceBlncfl` | Bronce | `@dp.table` | `bronce.lab1.blncfl` |
| 4 | `LsdpPlataClientesSaldos` | Plata | `@dp.materialized_view` | `plata.lab1.clientes_saldos_consolidados` |
| 5 | `LsdpPlataTransacciones` | Plata | `@dp.materialized_view` | `plata.lab1.transacciones_enriquecidas` |
| 6 | `LsdpOroClientes` | Oro | `@dp.materialized_view` | `oro.lab1.comportamiento_atm_cliente` + `oro.lab1.resumen_integral_cliente` |

> Los scripts tambien importan automaticamente los modulos de `utilities/` (`LsdpConexionParametros.py`, `LsdpConstructorRutas.py`, etc.) gracias a la resolucion automatica de imports del runtime LSDP.

### 6.3 Configurar los 9 parametros del pipeline

1. En el editor del pipeline, hacer clic en **Settings** (barra superior derecha).
2. En la seccion **Configuration**, agregar cada parametro haciendo clic en **Add configuration**.
3. Cada parametro se agrega como un par clave-valor. La clave debe incluir el prefijo `pipelines.parameters.`:

| # | Clave completa | Valor |
|---|----------------|-------|
| 1 | `pipelines.parameters.catalogoParametro` | `control` |
| 2 | `pipelines.parameters.esquemaParametro` | `lab1` |
| 3 | `pipelines.parameters.tablaParametros` | `Parametros` |
| 4 | `pipelines.parameters.rutaCompletaMaestroCliente` | *(ruta relativa al parquet CMSTFL)* |
| 5 | `pipelines.parameters.rutaCompletaTransaccional` | *(ruta relativa al parquet TRXPFL)* |
| 6 | `pipelines.parameters.rutaCompletaSaldoCliente` | *(ruta relativa al parquet BLNCFL)* |
| 7 | `pipelines.parameters.rutaSchemaLocationCmstfl` | *(ruta para schema evolution CMSTFL)* |
| 8 | `pipelines.parameters.rutaSchemaLocationTrxpfl` | *(ruta para schema evolution TRXPFL)* |
| 9 | `pipelines.parameters.rutaSchemaLocationBlncfl` | *(ruta para schema evolution BLNCFL)* |

> Los parametros 1-3 permiten al pipeline leer la tabla `control.lab1.Parametros` para obtener el resto de la configuracion (catalogos, esquemas, tipo de almacenamiento). Los parametros 4-9 definen las rutas de los archivos fuente y las ubicaciones de schema evolution de AutoLoader.

4. Verificar en el panel de **Settings** que aparecen los 9 parametros correctamente. El panel muestra: Default catalog = `bronce`, Default schema = `lab1`, Compute = `Serverless`, y la lista completa de configuraciones.

> Para el detalle completo de como el pipeline usa estos parametros, ver la Seccion 5 del [Manual Tecnico](../docs/ManualTecnico.md#5-parametros-del-pipeline-y-notebooks).

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

> **Video**: [3 - Lakeflow Spark Declarative Pipelines](videos/3%20-%20Lakeflow_Spark_Declarative_Pipelines.mp4) — Incluye un recorrido completo por el codigo de las transformaciones y la ejecucion del pipeline con el grafo de dependencias en tiempo real. La ejecucion inicia aproximadamente en el minuto 24:30.

---

### 7.1 Iniciar la ejecucion (Full Refresh)

1. En la interfaz de Databricks, navegar al pipeline `LSDP_Laboratorio_Basico`.
2. Hacer clic en el boton **Run pipeline** (boton verde en la esquina superior derecha).
3. Para la primera ejecucion, se realiza automaticamente un **Full refresh all** — esto crea todas las tablas y vistas desde cero.

### 7.2 Orden de ejecucion (automatico)

El motor LSDP resuelve las dependencias entre tablas y ejecuta en el orden optimo. El **Pipeline graph** (grafo de dependencias) se puede visualizar en tiempo real haciendo clic en **Pipeline graph** > **Maximized**:

```
Etapa 1 — Bronce (en paralelo, Streaming Tables con AutoLoader):
  ├── trxpfl  ──────────→  transacciones_enriquecidas  ──→  comportamiento_atm_cliente
  │   (Streaming Table)      (Materialized View, Plata)       (Materialized View, Oro)
  │
  ├── blncfl  ──┐
  │   (ST)      ├────────→  clientes_saldos_consolidados ──→  resumen_integral_cliente
  └── cmstfl  ──┘               (Materialized View, Plata)     (Materialized View, Oro)
      (ST)
```

**Flujo de datos**:
- **Etapa 1**: Las 3 Streaming Tables de bronce ingestan los parquets via AutoLoader (cloudFiles) con schema evolution (`addNewColumns`).
- **Etapa 2**: Las 2 Vistas Materializadas de plata consolidan y enriquecen los datos. `clientes_saldos_consolidados` hace LEFT JOIN entre cmstfl y blncfl. `transacciones_enriquecidas` enriquece trxpfl.
- **Etapa 3**: Las 2 Vistas Materializadas de oro generan metricas analiticas. `comportamiento_atm_cliente` agrega por `identificador_cliente` con groupBy. `resumen_integral_cliente` combina datos de las 2 vistas de plata.

### 7.3 Monitorear la ejecucion

Durante la ejecucion, el **Pipeline graph** muestra en tiempo real el estado de cada nodo:

| Color del nodo | Significado |
|----------------|-------------|
| **Gris** | Pendiente — aun no ha comenzado |
| **Azul** | En ejecucion — procesando datos |
| **Verde (✓)** | Completado — datos disponibles |
| **Rojo** | Error — la ejecucion fallo (ver logs del nodo) |

La seccion inferior muestra dos pestanas:
- **Tables**: Lista todas las tablas con su catalogo, esquema, tipo, duracion, registros de salida, expectativas de calidad y modo de incrementalizacion.
- **Performance**: Metricas de rendimiento por tabla.

Al hacer clic en cualquier nodo del grafo se puede ver:
- Cantidad de registros procesados (Output records).
- Tiempo de ejecucion del paso (Duration).
- Resultados de las expectativas de calidad (Expectations: `met | unmet`).
- Warnings de calidad de datos.
- Logs detallados del Pipeline event log.

### 7.4 Resultados esperados (primera ejecucion)

| Tabla | Catalogo | Tipo | Duracion aprox. | Registros |
|-------|----------|------|-----------------|-----------|
| `blncfl` | bronce | Streaming Table | ~43s | Igual a cantidad de clientes |
| `cmstfl` | bronce | Streaming Table | ~45s | Igual a cantidad de clientes |
| `trxpfl` | bronce | Streaming Table | ~54s | Igual a cantidad de transacciones |
| `clientes_saldos_consolidados` | plata | Materialized View | ~1-2 min | Igual a cantidad de clientes |
| `transacciones_enriquecidas` | plata | Materialized View | ~1-2 min | Igual a cantidad de transacciones |
| `comportamiento_atm_cliente` | oro | Materialized View | ~7s | Igual a cantidad de clientes |
| `resumen_integral_cliente` | oro | Materialized View | ~13s | Igual a cantidad de clientes |

**Tiempo total**: ~4-5 minutos (primera ejecucion con Full refresh all).

### Verificacion rapida post-ejecucion

Una vez el pipeline muestre todos los nodos en verde (✓):

```sql
-- Verificar datos en bronce
SELECT * FROM bronce.lab1.cmstfl LIMIT 10;

-- Verificar datos en plata (transacciones enriquecidas)
SELECT * FROM plata.lab1.transacciones_enriquecidas LIMIT 10;

-- Verificar datos en oro (resumen integral del cliente)
SELECT * FROM oro.lab1.resumen_integral_cliente LIMIT 10;
```

Ademas, verificar en el **Catalog Explorer** que las tablas de oro aparecen correctamente:
- Navegar a `oro` > `lab1` > deben verse `comportamiento_atm_cliente` y `resumen_integral_cliente`.

---

## Paso 8: Ejecucion Incremental del Pipeline

Una de las ventajas de LSDP es que soporta **ejecucion incremental** de forma nativa. Tras la primera ejecucion (Full refresh), las siguientes ejecuciones solo procesan los datos nuevos.

### Video demostrativo

> **Video**: [4 - Ejecucion Incremental de LSDP](videos/4%20-%20Ejecucion_Incremental_de_LSDP_sub.mp4) — Demuestra la generacion de nuevos datos para un segundo dia y la ejecucion incremental del pipeline.

---

### 8.1 Generar datos incrementales (segundo dia)

Para simular la llegada de nuevos datos al sistema, se re-ejecutan los notebooks de generacion apuntando a un nuevo dia:

1. **NbGenerarMaestroCliente**: Configurar el widget `rutaMaestroClienteExistente` con la ruta del dia anterior (ej: `.../año=2026/mes=04/dia=01/`) y `rutaRelativaMaestroCliente` con la nueva fecha (ej: `.../año=2026/mes=04/dia=02/`). El notebook lee el maestro existente, aplica mutaciones (20% de registros mutados, 0.6% nuevos) y genera el parquet del nuevo dia.

2. **NbGenerarSaldosCliente**: Actualizar la ruta destino al nuevo dia (`.../BLNCFL/año=2026/mes=04/dia=02/`).

3. **NbGenerarTransaccionalCliente**: Actualizar la ruta destino al nuevo dia (`.../TRXPFL/año=2026/mes=04/dia=02/`).

### 8.2 Ejecutar el pipeline (incremental, no Full refresh)

1. Navegar al pipeline `LSDP_Laboratorio_Basico`.
2. Hacer clic en la flecha del boton **Run pipeline** para ver las opciones:
   - **Run pipeline** ← Seleccionar esta opcion (ejecucion incremental por defecto).
   - **Run pipeline with full table refresh** ← NO seleccionar (reprocesa todo desde cero).
   - **Run now with different settings** ← Para ejecuciones cuando la configuracion ha sido modificada o tienes elementos avanzados.
3. El pipeline detecta automaticamente los nuevos archivos via AutoLoader y procesa solo el incremento.

### 8.3 Como funciona la incrementalizacion

El pipeline optimiza automaticamente la estrategia de ejecucion para cada tabla:

| Tabla | Tipo | Modo Incremental | Descripcion |
|-------|------|-------------------|-------------|
| `cmstfl` | Streaming Table | AutoLoader | Detecta nuevos archivos automaticamente |
| `blncfl` | Streaming Table | AutoLoader | Detecta nuevos archivos automaticamente |
| `trxpfl` | Streaming Table | AutoLoader | Detecta nuevos archivos automaticamente |
| `transacciones_enriquecidas` | Materialized View | **APPEND_ONLY** | Solo agrega registros nuevos sin reprocesar historico |
| `comportamiento_atm_cliente` | Materialized View | **GROUP_AGGREGATE** | Re-agrega solo los grupos afectados por datos nuevos |
| `clientes_saldos_consolidados` | Materialized View | Incremental | Procesa incrementalmente los cambios |
| `resumen_integral_cliente` | Materialized View | **Full recompute** | Se recalcula completa (ultimo nivel de agregacion) |

> Los modos de incrementalizacion se pueden verificar en la columna **Incrementalization** de la tabla de metricas del pipeline, y en el **Pipeline event log details** de cada nodo.

### 8.4 Verificar la ejecucion incremental

Tras la segunda ejecucion, verificar que:

1. **Registros acumulados**: Las Streaming Tables muestran los registros de ambos dias combinados.
2. **Etiqueta "Incremental"**: En el Pipeline graph, las vistas materializadas muestran la etiqueta **Incremental** entre los nodos.
3. **Tiempo de ejecucion**: La ejecucion incremental tiene un tiempo similar o menor que el Full refresh, dependiendo del volumen de datos nuevos.

---

## Paso 9: Verificar Resultados con Consultas SQL

Una vez el pipeline ha completado exitosamente, se ejecutan las siguientes consultas de validacion para confirmar que los datos fluyen correctamente por las 3 capas del medallion.

### 9.1 Validacion completa de la arquitectura medallion

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

### 9.2 Verificar campos calculados de plata

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

### 9.3 Verificar metricas ATM de oro

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

### 9.4 Verificar el resumen integral de cliente (oro)

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

### 9.5 Verificar la tabla Parametros

Confirmar que los 15 registros de configuracion se mantienen intactos:

```sql
SELECT Clave, Valor
FROM control.lab1.Parametros
ORDER BY Clave;
```

**Resultado esperado**: 15 filas con las claves y valores de configuracion (todas con esquemas `lab1`).

### 9.6 Verificar expectativas de calidad de datos

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
| Pipeline falla con "parameter not found" | Los 9 parametros del pipeline no fueron configurados | Verificar en **Settings** > **Configuration** del pipeline que los 9 parametros estan presentes con los valores del [Paso 6.3](#63-configurar-los-9-parametros-del-pipeline) |
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
| CMSTFL (Maestro Clientes) | `/Volumes/bronce/lab1/datos_bronce/archivos/LSDP_Base/As400/CMSTFL/` |
| TRXPFL (Transaccional) | `/Volumes/bronce/lab1/datos_bronce/archivos/LSDP_Base/As400/TRXPFL/` |
| BLNCFL (Saldos) | `/Volumes/bronce/lab1/datos_bronce/archivos/LSDP_Base/As400/BLNCFL/` |

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

### Videos demostrativos

| # | Video | Duracion | Pasos que cubre |
|---|-------|----------|-----------------|
| 0 | [Introduccion](videos/0%20-%20Introduccion_sub.mp4) | 3 min | Contexto general del proyecto |
| 1 | [Importar Solucion a Databricks Free Edition](videos/1%20-%20Importar_Solución_a_Databricks_Free_Edition_sub.mp4) | 20 min | Pasos 2, 4 y 6 (importar repo, configuracion inicial, crear pipeline) |
| 2 | [Generar Archivos Parquets](videos/2%20-%20Generar_Archivos_Parquets_sub.mp4) | 11 min | Paso 5 (generar datos de prueba) |
| 3 | [Lakeflow Spark Declarative Pipelines](videos/3%20-%20Lakeflow_Spark_Declarative_Pipelines.mp4) | 32 min | Paso 7 (recorrido del codigo y primera ejecucion del pipeline) |
| 4 | [Ejecucion Incremental de LSDP](videos/4%20-%20Ejecucion_Incremental_de_LSDP_sub.mp4) | 20 min | Paso 8 (generacion de datos incrementales y segunda ejecucion) |
| 5 | [Spec-Driven Development Utilizado](videos/5%20-%20Spec-Driven_Development_utilizado_Sub.mp4) | 27 min | Metodologia de desarrollo utilizada en el proyecto |

---

<div align="center">

**[Volver al README](../README.md)**

</div>
