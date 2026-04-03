# Guia Rapida: Incremento 1 - Configuracion Base

**Fecha**: 2026-04-03
**Branch**: `001-incremento1-config-inicial`

## Prerequisitos

1. Workspace de Databricks Free Edition creado y accesible.
2. Unity Catalog habilitado en el workspace.
3. Computo Serverless disponible.
4. Visual Studio Code instalado con las extensiones:
   - Databricks Extension for Visual Studio Code
   - Databricks Driver for SQLTools
5. Conexion configurada al workspace de Databricks Free Edition desde VS Code.

## Pasos para Ejecutar

### Paso 1: Verificar extensiones de VS Code

Abrir VS Code y verificar que las extensiones de Databricks estan instaladas:
- Buscar "Databricks" en la barra lateral de extensiones.
- Confirmar conexion al workspace haciendo clic en el logo de Databricks en la barra lateral.
- Verificar que se listan los computos disponibles (Serverless).

### Paso 2: Ejecutar el notebook de configuracion inicial

El archivo `conf/NbConfiguracionInicial.py` se ejecuta como notebook en Databricks.

**Parametros del notebook** (via `dbutils.widgets`, modificables antes de ejecutar):

| Parametro | Valor por Defecto | Descripcion |
|-----------|-------------------|-------------|
| catalogoParametro | control | Catalogo para la tabla de parametros |
| esquemaParametro | regional | Esquema para la tabla de parametros |
| tablaParametros | Parametros | Nombre de la tabla de parametros |

**Opciones de ejecucion**:
- Desde VS Code: click derecho en el archivo -> "Run on Serverless".
- Desde la UI de Databricks: abrir el notebook -> editar widgets si es necesario -> Run All.

### Paso 3: Verificar resultados

Despues de la ejecucion, verificar en la salida de pantalla:
1. Los 3 parametros de entrada fueron capturados correctamente.
2. Los 4 catalogos (control, bronce, plata, oro) fueron creados o ya existian.
3. Los 4 esquemas fueron creados o ya existian.
4. La tabla de parametros fue creada con 15 registros.
5. El Volume gestionado fue creado o ya existia.

**Consulta de verificacion** (ejecutar en un notebook o SQL Editor):

```sql
SELECT * FROM control.regional.Parametros ORDER BY Clave;
```

Resultado esperado: 15 filas con columnas `Clave` y `Valor`.

## Estructura de Archivos (Incremento 1)

```text
DbsFreeLakeflowSparkDeclarativePipelinesBase/
  conf/
    NbConfiguracionInicial.py    # Ejecutar este notebook
  README.md
  SYSTEM.md
```

## Problemas Frecuentes

| Problema | Causa | Solucion |
|----------|-------|----------|
| Error de permisos al crear catalogo | Usuario sin privilegio CREATE CATALOG | Solicitar permisos al administrador del workspace |
| Notebook no ejecuta en VS Code | Extension no conectada al workspace | Verificar conexion en barra lateral de Databricks |
| Tabla no aparece en la consulta | Catalogo/esquema incorrecto | Verificar valores de los widgets en la salida de pantalla |
| Error de cuota | Cuota de Databricks Free Edition agotada | Esperar renovacion de cuota o reducir uso |
