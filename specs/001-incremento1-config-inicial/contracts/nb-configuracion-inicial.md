# Contrato: Notebook de Configuracion Inicial (NbConfiguracionInicial.py)

**Fecha**: 2026-04-03
**Tipo**: Notebook interactivo de Databricks (.py)

## Interfaz de Entrada (dbutils.widgets)

El notebook recibe 3 parametros de entrada via `dbutils.widgets.text()`:

| Parametro | Tipo | Valor por Defecto | Descripcion |
|-----------|------|-------------------|-------------|
| catalogoParametro | string | control | Catalogo de Unity Catalog donde se crea la tabla de parametros |
| esquemaParametro | string | regional | Esquema dentro del catalogo donde se crea la tabla de parametros |
| tablaParametros | string | Parametros | Nombre de la tabla de parametros a crear |

## Interfaz de Salida

### Recursos creados en Unity Catalog

| Recurso | Tipo | Ubicacion |
|---------|------|-----------|
| Catalogo control | CREATE CATALOG IF NOT EXISTS | Unity Catalog |
| Catalogo bronce | CREATE CATALOG IF NOT EXISTS | Unity Catalog |
| Catalogo plata | CREATE CATALOG IF NOT EXISTS | Unity Catalog |
| Catalogo oro | CREATE CATALOG IF NOT EXISTS | Unity Catalog |
| Esquema control.{esquemaControl} | CREATE SCHEMA IF NOT EXISTS | Dentro de catalogo control |
| Esquema bronce.{esquemaBronce} | CREATE SCHEMA IF NOT EXISTS | Dentro de catalogo bronce |
| Esquema plata.{esquemaPlata} | CREATE SCHEMA IF NOT EXISTS | Dentro de catalogo plata |
| Esquema oro.{esquemaOro} | CREATE SCHEMA IF NOT EXISTS | Dentro de catalogo oro |
| Tabla Parametros | CREATE OR REPLACE TABLE | {catalogoParametro}.{esquemaParametro}.{tablaParametros} |
| Volume gestionado | CREATE VOLUME IF NOT EXISTS | {catalogoVolume}.{esquemaVolume}.{nombreVolume} |

### Salida por pantalla (observabilidad)

El notebook imprime en consola:

1. **Bloque de resumen inicial**: todos los parametros recibidos y sus valores.
2. **Progreso de creacion**: cada catalogo, esquema, tabla y volume creado con su estado.
3. **Registros insertados**: cada par Clave-Valor insertado en la tabla.
4. **Tiempos de ejecucion**: duracion de operaciones criticas.
5. **Bloque de resumen final**: metricas totales de ejecucion (registros insertados, recursos creados, tiempo total).

## Flujo de Ejecucion

```text
1. Definir widgets (dbutils.widgets.text)
2. Capturar valores de parametros
3. Validar parametros no vacios
4. Imprimir bloque de resumen inicial
5. Crear catalogos (control, bronce, plata, oro) con IF NOT EXISTS
6. Crear tabla Parametros con CREATE OR REPLACE TABLE
7. Insertar 15 registros de parametros
8. Leer valores de tabla para obtener esquemas y datos de Volume
9. Crear esquemas correspondientes a cada catalogo con IF NOT EXISTS
10. Crear Volume gestionado con IF NOT EXISTS
11. Imprimir bloque de resumen final con metricas
```

## Precondiciones

- Workspace de Databricks Free Edition activo con Unity Catalog habilitado.
- Computo Serverless disponible.
- Permisos suficientes para crear catalogos, esquemas, tablas y volumes.

## Postcondiciones

- 4 catalogos creados (o ya existentes).
- 4 esquemas creados (o ya existentes, uno por catalogo).
- 1 tabla Delta con exactamente 15 registros.
- 1 Volume gestionado creado (o ya existente).
- Toda la salida de observabilidad impresa en pantalla.
