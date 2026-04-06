# Modelo de Datos: Incremento 1 - Research Inicial y Configuracion Base

**Fecha**: 2026-04-03
**Branch**: `001-incremento1-config-inicial`

## Entidades

### 1. Tabla Parametros

**Ubicacion**: `{catalogoParametro}.{esquemaParametro}.{tablaParametros}` (por defecto: `control.regional.Parametros`)
**Tipo**: Tabla Delta gestionada en Unity Catalog
**Estrategia de idempotencia**: `CREATE OR REPLACE TABLE` + INSERT completo

| Campo | Tipo de Dato | Descripcion | Restricciones |
|-------|-------------|-------------|---------------|
| Clave | STRING | Identificador unico del parametro de configuracion | NOT NULL, unico logico |
| Valor | STRING | Valor asociado al parametro de configuracion | NOT NULL (puede ser cadena vacia para parametros opcionales) |

#### Registros (15 filas)

| Clave | Valor por Defecto | Descripcion | Categoria |
|-------|-------------------|-------------|-----------|
| catalogoBronce | bronce | Catalogo Unity Catalog para la medalla de bronce | Catalogos |
| esquemaBronce | regional | Esquema dentro del catalogo de bronce | Esquemas |
| contenedorBronce | bronce | Nombre del directorio contenedor de bronce dentro del almacenamiento | Storage |
| TipoStorage | Volume | Tipo de almacenamiento: "Volume" o "AmazonS3" | Storage |
| catalogoVolume | bronce | Catalogo del Volume (aplica si TipoStorage=Volume) | Storage Volume |
| esquemaVolume | regional | Esquema del Volume (aplica si TipoStorage=Volume) | Storage Volume |
| nombreVolume | datos_bronce | Nombre del Volume gestionado (aplica si TipoStorage=Volume) | Storage Volume |
| bucketS3 | (vacio) | Nombre del bucket S3 (aplica si TipoStorage=AmazonS3) | Storage S3 |
| prefijoS3 | (vacio) | Prefijo dentro del bucket S3 (aplica si TipoStorage=AmazonS3) | Storage S3 |
| DirectorioBronce | archivos | Subdirectorio dentro del contenedorBronce donde se almacenan los datos | Storage |
| catalogoPlata | plata | Catalogo Unity Catalog para la medalla de plata | Catalogos |
| esquemaPlata | regional | Esquema dentro del catalogo de plata | Esquemas |
| catalogoOro | oro | Catalogo Unity Catalog para la medalla de oro | Catalogos |
| esquemaOro | regional | Esquema dentro del catalogo de oro | Esquemas |
| esquemaControl | regional | Esquema dentro del catalogo de control | Esquemas |

#### Transiciones de Estado

La tabla Parametros no tiene transiciones de estado propiamente dichas. Su ciclo de vida es:
1. **Inexistente** -> Se ejecuta el notebook -> **Creada con 15 registros**
2. **Existente (ejecucion previa)** -> Se re-ejecuta el notebook -> **Recreada con 15 registros** (via CREATE OR REPLACE)

### 2. Volume Unity Catalog

**Ubicacion**: `{catalogoVolume}.{esquemaVolume}.{nombreVolume}` (por defecto: `bronce.regional.datos_bronce`)
**Tipo**: Volume gestionado (MANAGED) en Unity Catalog
**Ruta de acceso**: `/Volumes/{catalogoVolume}/{esquemaVolume}/{nombreVolume}/`
**Estrategia de idempotencia**: `CREATE VOLUME IF NOT EXISTS`

El Volume no tiene campos ni registros. Es un recurso de almacenamiento para archivos parquets.

### 3. Catalogos Unity Catalog (4 catalogos)

Creados automaticamente por el notebook con `CREATE CATALOG IF NOT EXISTS`.

| Catalogo | Proposito | Esquema Asociado |
|----------|-----------|-----------------|
| control | Tablas de parametros, metadatos y control del pipeline | esquemaControl (defecto: regional) |
| bronce | Streaming tables de ingesta desde parquets AS400 | esquemaBronce (defecto: regional) |
| plata | Vistas materializadas de transformacion y enriquecimiento | esquemaPlata (defecto: regional) |
| oro | Vistas materializadas de agregacion y producto de datos final | esquemaOro (defecto: regional) |

### 4. Esquemas Unity Catalog (4 esquemas)

Creados automaticamente por el notebook con `CREATE SCHEMA IF NOT EXISTS`, uno por cada catalogo.

| Esquema | Catalogo | Parametro en tabla |
|---------|----------|-------------------|
| regional (defecto) | control | esquemaControl |
| regional (defecto) | bronce | esquemaBronce |
| regional (defecto) | plata | esquemaPlata |
| regional (defecto) | oro | esquemaOro |

## Relaciones entre Entidades

```text
Catalogos (4)
  |-- cada uno contiene un Esquema
       |-- control.{esquemaControl}
       |     |-- Tabla Parametros (Clave, Valor) [15 registros]
       |-- bronce.{esquemaBronce}
       |     |-- Volume gestionado ({nombreVolume})
       |-- plata.{esquemaPlata}
       |     |-- (vacio en Incremento 1)
       |-- oro.{esquemaOro}
             |-- (vacio en Incremento 1)
```

## Reglas de Validacion

- La columna `Clave` de la tabla Parametros debe tener exactamente 15 valores distintos despues de cada ejecucion.
- La columna `Valor` acepta cadenas vacias para parametros opcionales (como `bucketS3` y `prefijoS3` cuando `TipoStorage = "Volume"`).
- El parametro `TipoStorage` solo acepta dos valores: `"Volume"` o `"AmazonS3"`.
- Los nombres de catalogo y esquema deben cumplir con las reglas de nomenclatura de Unity Catalog (alfanumericos y guiones bajos).
