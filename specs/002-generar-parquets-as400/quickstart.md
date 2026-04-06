# Guia de Inicio Rapido: Generacion de Parquets AS400

**Feature**: 002-generar-parquets-as400

## Prerequisitos

1. **Incremento 1 completado**: El notebook `conf/NbConfiguracionInicial.py` debe haberse ejecutado exitosamente, creando:
   - Catalogos: `control`, `bronce`, `plata`, `oro`
   - Esquemas: `lab1` en cada catalogo
   - Tabla: `control.lab1.Parametros` (15 registros)
   - Volume: `bronce.lab1.datos_bronce`

2. **Workspace de Databricks Free Edition** activo con Unity Catalog habilitado y Serverless Compute disponible.

3. **Extensiones VS Code** instaladas y configuradas:
   - Databricks Extension for Visual Studio Code
   - Databricks Driver for SQLTools

4. **(Opcional) Faker**: Si se desea datos mas realistas, instalar Faker en el entorno Serverless via panel Environment o ejecutar `%pip install faker` como primera celda. Si no esta disponible, los notebooks usaran listas estaticas automaticamente.

## Orden de Ejecucion

Los notebooks deben ejecutarse en este orden estricto debido a las dependencias entre entidades:

```text
1. NbGenerarMaestroCliente.py      (CMSTFL - sin dependencias)
2. NbGenerarTransaccionalCliente.py (TRXPFL - requiere CMSTFL)
3. NbGenerarSaldosCliente.py       (BLNCFL - requiere CMSTFL)
4. NbTddGenerarParquets.py         (Pruebas - requiere los 3 parquets)
```

Los notebooks 2 y 3 pueden ejecutarse en cualquier orden entre si, pero ambos dependen de que el notebook 1 haya sido ejecutado previamente.

## Ejecucion Paso a Paso

### Paso 1: Generar Maestro de Clientes

**Archivo**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarMaestroCliente.py`

**Parametros por defecto** (modificables via widgets):
| Widget | Defecto |
|--------|---------|
| catalogoParametro | control |
| esquemaParametro | lab1 |
| tablaParametros | Parametros |
| cantidadClientes | 50000 |
| rutaRelativaMaestroCliente | LSDP_Base/As400/MaestroCliente/ |

> Para la lista completa de widgets (montoMinimo, montoMaximo, numeroParticiones, shufflePartitions, porcentajeMutacion, porcentajeNuevos, camposMutacion, rutaMaestroClienteExistente), consultar el [contrato del notebook](contracts/notebooks-generacion-parquets.md).

**Ejecucion**: Abrir el notebook en VS Code, conectar a Serverless, ejecutar todas las celdas.

**Resultado esperado**:
- Parquet con 50,000 registros y 70 columnas en `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/MaestroCliente/`
- Resumen impreso en consola con parametros, ruta y metricas

### Paso 2: Generar Transaccional de Clientes

**Archivo**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py`

**Parametros por defecto**:
| Widget | Defecto |
|--------|---------|
| catalogoParametro | control |
| esquemaParametro | lab1 |
| tablaParametros | Parametros |
| cantidadTransacciones | 150000 |
| fechaTransaccion | (obligatorio, ej: 2026-04-03) |
| rutaRelativaTransaccional | LSDP_Base/As400/Transaccional/ |
| rutaRelativaMaestroCliente | LSDP_Base/As400/MaestroCliente/ |

> Para la lista completa de widgets (montoMinimo, montoMaximo, numeroParticiones, shufflePartitions), consultar el [contrato del notebook](contracts/notebooks-generacion-parquets.md).

**Nota**: El parametro `fechaTransaccion` no tiene valor por defecto y debe proporcionarse.

**Nota**: El parametro `rutaRelativaMaestroCliente` puede apuntar a una carpeta padre (ej: `archivos/LSDP_Base/As400/CMSTFL`) que contenga multiples particiones por dia. El notebook lee recursivamente todos los parquets y extrae CUSTIDs unicos via `.distinct()`.

**Resultado esperado**:
- Parquet con 150,000 registros y 60 columnas en `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/Transaccional/`

### Paso 3: Generar Saldos de Clientes

**Archivo**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbGenerarSaldosCliente.py`

**Parametros por defecto**:
| Widget | Defecto |
|--------|---------|
| catalogoParametro | control |
| esquemaParametro | lab1 |
| tablaParametros | Parametros |
| rutaRelativaSaldoCliente | LSDP_Base/As400/SaldoCliente/ |
| rutaRelativaMaestroCliente | LSDP_Base/As400/MaestroCliente/ |

> Para la lista completa de widgets (montoMinimo, montoMaximo, numeroParticiones, shufflePartitions), consultar el [contrato del notebook](contracts/notebooks-generacion-parquets.md).

**Nota**: El parametro `rutaRelativaMaestroCliente` puede apuntar a una carpeta padre (ej: `archivos/LSDP_Base/As400/CMSTFL`) que contenga multiples particiones por dia. El notebook lee recursivamente todos los parquets y extrae CUSTIDs unicos via `.distinct()`, generando exactamente 1 saldo por cada CUSTID unico.

**Resultado esperado**:
- Parquet con 50,000 registros (1:1 con CUSTIDs unicos de CMSTFL) y 100 columnas en `/Volumes/bronce/lab1/datos_bronce/LSDP_Base/As400/SaldoCliente/`

### Paso 4: Ejecutar Pruebas TDD

**Archivo**: `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/NbTddGenerarParquets.py`

**Resultado esperado**:
- Todas las pruebas PASS: estructura de columnas, tipos de datos, cantidades, distribuciones, integridad referencial

## Verificacion Rapida

Despues de ejecutar los 4 notebooks, verificar:

| Entidad | Registros | Columnas | Ruta |
|---------|-----------|----------|------|
| CMSTFL | 50,000 | 70 | `.../LSDP_Base/As400/MaestroCliente/` |
| TRXPFL | 150,000 | 60 | `.../LSDP_Base/As400/Transaccional/` |
| BLNCFL | 50,000 | 100 | `.../LSDP_Base/As400/SaldoCliente/` |

## Re-ejecuciones

- **CMSTFL**: Si se proporciona `rutaMaestroClienteExistente` con la ruta del parquet previo, lee los clientes existentes, muta `porcentajeMutacion` (defecto 0.20 = 20%) de registros en los campos de `camposMutacion`, y agrega `porcentajeNuevos` (defecto 0.006 = 0.6%) nuevos. Si `rutaMaestroClienteExistente` esta vacio, genera desde cero.
- **TRXPFL**: Genera lote completamente nuevo (independiente de ejecuciones previas). `rutaRelativaMaestroCliente` puede apuntar a carpeta padre con multiples dias; los CUSTIDs se deduplicaran automaticamente.
- **BLNCFL**: Regenera completamente (1:1 con CUSTIDs unicos del CMSTFL). `rutaRelativaMaestroCliente` puede apuntar a carpeta padre con multiples dias.

## Resolucion de Problemas

| Problema | Solucion |
|----------|----------|
| "TipoStorage no reconocido" | Verificar que la tabla Parametros tenga TipoStorage = "Volume" o "AmazonS3" |
| "Maestro de Clientes no encontrado" | Ejecutar NbGenerarMaestroCliente.py antes que los demas notebooks |
| "fechaTransaccion formato invalido" | Proporcionar fecha en formato YYYY-MM-DD (ej: 2026-04-03) |
| "Faker no disponible" | Normal en Free Edition; los notebooks usan listas estaticas automaticamente |
| "Recursos insuficientes" | Reducir cantidadClientes/cantidadTransacciones via widgets |
