# Modelado de Datos: LSDP Laboratorio Basico

**Proyecto**: DbsFreeLakeflowSparkDeclarativePipelinesBase
**Plataforma**: Databricks Free Edition con Unity Catalog
**Fecha**: 2026-04-06
**Arquitectura**: Medallion (Bronce / Plata / Oro)

---

## Tabla de Contenidos

1. [Resumen de Entidades](#1-resumen-de-entidades)
2. [Diagrama de Linaje](#2-diagrama-de-linaje)
3. [Parquets AS400 (Fuente)](#3-parquets-as400-fuente)
4. [Streaming Tables Bronce](#4-streaming-tables-bronce)
5. [Vistas Materializadas Plata](#5-vistas-materializadas-plata)
6. [Vistas Materializadas Oro](#6-vistas-materializadas-oro)
7. [Tabla Parametros](#7-tabla-parametros)
8. [Campos Calculados](#8-campos-calculados)
9. [Expectativas de Calidad de Datos](#9-expectativas-de-calidad-de-datos)

---

## 1. Resumen de Entidades

| # | Entidad | Capa | Tipo | Columnas |
|---|---------|------|------|----------|
| 1 | CMSTFL | Fuente (Parquet AS400) | Archivo Parquet | 70 |
| 2 | TRXPFL | Fuente (Parquet AS400) | Archivo Parquet | 60 |
| 3 | BLNCFL | Fuente (Parquet AS400) | Archivo Parquet | 100 |
| 4 | bronce.lab1.cmstfl | Bronce | Streaming Table | 75 |
| 5 | bronce.lab1.trxpfl | Bronce | Streaming Table | 65 |
| 6 | bronce.lab1.blncfl | Bronce | Streaming Table | 105 |
| 7 | plata.lab1.clientes_saldos_consolidados | Plata | Vista Materializada | 173 |
| 8 | plata.lab1.transacciones_enriquecidas | Plata | Vista Materializada | 64 |
| 9 | oro.lab1.comportamiento_atm_cliente | Oro | Vista Materializada | 6 |
| 10 | oro.lab1.resumen_integral_cliente | Oro | Vista Materializada | 22 |

**Total de campos documentados**: ~580 (incluyendo propagacion entre capas)

---

## 2. Diagrama de Linaje

```
Parquets AS400
  CMSTFL (70 cols) ─────────────────────┐
  TRXPFL (60 cols) ──────────────┐      │
  BLNCFL (100 cols) ─────┐       │      │
                          │       │      │
  AutoLoader Streaming    │       │      │
  (cloudFiles, mode=      │       │      │
   addNewColumns)         │       │      │
                          v       v      v
BRONCE (Streaming Tables — append-only)
  bronce.lab1.blncfl  (105 cols = 100 fuente + 5 control)
  bronce.lab1.trxpfl  ( 65 cols =  60 fuente + 5 control)
  bronce.lab1.cmstfl  ( 75 cols =  70 fuente + 5 control)
   │                          │
   │    LEFT JOIN             │
   │    CUSTID + Dim.Tipo 1   │    Sin filtros
   v                          v    (incremental)
PLATA (Vistas Materializadas)
  plata.lab1.clientes_saldos_consolidados (173 cols)
    <- cmstfl (70) + blncfl (99, sin CUSTID) + 4 calculados
    <- 5 expectativas de calidad
  plata.lab1.transacciones_enriquecidas (64 cols)
    <- trxpfl (60) + 4 calculados
    <- 4 expectativas de calidad
           │                       │
           └──────────┬────────────┘
                      │ groupBy + INNER JOIN
                      v
ORO (Vistas Materializadas — producto de datos final)
  oro.lab1.comportamiento_atm_cliente (6 cols)
    <- groupBy identificador_cliente desde transacciones
    <- 5 metricas ATM (DATM, CATM, PGSL)
  oro.lab1.resumen_integral_cliente (22 cols)
    <- INNER JOIN clientes_saldos + comportamiento_atm
```

**Columnas de control de bronce** (5, presentes en cmstfl, trxpfl y blncfl):

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `FechaIngestaDatos` | TimestampType | Marca de tiempo de ingesta del registro por AutoLoader |
| `_rescued_data` | StringType | Columnas no mapeadas al schema (schema evolution) |
| `anio` | IntegerType | Particion calculada: anio de ingestacion |
| `mes` | IntegerType | Particion calculada: mes de ingestacion |
| `dia` | IntegerType | Particion calculada: dia de ingestacion |

---

## 3. Parquets AS400 (Fuente)

### 3.1 CMSTFL — Maestro de Clientes (70 columnas)

**Tipo de datos**: 1 LongType (CUSTID) + 40 StringType + 7 LongType + 18 DateType + 2 DoubleType = 70 columnas
**Registros**: 50,000 clientes unicos

| Campo AS400 | Nombre en Espanol | Tipo PySpark | Descripcion |
|-------------|-------------------|-------------|-------------|
| `CUSTID` | identificador_cliente | LongType | Identificador unico del cliente (clave primaria) |
| `CUSNM` | nombre_cliente | StringType | Nombre de pila del cliente |
| `CUSLN` | apellido_cliente | StringType | Apellido del cliente |
| `CUSMD` | nombre_medio_cliente | StringType | Nombre medio o segundo nombre |
| `CUSFN` | nombre_completo_cliente | StringType | Nombre completo concatenado |
| `CUSSX` | sexo_cliente | StringType | Sexo del cliente (M/F/O) |
| `CUSTT` | tratamiento_cliente | StringType | Tratamiento formal (Sr., Sra., Dr., etc.) |
| `CUSDB` | fecha_nacimiento | DateType | Fecha de nacimiento del cliente |
| `CUSYR` | anio_nacimiento | LongType | Anio de nacimiento derivado de CUSDB |
| `CUSAG2` | edad_cliente | LongType | Edad calculada a la fecha actual |
| `CUSAD` | direccion_calle | StringType | Direccion de la calle principal |
| `CUSA2` | direccion_apartamento | StringType | Departamento o numero de apartamento |
| `CUSCT` | ciudad_residencia | StringType | Ciudad de residencia |
| `CUSST` | estado_provincia | StringType | Estado o provincia de residencia |
| `CUSZP` | codigo_postal | StringType | Codigo postal de la residencia |
| `CUSCN` | pais_residencia | StringType | Pais de residencia (ISO 3166) |
| `CUSNA` | nacionalidad_cliente | StringType | Nacionalidad del cliente |
| `CUSPH` | telefono_principal | StringType | Telefono principal de contacto |
| `CUSMB` | telefono_movil | StringType | Numero de telefono movil |
| `CUSEM` | correo_electronico | StringType | Correo electronico del cliente |
| `CUSMS` | estado_civil | StringType | Estado civil (S/C/D/V) |
| `CUSOC` | ocupacion_cliente | StringType | Ocupacion o profesion |
| `CUSED` | nivel_educativo | StringType | Nivel de educacion alcanzado |
| `CUSDL` | numero_licencia_conducir | StringType | Numero de licencia de conducir |
| `CUSDP` | tipo_documento_pasaporte | StringType | Tipo de documento de identidad |
| `CUSDP2` | cantidad_pasaportes | LongType | Cantidad de documentos de identidad registrados |
| `CUSLG` | idioma_preferido | StringType | Idioma de preferencia del cliente |
| `CUSRG` | region_geografica | StringType | Region geografica de clasificacion interna |
| `CUSTP` | tipo_cliente | StringType | Tipo de cliente (RETAIL, CORP, PYME) |
| `CUSSG` | segmento_cliente | StringType | Segmento de cliente (PREMIUM, STANDARD, etc.) |
| `CUSBR` | sucursal_principal | StringType | Codigo de la sucursal principal asignada |
| `CUSMG` | gerente_asignado | StringType | Codigo del gerente de cuenta asignado |
| `CUSRF` | referencia_interna | StringType | Referencia interna del cliente |
| `CUSRS` | fuente_referencia | StringType | Fuente de referencia del cliente |
| `CUSAG` | grupo_afinidad | StringType | Grupo de afinidad de marketing |
| `CUSPC` | preferencia_comunicacion | StringType | Canal de comunicacion preferido |
| `CUSRK` | nivel_riesgo | StringType | Nivel de riesgo (BAJO, MEDIO, ALTO) |
| `CUSVP` | indicador_vip | StringType | Indicador de cliente VIP (S/N) |
| `CUSPF` | estado_perfil | StringType | Estado del perfil del cliente (ACTIVO, etc.) |
| `CUSKT` | estado_kyc | StringType | Estado del proceso KYC (COMPLETO, PENDIENTE) |
| `CUSFM` | indicador_flags | StringType | Indicadores de alertas internas |
| `CUSLC` | ultimo_canal | StringType | Ultimo canal de interaccion |
| `CUSCR` | calificacion_crediticia | StringType | Calificacion crediticia interna |
| `CUSAC` | cuenta_activa | StringType | Indicador de cuenta activa principal (S/N) |
| `CUSCL` | clasificacion_interna | StringType | Clasificacion interna del cliente |
| `CUSAC2` | cantidad_cuentas | LongType | Cantidad total de cuentas activas |
| `CUSTX` | cantidad_transacciones | LongType | Total de transacciones historicas |
| `CUSSC` | score_cliente | LongType | Score de riesgo interno (300-1150) |
| `CUSLR` | ranking_prestamos | LongType | Ranking de prestamos (0-9) |
| `CUSRC` | cantidad_registros | LongType | Cantidad de registros de historial |
| `CUSIN` | ingresos_cliente | DoubleType | Ingresos mensuales estimados |
| `CUSBL` | saldo_disponible_maestro | DoubleType | Saldo disponible consolidado del maestro |
| `CUSNT` | nota_cliente | StringType | Nota o comentario interno del agente |
| `CUSOD` | fecha_apertura_relacion | DateType | Fecha de apertura de la relacion bancaria |
| `CUSCD` | fecha_cierre_relacion | DateType | Fecha de cierre de la relacion bancaria |
| `CUSLV` | fecha_ultima_visita | DateType | Fecha de la ultima visita a sucursal |
| `CUSUD` | fecha_ultima_actualizacion | DateType | Fecha de la ultima actualizacion del perfil |
| `CUSKD` | fecha_verificacion_kyc | DateType | Fecha de la ultima verificacion KYC |
| `CUSRD` | fecha_renovacion | DateType | Fecha de renovacion del contrato |
| `CUSXD` | fecha_expiracion | DateType | Fecha de expiracion del documento principal |
| `CUSFD` | fecha_primer_producto | DateType | Fecha de adquisicion del primer producto |
| `CUSLD` | fecha_ultimo_producto | DateType | Fecha de adquisicion del ultimo producto |
| `CUSMD2` | fecha_migracion | DateType | Fecha de migracion al sistema actual |
| `CUSAD2` | fecha_activacion | DateType | Fecha de activacion del cliente |
| `CUSBD` | fecha_bloqueo | DateType | Fecha del ultimo bloqueo de cuenta |
| `CUSVD` | fecha_verificacion | DateType | Fecha de la ultima verificacion de identidad |
| `CUSPD` | fecha_promocion | DateType | Fecha de la ultima promocion aplicada |
| `CUSDD` | fecha_desactivacion | DateType | Fecha de desactivacion del cliente |
| `CUSED2` | fecha_educacion_financiera | DateType | Fecha de completar educacion financiera |
| `CUSND` | fecha_notificacion | DateType | Fecha de la ultima notificacion enviada |

### 3.2 TRXPFL — Transaccional de Clientes (60 columnas)

**Tipo de datos**: 1 LongType (CUSTID) + mixtos (StringType, LongType, DoubleType, DateType, TimestampType)
**Registros**: 150,000 transacciones

| Campo AS400 | Nombre en Espanol | Tipo PySpark | Descripcion |
|-------------|-------------------|-------------|-------------|
| `TRXID` | identificador_transaccion | LongType | Identificador unico de la transaccion |
| `CUSTID` | identificador_cliente | LongType | Identificador del cliente (FK hacia CMSTFL) |
| `TRXSQ` | secuencia_transaccion | LongType | Numero de secuencia de la transaccion |
| `TRXTYP` | tipo_transaccion | StringType | Tipo de transaccion (DATM, CATM, PGSL, etc.) |
| `TRXCUR` | moneda_transaccion | StringType | Moneda de la transaccion (ISO 4217) |
| `TRXST` | estado_transaccion | StringType | Estado de la transaccion (APROBADA, REVERTIDA) |
| `TRXCH` | canal_transaccion | StringType | Canal de la transaccion (ATM, WEB, APP, etc.) |
| `TRXDSC` | descripcion_transaccion | StringType | Descripcion descriptiva de la transaccion |
| `TRXREF` | referencia_externa | StringType | Referencia externa o codigo de autorizacion |
| `TRXAMT` | monto_principal | DoubleType | Monto principal de la transaccion |
| `TRXCM` | comision_transaccion | DoubleType | Comision cobrada por la transaccion |
| `TRXBA` | saldo_posterior | DoubleType | Saldo de la cuenta despues de la transaccion |
| `TRXBP` | saldo_anterior | DoubleType | Saldo de la cuenta antes de la transaccion |
| `TRXTC` | cargo_fiscal | DoubleType | Cargo fiscal aplicado a la transaccion |
| `TRXAL` | monto_local | DoubleType | Monto equivalente en moneda local |
| `TRXPN` | monto_pago | DoubleType | Monto de pago aplicado |
| `TRXBF` | beneficio_transaccion | DoubleType | Beneficio o cashback asociado |
| `TRXRL` | perdida_tasa | DoubleType | Perdida por diferencia de tasa de cambio |
| `TRXMX` | monto_maximo | DoubleType | Monto maximo autorizado para el tipo |
| `TRXMN` | monto_minimo | DoubleType | Monto minimo requerido para el tipo |
| `TRXAV` | monto_promedio | DoubleType | Promedio historico del monto de transacciones |
| `TRXDV` | desviacion_monto | DoubleType | Desviacion estandar del monto promedio |
| `TRXRK` | riesgo_transaccion | DoubleType | Factor de riesgo de la transaccion (0.0-1.0) |
| `TRXFR` | riesgo_fraude | DoubleType | Probabilidad de fraude calculada (0.0-1.0) |
| `TRXLM` | limite_transaccion | DoubleType | Limite por transaccion configurado |
| `TRXLP` | porcentaje_limite | DoubleType | Porcentaje del limite utilizado |
| `TRXCP` | cargo_plataforma | DoubleType | Cargo de la plataforma de pagos |
| `TRXCI` | cargo_institucion | DoubleType | Cargo cobrado por la institucion |
| `TRXCF` | cargo_extranjero | DoubleType | Cargo por transaccion en moneda extranjera |
| `TRXCV` | cargo_varianza | DoubleType | Cargo adicional por varianza de monto |
| `TRXSB` | subtotal_transaccion | DoubleType | Subtotal antes de impuestos y cargos |
| `TRXTL` | total_transaccion | DoubleType | Total final de la transaccion |
| `TRXRS` | residuo_transaccion | DoubleType | Residuo o diferencia de redondeo |
| `TRXIM` | margen_interes | DoubleType | Margen de interes aplicado |
| `TRXNT` | monto_neto | DoubleType | Monto neto de la transaccion (TRXAMT - TRXCM) |
| `TRXAO` | monto_original | DoubleType | Monto original antes de ajustes |
| `TRXIN` | monto_inversion | DoubleType | Monto destinado a componente de inversion |
| `TRXDS` | descuento_transaccion | DoubleType | Descuento aplicado promocionalmente |
| `TRXPT` | monto_principal_prestamo | DoubleType | Porcion de amortizacion del prestamo |
| `TRXDT` | fecha_transaccion | DateType | Fecha de la transaccion |
| `TRXVD` | fecha_valor | DateType | Fecha valor de la transaccion |
| `TRXPD` | fecha_procesamiento | DateType | Fecha de procesamiento en el sistema |
| `TRXSD` | fecha_liquidacion | DateType | Fecha de liquidacion de la transaccion |
| `TRXCD` | fecha_compensacion | DateType | Fecha de compensacion interbancaria |
| `TRXED` | fecha_efectiva | DateType | Fecha efectiva de aplicacion |
| `TRXRD` | fecha_reverso | DateType | Fecha de reverso de la transaccion (si aplica) |
| `TRXAD` | fecha_autorizacion | DateType | Fecha y hora de autorizacion |
| `TRXND` | fecha_notificacion_trx | DateType | Fecha de notificacion al cliente |
| `TRXXD` | fecha_expiracion_trx | DateType | Fecha de expiracion de la autorizacion |
| `TRXFD` | fecha_fondeo_trx | DateType | Fecha de fondeo de la transaccion |
| `TRXGD` | fecha_gracia_trx | DateType | Fecha de periodo de gracia |
| `TRXHD` | fecha_historica_trx | DateType | Fecha historica de la transaccion AS400 |
| `TRXBD` | fecha_bloqueo_trx | DateType | Fecha de bloqueo temporal de fondos |
| `TRXMD` | fecha_maduracion_trx | DateType | Fecha de maduracion del instrumento |
| `TRXLD` | fecha_limite_trx | DateType | Fecha limite de ejecucion |
| `TRXUD` | fecha_actualizacion_trx | DateType | Fecha de ultima actualizacion del registro |
| `TRXOD` | fecha_origen_trx | DateType | Fecha de origen de la instruccion |
| `TRXKD` | fecha_kyc_trx | DateType | Fecha de verificacion KYC asociada |
| `TRXTS` | timestamp_transaccion | TimestampType | Timestamp de la transaccion (nanosegundos) |
| `TRXUS` | timestamp_actualizacion | TimestampType | Timestamp de la ultima actualizacion |

### 3.3 BLNCFL — Saldos de Clientes (100 columnas)

**Tipo de datos**: LongType (CUSTID) + StringType (atributos) + DoubleType (saldos) + DateType (fechas)
**Registros**: 50,000 clientes (1:1 con CMSTFL por CUSTID)

| Campo AS400 | Nombre en Espanol | Tipo PySpark | Descripcion |
|-------------|-------------------|-------------|-------------|
| `CUSTID` | identificador_cliente | LongType | Identificador del cliente (clave primaria/FK hacia CMSTFL) |
| `BLSQ` | secuencia_saldo | LongType | Secuencia del registro de saldo |
| `BLACT` | tipo_cuenta | StringType | Tipo de cuenta bancaria (AHORRO, CORRIENTE, etc.) |
| `BLACN` | numero_cuenta | StringType | Numero de cuenta bancaria |
| `BLCUR` | moneda_cuenta | StringType | Moneda de la cuenta (ISO 4217) |
| `BLST` | estado_cuenta | StringType | Estado de la cuenta (ACTIVA, BLOQUEADA, etc.) |
| `BLBR` | sucursal_cuenta | StringType | Codigo de la sucursal de la cuenta |
| `BLPR` | producto_cuenta | StringType | Codigo del producto bancario |
| `BLSP` | subproducto_cuenta | StringType | Codigo del subproducto bancario |
| `BLNM` | nombre_cuenta | StringType | Nombre descriptivo de la cuenta |
| `BLCL` | clase_cuenta | StringType | Clase o categoria de la cuenta |
| `BLRK` | riesgo_cuenta | StringType | Nivel de riesgo asignado a la cuenta |
| `BLTP` | tipo_producto_cuenta | StringType | Tipo especifico del producto |
| `BLMG` | gerente_cuenta | StringType | Codigo del gerente asignado a la cuenta |
| `BLRF` | referencia_cuenta | StringType | Referencia interna de la cuenta |
| `BLCC` | centro_costos_cuenta | StringType | Centro de costos contable |
| `BLAG` | grupo_afinidad_cuenta | StringType | Grupo de afinidad de la cuenta |
| `BLPL` | plan_cuenta | StringType | Plan tarifario de la cuenta |
| `BLRG` | region_cuenta | StringType | Region geografica de la cuenta |
| `BLSF` | sufijo_cuenta | StringType | Sufijo numerico de la cuenta |
| `BLNT` | nota_cuenta | StringType | Nota interna del operador |
| `BLLC` | ultimo_canal_cuenta | StringType | Ultimo canal de acceso a la cuenta |
| `BLPF` | perfil_cuenta | StringType | Perfil de uso de la cuenta |
| `BLAU` | autorizado_cuenta | StringType | Indicador de autorizacion activa (S/N) |
| `BLTX` | texto_cuenta | StringType | Texto libre descriptivo |
| `BLGR` | grupo_cuenta | StringType | Grupo contable de la cuenta |
| `BLEM` | email_cuenta | StringType | Email alternativo asociado a la cuenta |
| `BLFR` | frecuencia_cuenta | StringType | Frecuencia de transacciones (ALTA, MEDIA, BAJA) |
| `BLKY` | clave_cuenta | StringType | Clave de seguridad interna |
| `BLVP` | vip_cuenta | StringType | Indicador de cuenta VIP (S/N) |
| `BLFC` | factor_cuenta | StringType | Factor de clasificacion interna |
| `BLAV` | saldo_disponible | DoubleType | Saldo disponible para transacciones |
| `BLTB` | saldo_total | DoubleType | Saldo total de la cuenta |
| `BLRV` | saldo_reservado | DoubleType | Monto reservado o bloqueado |
| `BLBK` | saldo_bloqueado | DoubleType | Saldo bloqueado por orden judicial u otro |
| `BLCR` | limite_credito | DoubleType | Limite de credito aprobado |
| `BLCN` | credito_utilizado | DoubleType | Credito actualmente utilizado |
| `BLCD` | credito_disponible | DoubleType | Credito disponible (limite - utilizado) |
| `BLOV` | valor_sobregiro | DoubleType | Valor del sobregiro actual |
| `BLOL` | limite_sobregiro | DoubleType | Limite de sobregiro autorizado |
| `BLPD` | depositos_pendientes | DoubleType | Total depositos en proceso de acreditacion |
| `BLPC` | cargos_pendientes | DoubleType | Total cargos pendientes de debitar |
| `BLPA` | ajustes_pendientes | DoubleType | Ajustes contables pendientes de aplicar |
| `BLDI` | depositos_ingreso | DoubleType | Total de depositos del periodo |
| `BLWI` | retenciones_cuenta | DoubleType | Retenciones aplicadas a la cuenta |
| `BLTI` | transferencias_ingreso | DoubleType | Total transferencias recibidas del periodo |
| `BLTC` | cargos_transferencia | DoubleType | Cargos por transferencias |
| `BLCA` | comisiones_anuales | DoubleType | Total comisiones anuales acumuladas |
| `BLIM` | intereses_mensuales | DoubleType | Total intereses del mes |
| `BLRF2` | reembolsos_cuenta | DoubleType | Total reembolsos acreditados |
| `BLPN` | penalidades_cuenta | DoubleType | Total penalidades cobradas |
| `BLBN` | bonificaciones_cuenta | DoubleType | Total bonificaciones acreditadas |
| `BLAP` | ajustes_positivos | DoubleType | Ajustes positivos (creditos) |
| `BLAM` | ajustes_miscelaneos | DoubleType | Ajustes varios (debitos/creditos) |
| `BLAY` | ajustes_anuales | DoubleType | Ajustes anuales acumulados |
| `BLHI` | marca_alta_saldo | DoubleType | Saldo mas alto registrado en el periodo |
| `BLLO` | marca_baja_saldo | DoubleType | Saldo mas bajo registrado en el periodo |
| `BLVR` | varianza_saldo | DoubleType | Varianza estadistica del saldo |
| `BLRT` | ratio_cuenta | DoubleType | Ratio de utilizacion de credito |
| `BLCP` | porcentaje_aporte | DoubleType | Porcentaje de aporte al pool de fondos |
| `BLCI` | ingresos_aporte | DoubleType | Ingresos derivados del aporte |
| `BLMN` | saldo_minimo | DoubleType | Saldo minimo requerido por el producto |
| `BLMX` | saldo_maximo | DoubleType | Saldo maximo permitido por el producto |
| `BLIR` | tasa_interes | DoubleType | Tasa de interes nominal anual |
| `BLPM` | multiplicador_penalidad | DoubleType | Multiplicador para calculo de penalidades |
| `BLOD` | fecha_apertura_cuenta | DateType | Fecha de apertura de la cuenta |
| `BLXD` | fecha_expiracion_cuenta | DateType | Fecha de expiracion del contrato |
| `BLUD` | fecha_actualizacion_cuenta | DateType | Fecha de la ultima actualizacion |
| `BLLD` | fecha_ultimo_movimiento | DateType | Fecha del ultimo movimiento registrado |
| `BLSD` | fecha_estado_cuenta | DateType | Fecha del ultimo cambio de estado |
| `BLPD2` | fecha_penalidad | DateType | Fecha de la ultima penalidad aplicada |
| `BLRD` | fecha_renovacion_cuenta | DateType | Fecha de renovacion del contrato |
| `BLMD` | fecha_maduracion | DateType | Fecha de maduracion del instrumento financiero |
| `BLCD2` | fecha_cierre_cuenta | DateType | Fecha de cierre de la cuenta |
| `BLBD` | fecha_bloqueo_cuenta | DateType | Fecha del ultimo bloqueo |
| `BLFD` | fecha_fondeo | DateType | Fecha de fondeo inicial |
| `BLGD` | fecha_gracia | DateType | Fecha de fin del periodo de gracia |
| `BLHD` | fecha_historica | DateType | Fecha historica de referencia AS400 |
| `BLID` | fecha_interes | DateType | Fecha de liquidacion de intereses |
| `BLJD` | fecha_ajuste | DateType | Fecha del ultimo ajuste contable |
| `BLKD` | fecha_kyc_cuenta | DateType | Fecha de verificacion KYC de la cuenta |
| `BLND` | fecha_notificacion_cuenta | DateType | Fecha de la ultima notificacion de la cuenta |
| `BLTD` | fecha_transferencia | DateType | Fecha de la ultima transferencia |
| `BLVD` | fecha_verificacion_cuenta | DateType | Fecha de verificacion de la cuenta |
| `BLWD` | fecha_retiro | DateType | Fecha del ultimo retiro |
| `BLYD` | fecha_rendimiento | DateType | Fecha de calculo de rendimiento |
| `BLZD` | fecha_cierre_periodo | DateType | Fecha de cierre del periodo contable |
| `BLED` | fecha_evaluacion | DateType | Fecha de evaluacion del riesgo |
| `BLAD2` | fecha_activacion_cuenta | DateType | Fecha de activacion de la cuenta |
| `BLDD` | fecha_desactivacion_cuenta | DateType | Fecha de desactivacion de la cuenta |
| `BLFP` | fecha_primer_pago | DateType | Fecha del primer pago realizado |
| `BLLP` | fecha_ultimo_pago | DateType | Fecha del ultimo pago realizado |
| `BLMP` | fecha_mora_pago | DateType | Fecha de inicio de mora |
| `BLNP` | fecha_proximo_pago | DateType | Fecha del proximo pago programado |
| `BLOP` | fecha_origen_pago | DateType | Fecha de origen de la instruccion de pago |
| `BLPP` | fecha_programacion_pago | DateType | Fecha de programacion del pago |
| `BLQP` | fecha_quiebre_pago | DateType | Fecha de quiebre del acuerdo de pago |
| `BLRP` | fecha_regularizacion_pago | DateType | Fecha de regularizacion del pago |
| `BLSP2` | fecha_suspension_pago | DateType | Fecha de suspension del pago |
| `BLTP2` | fecha_tercero_pago | DateType | Fecha de pago a tercero |

---

## 4. Streaming Tables Bronce

Las streaming tables de bronce replican las columnas fuente (parquets AS400) con el mismo
nombre AS400 original, agregando 5 columnas de control. AutoLoader con schema evolution
`addNewColumns` permite incorporar nuevas columnas sin re-procesar datos historicos.

### Propiedades Delta (comunes a las 3 tablas)

| Propiedad | Valor |
|-----------|-------|
| `delta.enableChangeDataFeed` | `true` |
| `delta.autoOptimize.autoCompact` | `true` |
| `delta.autoOptimize.optimizeWrite` | `true` |
| `delta.deletedFileRetentionDuration` | `interval 30 days` |
| `delta.logRetentionDuration` | `interval 60 days` |

### 4.1 bronce.lab1.cmstfl (75 columnas)

**70 columnas AS400** (mismos nombres que CMSTFL — ver seccion 3.1) **+ 5 columnas de control**:

| Campo de Control | Tipo | Descripcion |
|-----------------|------|-------------|
| `FechaIngestaDatos` | TimestampType | Timestamp de ingesta por AutoLoader (Liquid Cluster col 1) |
| `_rescued_data` | StringType | Columnas no reconocidas en el schema (schema evolution) |
| `anio` | IntegerType | Anio de la fecha de ingesta (particion logica) |
| `mes` | IntegerType | Mes de la fecha de ingesta (particion logica) |
| `dia` | IntegerType | Dia de la fecha de ingesta (particion logica) |

**Liquid Cluster**: `["FechaIngestaDatos", "CUSTID"]`

### 4.2 bronce.lab1.trxpfl (65 columnas)

**60 columnas AS400** (mismos nombres que TRXPFL — ver seccion 3.2) **+ 5 columnas de control**:

Mismas 5 columnas de control que cmstfl (ver tabla anterior).

**Liquid Cluster**: `["FechaIngestaDatos", "CUSTID"]`

### 4.3 bronce.lab1.blncfl (105 columnas)

**100 columnas AS400** (mismos nombres que BLNCFL — ver seccion 3.3) **+ 5 columnas de control**:

Mismas 5 columnas de control que cmstfl (ver tabla anterior).

**Liquid Cluster**: `["FechaIngestaDatos", "CUSTID"]`

---

## 5. Vistas Materializadas Plata

### 5.1 plata.lab1.clientes_saldos_consolidados (173 columnas)

**Origen**: LEFT JOIN entre `bronce.lab1.cmstfl` (70 cols) y `bronce.lab1.blncfl` (99 cols sin CUSTID) + 4 campos calculados.
**Estrategia**: Dimension Tipo 1 — solo el registro mas reciente por CUSTID (Window + ROW_NUMBER ordenado por FechaIngestaDatos DESC).
**Columnas de bronce excluidas**: `FechaIngestaDatos`, `_rescued_data`, `anio`, `mes`, `dia`.

| Campo Espanol | Campo AS400 Origen | Tipo | Descripcion |
|---------------|-------------------|------|-------------|
| `identificador_cliente` | CUSTID | LongType | Clave primaria del cliente |
| `nombre_cliente` | CUSNM | StringType | Nombre de pila del cliente |
| `apellido_cliente` | CUSLN | StringType | Apellido del cliente |
| `nombre_medio_cliente` | CUSMD | StringType | Nombre medio |
| `nombre_completo_cliente` | CUSFN | StringType | Nombre completo |
| `sexo_cliente` | CUSSX | StringType | Sexo (M/F/O) |
| `tratamiento_cliente` | CUSTT | StringType | Tratamiento formal |
| `fecha_nacimiento` | CUSDB | DateType | Fecha de nacimiento |
| `anio_nacimiento` | CUSYR | LongType | Anio de nacimiento |
| `edad_cliente` | CUSAG2 | LongType | Edad calculada |
| `direccion_calle` | CUSAD | StringType | Direccion calle |
| `direccion_apartamento` | CUSA2 | StringType | Apartamento |
| `ciudad_residencia` | CUSCT | StringType | Ciudad |
| `estado_provincia` | CUSST | StringType | Estado/Provincia |
| `codigo_postal` | CUSZP | StringType | Codigo postal |
| `pais_residencia` | CUSCN | StringType | Pais |
| `nacionalidad_cliente` | CUSNA | StringType | Nacionalidad |
| `telefono_principal` | CUSPH | StringType | Telefono principal |
| `telefono_movil` | CUSMB | StringType | Telefono movil |
| `correo_electronico` | CUSEM | StringType | Email |
| `estado_civil` | CUSMS | StringType | Estado civil |
| `ocupacion_cliente` | CUSOC | StringType | Ocupacion |
| `nivel_educativo` | CUSED | StringType | Nivel educativo |
| `numero_licencia_conducir` | CUSDL | StringType | Licencia de conducir |
| `tipo_documento_pasaporte` | CUSDP | StringType | Tipo de documento |
| `cantidad_pasaportes` | CUSDP2 | LongType | Cantidad de documentos |
| `idioma_preferido` | CUSLG | StringType | Idioma preferido |
| `region_geografica` | CUSRG | StringType | Region geografica |
| `tipo_cliente` | CUSTP | StringType | Tipo de cliente |
| `segmento_cliente` | CUSSG | StringType | Segmento de cliente |
| `sucursal_principal` | CUSBR | StringType | Sucursal principal |
| `gerente_asignado` | CUSMG | StringType | Gerente de cuenta |
| `referencia_interna` | CUSRF | StringType | Referencia interna |
| `fuente_referencia` | CUSRS | StringType | Fuente de referencia |
| `grupo_afinidad` | CUSAG | StringType | Grupo de afinidad |
| `preferencia_comunicacion` | CUSPC | StringType | Canal preferido |
| `nivel_riesgo` | CUSRK | StringType | Nivel de riesgo |
| `indicador_vip` | CUSVP | StringType | VIP (S/N) |
| `estado_perfil` | CUSPF | StringType | Estado del perfil |
| `estado_kyc` | CUSKT | StringType | Estado KYC |
| `indicador_flags` | CUSFM | StringType | Flags internas |
| `ultimo_canal` | CUSLC | StringType | Ultimo canal |
| `calificacion_crediticia` | CUSCR | StringType | Calificacion crediticia |
| `cuenta_activa` | CUSAC | StringType | Cuenta activa (S/N) |
| `clasificacion_interna` | CUSCL | StringType | Clasificacion interna |
| `cantidad_cuentas` | CUSAC2 | LongType | Cantidad de cuentas |
| `cantidad_transacciones` | CUSTX | LongType | Total transacciones |
| `score_cliente` | CUSSC | LongType | Score de riesgo |
| `ranking_prestamos` | CUSLR | LongType | Ranking de prestamos |
| `cantidad_registros` | CUSRC | LongType | Cantidad de registros |
| `ingresos_cliente` | CUSIN | DoubleType | Ingresos mensuales |
| `saldo_disponible_maestro` | CUSBL | DoubleType | Saldo disponible (maestro) |
| `nota_cliente` | CUSNT | StringType | Nota del cliente |
| `fecha_apertura_relacion` | CUSOD | DateType | Apertura relacion bancaria |
| `fecha_cierre_relacion` | CUSCD | DateType | Cierre relacion bancaria |
| `fecha_ultima_visita` | CUSLV | DateType | Ultima visita sucursal |
| `fecha_ultima_actualizacion` | CUSUD | DateType | Ultima actualizacion perfil |
| `fecha_verificacion_kyc` | CUSKD | DateType | Ultima verificacion KYC |
| `fecha_renovacion` | CUSRD | DateType | Fecha de renovacion |
| `fecha_expiracion` | CUSXD | DateType | Fecha de expiracion |
| `fecha_primer_producto` | CUSFD | DateType | Primer producto adquirido |
| `fecha_ultimo_producto` | CUSLD | DateType | Ultimo producto adquirido |
| `fecha_migracion` | CUSMD2 | DateType | Migracion al sistema |
| `fecha_activacion` | CUSAD2 | DateType | Activacion del cliente |
| `fecha_bloqueo` | CUSBD | DateType | Ultimo bloqueo |
| `fecha_verificacion` | CUSVD | DateType | Ultima verificacion |
| `fecha_promocion` | CUSPD | DateType | Ultima promocion |
| `fecha_desactivacion` | CUSDD | DateType | Desactivacion |
| `fecha_educacion_financiera` | CUSED2 | DateType | Educacion financiera |
| `fecha_notificacion` | CUSND | DateType | Ultima notificacion |
| `secuencia_saldo` | BLSQ | LongType | Secuencia del saldo (BLNCFL) |
| `tipo_cuenta` | BLACT | StringType | Tipo de cuenta bancaria |
| `numero_cuenta` | BLACN | StringType | Numero de cuenta |
| `moneda_cuenta` | BLCUR | StringType | Moneda de la cuenta |
| `estado_cuenta` | BLST | StringType | Estado de la cuenta |
| `sucursal_cuenta` | BLBR | StringType | Sucursal de la cuenta |
| `producto_cuenta` | BLPR | StringType | Producto bancario |
| `subproducto_cuenta` | BLSP | StringType | Subproducto bancario |
| `nombre_cuenta` | BLNM | StringType | Nombre de la cuenta |
| `clase_cuenta` | BLCL | StringType | Clase de cuenta |
| `riesgo_cuenta` | BLRK | StringType | Riesgo de la cuenta |
| `tipo_producto_cuenta` | BLTP | StringType | Tipo de producto |
| `gerente_cuenta` | BLMG | StringType | Gerente de la cuenta |
| `referencia_cuenta` | BLRF | StringType | Referencia de la cuenta |
| `centro_costos_cuenta` | BLCC | StringType | Centro de costos |
| `grupo_afinidad_cuenta` | BLAG | StringType | Grupo de afinidad |
| `plan_cuenta` | BLPL | StringType | Plan tarifario |
| `region_cuenta` | BLRG | StringType | Region de la cuenta |
| `sufijo_cuenta` | BLSF | StringType | Sufijo de cuenta |
| `nota_cuenta` | BLNT | StringType | Nota interna |
| `ultimo_canal_cuenta` | BLLC | StringType | Ultimo canal de acceso |
| `perfil_cuenta` | BLPF | StringType | Perfil de uso |
| `autorizado_cuenta` | BLAU | StringType | Autorizado (S/N) |
| `texto_cuenta` | BLTX | StringType | Texto libre |
| `grupo_cuenta` | BLGR | StringType | Grupo contable |
| `email_cuenta` | BLEM | StringType | Email de la cuenta |
| `frecuencia_cuenta` | BLFR | StringType | Frecuencia de uso |
| `clave_cuenta` | BLKY | StringType | Clave de seguridad |
| `vip_cuenta` | BLVP | StringType | VIP de cuenta (S/N) |
| `factor_cuenta` | BLFC | StringType | Factor de clasificacion |
| `saldo_disponible` | BLAV | DoubleType | Saldo disponible actual |
| `saldo_total` | BLTB | DoubleType | Saldo total |
| `saldo_reservado` | BLRV | DoubleType | Saldo reservado |
| `saldo_bloqueado` | BLBK | DoubleType | Saldo bloqueado |
| `limite_credito` | BLCR | DoubleType | Limite de credito |
| `credito_utilizado` | BLCN | DoubleType | Credito utilizado |
| `credito_disponible` | BLCD | DoubleType | Credito disponible |
| `valor_sobregiro` | BLOV | DoubleType | Valor del sobregiro |
| `limite_sobregiro` | BLOL | DoubleType | Limite de sobregiro |
| `depositos_pendientes` | BLPD | DoubleType | Depositos pendientes |
| `cargos_pendientes` | BLPC | DoubleType | Cargos pendientes |
| `ajustes_pendientes` | BLPA | DoubleType | Ajustes pendientes |
| `depositos_ingreso` | BLDI | DoubleType | Depositos del periodo |
| `retenciones_cuenta` | BLWI | DoubleType | Retenciones |
| `transferencias_ingreso` | BLTI | DoubleType | Transferencias recibidas |
| `cargos_transferencia` | BLTC | DoubleType | Cargos por transferencias |
| `comisiones_anuales` | BLCA | DoubleType | Comisiones anuales |
| `intereses_mensuales` | BLIM | DoubleType | Intereses del mes |
| `reembolsos_cuenta` | BLRF2 | DoubleType | Reembolsos |
| `penalidades_cuenta` | BLPN | DoubleType | Penalidades |
| `bonificaciones_cuenta` | BLBN | DoubleType | Bonificaciones |
| `ajustes_positivos` | BLAP | DoubleType | Ajustes positivos |
| `ajustes_miscelaneos` | BLAM | DoubleType | Ajustes varios |
| `ajustes_anuales` | BLAY | DoubleType | Ajustes anuales |
| `marca_alta_saldo` | BLHI | DoubleType | Saldo maximo del periodo |
| `marca_baja_saldo` | BLLO | DoubleType | Saldo minimo del periodo |
| `varianza_saldo` | BLVR | DoubleType | Varianza del saldo |
| `ratio_cuenta` | BLRT | DoubleType | Ratio de utilizacion |
| `porcentaje_aporte` | BLCP | DoubleType | Porcentaje de aporte |
| `ingresos_aporte` | BLCI | DoubleType | Ingresos del aporte |
| `saldo_minimo` | BLMN | DoubleType | Saldo minimo requerido |
| `saldo_maximo` | BLMX | DoubleType | Saldo maximo permitido |
| `tasa_interes` | BLIR | DoubleType | Tasa de interes |
| `multiplicador_penalidad` | BLPM | DoubleType | Multiplicador de penalidad |
| `fecha_apertura_cuenta` | BLOD | DateType | Apertura de la cuenta |
| `fecha_expiracion_cuenta` | BLXD | DateType | Expiracion del contrato |
| `fecha_actualizacion_cuenta` | BLUD | DateType | Ultima actualizacion |
| `fecha_ultimo_movimiento` | BLLD | DateType | Ultimo movimiento |
| `fecha_estado_cuenta` | BLSD | DateType | Ultimo cambio de estado |
| `fecha_penalidad` | BLPD2 | DateType | Ultima penalidad |
| `fecha_renovacion_cuenta` | BLRD | DateType | Renovacion del contrato |
| `fecha_maduracion` | BLMD | DateType | Maduracion del instrumento |
| `fecha_cierre_cuenta` | BLCD2 | DateType | Cierre de la cuenta |
| `fecha_bloqueo_cuenta` | BLBD | DateType | Ultimo bloqueo |
| `fecha_fondeo` | BLFD | DateType | Fondeo inicial |
| `fecha_gracia` | BLGD | DateType | Fin del periodo de gracia |
| `fecha_historica` | BLHD | DateType | Fecha historica AS400 |
| `fecha_interes` | BLID | DateType | Liquidacion de intereses |
| `fecha_ajuste` | BLJD | DateType | Ultimo ajuste contable |
| `fecha_kyc_cuenta` | BLKD | DateType | KYC de la cuenta |
| `fecha_notificacion_cuenta` | BLND | DateType | Ultima notificacion |
| `fecha_transferencia` | BLTD | DateType | Ultima transferencia |
| `fecha_verificacion_cuenta` | BLVD | DateType | Verificacion de cuenta |
| `fecha_retiro` | BLWD | DateType | Ultimo retiro |
| `fecha_rendimiento` | BLYD | DateType | Calculo de rendimiento |
| `fecha_cierre_periodo` | BLZD | DateType | Cierre del periodo |
| `fecha_evaluacion` | BLED | DateType | Evaluacion de riesgo |
| `fecha_activacion_cuenta` | BLAD2 | DateType | Activacion de la cuenta |
| `fecha_desactivacion_cuenta` | BLDD | DateType | Desactivacion de la cuenta |
| `fecha_primer_pago` | BLFP | DateType | Primer pago realizado |
| `fecha_ultimo_pago` | BLLP | DateType | Ultimo pago realizado |
| `fecha_mora_pago` | BLMP | DateType | Inicio de mora |
| `fecha_proximo_pago` | BLNP | DateType | Proximo pago programado |
| `fecha_origen_pago` | BLOP | DateType | Origen de instruccion de pago |
| `fecha_programacion_pago` | BLPP | DateType | Programacion del pago |
| `fecha_quiebre_pago` | BLQP | DateType | Quiebre del acuerdo de pago |
| `fecha_regularizacion_pago` | BLRP | DateType | Regularizacion del pago |
| `fecha_suspension_pago` | BLSP2 | DateType | Suspension del pago |
| `fecha_tercero_pago` | BLTP2 | DateType | Pago a tercero |
| `clasificacion_riesgo_cliente` | _calculado_ | StringType | CASE sobre score_cliente, nivel_riesgo, calificacion_crediticia. Ver seccion 8. |
| `categoria_saldo_disponible` | _calculado_ | StringType | CASE sobre saldo_disponible y limite_credito. Ver seccion 8. |
| `perfil_actividad_bancaria` | _calculado_ | StringType | CASE sobre cantidad_transacciones, cantidad_cuentas, ranking_prestamos. Ver seccion 8. |
| `huella_identificacion_cliente` | _calculado_ | StringType | SHA2-256 de identificador_cliente. Ver seccion 8. |

**Liquid Cluster**: `["huella_identificacion_cliente", "identificador_cliente"]`

### 5.2 plata.lab1.transacciones_enriquecidas (64 columnas)

**Origen**: `bronce.lab1.trxpfl` (60 cols) sin filtros. Excluye 5 columnas de control de bronce. Agrega 4 campos calculados.

| Campo Espanol | Campo AS400 Origen | Tipo | Descripcion |
|---------------|-------------------|------|-------------|
| `identificador_transaccion` | TRXID | LongType | Identificador unico de la transaccion |
| `identificador_cliente` | CUSTID | LongType | FK hacia clientes |
| `secuencia_transaccion` | TRXSQ | LongType | Numero de secuencia |
| `tipo_transaccion` | TRXTYP | StringType | Tipo (DATM, CATM, PGSL, etc.) |
| `moneda_transaccion` | TRXCUR | StringType | Moneda (ISO 4217) |
| `estado_transaccion` | TRXST | StringType | Estado de la transaccion |
| `canal_transaccion` | TRXCH | StringType | Canal (ATM, WEB, APP) |
| `descripcion_transaccion` | TRXDSC | StringType | Descripcion |
| `referencia_externa` | TRXREF | StringType | Referencia externa |
| `monto_principal` | TRXAMT | DoubleType | Monto principal |
| `comision_transaccion` | TRXCM | DoubleType | Comision cobrada |
| `saldo_posterior` | TRXBA | DoubleType | Saldo despues de la transaccion |
| `saldo_anterior` | TRXBP | DoubleType | Saldo antes de la transaccion |
| `cargo_fiscal` | TRXTC | DoubleType | Cargo fiscal |
| `monto_local` | TRXAL | DoubleType | Equivalente en moneda local |
| `monto_pago` | TRXPN | DoubleType | Monto de pago |
| `beneficio_transaccion` | TRXBF | DoubleType | Beneficio/cashback |
| `perdida_tasa` | TRXRL | DoubleType | Perdida por tasa de cambio |
| `monto_maximo` | TRXMX | DoubleType | Monto maximo autorizado |
| `monto_minimo` | TRXMN | DoubleType | Monto minimo requerido |
| `monto_promedio` | TRXAV | DoubleType | Promedio historico de montos |
| `desviacion_monto` | TRXDV | DoubleType | Desviacion estandar del monto |
| `riesgo_transaccion` | TRXRK | DoubleType | Factor de riesgo (0.0-1.0) |
| `riesgo_fraude` | TRXFR | DoubleType | Probabilidad de fraude (0.0-1.0) |
| `limite_transaccion` | TRXLM | DoubleType | Limite por transaccion |
| `porcentaje_limite` | TRXLP | DoubleType | Porcentaje del limite utilizado |
| `cargo_plataforma` | TRXCP | DoubleType | Cargo de la plataforma |
| `cargo_institucion` | TRXCI | DoubleType | Cargo institucional |
| `cargo_extranjero` | TRXCF | DoubleType | Cargo por moneda extranjera |
| `cargo_varianza` | TRXCV | DoubleType | Cargo por varianza |
| `subtotal_transaccion` | TRXSB | DoubleType | Subtotal |
| `total_transaccion` | TRXTL | DoubleType | Total final |
| `residuo_transaccion` | TRXRS | DoubleType | Residuo de redondeo |
| `margen_interes` | TRXIM | DoubleType | Margen de interes |
| `monto_neto` | TRXNT | DoubleType | Monto neto original |
| `monto_original` | TRXAO | DoubleType | Monto original sin ajustes |
| `monto_inversion` | TRXIN | DoubleType | Componente de inversion |
| `descuento_transaccion` | TRXDS | DoubleType | Descuento promocional |
| `monto_principal_prestamo` | TRXPT | DoubleType | Amortizacion del prestamo |
| `fecha_transaccion` | TRXDT | DateType | Fecha de la transaccion |
| `fecha_valor` | TRXVD | DateType | Fecha valor |
| `fecha_procesamiento` | TRXPD | DateType | Fecha de procesamiento |
| `fecha_liquidacion` | TRXSD | DateType | Fecha de liquidacion |
| `fecha_compensacion` | TRXCD | DateType | Fecha de compensacion |
| `fecha_efectiva` | TRXED | DateType | Fecha efectiva |
| `fecha_reverso` | TRXRD | DateType | Fecha de reverso |
| `fecha_autorizacion` | TRXAD | DateType | Fecha de autorizacion |
| `fecha_notificacion_trx` | TRXND | DateType | Fecha de notificacion |
| `fecha_expiracion_trx` | TRXXD | DateType | Fecha de expiracion |
| `fecha_fondeo_trx` | TRXFD | DateType | Fecha de fondeo |
| `fecha_gracia_trx` | TRXGD | DateType | Fecha de gracia |
| `fecha_historica_trx` | TRXHD | DateType | Fecha historica AS400 |
| `fecha_bloqueo_trx` | TRXBD | DateType | Fecha de bloqueo |
| `fecha_maduracion_trx` | TRXMD | DateType | Fecha de maduracion |
| `fecha_limite_trx` | TRXLD | DateType | Fecha limite |
| `fecha_actualizacion_trx` | TRXUD | DateType | Ultima actualizacion |
| `fecha_origen_trx` | TRXOD | DateType | Fecha de origen |
| `fecha_kyc_trx` | TRXKD | DateType | KYC de la transaccion |
| `timestamp_transaccion` | TRXTS | TimestampType | Timestamp de la transaccion |
| `timestamp_actualizacion` | TRXUS | TimestampType | Timestamp de actualizacion |
| `monto_neto_comisiones` | _calculado_ | DoubleType | monto_principal - comision_transaccion. Ver seccion 8. |
| `porcentaje_comision_sobre_monto` | _calculado_ | DoubleType | (comision / monto_principal) * 100, proteccion division por cero. Ver seccion 8. |
| `variacion_saldo_transaccion` | _calculado_ | DoubleType | saldo_posterior - saldo_anterior. Ver seccion 8. |
| `indicador_impacto_financiero` | _calculado_ | DoubleType | monto_principal * riesgo_transaccion. Ver seccion 8. |

**Liquid Cluster**: `["fecha_transaccion", "identificador_cliente", "tipo_transaccion"]`

---

## 6. Vistas Materializadas Oro

### 6.1 oro.lab1.comportamiento_atm_cliente (6 columnas)

**Origen**: `plata.lab1.transacciones_enriquecidas`, groupBy `identificador_cliente`.
**Tecnica**: Agregacion condicional con `F.count/F.avg/F.sum` + `F.when` sin filtros previos.
**Tipos de transaccion**: Leidos de la tabla Parametros clave `TiposTransaccionesLabBase` (DATM, CATM, PGSL).

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `identificador_cliente` | LongType | Clave primaria del cliente (Liquid Cluster) |
| `cantidad_depositos_atm` | LongType | Cantidad de transacciones tipo DATM (deposito ATM) |
| `cantidad_retiros_atm` | LongType | Cantidad de transacciones tipo CATM (retiro ATM) |
| `promedio_monto_depositos_atm` | DoubleType | Promedio del monto_principal para transacciones DATM |
| `promedio_monto_retiros_atm` | DoubleType | Promedio del monto_principal para transacciones CATM |
| `total_pagos_saldo_cliente` | DoubleType | Suma del monto_principal para transacciones PGSL |

**Tratamiento de nulos**: `F.coalesce(F.col(campo), F.lit(0))` en las 5 metricas.
**Liquid Cluster**: `["identificador_cliente"]`

### 6.2 oro.lab1.resumen_integral_cliente (22 columnas)

**Origen**: INNER JOIN entre `plata.lab1.clientes_saldos_consolidados` y `oro.lab1.comportamiento_atm_cliente` por `identificador_cliente`.
**Comportamiento**: Solo incluye clientes presentes en AMBAS fuentes.

| # | Campo | Origen | Tipo | Descripcion |
|---|-------|--------|------|-------------|
| 1 | `huella_identificacion_cliente` | Plata | StringType | Hash SHA2-256 del identificador (Liquid Cluster col 1) |
| 2 | `identificador_cliente` | Plata/Oro | LongType | Clave del cliente (JOIN key, Liquid Cluster col 2) |
| 3 | `nombre_cliente` | Plata | StringType | Nombre de pila |
| 4 | `apellido_cliente` | Plata | StringType | Apellido |
| 5 | `nacionalidad_cliente` | Plata | StringType | Nacionalidad |
| 6 | `pais_residencia` | Plata | StringType | Pais de residencia |
| 7 | `ciudad_residencia` | Plata | StringType | Ciudad de residencia |
| 8 | `ocupacion_cliente` | Plata | StringType | Ocupacion |
| 9 | `nivel_educativo` | Plata | StringType | Nivel educativo |
| 10 | `clasificacion_riesgo_cliente` | Plata | StringType | Clasificacion de riesgo calculada |
| 11 | `categoria_saldo_disponible` | Plata | StringType | Categoria del saldo disponible |
| 12 | `perfil_actividad_bancaria` | Plata | StringType | Perfil de actividad bancaria |
| 13 | `limite_credito` | Plata | DoubleType | Limite de credito aprobado |
| 14 | `saldo_disponible` | Plata | DoubleType | Saldo disponible en cuenta |
| 15 | `fecha_apertura_cuenta` | Plata | DateType | Fecha de apertura de la cuenta principal |
| 16 | `estado_cuenta` | Plata | StringType | Estado de la cuenta |
| 17 | `tipo_cuenta` | Plata | StringType | Tipo de cuenta |
| 18 | `cantidad_depositos_atm` | Oro | LongType | Depositos ATM del cliente |
| 19 | `cantidad_retiros_atm` | Oro | LongType | Retiros ATM del cliente |
| 20 | `promedio_monto_depositos_atm` | Oro | DoubleType | Promedio depositos ATM |
| 21 | `promedio_monto_retiros_atm` | Oro | DoubleType | Promedio retiros ATM |
| 22 | `total_pagos_saldo_cliente` | Oro | DoubleType | Total pagos al saldo del cliente |

**Liquid Cluster**: `["huella_identificacion_cliente", "identificador_cliente"]`

---

## 7. Tabla Parametros

### control.lab1.Parametros (2 columnas x 16 registros)

**Tipo**: Delta Table (Unity Catalog Managed)
**Schema**: `Clave STRING NOT NULL, Valor STRING NOT NULL`

| Clave | Valor Default | Descripcion |
|-------|--------------|-------------|
| `catalogoBronce` | `bronce` | Catalogo Unity Catalog de la capa Bronce |
| `esquemaBronce` | `lab1` | Esquema del catalogo de bronce |
| `contenedorBronce` | `bronce` | Directorio contenedor base en el almacenamiento |
| `TipoStorage` | `Volume` | Tipo de almacenamiento: `Volume` o `AmazonS3` |
| `catalogoVolume` | `bronce` | Catalogo del Volume UC (TipoStorage=Volume) |
| `esquemaVolume` | `lab1` | Esquema del Volume UC (TipoStorage=Volume) |
| `nombreVolume` | `datos_bronce` | Nombre del Volume UC |
| `bucketS3` | `` | Nombre del bucket S3 (TipoStorage=AmazonS3) |
| `prefijoS3` | `` | Prefijo base en S3 (TipoStorage=AmazonS3) |
| `DirectorioBronce` | `archivos` | Subdirectorio base dentro del almacenamiento |
| `catalogoPlata` | `plata` | Catalogo Unity Catalog de la capa Plata |
| `esquemaPlata` | `lab1` | Esquema del catalogo de plata |
| `catalogoOro` | `oro` | Catalogo Unity Catalog de la capa Oro |
| `esquemaOro` | `lab1` | Esquema del catalogo de oro |
| `esquemaControl` | `lab1` | Esquema del catalogo de control |
| `TiposTransaccionesLabBase` | `DATM,CATM,PGSL` | Tipos de transaccion ATM separados por coma (DATM=Deposito ATM, CATM=Retiro ATM, PGSL=Pago al Saldo) |

---

## 8. Campos Calculados

### clientes_saldos_consolidados

| Campo | Logica | Descripcion |
|-------|--------|-------------|
| `clasificacion_riesgo_cliente` | `CASE WHEN score_cliente >= 750 AND nivel_riesgo = 'BAJO' AND calificacion_crediticia IN ('AAA','AA','A') THEN 'RIESGO_BAJO' WHEN score_cliente >= 600 AND nivel_riesgo <> 'ALTO' THEN 'RIESGO_MODERADO' WHEN nivel_riesgo = 'ALTO' OR score_cliente < 400 THEN 'RIESGO_ALTO' ELSE 'RIESGO_INDETERMINADO' END` | Clasificacion de riesgo del cliente basada en score, nivel de riesgo y calificacion crediticia |
| `categoria_saldo_disponible` | `CASE WHEN saldo_disponible >= limite_credito * 0.8 THEN 'SALDO_ALTO' WHEN saldo_disponible >= limite_credito * 0.4 THEN 'SALDO_MEDIO' WHEN saldo_disponible > 0 THEN 'SALDO_BAJO' ELSE 'SIN_SALDO' END` | Categoria del saldo disponible relativa al limite de credito |
| `perfil_actividad_bancaria` | `CASE WHEN cantidad_transacciones >= 200 AND cantidad_cuentas >= 3 AND ranking_prestamos >= 7 THEN 'PERFIL_ACTIVO_PREMIUM' WHEN cantidad_transacciones >= 50 THEN 'PERFIL_ACTIVO_ESTANDAR' ELSE 'PERFIL_BAJO_USO' END` | Perfil de actividad basado en transacciones, cuentas y ranking de prestamos |
| `huella_identificacion_cliente` | `F.sha2(F.concat(F.col("identificador_cliente").cast("string"), F.lit("_LSDP")), 256)` | Identificador anonimizado via SHA2-256 para uso en reportes de oro |

### transacciones_enriquecidas

| Campo | Logica | Descripcion |
|-------|--------|-------------|
| `monto_neto_comisiones` | `F.coalesce(monto_principal - comision_transaccion, lit(0.0))` | Monto neto despues de deducir comision; nulo reemplazado por 0 |
| `porcentaje_comision_sobre_monto` | `F.when(monto_principal != 0, (comision_transaccion / monto_principal) * 100).otherwise(lit(0.0))` | Porcentaje que representa la comision sobre el monto; proteccion de division por cero |
| `variacion_saldo_transaccion` | `F.coalesce(saldo_posterior - saldo_anterior, lit(0.0))` | Diferencia de saldo entre antes y despues de la transaccion |
| `indicador_impacto_financiero` | `F.coalesce(monto_principal * riesgo_transaccion, lit(0.0))` | Producto del monto por el factor de riesgo como indicador de impacto financiero |

### comportamiento_atm_cliente

| Campo | Logica | Descripcion |
|-------|--------|-------------|
| `cantidad_depositos_atm` | `F.count(F.when(tipo_transaccion == 'DATM', monto_principal))` | Conteo de transacciones de deposito ATM por cliente |
| `cantidad_retiros_atm` | `F.count(F.when(tipo_transaccion == 'CATM', monto_principal))` | Conteo de transacciones de retiro ATM por cliente |
| `promedio_monto_depositos_atm` | `F.avg(F.when(tipo_transaccion == 'DATM', monto_principal))` con `F.coalesce(..., lit(0.0))` | Promedio de monto en depositos ATM; 0.0 si no hay depositos |
| `promedio_monto_retiros_atm` | `F.avg(F.when(tipo_transaccion == 'CATM', monto_principal))` con `F.coalesce(..., lit(0.0))` | Promedio de monto en retiros ATM; 0.0 si no hay retiros |
| `total_pagos_saldo_cliente` | `F.sum(F.when(tipo_transaccion == 'PGSL', monto_principal))` con `F.coalesce(..., lit(0.0))` | Total de pagos al saldo del cliente; 0.0 si no hay pagos |

---

## 9. Expectativas de Calidad de Datos

Las expectativas estan en modo observacional (`@dp.expect` sin `_on_violation`).
Registran metricas de calidad en el historial del pipeline sin bloquear la escritura.

### plata.lab1.clientes_saldos_consolidados (5 expectativas)

| Nombre | Expresion SQL | Impacto |
|--------|---------------|---------|
| `limite_credito_positivo` | `limite_credito > 0` | Detecta cuentas con limite de credito invalido |
| `identificador_cliente_no_nulo` | `identificador_cliente IS NOT NULL` | Garantiza integridad de clave primaria |
| `limite_credito_no_nulo` | `limite_credito IS NOT NULL` | Garantiza disponibilidad del campo de credito |
| `fecha_apertura_cuenta_valida` | `fecha_apertura_cuenta > '2020-12-31'` | Detecta fechas de apertura anteriores al sistema (anomalia de migracion) |
| `fecha_nacimiento_valida` | `fecha_nacimiento < '2009-01-01'` | Detecta clientes potencialmente menores de edad |

### plata.lab1.transacciones_enriquecidas (4 expectativas)

| Nombre | Expresion SQL | Impacto |
|--------|---------------|---------|
| `identificador_cliente_no_nulo` | `identificador_cliente IS NOT NULL` | Garantiza integridad referencial con clientes |
| `monto_neto_positivo` | `monto_neto > 0` | Detecta transacciones con monto neto negativo (anomalias) |
| `monto_neto_no_nulo` | `monto_neto IS NOT NULL` | Detecta transacciones sin monto neto calculado |
| `moneda_transaccion_no_nula` | `moneda_transaccion IS NOT NULL` | Detecta transacciones sin moneda registrada |
