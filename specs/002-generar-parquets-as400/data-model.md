# Modelo de Datos: Generacion de Parquets Simulando Data AS400

**Feature**: 002-generar-parquets-as400
**Fecha**: 2026-04-03

## Entidades

### 1. CMSTFL — Maestro de Clientes

**Descripcion**: Tabla maestra de clientes del sistema AS400 de una entidad bancaria.
**Cantidad de columnas**: 70 (41 StringType, 18 DateType, 9 LongType, 2 DoubleType)
**Clave primaria**: CUSTID (LongType, numerico secuencial comenzando en 1)
**Volumetria**: 50,000 registros base (configurable) + 0.6% incremental por re-ejecucion (porcentajeNuevos=0.006)
**Relaciones**: Origen de claves foraneas en TRXPFL y BLNCFL

| # | Campo | Tipo | Descripcion |
|---|-------|------|-------------|
| 1 | CUSTID | LongType | Identificador unico del cliente (secuencial, PK) |
| 2 | CUSNM | StringType | Nombre del cliente |
| 3 | CUSLN | StringType | Apellido del cliente |
| 4 | CUSMD | StringType | Segundo nombre del cliente |
| 5 | CUSFN | StringType | Nombre completo formateado |
| 6 | CUSSX | StringType | Genero del cliente (M/F) |
| 7 | CUSTT | StringType | Titulo del cliente (Mr/Mrs/Ms/Dr) |
| 8 | CUSAD | StringType | Direccion linea 1 |
| 9 | CUSA2 | StringType | Direccion linea 2 |
| 10 | CUSCT | StringType | Ciudad |
| 11 | CUSST | StringType | Estado o provincia |
| 12 | CUSZP | StringType | Codigo postal |
| 13 | CUSCN | StringType | Pais |
| 14 | CUSPH | StringType | Telefono principal |
| 15 | CUSMB | StringType | Telefono movil |
| 16 | CUSEM | StringType | Correo electronico |
| 17 | CUSTP | StringType | Tipo de cliente (IND/COR) |
| 18 | CUSSG | StringType | Segmento del cliente (PREM/STD/BAS) |
| 19 | CUSMS | StringType | Estado civil (SNG/MRD/DIV/WDW) |
| 20 | CUSOC | StringType | Ocupacion |
| 21 | CUSED | StringType | Nivel educativo (PHD/MST/BSC/HSC/OTH) |
| 22 | CUSNA | StringType | Nacionalidad |
| 23 | CUSDL | StringType | Numero de documento de identidad |
| 24 | CUSDP | StringType | Tipo de documento (PASS/NAID/DRVL) |
| 25 | CUSRG | StringType | Region geografica |
| 26 | CUSBR | StringType | Sucursal asignada |
| 27 | CUSMG | StringType | Gerente de cuenta asignado |
| 28 | CUSRF | StringType | Codigo de referencia externo |
| 29 | CUSRS | StringType | Fuente de referencia |
| 30 | CUSLG | StringType | Idioma preferido (HEB/ARA/ENG) |
| 31 | CUSNT | StringType | Notas del cliente |
| 32 | CUSAG | StringType | Grupo de afinidad |
| 33 | CUSPC | StringType | Preferencia de comunicacion (EML/SMS/PHL/MIL) |
| 34 | CUSRK | StringType | Nivel de riesgo (LOW/MED/HIG/CRT) |
| 35 | CUSVP | StringType | Indicador VIP (Y/N) |
| 36 | CUSPF | StringType | Indicador PEP (Y/N) |
| 37 | CUSKT | StringType | Estado de KYC (COMP/PEND/EXPD) |
| 38 | CUSFM | StringType | Indicador FATCA (Y/N) |
| 39 | CUSLC | StringType | Ultimo canal de contacto (BRN/ATM/ONL/MOB) |
| 40 | CUSCR | StringType | Codigo de categoria crediticia |
| 41 | CUSAC | StringType | Indicador de cuenta activa (A/I/S) |
| 42 | CUSCL | StringType | Clasificacion interna del cliente |
| 43 | CUSDB | DateType | Fecha de nacimiento |
| 44 | CUSOD | DateType | Fecha de apertura de la relacion |
| 45 | CUSCD | DateType | Fecha de cierre de la relacion |
| 46 | CUSLV | DateType | Fecha de ultimo movimiento |
| 47 | CUSUD | DateType | Fecha de ultima actualizacion |
| 48 | CUSKD | DateType | Fecha de vencimiento KYC |
| 49 | CUSRD | DateType | Fecha de ultima revision |
| 50 | CUSXD | DateType | Fecha de expiracion de documento |
| 51 | CUSFD | DateType | Fecha de primera transaccion |
| 52 | CUSLD | DateType | Fecha de ultima transaccion |
| 53 | CUSMD2 | DateType | Fecha de modificacion de datos |
| 54 | CUSAD2 | DateType | Fecha de asignacion de gerente |
| 55 | CUSBD | DateType | Fecha de cambio de sucursal |
| 56 | CUSVD | DateType | Fecha de validacion de datos |
| 57 | CUSPD | DateType | Fecha de procesamiento |
| 58 | CUSDD | DateType | Fecha de deteccion de duplicado |
| 59 | CUSED2 | DateType | Fecha de enrolamiento digital |
| 60 | CUSND | DateType | Fecha de proxima revision |
| 61 | CUSYR | LongType | Anio de nacimiento |
| 62 | CUSAG2 | LongType | Edad del cliente |
| 63 | CUSDP2 | LongType | Numero de dependientes |
| 64 | CUSAC2 | LongType | Numero de cuentas activas |
| 65 | CUSTX | LongType | Total de transacciones historicas |
| 66 | CUSSC | LongType | Puntaje crediticio |
| 67 | CUSLR | LongType | Numero de reclamos |
| 68 | CUSRC | LongType | Numero de contactos en el ultimo anio |
| 69 | CUSIN | DoubleType | Ingreso estimado anual |
| 70 | CUSBL | DoubleType | Balance promedio historico |

**Campos demograficos sujetos a mutacion (parametro `camposMutacion`, por defecto `porcentajeMutacion`=0.20, equivalente a 20% en re-ejecuciones)**: CUSNM, CUSLN, CUSMD, CUSFN, CUSAD, CUSA2, CUSCT, CUSST, CUSZP, CUSPH, CUSMB, CUSEM, CUSMS, CUSOC, CUSED

**Reglas de validacion**:
- CUSTID debe ser secuencial, unico y positivo (LongType comenzando en 1).
- CUSSX limitado a valores: "M", "F".
- CUSTP limitado a: "IND", "COR".
- CUSSG limitado a: "PREM", "STD", "BAS".
- Nombres (CUSNM, CUSLN): exclusivamente hebreos, egipcios e ingleses. Prohibidos latinos.
- Fecha de nacimiento (CUSDB): rango 1970-2007.
- Demas fechas (CUSOD, CUSCD, CUSLV, etc.): rango 2005-2025.
- Montos (CUSIN, CUSBL): generados entre los parametros `montoMinimo` (defecto 10) y `montoMaximo` (defecto 100000).

---

### 2. TRXPFL — Transaccional de Clientes

**Descripcion**: Tabla de transacciones bancarias del sistema AS400.
**Cantidad de columnas**: 60 (7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType)
**Clave primaria**: TRXID (StringType, alfanumerico con prefijo tipo + secuencial)
**Volumetria**: 150,000 registros por ejecucion (configurable)
**Relaciones**: FK CUSTID referenciando a CMSTFL

| # | Campo | Tipo | Descripcion |
|---|-------|------|-------------|
| 1 | TRXID | StringType | Identificador unico de transaccion (ej: "CATM00000001") |
| 2 | CUSTID | LongType | Identificador del cliente (FK a CMSTFL) |
| 3 | TRXTYP | StringType | Tipo de transaccion (15 tipos) |
| 4 | TRXAMT | DoubleType | Monto de la transaccion |
| 5 | TRXCUR | StringType | Moneda de la transaccion (USD/EUR/ILS/EGP/GBP) |
| 6 | TRXST | StringType | Estado de la transaccion (APPR/DECL/PEND/REVS) |
| 7 | TRXCH | StringType | Canal de la transaccion (ATM/BRN/ONL/MOB/POS) |
| 8 | TRXDSC | StringType | Descripcion de la transaccion |
| 9 | TRXREF | StringType | Referencia externa de la transaccion |
| 10 | TRXSQ | LongType | Numero secuencial de la transaccion |
| 11 | TRXDT | DateType | Fecha de la transaccion (parametro de entrada) |
| 12 | TRXVD | DateType | Fecha valor de la transaccion |
| 13 | TRXPD | DateType | Fecha de procesamiento |
| 14 | TRXSD | DateType | Fecha de liquidacion |
| 15 | TRXCD | DateType | Fecha de compensacion |
| 16 | TRXED | DateType | Fecha efectiva |
| 17 | TRXRD | DateType | Fecha de registro contable |
| 18 | TRXAD | DateType | Fecha de autorizacion |
| 19 | TRXND | DateType | Fecha de notificacion |
| 20 | TRXXD | DateType | Fecha de expiracion |
| 21 | TRXFD | DateType | Fecha de cierre |
| 22 | TRXGD | DateType | Fecha de generacion del lote |
| 23 | TRXHD | DateType | Fecha de alta en el sistema |
| 24 | TRXBD | DateType | Fecha del balance afectado |
| 25 | TRXMD | DateType | Fecha de modificacion |
| 26 | TRXLD | DateType | Fecha limite de reclamo |
| 27 | TRXUD | DateType | Fecha de ultima actualizacion |
| 28 | TRXOD | DateType | Fecha original de la operacion |
| 29 | TRXKD | DateType | Fecha de verificacion KYC |
| 30 | TRXTS | TimestampType | Marca de tiempo de creacion |
| 31 | TRXUS | TimestampType | Marca de tiempo de ultima modificacion |
| 32 | TRXBA | DoubleType | Balance anterior a la transaccion |
| 33 | TRXBP | DoubleType | Balance posterior a la transaccion |
| 34 | TRXCM | DoubleType | Comision de la transaccion |
| 35 | TRXIM | DoubleType | Impuesto de la transaccion |
| 36 | TRXNT | DoubleType | Monto neto (monto - comision - impuesto) |
| 37 | TRXTC | DoubleType | Tasa de cambio aplicada |
| 38 | TRXAO | DoubleType | Monto en moneda original |
| 39 | TRXAL | DoubleType | Monto en moneda local |
| 40 | TRXIN | DoubleType | Interes generado |
| 41 | TRXPN | DoubleType | Penalizacion aplicada |
| 42 | TRXDS | DoubleType | Descuento aplicado |
| 43 | TRXBF | DoubleType | Beneficio acumulado |
| 44 | TRXPT | DoubleType | Puntos generados |
| 45 | TRXRL | DoubleType | Tasa de retorno |
| 46 | TRXMX | DoubleType | Monto maximo permitido |
| 47 | TRXMN | DoubleType | Monto minimo permitido |
| 48 | TRXAV | DoubleType | Monto promedio historico del cliente |
| 49 | TRXDV | DoubleType | Desviacion respecto al promedio |
| 50 | TRXRK | DoubleType | Score de riesgo de la transaccion |
| 51 | TRXFR | DoubleType | Score de fraude de la transaccion |
| 52 | TRXLM | DoubleType | Limite disponible pre-transaccion |
| 53 | TRXLP | DoubleType | Limite disponible post-transaccion |
| 54 | TRXCP | DoubleType | Cargo por procesamiento |
| 55 | TRXCI | DoubleType | Cargo por interbank |
| 56 | TRXCF | DoubleType | Cargo fijo |
| 57 | TRXCV | DoubleType | Cargo variable |
| 58 | TRXSB | DoubleType | Subtotal antes de cargos |
| 59 | TRXTL | DoubleType | Total con cargos incluidos |
| 60 | TRXRS | DoubleType | Monto de reembolso (si aplica) |

**Distribucion de tipos de transaccion (TRXTYP)**:

| Grupo | Tipo | Descripcion | Frecuencia |
|-------|------|-------------|------------|
| Alta (~60%) | CATM | Retiro cajero automatico | ~15% |
| Alta (~60%) | DATM | Deposito cajero automatico | ~14% |
| Alta (~60%) | CMPR | Compra punto de venta | ~13% |
| Alta (~60%) | TINT | Transferencia interbancaria | ~10% |
| Alta (~60%) | DPST | Deposito en ventanilla | ~8% |
| Media (~30%) | PGSL | Pago de saldo | ~7% |
| Media (~30%) | TEXT | Transferencia externa | ~6% |
| Media (~30%) | RTRO | Retiro en ventanilla | ~5% |
| Media (~30%) | PGSV | Pago de servicios | ~5% |
| Media (~30%) | NMNA | Nomina | ~4% |
| Media (~30%) | INTR | Pago de intereses | ~3% |
| Baja (~10%) | ADSL | Adelanto de salario | ~3% |
| Baja (~10%) | IMPT | Pago de impuestos | ~3% |
| Baja (~10%) | DMCL | Domiciliacion | ~2% |
| Baja (~10%) | CMSN | Comision bancaria | ~2% |

**Reglas de validacion**:
- TRXID formato: prefijo tipo (4 chars) + secuencial (8 digitos, zero-padded), ej: "CATM00000001".
- CUSTID debe existir en el Maestro de Clientes (FK).
- TRXTYP limitado a los 15 tipos definidos.
- TRXAMT debe ser positivo para depositos y operaciones, negativo para cargos de comision. Montos generados entre los parametros `montoMinimo` (defecto 10) y `montoMaximo` (defecto 100000).
- TRXDT debe coincidir con el parametro `fechaTransaccion` recibido via widget.
- Demas fechas (TRXVD, TRXPD, TRXSD, etc.): rango 2005-2025.

---

### 3. BLNCFL — Saldos de Clientes

**Descripcion**: Tabla de saldos y cuentas de los clientes del sistema AS400.
**Cantidad de columnas**: 100 (2 LongType, 29 StringType, 34 DoubleType, 35 DateType)
**Clave primaria**: Compuesta por CUSTID + tipo de cuenta (relacion 1:1 con CMSTFL por CUSTID)
**Volumetria**: 1 registro por cada cliente del Maestro (1:1 con CMSTFL)
**Relaciones**: FK CUSTID referenciando a CMSTFL (relacion 1:1)

| # | Campo | Tipo | Descripcion |
|---|-------|------|-------------|
| 1 | CUSTID | LongType | Identificador del cliente (FK a CMSTFL) |
| 2 | BLSQ | LongType | Numero secuencial del registro de saldo |
| 3 | BLACT | StringType | Tipo de cuenta (AHRO/CRTE/PRES/INVR) |
| 4 | BLACN | StringType | Numero de cuenta |
| 5 | BLCUR | StringType | Moneda de la cuenta (USD/EUR/ILS/EGP/GBP) |
| 6 | BLST | StringType | Estado de la cuenta (ACTV/INAC/SUSP/CERR) |
| 7 | BLBR | StringType | Sucursal de la cuenta |
| 8 | BLPR | StringType | Producto bancario |
| 9 | BLSP | StringType | Subproducto bancario |
| 10 | BLNM | StringType | Nombre de la cuenta |
| 11 | BLCL | StringType | Clasificacion de la cuenta |
| 12 | BLRK | StringType | Nivel de riesgo de la cuenta (LOW/MED/HIG) |
| 13 | BLTP | StringType | Tipo de titular (PRI/SEC/AUT) |
| 14 | BLMG | StringType | Gerente asignado |
| 15 | BLRF | StringType | Referencia de la cuenta |
| 16 | BLCC | StringType | Centro de costo |
| 17 | BLAG | StringType | Grupo de afinidad |
| 18 | BLPL | StringType | Plan de la cuenta |
| 19 | BLRG | StringType | Region de la cuenta |
| 20 | BLSF | StringType | Sufijo de la cuenta |
| 21 | BLNT | StringType | Notas de la cuenta |
| 22 | BLLC | StringType | Ultimo canal de operacion (ATM/BRN/ONL/MOB) |
| 23 | BLPF | StringType | Indicador de perfil preferencial (Y/N) |
| 24 | BLAU | StringType | Indicador de auto-renovacion (Y/N) |
| 25 | BLTX | StringType | Indicador de exento de impuestos (Y/N) |
| 26 | BLGR | StringType | Indicador de garantia (Y/N) |
| 27 | BLEM | StringType | Indicador de embargo (Y/N) |
| 28 | BLFR | StringType | Indicador de fraude detectado (Y/N) |
| 29 | BLKY | StringType | Estado de KYC de la cuenta (COMP/PEND) |
| 30 | BLVP | StringType | Indicador de cuenta VIP (Y/N) |
| 31 | BLFC | StringType | Codigo de frecuencia de estado de cuenta |
| 32 | BLAV | DoubleType | Saldo disponible |
| 33 | BLTB | DoubleType | Saldo total (libro) |
| 34 | BLRV | DoubleType | Saldo reservado |
| 35 | BLBK | DoubleType | Saldo bloqueado |
| 36 | BLMN | DoubleType | Saldo minimo requerido |
| 37 | BLMX | DoubleType | Saldo maximo permitido |
| 38 | BLIR | DoubleType | Tasa de interes activa |
| 39 | BLPM | DoubleType | Tasa de interes pasiva |
| 40 | BLCR | DoubleType | Limite de credito |
| 41 | BLCU | DoubleType | Credito utilizado |
| 42 | BLCD | DoubleType | Credito disponible |
| 43 | BLOV | DoubleType | Monto en sobregiro |
| 44 | BLOL | DoubleType | Limite de sobregiro |
| 45 | BLPD | DoubleType | Pago minimo del periodo |
| 46 | BLPC | DoubleType | Pago corriente registrado |
| 47 | BLPA | DoubleType | Pago acumulado del anio |
| 48 | BLDI | DoubleType | Depositos del periodo |
| 49 | BLWI | DoubleType | Retiros del periodo |
| 50 | BLTI | DoubleType | Intereses generados del periodo |
| 51 | BLTC | DoubleType | Comisiones del periodo |
| 52 | BLCA | DoubleType | Cargos administrativos del periodo |
| 53 | BLIM | DoubleType | Impuestos del periodo |
| 54 | BLRF2 | DoubleType | Monto de refinanciamiento |
| 55 | BLPN | DoubleType | Penalizaciones del periodo |
| 56 | BLBN | DoubleType | Bonificaciones del periodo |
| 57 | BLAP | DoubleType | Saldo promedio del periodo |
| 58 | BLAM | DoubleType | Saldo promedio del mes |
| 59 | BLAY | DoubleType | Saldo promedio del anio |
| 60 | BLHI | DoubleType | Saldo maximo historico |
| 61 | BLLO | DoubleType | Saldo minimo historico |
| 62 | BLVR | DoubleType | Variacion respecto al periodo anterior |
| 63 | BLRT | DoubleType | Tasa de rendimiento |
| 64 | BLCP | DoubleType | Capital vigente (prestamos) |
| 65 | BLCI | DoubleType | Capital vencido (prestamos) |
| 66 | BLOD | DateType | Fecha de apertura de la cuenta |
| 67 | BLXD | DateType | Fecha de expiracion de la cuenta |
| 68 | BLUD | DateType | Fecha de ultima actualizacion |
| 69 | BLLD | DateType | Fecha de ultimo movimiento |
| 70 | BLSD | DateType | Fecha de ultimo estado de cuenta |
| 71 | BLPD2 | DateType | Fecha de proximo pago |
| 72 | BLRD | DateType | Fecha de ultima revision |
| 73 | BLMD | DateType | Fecha de modificacion de datos |
| 74 | BLCD2 | DateType | Fecha de cierre de cuenta |
| 75 | BLBD | DateType | Fecha de bloqueo |
| 76 | BLFD | DateType | Fecha de desbloqueo |
| 77 | BLGD | DateType | Fecha de generacion del reporte |
| 78 | BLHD | DateType | Fecha de alta del producto |
| 79 | BLID | DateType | Fecha de ingreso a cartera |
| 80 | BLJD | DateType | Fecha de ajuste |
| 81 | BLKD | DateType | Fecha de verificacion KYC |
| 82 | BLND | DateType | Fecha de notificacion |
| 83 | BLTD | DateType | Fecha de transferencia |
| 84 | BLVD | DateType | Fecha de vencimiento |
| 85 | BLWD | DateType | Fecha de retiro |
| 86 | BLYD | DateType | Fecha de inicio del anio fiscal |
| 87 | BLZD | DateType | Fecha de corte |
| 88 | BLED | DateType | Fecha de evaluacion |
| 89 | BLAD2 | DateType | Fecha de autorizacion |
| 90 | BLDD | DateType | Fecha de desembolso |
| 91 | BLFP | DateType | Fecha de primer pago |
| 92 | BLLP | DateType | Fecha de ultimo pago |
| 93 | BLMP | DateType | Fecha de mora |
| 94 | BLNP | DateType | Fecha de normalizacion |
| 95 | BLOP | DateType | Fecha de operacion contable |
| 96 | BLPP | DateType | Fecha de proceso |
| 97 | BLQP | DateType | Fecha de liquidacion |
| 98 | BLRP | DateType | Fecha de renovacion |
| 99 | BLSP2 | DateType | Fecha de suspension |
| 100 | BLTP2 | DateType | Fecha de tramitacion |

**Distribucion de tipos de cuenta (BLACT)**:

| Tipo | Descripcion | Proporcion |
|------|-------------|------------|
| AHRO | Cuenta de ahorro | 40% |
| CRTE | Cuenta corriente | 30% |
| PRES | Prestamo | 20% |
| INVR | Inversion | 10% |

**Reglas de validacion**:
- Exactamente 1 registro por CUSTID (relacion 1:1 con CMSTFL).
- Todos los CUSTID deben existir en el Maestro de Clientes.
- BLACT limitado a: "AHRO", "CRTE", "PRES", "INVR".
- Saldos (BLAV, BLTB y demas DoubleType) generados entre los parametros `montoMinimo` (defecto 10) y `montoMaximo` (defecto 100000); deben ser coherentes con el tipo de cuenta.
- Fechas (BLOD, BLXD, BLUD, etc.): rango 2005-2025.

---

## Diagrama de Relaciones

```text
+------------------+        +------------------+        +------------------+
|     CMSTFL       |        |     TRXPFL       |        |     BLNCFL       |
| (Maestro Client) |        |  (Transaccional) |        |    (Saldos)      |
+------------------+        +------------------+        +------------------+
| PK: CUSTID (Long)|<-------| FK: CUSTID (Long)|  |---->| FK: CUSTID (Long)|
| 70 columnas      |   1:N  | PK: TRXID (Str)  |  |    | 100 columnas     |
| 41 Str, 18 Date  |        | 60 columnas      |  |    | 29 Str, 35 Date  |
| 9 Long, 2 Double |        | 7 Str, 19 Date   |  |    | 34 Double, 2 Long|
+------------------+        | 2 Ts, 2 Long     |  |    +------------------+
        |                    | 30 Double         |  |           |
        |                    +------------------+  |           |
        |                                          |           |
        +------------------------------------------+           |
                         1:1                                   |
        +------------------------------------------------------+
```

- **CMSTFL -> TRXPFL**: Relacion 1:N. Un cliente puede tener multiples transacciones.
- **CMSTFL -> BLNCFL**: Relacion 1:1. Cada cliente tiene exactamente un registro de saldo.

## Resumen de Tipos por Entidad

| Entidad | StringType | DateType | LongType | DoubleType | TimestampType | Total |
|---------|-----------|----------|----------|------------|---------------|-------|
| CMSTFL | 41 | 18 | 9 | 2 | 0 | 70 |
| TRXPFL | 7 | 19 | 2 | 30 | 2 | 60 |
| BLNCFL | 29 | 35 | 2 | 34 | 0 | 100 |
| **Total** | **77** | **72** | **13** | **66** | **2** | **230** |
