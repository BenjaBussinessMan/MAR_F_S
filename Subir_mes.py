# -*- coding: utf-8 -*-
"""
Subir_mes.py (reemplazo por periodo)
Lee un XLSX, filtra por un mes (YYYY-MM), muestra la suma a subir, 
borra el periodo en la tabla destino y vuelve a insertar, 
verificando que la suma en DB para ese periodo coincida con la del archivo.
"""
import argparse
from datetime import date
import os
import sys

# Forzar utf-8 para la consola de Windows (evita el error con los emojis)
if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

import pandas as pd
from sqlalchemy import create_engine, text


def month_bounds(periodo: str):
    """Recibe 'YYYY-MM' y retorna (inicio_mes, inicio_mes_siguiente) como date."""
    try:
        year, month = map(int, periodo.split("-"))
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1)
        else:
            end = date(year, month + 1, 1)
        return start, end
    except Exception as e:
        raise ValueError("El parámetro --periodo debe tener formato YYYY-MM (ej: 2026-02).") from e


def normalize_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas y castea tipos a los esperados por la tabla."""
    # Normaliza nombres
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    expected = [
        "fecha_proceso", "segmento", "marca", "modelo", "familia",
        "provincia", "tipo_combustible", "origen", "tipo_hibridacion", "unidades"
    ]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en el XLSX: {missing}")

    # Tipos
    df["fecha_proceso"] = pd.to_datetime(df["fecha_proceso"], errors="coerce").dt.date

    text_cols = ["segmento","marca","modelo","familia","provincia","tipo_combustible","origen","tipo_hibridacion"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    # Limpiar y castear unidades -> int64
    df["unidades"] = (
        df["unidades"].astype(str)
        .str.replace(r"[^0-9\-\+\.]", "", regex=True)
        .replace({"": None, ".": None})
    )
    df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce").fillna(0).astype("int64")

    return df[expected]


def main():
    parser = argparse.ArgumentParser(description="Carga mensual a Neon con borrado del periodo y verificación de sumas.")
    parser.add_argument("--xlsx", required=True, help="Ruta al archivo XLSX (ej: ventas_febrero.xlsx)")
    parser.add_argument("--periodo", required=True, help="Periodo en formato YYYY-MM (ej: 2026-02)")
    parser.add_argument("--hoja", default=0, help="Nombre o índice de la hoja (por defecto 0)")
    parser.add_argument("--dsn", default=os.getenv("NEON_DSN", "postgresql+psycopg2://USUARIO:PASS@HOST:PUERTO/DB?sslmode=require"),
                        help="DSN de conexión a Neon. También puedes usar la variable de entorno NEON_DSN.")
    parser.add_argument("--schema", default="public", help="Esquema destino (por defecto 'public')")
    parser.add_argument("--tabla", default="sales_granular", help="Tabla destino (por defecto 'sales_granular')")
    args = parser.parse_args()

    periodo_ini, periodo_fin = month_bounds(args.periodo)

    print(f"➡️  Leyendo Excel: {args.xlsx} | Hoja: {args.hoja}")
    try:
        df = pd.read_excel(args.xlsx, sheet_name=args.hoja, dtype=str)
    except Exception as e:
        print(f"Error leyendo el Excel: {e}")
        sys.exit(1)

    try:
        df = normalize_and_cast(df)
    except Exception as e:
        print(f"Error normalizando/casteando datos: {e}")
        sys.exit(1)

    # Filtrar SOLO el periodo solicitado (por si el archivo trae más fechas)
    df_periodo = df[
        (pd.to_datetime(df["fecha_proceso"]).dt.date >= periodo_ini) &
        (pd.to_datetime(df["fecha_proceso"]).dt.date < periodo_fin)
    ].copy()

    if df_periodo.empty:
        print(f"No se encontraron filas en el periodo {args.periodo} en el archivo.")
        sys.exit(1)

    filas_archivo = len(df_periodo)
    suma_archivo = int(df_periodo["unidades"].sum())

    print(f"📅 Periodo: {args.periodo} | Rango: [{periodo_ini} .. {periodo_fin})")
    print(f"🧮 Archivo - filas a subir: {filas_archivo} | SUM(unidades) a subir: {suma_archivo:,}")

    # Conexión
    try:
        db_url = args.dsn
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
            
        # Limpiar parámetros que psycopg2 NO soporta en la URL
        for param in ("channel_binding=require", "sslmode=require",
                      "sslmode=verify-full", "sslmode=prefer"):
            db_url = db_url.replace(f"&{param}", "").replace(f"?{param}&", "?").replace(f"?{param}", "")
        db_url = db_url.rstrip("?")

        engine = create_engine(
            db_url,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 10,
            },
            pool_pre_ping=True
        )
    except Exception as e:
        print(f"Error creando engine de conexión (DSN): {e}")
        sys.exit(1)

    # Proceso transaccional: medir -> borrar -> insertar -> medir
    sql_sum = text(f"""
        SELECT COALESCE(SUM(unidades), 0) AS suma_db, COUNT(*) AS filas_db
        FROM {args.schema}.{args.tabla}
        WHERE fecha_proceso >= :ini AND fecha_proceso < :fin
    """)

    sql_delete = text(f"""
        DELETE FROM {args.schema}.{args.tabla}
        WHERE fecha_proceso >= :ini AND fecha_proceso < :fin
    """)

    try:
        with engine.begin() as conn:
            # Medición previa
            res_before = conn.execute(sql_sum, {"ini": periodo_ini, "fin": periodo_fin}).mappings().one()
            suma_db_before = int(res_before["suma_db"]) if res_before["suma_db"] is not None else 0
            filas_db_before = int(res_before["filas_db"]) if res_before["filas_db"] is not None else 0
            print(f"🗄️  Antes de reemplazo - DB periodo: filas={filas_db_before:,} | SUM(unidades)={suma_db_before:,}")

            # Borrado del periodo
            del_result = conn.execute(sql_delete, {"ini": periodo_ini, "fin": periodo_fin})
            borradas = del_result.rowcount if del_result.rowcount is not None else 0
            print(f"🧹 Borradas {borradas:,} filas del periodo {args.periodo} en {args.schema}.{args.tabla}.")

            # Inserción por lotes (append)
            df_periodo.to_sql(
                args.tabla,
                con=conn,
                schema=args.schema,
                if_exists="append",
                index=False,
                chunksize=10_000,
                method="multi"
            )

            # Medición posterior
            res_after = conn.execute(sql_sum, {"ini": periodo_ini, "fin": periodo_fin}).mappings().one()
            suma_db_after = int(res_after["suma_db"]) if res_after["suma_db"] is not None else 0
            filas_db_after = int(res_after["filas_db"]) if res_after["filas_db"] is not None else 0

        # Fuera del bloque 'begin': transacción ya confirmada si no hubo excepción
        print(f"✅ Después de carga - DB periodo: filas={filas_db_after:,} | SUM(unidades)={suma_db_after:,}")

        # Comparación
        if suma_db_after == suma_archivo:
            print("✅ Verificación OK: la SUM(unidades) del archivo coincide con la SUM(unidades) en la base para el periodo.")
        else:
            print("⚠️ Atención: la SUM(unidades) en DB NO coincide con la del archivo para el periodo.")
            print(f"    Archivo: {suma_archivo:,} | DB: {suma_db_after:,}")
            print("    Revisa si el archivo trae duplicados o si hubo filtrado distinto.")

        print("✔️  Proceso finalizado.")

    except Exception as e:
        print(f"❌ Error durante la transacción de reemplazo del periodo: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
