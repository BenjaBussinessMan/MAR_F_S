# db.py
import os
import streamlit as st
from sqlalchemy import create_engine

def get_engine():
    db_url = None

    # 1) Streamlit Cloud / secrets.toml
    try:
        if "DATABASE_URL" in st.secrets:
            db_url = st.secrets["DATABASE_URL"]
    except Exception:
        pass

    # 2) Local env var
    if not db_url:
        db_url = os.getenv("DATABASE_URL") or os.getenv("DB_URL")

    if not db_url:
        raise RuntimeError("No se encontró DATABASE_URL en st.secrets ni en variables de entorno.")

    # SQLAlchemy necesita el driver psycopg2
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    # ── Limpiar parámetros que psycopg2 NO soporta en la URL ──────────
    # channel_binding=require causa "server closed unexpectedly" con Neon
    # sslmode se pasa por connect_args para evitar conflictos
    for param in ("channel_binding=require", "sslmode=require",
                  "sslmode=verify-full", "sslmode=prefer"):
        db_url = db_url.replace(f"&{param}", "").replace(f"?{param}&", "?").replace(f"?{param}", "")

    # Eliminar '?' huérfano si quedó la query vacía
    db_url = db_url.rstrip("?")

    engine = create_engine(
        db_url,
        connect_args={
            "sslmode": "require",       # SSL obligatorio para Neon
            "connect_timeout": 10,      # evita esperas indefinidas en cold-start
        },
        pool_pre_ping=True,            # detecta conexiones caídas antes de usarlas
        pool_recycle=300,              # recicla conexiones cada 5 min
        pool_size=3,                   # conexiones simultáneas máximas
        max_overflow=2,
    )
    return engine
