# Usar una imagen oficial de Python ligera
FROM python:3.11-slim

# Variables de entorno para Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Instalar dependencias del sistema operativo (C++ build tools) esenciales 
# para Prophet, xgboost, tensorflow y el driver psycopg2 de PostgreSQL
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Configurar el directorio de trabajo
WORKDIR /app

# Copiar dependencias del proyecto al contenedor
COPY requirements.txt .

# Instalar los paquetes de Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente completo
COPY . .

# Exponer el puerto por el que Streamlit levanta el servicio por defecto
EXPOSE 8501

# Comando por defecto para correr la app al iniciar el contenedor
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
