# Usa una imagen oficial ligera de Python
FROM python:3.10-slim

# No buffers en stdout/stderr (útil para logs)
ENV PYTHONUNBUFFERED=1

# Crea y define el directorio de trabajo
WORKDIR /app

# Copia primero sólo requirements para aprovechar el cache de Docker
COPY requirements.txt .

# Instala las dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de tu código al contenedor
COPY . .

# Expone el puerto que usa Uvicorn / FastAPI
EXPOSE 8080
# Puerto que usará FastAPI / Cloud Run
ENV PORT=8080


# Lanza uvicorn vinculándolo al puerto $PORT
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port $PORT"]


