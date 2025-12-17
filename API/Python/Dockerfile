FROM python:3.9

WORKDIR /app

# Copiar archivos de la aplicación
COPY main.py config.py requirements.txt azure_connections.py .env /app/

# Instalar dependencias
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Exponer el puerto
EXPOSE 8000

# Comando para ejecutar la aplicación
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

