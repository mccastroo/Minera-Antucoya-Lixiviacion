# API Python - Docker

## Construcción de la Imagen Docker

Para construir la imagen Docker de la API Python, es necesario utilizar `docker buildx` con la plataforma `linux/amd64`:

```bash
docker buildx build --platform linux/amd64 -t uaaaminerals.azurecr.io/antucoya_ontologia:latest .
```

**Nota:** Este comando debe ejecutarse desde el directorio `API/Python/` donde se encuentra el `Dockerfile`.

### Requisitos

- Docker con soporte para `buildx`
- Archivo `.env` con las variables de entorno necesarias
- Archivos requeridos: `main.py`, `config.py`, `requirements.txt`, `azure_connections.py`

### Ejecución del Contenedor

Una vez construida la imagen, puedes ejecutar el contenedor con:

```bash
docker run -p 8000:8000 uaaaminerals.azurecr.io/antucoya_ontologia:latest
```

La API estará disponible en `http://localhost:8000`.
