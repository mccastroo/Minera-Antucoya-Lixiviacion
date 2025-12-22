# API - Docker Build Instructions

Este directorio contiene dos APIs que deben construirse como imágenes Docker usando `docker buildx` con la plataforma `linux/amd64`.

## API Python

### Construcción de la Imagen Docker

Para construir la imagen Docker de la API Python, ejecuta el siguiente comando desde el directorio `API/Python/`:

```bash
docker buildx build --platform linux/amd64 -t uaaaminerals.azurecr.io/antucoya_ontologia:latest .
```

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

---

## API R (Plumber)

### Construcción de la Imagen Docker

Para construir la imagen Docker de la API R (Plumber), ejecuta el siguiente comando desde el directorio `API/R/`:

```bash
docker buildx build --platform linux/amd64 -t uaaaminerals.azurecr.io/antucoya-vpd-r:latest .
```

### Requisitos

- Docker con soporte para `buildx`
- Archivos requeridos: `plumber.R`, `execute_plumber.R`

### Ejecución del Contenedor

Una vez construida la imagen, puedes ejecutar el contenedor con:

```bash
docker run -p 8787:8787 uaaaminerals.azurecr.io/antucoya-vpd-r:latest
```

La API estará disponible en el puerto `8787`.

---

## Nota Importante

**Todas las imágenes Docker deben construirse usando `docker buildx build --platform linux/amd64`** para garantizar compatibilidad con la arquitectura de destino en Azure Container Registry.
