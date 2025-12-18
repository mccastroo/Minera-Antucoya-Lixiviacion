FROM rocker/tidyverse:latest

# Instalar dependencias del sistema necesarias para paquetes R
RUN apt-get update -qq && apt-get install -y \
    libssl-dev \
    libcurl4-gnutls-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

# Establecer directorio de trabajo
WORKDIR /R

# Copiar archivos R
COPY plumber.R execute_plumber.R ./

# Instalar paquetes R necesarios usando install2.r
RUN install2.r plumber jsonlite AzureStor AzureCosmosR && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

# Exponer el puerto
EXPOSE 8787

# Comando para ejecutar la aplicación Plumber
ENTRYPOINT ["Rscript", "execute_plumber.R"]
