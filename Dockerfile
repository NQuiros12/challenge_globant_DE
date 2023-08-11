# Utiliza una imagen oficial de MySQL como base
FROM mysql:latest

# Establece las variables de entorno para la base de datos
ENV MYSQL_ROOT_PASSWORD=micolash12
ENV MYSQL_DATABASE=globant_db

# Copia un archivo SQL al contenedor (donde "init.sql" contiene tus comandos SQL)
COPY db/create_db.sql /docker-entrypoint-initdb.d/

# Expone el puerto de MySQL
EXPOSE 3306
