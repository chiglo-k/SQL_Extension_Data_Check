FROM postgres:14

# Устанавливаем необходимые зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-plpython3-14 \
    python3 \
    python3-pip \
    python3-pandas \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Создаем директории для расширения
RUN mkdir -p /usr/share/postgresql/14/extension/

# Копируем файлы расширения
COPY missing_data_check.control /usr/share/postgresql/14/extension/
COPY missing_data_check--1.0.sql /usr/share/postgresql/14/extension/

# Копируем SQL-скрипт для инициализации
COPY init.sql /docker-entrypoint-initdb.d/

# Создаем скрипт инициализации для расширения
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL\n\
    CREATE EXTENSION IF NOT EXISTS plpython3u;\n\
    CREATE EXTENSION IF NOT EXISTS missing_data_check VERSION '\''1.0'\'';\n\
EOSQL\n' > /docker-entrypoint-initdb.d/01-init-extension.sh

# Делаем скрипт исполняемым
RUN chmod +x /docker-entrypoint-initdb.d/01-init-extension.sh

# Открываем порт PostgreSQL
EXPOSE 5432

# Запускаем PostgreSQL
CMD ["postgres"]