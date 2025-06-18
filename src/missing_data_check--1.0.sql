-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION missing_data_check" to load this file. \quit

-- Создаем схему для нашего расширения
CREATE SCHEMA IF NOT EXISTS data_quality;

-- Создаем таблицу для хранения информации о пропущенных данных
CREATE TABLE IF NOT EXISTS data_quality.missing_data_results (
    id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    missing_column TEXT NOT NULL,
    frame_name TEXT NOT NULL,
    null_count INTEGER NOT NULL,
    check_date TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Создаем функцию для сохранения результатов проверки в постоянную таблицу
CREATE OR REPLACE FUNCTION data_quality.save_missing_data_results(
    table_name TEXT,
    exclude_columns TEXT[] DEFAULT NULL
) RETURNS INTEGER
AS $$

import pandas as pd
from datetime import datetime
import plpy

# Определяем класс MissingDataChecker прямо здесь
class MissingDataChecker:
    """
    Класс для проверки пропущенных значений в таблицах PostgreSQL
    """

    def __init__(self):
        self.current_date = datetime.now()

    def check_missing_values(self, table_name, exclude_columns=None):
        """
        Проверяет пропущенные значения в указанной таблице

        Args:
            table_name (str): Имя таблицы для проверки
            exclude_columns (list): Список колонок, которые нужно исключить из проверки

        Returns:
            list: Список словарей с информацией о пропущенных данных
        """
        # Получаем список колонок таблицы
        columns_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """

        try:
            columns_result = plpy.execute(columns_query)
            columns = [row['column_name'] for row in columns_result]

            # Исключаем указанные колонки
            if exclude_columns:
                columns = [col for col in columns if col not in exclude_columns]

            missing_data = []

            # Проверяем каждую колонку на наличие NULL значений
            for column in columns:
                null_check_query = f"""
                    SELECT COUNT(*) as null_count
                    FROM {table_name}
                    WHERE "{column}" IS NULL
                """

                null_result = plpy.execute(null_check_query)
                null_count = null_result[0]['null_count']

                if null_count > 0:
                    # Получаем информацию о файлах с пропущенными данными
                    if 'Файлы' in columns:
                        file_query = f"""
                            SELECT DISTINCT "Файлы"
                            FROM {table_name}
                            WHERE "{column}" IS NULL
                        """

                        file_results = plpy.execute(file_query)

                        for row in file_results:
                            file_name = row['Файлы']

                            missing_data.append({
                                'file_name': file_name,
                                'missing_column': column,
                                'frame_name': table_name,
                                'null_count': null_count,
                                'check_date': self.current_date.strftime('%Y-%m-%d %H:%M:%S')
                            })
                    else:
                        # Если колонки "Файлы" нет, просто добавляем информацию о пропущенных данных
                        missing_data.append({
                            'file_name': 'N/A',
                            'missing_column': column,
                            'frame_name': table_name,
                            'null_count': null_count,
                            'check_date': self.current_date.strftime('%Y-%m-%d %H:%M:%S')
                        })

            return missing_data

        except Exception as e:
            plpy.error(f"Ошибка при проверке пропущенных данных: {str(e)}")
            return []

    def create_temp_table(self, table_name):
        """
        Создает временную таблицу для хранения результатов проверки

        Args:
            table_name (str): Имя временной таблицы

        Returns:
            bool: True, если таблица успешно создана, иначе False
        """
        try:
            create_table_query = f"""
                CREATE TEMP TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    file_name TEXT,
                    missing_column TEXT,
                    frame_name TEXT,
                    null_count INTEGER,
                    check_date TIMESTAMP
                )
            """

            plpy.execute(create_table_query)
            return True

        except Exception as e:
            plpy.error(f"Ошибка при создании временной таблицы: {str(e)}")
            return False

    def store_missing_data(self, temp_table, missing_data):
        """
        Сохраняет информацию о пропущенных данных во временную таблицу

        Args:
            temp_table (str): Имя временной таблицы
            missing_data (list): Список словарей с информацией о пропущенных данных

        Returns:
            int: Количество добавленных записей
        """
        if not missing_data:
            return 0

        try:
            count = 0
            for data in missing_data:
                insert_query = f"""
                    INSERT INTO {temp_table}
                    (file_name, missing_column, frame_name, null_count, check_date)
                    VALUES
                    ('{data['file_name']}', '{data['missing_column']}',
                     '{data['frame_name']}', {data['null_count']},
                     '{data['check_date']}')
                """

                plpy.execute(insert_query)
                count += 1

            return count

        except Exception as e:
            plpy.error(f"Ошибка при сохранении данных: {str(e)}")
            return 0

# Проверяем существование таблицы
check_table_query = """
SELECT EXISTS (
    SELECT FROM information_schema.tables
    WHERE table_schema = 'data_quality'
    AND table_name = 'missing_data_results'
)
"""
table_exists = plpy.execute(check_table_query)[0]['exists']

if not table_exists:
    # Создаем таблицу, если она не существует
    create_table_query = """
    CREATE TABLE IF NOT EXISTS data_quality.missing_data_results (
        id SERIAL PRIMARY KEY,
        file_name TEXT NOT NULL,
        missing_column TEXT NOT NULL,
        frame_name TEXT NOT NULL,
        null_count INTEGER NOT NULL,
        check_date TIMESTAMP WITH TIME ZONE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )
    """
    plpy.execute(create_table_query)

# Создаем экземпляр класса
checker = MissingDataChecker()
missing_data = checker.check_missing_values(table_name, exclude_columns)

# Сохраняем результаты в постоянную таблицу
count = 0
for data in missing_data:
    query = f"""
        INSERT INTO data_quality.missing_data_results
        (file_name, missing_column, frame_name, null_count, check_date)
        VALUES
        ('{data['file_name']}', '{data['missing_column']}',
         '{data['frame_name']}', {data['null_count']},
         '{data['check_date']}')
    """
    plpy.execute(query)
    count += 1

return count
$$ LANGUAGE plpython3u;

-- Создаем функцию для получения текущей даты и времени
CREATE OR REPLACE FUNCTION data_quality.get_current_datetime()
RETURNS TIMESTAMP WITH TIME ZONE
AS $$
    from datetime import datetime
    return datetime.now()
$$ LANGUAGE plpython3u;

-- Комментарии к схеме и таблицам
COMMENT ON SCHEMA data_quality IS 'Schema for data quality extension';
COMMENT ON TABLE data_quality.missing_data_results IS 'Table for storing information about missing data';
COMMENT ON FUNCTION data_quality.save_missing_data_results(TEXT, TEXT[]) IS 'Saves missing data check results to the permanent table';
COMMENT ON FUNCTION data_quality.get_current_datetime() IS 'Returns the current date and time';

-- Предоставляем права на использование функций и таблиц
GRANT USAGE ON SCHEMA data_quality TO PUBLIC;
GRANT SELECT, INSERT ON data_quality.missing_data_results TO PUBLIC;
GRANT EXECUTE ON FUNCTION data_quality.save_missing_data_results(TEXT, TEXT[]) TO PUBLIC;
GRANT EXECUTE ON FUNCTION data_quality.get_current_datetime() TO PUBLIC;