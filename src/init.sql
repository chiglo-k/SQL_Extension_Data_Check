-- Скрипты для создания тестовых таблиц
CREATE TABLE test_table (
    "Файлы" TEXT,
    "Данные" TEXT
);

-- Вставляем тестовые данные
INSERT INTO test_table VALUES
    ('file1.xlsx', 'data1'),
    ('file2.xlsx', NULL),
    ('file3.xlsx', 'data3'),
    ('file4.xlsx', NULL);


CREATE TABLE test_table_multi (
    "Файлы" TEXT,
    "Колонка1" TEXT,
    "Колонка2" INTEGER,
    "Колонка3" DATE
);


INSERT INTO test_table_multi VALUES
    ('file1.xlsx', 'значение1', 10, '2023-01-01'),
    ('file2.xlsx', NULL, 20, '2023-01-02'),
    ('file3.xlsx', 'значение3', NULL, '2023-01-03'),
    ('file4.xlsx', 'значение4', 40, NULL),
    ('file5.xlsx', NULL, NULL, NULL);


CREATE TABLE sales_data (
    "Файлы" TEXT,
    "Дата" DATE,
    "Продукт" TEXT,
    "Количество" INTEGER,
    "Цена" NUMERIC(10, 2),
    "Сумма" NUMERIC(10, 2)
);

-- Вставляем данные о продажах с пропусками
INSERT INTO sales_data VALUES
    ('sales_jan.xlsx', '2023-01-15', 'Товар A', 10, 100.00, 1000.00),
    ('sales_jan.xlsx', '2023-01-16', 'Товар B', NULL, 200.00, NULL),
    ('sales_feb.xlsx', '2023-02-10', 'Товар A', 15, 100.00, 1500.00),
    ('sales_feb.xlsx', '2023-02-11', 'Товар C', 5, NULL, NULL),
    ('sales_mar.xlsx', '2023-03-05', NULL, 20, 150.00, 3000.00),
    ('sales_mar.xlsx', '2023-03-06', 'Товар D', 8, 300.00, 2400.00);

-- Создаем таблицу с данными о клиентах
CREATE TABLE customer_data (
    "Файлы" TEXT,
    "ID" INTEGER,
    "Имя" TEXT,
    "Фамилия" TEXT,
    "Email" TEXT,
    "Телефон" TEXT,
    "Дата_регистрации" DATE
);

-- Вставляем данные о клиентах с пропусками
INSERT INTO customer_data VALUES
    ('customers_2023.xlsx', 1, 'Иван', 'Иванов', 'ivan@example.com', '+7 (123) 456-7890', '2023-01-10'),
    ('customers_2023.xlsx', 2, 'Петр', 'Петров', NULL, '+7 (234) 567-8901', '2023-01-15'),
    ('customers_2023.xlsx', 3, 'Анна', 'Сидорова', 'anna@example.com', NULL, '2023-02-01'),
    ('customers_2023.xlsx', 4, NULL, 'Козлов', 'kozlov@example.com', '+7 (345) 678-9012', '2023-02-10'),
    ('customers_2023.xlsx', 5, 'Елена', NULL, 'elena@example.com', '+7 (456) 789-0123', '2023-03-05'),
    ('customers_2023.xlsx', 6, 'Алексей', 'Смирнов', 'alex@example.com', '+7 (567) 890-1234', NULL);

-- Анализируем пропущенные данные
SELECT data_quality.save_missing_data_results('test_table');
SELECT data_quality.save_missing_data_results('test_table_multi');
SELECT data_quality.save_missing_data_results('sales_data');
SELECT data_quality.save_missing_data_results('customer_data');

