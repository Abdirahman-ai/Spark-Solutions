-- ============================================================
-- Snowflake-Managed Iceberg Table
-- Internal Catalog with S3 Storage
-- ============================================================
USE WAREHOUSE demo_wh;
USE DATABASE iceberg_demo;
USE SCHEMA demo;

---------------------------------------------------------------
-- 1. Create Snowflake-Managed Iceberg Table
---------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE titanic_internal (
    passenger_id INTEGER,
    survived INTEGER,
    passenger_class INTEGER,
    passenger_name STRING,
    sex STRING,
    age FLOAT,
    fare FLOAT,
    embarked STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'MY_EXTERNAL_VOLUME'
BASE_LOCATION = 'snowflake_managed/titanic_internal';

---------------------------------------------------------------
-- 2. Insert Records
---------------------------------------------------------------

INSERT INTO titanic_internal (
    passenger_id,
    survived,
    passenger_class,
    passenger_name,
    sex,
    age,
    fare,
    embarked
)
VALUES
    (1001, 1, 1, 'John Smith', 'male', 35, 80.50, 'S'),
    (1002, 0, 3, 'Mary Johnson', 'female', 24, 12.75, 'C'),
    (1003, 1, 2, 'David Brown', 'male', 42, 32.00, 'Q');


---------------------------------------------------------------
-- 3. Query Records
---------------------------------------------------------------

SELECT *
FROM titanic_internal
ORDER BY passenger_id;

---------------------------------------------------------------
-- 4. Update a Record
---------------------------------------------------------------

UPDATE titanic_internal
SET fare = 90.00
WHERE passenger_id = 1001;

---------------------------------------------------------------
-- 5. Delete a Record
---------------------------------------------------------------

DELETE FROM titanic_internal
WHERE passenger_id = 1002;

---------------------------------------------------------------
-- 6. Insert Another Record
---------------------------------------------------------------

INSERT INTO titanic_internal
VALUES
    (1004, 1, 1, 'Sarah Davis', 'female', 29, 105.50, 'S');

---------------------------------------------------------------
-- 7. Verify Final Records
---------------------------------------------------------------

SELECT *
FROM titanic_internal
ORDER BY passenger_id;

---------------------------------------------------------------
-- 8. Inspect Iceberg Metadata
---------------------------------------------------------------

SHOW ICEBERG TABLES LIKE 'TITANIC_INTERNAL';

DESCRIBE ICEBERG TABLE titanic_internal;

SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(
    'ICEBERG_DEMO.DEMO.TITANIC_INTERNAL'
);