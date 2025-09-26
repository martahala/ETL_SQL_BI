-- Initial load script
-- 1) create schemas + tables 
\i /schema_tables.sql

-- 2) load staging initial CSV 
TRUNCATE stage.orders;
\copy stage.orders FROM '/initial_load.csv' CSV HEADER;

-- 3) load ETL functions Terminal
\i /etl.sql

-- 4) run ETL
SELECT core.run_etl();

-- 5) verify
SELECT COUNT(*) FROM core.dim_product;
SELECT COUNT(*) FROM core.dim_customer;
SELECT COUNT(*) FROM core.fact_sales;
SELECT COUNT(*) FROM mart.sales_mart;

