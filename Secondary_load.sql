-- Secondary load script
-- 1) Load new data into staging
TRUNCATE stage.orders;  -- clear staging first
\copy stage.orders FROM '/secondary_load.csv' CSV HEADER;

-- 2) Run ETL (will handle new rows, skip duplicates, apply SCD rules)
SELECT core.run_etl();

-- 3) Verify changes
SELECT COUNT(*) FROM core.dim_product;
SELECT COUNT(*) FROM core.dim_customer;
SELECT COUNT(*) FROM core.fact_sales;
SELECT COUNT(*) FROM mart.sales_mart;