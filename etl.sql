-- etl.sql
SET search_path = core,public;

-- 1) Upsert products using ON CONFLICT (SCD1)
CREATE OR REPLACE FUNCTION core.upsert_products()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO core.dim_product (product_id, product_name, category, sub_category, is_current, last_update)
SELECT product_id,
       MAX(product_name) AS product_name,
       MAX(category) AS category,
       MAX(sub_category) AS sub_category,
       true,
       now()
FROM stage.orders
WHERE product_id IS NOT NULL
GROUP BY product_id
ON CONFLICT (product_id)
DO UPDATE
SET product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    sub_category = EXCLUDED.sub_category,
    last_update = now(),
    is_current = true;
END;
$$;


-- 2) Populate dim_date
CREATE OR REPLACE FUNCTION core.populate_dim_date()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO core.dim_date(date_sk, year, quarter, month, day, weekday)
  SELECT DISTINCT (order_date::date) AS dt,
         EXTRACT(YEAR FROM order_date)::int,
         EXTRACT(QUARTER FROM order_date)::int,
         EXTRACT(MONTH FROM order_date)::int,
         EXTRACT(DAY FROM order_date)::int,
         EXTRACT(DOW FROM order_date)::int
  FROM stage.orders
  WHERE order_date IS NOT NULL
  ON CONFLICT (date_sk) DO NOTHING;
END;
$$;


-- 3) Customers SCD handling (SCD1 for name/segment; SCD2 for region)
-- It checks current row, compares attributes and only inserts new version if region actually changed
CREATE OR REPLACE FUNCTION core.handle_customers_scd()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  rec RECORD;
  cur_rec core.dim_customer%ROWTYPE;
BEGIN
  FOR rec IN SELECT DISTINCT customer_id, customer_name, segment, region FROM stage.orders WHERE customer_id IS NOT NULL
  LOOP
    -- if no current row, insert
    SELECT * INTO cur_rec FROM core.dim_customer WHERE customer_id = rec.customer_id AND is_current = true LIMIT 1;

    IF NOT FOUND THEN
      INSERT INTO core.dim_customer (customer_id, customer_name, segment, region, valid_from, valid_to, is_current)
      VALUES (rec.customer_id, rec.customer_name, rec.segment, rec.region, now(), NULL, true);
    ELSE
      -- if region changed -> SCD2: expire old and insert new version
      IF cur_rec.region IS DISTINCT FROM rec.region THEN
        -- expire existing only if it's still current
        UPDATE core.dim_customer
        SET valid_to = now(), is_current = false
        WHERE customer_sk = cur_rec.customer_sk AND is_current = true;

        INSERT INTO core.dim_customer (customer_id, customer_name, segment, region, valid_from, valid_to, is_current)
        VALUES (rec.customer_id, rec.customer_name, rec.segment, rec.region, now(), NULL, true);
      ELSE
        -- region same -> SCD1 update of name/segment if changed
        UPDATE core.dim_customer
        SET customer_name = rec.customer_name,
            segment = rec.segment
        WHERE customer_sk = cur_rec.customer_sk AND is_current = true;
      END IF;
    END IF;
  END LOOP;
END;
$$;


-- 4) Deduplicate stage and load facts
CREATE OR REPLACE FUNCTION core.load_fact_sales()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  WITH staged AS (
    SELECT DISTINCT ON (order_id, product_id)
      order_id, order_date, ship_date, ship_mode, customer_id, product_id, sales, quantity, discount, profit
    FROM stage.orders
    ORDER BY order_id, product_id, order_date DESC NULLS LAST
  ),
  enrich AS (
    SELECT s.*,
           (SELECT customer_sk FROM core.dim_customer c WHERE c.customer_id = s.customer_id AND c.is_current = true LIMIT 1) as customer_sk,
           (SELECT product_sk FROM core.dim_product p WHERE p.product_id = s.product_id LIMIT 1) as product_sk
    FROM staged s
  )
  INSERT INTO core.fact_sales (order_id, order_date, date_sk, customer_sk, product_sk, ship_date, ship_mode, sales, quantity, discount, profit)
  SELECT e.order_id, e.order_date, e.order_date::date, e.customer_sk, e.product_sk, e.ship_date, e.ship_mode, e.sales, e.quantity, e.discount, e.profit
  FROM enrich e
  LEFT JOIN core.fact_sales f ON f.order_id = e.order_id AND f.product_sk = e.product_sk
  WHERE e.customer_sk IS NOT NULL AND e.product_sk IS NOT NULL
    AND f.fact_sk IS NULL; -- skip if identical fact already exists
END;
$$;


-- 5) Refresh mart 
CREATE OR REPLACE FUNCTION mart.refresh_sales_mart()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  TRUNCATE TABLE mart.sales_mart;
  INSERT INTO mart.sales_mart (
    order_id, order_date, year, month, customer_id, customer_name, segment, region,
    product_id, product_name, category, sub_category, sales, quantity, discount, profit
  )
  SELECT
    f.order_id,
    f.order_date,
    EXTRACT(YEAR FROM f.order_date)::int,
    EXTRACT(MONTH FROM f.order_date)::int,
    c.customer_id,
    c.customer_name,
    c.segment,
    c.region,
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    f.sales,
    f.quantity,
    f.discount,
    f.profit
  FROM core.fact_sales f
  JOIN core.dim_customer c ON c.customer_sk = f.customer_sk
  JOIN core.dim_product p ON p.product_sk = f.product_sk
  WHERE c.is_current = true;
END;
$$;


-- 6) Master ETL 
CREATE OR REPLACE FUNCTION core.run_etl()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM core.upsert_products();
  PERFORM core.populate_dim_date();
  PERFORM core.handle_customers_scd();
  PERFORM core.load_fact_sales();
  PERFORM mart.refresh_sales_mart();
END;
$$;
