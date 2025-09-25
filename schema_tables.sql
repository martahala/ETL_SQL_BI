-- schema_tables.sql
-- Run inside superstore_dw database

-- Clean up any old objects 
DROP SCHEMA IF EXISTS stage CASCADE;
DROP SCHEMA IF EXISTS core CASCADE;
DROP SCHEMA IF EXISTS mart CASCADE;

-- Create schemas
CREATE SCHEMA stage;
CREATE SCHEMA core;
CREATE SCHEMA mart;

-- STAGE
CREATE TABLE stage.orders (
  order_id TEXT,
  order_date TIMESTAMP,
  ship_date TIMESTAMP,
  ship_mode TEXT,
  customer_id TEXT,
  customer_name TEXT,
  segment TEXT,
  country TEXT,
  city TEXT,
  state TEXT,
  postal_code TEXT,
  region TEXT,
  product_id TEXT,
  category TEXT,
  sub_category TEXT,
  product_name TEXT,
  sales NUMERIC,
  quantity INTEGER,
  discount NUMERIC,
  profit NUMERIC
);

-- CORE
CREATE TABLE core.dim_product (
  product_sk BIGSERIAL PRIMARY KEY,
  product_id TEXT NOT NULL UNIQUE,
  product_name TEXT,
  category TEXT,
  sub_category TEXT,
  is_current BOOLEAN NOT NULL DEFAULT true,
  last_update TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

CREATE TABLE core.dim_customer (
  customer_sk BIGSERIAL PRIMARY KEY,
  customer_id TEXT NOT NULL,
  customer_name TEXT,
  segment TEXT,
  region TEXT,
  valid_from TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  valid_to TIMESTAMP WITHOUT TIME ZONE,
  is_current BOOLEAN NOT NULL DEFAULT true
);
-- index
CREATE INDEX idx_dim_customer_customerid_current ON core.dim_customer (customer_id) WHERE is_current = true;

CREATE TABLE core.dim_date (
  date_sk DATE PRIMARY KEY,
  year INTEGER,
  quarter INTEGER,
  month INTEGER,
  day INTEGER,
  weekday INTEGER
);

CREATE TABLE core.fact_sales (
  fact_sk BIGSERIAL PRIMARY KEY,
  order_id TEXT,
  order_date TIMESTAMP,
  date_sk DATE,
  customer_sk BIGINT REFERENCES core.dim_customer(customer_sk),
  product_sk BIGINT REFERENCES core.dim_product(product_sk),
  ship_date TIMESTAMP,
  ship_mode TEXT,
  sales NUMERIC,
  quantity INTEGER,
  discount NUMERIC,
  profit NUMERIC,
  UNIQUE(order_id, product_sk)
);

-- MART
CREATE TABLE mart.sales_mart (
  order_id TEXT,
  order_date TIMESTAMP,
  year INTEGER,
  month INTEGER,
  customer_id TEXT,
  customer_name TEXT,
  segment TEXT,
  region TEXT,
  product_id TEXT,
  product_name TEXT,
  category TEXT,
  sub_category TEXT,
  sales NUMERIC,
  quantity INTEGER,
  discount NUMERIC,
  profit NUMERIC
);
