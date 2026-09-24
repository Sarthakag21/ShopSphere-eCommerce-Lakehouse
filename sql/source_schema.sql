-- ShopSphere source schema for local/disposable training use.
-- Database: SHOPSPHERE_SOURCE
-- Do not use production credentials or production data with this script.

CREATE DATABASE IF NOT EXISTS SHOPSPHERE_SOURCE;
USE SHOPSPHERE_SOURCE;

CREATE TABLE IF NOT EXISTS customers (
    customer_id   BIGINT NOT NULL,
    customer_name VARCHAR(255),
    email         VARCHAR(255),
    phone         VARCHAR(50),
    city          VARCHAR(100),
    state         VARCHAR(100),
    country       VARCHAR(100),
    signup_date   DATE,
    updated_at    DATETIME(6)
);

CREATE TABLE IF NOT EXISTS products (
    product_id    VARCHAR(50),
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         DECIMAL(12,2),
    currency      VARCHAR(10),
    launch_date   DATE,
    updated_at    DATETIME(6)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      VARCHAR(50),
    customer_id   BIGINT,
    product_id    VARCHAR(50),
    quantity      INT,
    order_date    DATETIME(6),
    updated_at    DATETIME(6),
    order_status  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id    VARCHAR(50),
    order_id      VARCHAR(50),
    amount        DECIMAL(12,2),
    payment_mode  VARCHAR(50),
    payment_ts    DATETIME(6),
    payment_status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_event_id VARCHAR(50),
    order_id          VARCHAR(50),
    event_ts          DATETIME(6),
    status            VARCHAR(50),
    carrier           VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS etl_control (
    source_table      VARCHAR(100) NOT NULL,
    watermark_column  VARCHAR(100) NOT NULL,
    last_watermark    DATETIME(6),
    target_path       VARCHAR(500),
    is_active         BOOLEAN DEFAULT TRUE
);

INSERT INTO etl_control(source_table, watermark_column, target_path, is_active)
SELECT 'customers', 'updated_at', 'landing/customers/incremental/', TRUE
WHERE NOT EXISTS (SELECT 1 FROM etl_control WHERE source_table='customers');

INSERT INTO etl_control(source_table, watermark_column, target_path, is_active)
SELECT 'products', 'updated_at', 'landing/products/incremental/', TRUE
WHERE NOT EXISTS (SELECT 1 FROM etl_control WHERE source_table='products');

INSERT INTO etl_control(source_table, watermark_column, target_path, is_active)
SELECT 'orders', 'updated_at', 'landing/orders/incremental/', TRUE
WHERE NOT EXISTS (SELECT 1 FROM etl_control WHERE source_table='orders');

INSERT INTO etl_control(source_table, watermark_column, target_path, is_active)
SELECT 'payments', 'payment_ts', 'landing/payments/incremental/', TRUE
WHERE NOT EXISTS (SELECT 1 FROM etl_control WHERE source_table='payments');

-- Shipments is handled by the live ADF metadata in this project.
INSERT INTO etl_control(source_table, watermark_column, target_path, is_active)
SELECT 'shipments', 'event_ts', 'landing/shipments/incremental/', TRUE
WHERE NOT EXISTS (SELECT 1 FROM etl_control WHERE source_table='shipments');
