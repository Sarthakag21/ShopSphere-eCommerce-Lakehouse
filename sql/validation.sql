USE SHOPSPHERE_SOURCE;

SELECT 'customers' AS source, COUNT(*) AS changed_rows
FROM customers
WHERE customer_id = 100004 AND city = 'Gurugram';

SELECT 'products' AS source, COUNT(*) AS changed_rows
FROM products
WHERE product_id = 'P00001' AND price = 2899.99;

SELECT 'orders' AS source, COUNT(*) AS changed_rows
FROM orders
WHERE order_id = 'O00000001' AND order_status = 'SHIPPED';

SELECT 'payments' AS source, COUNT(*) AS changed_rows
FROM payments
WHERE payment_id = 'PAY99999901';

SELECT 'shipments' AS source, COUNT(*) AS changed_rows
FROM shipments
WHERE shipment_event_id = 'SE99999901';
