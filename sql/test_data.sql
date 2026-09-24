-- End-to-end incremental smoke test examples.
-- Run only against disposable/training source data.

UPDATE customers
SET city = 'Gurugram', updated_at = NOW(6)
WHERE customer_id = 100004;

UPDATE products
SET price = 2899.99, updated_at = NOW(6)
WHERE product_id = 'P00001';

UPDATE orders
SET order_status = 'SHIPPED', updated_at = NOW(6)
WHERE order_id = 'O00000001';

-- Payments are event-like: insert a new payment event with a unique ID.
-- Make sure the ID is unused before running.
INSERT INTO payments
(payment_id, order_id, amount, payment_mode, payment_ts, payment_status)
VALUES
('PAY99999901', 'O00000001', 999.99, 'UPI', NOW(6), 'SUCCESS');

-- Shipments are event-like: insert a new shipment event with a unique ID.
INSERT INTO shipments
(shipment_event_id, order_id, event_ts, status, carrier)
VALUES
('SE99999901', 'O00000001', NOW(6), 'OUT_FOR_DELIVERY', 'Delhivery');
