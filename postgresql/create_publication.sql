-- WAL logical replication for Debezium CDC
-- Creates replication slot and publication for billing tables.

SELECT pg_create_logical_replication_slot('debezium_billing_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_billing_slot'
);

CREATE PUBLICATION billing_pub FOR TABLE billing_plans, subscribers;
