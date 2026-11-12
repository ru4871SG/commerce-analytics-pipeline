-- DC of the orders table
SELECT 'CREATE ROLE crm_cdc_reader LOGIN REPLICATION PASSWORD ''crm_cdc_reader'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'crm_cdc_reader')\gexec

-- One slot per database, named after it
SELECT pg_create_logical_replication_slot('cdc_' || current_database(), 'wal2json')
WHERE NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = 'cdc_' || current_database());
