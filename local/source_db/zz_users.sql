-- Users for the local demo. The Docker entrypoint runs every *.sql file in this directory once,
-- in name order; zz_ sorts after fake_crm.sql so the databases exist first. A psql script, like
-- fake_crm.sql. Grants are per table, so it runs again whenever fake_crm.sql recreates them.

-- Read-only: the pipeline only ever reads the CRM.
SELECT 'CREATE ROLE airflow_reader LOGIN PASSWORD ''airflow_reader'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_reader')\gexec

-- Demo only: the crm_daily_activity DAG plays the shop and writes a day of orders. A real
-- deployment has no such user; Airflow never writes to the CRM.
SELECT 'CREATE ROLE activity_simulator LOGIN PASSWORD ''activity_simulator'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'activity_simulator')\gexec

\connect crm_broomx
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airflow_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO activity_simulator;

\connect crm_wecare
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airflow_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO activity_simulator;

\connect crm_greenz
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airflow_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO activity_simulator;
