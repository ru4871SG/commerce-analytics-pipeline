-- One-time setup of the clickstream lakehouse in Unity Catalog, for production.

-- Where the shared tables live
CREATE STORAGE CREDENTIAL IF NOT EXISTS lakehouse_role
  WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789012:role/databricks-uc-lakehouse');
CREATE EXTERNAL LOCATION IF NOT EXISTS lakehouse_prod
  URL 's3://bigdata-data-lake/lakehouse/prod'
  WITH (STORAGE CREDENTIAL lakehouse_role)
  COMMENT 'The clickstream tables both engines write (crm_lakehouse)';

-- The landing volume the storefronts' collector exports to
CREATE EXTERNAL VOLUME IF NOT EXISTS prod.bronze.landing
  LOCATION 's3://bigdata-data-lake/delivery_app/'
  COMMENT 'Exports from the shops: the clickstream under web_events/';

-- The job's identity: create the tables (the __setup__ run), read the landing files, write
--    bronze, silver, gold and the batch ledger. 
GRANT USE CATALOG ON CATALOG prod TO `sp-lakehouse-runtime`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA prod.bronze TO `sp-lakehouse-runtime`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA prod.silver TO `sp-lakehouse-runtime`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA prod.gold TO `sp-lakehouse-runtime`;
GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION lakehouse_prod TO `sp-lakehouse-runtime`;
GRANT READ VOLUME ON VOLUME prod.bronze.landing TO `sp-lakehouse-runtime`;
-- After the first deployment's __setup__ run has created the tables, the runtime principal owns
-- them; hand ownership to the platform group:
--   ALTER TABLE prod.bronze.web_events OWNER TO `platform-team`; (and the three others)
--   GRANT SELECT, MODIFY ON TABLE prod.bronze.web_events TO `sp-lakehouse-runtime`; (and the others)

-- 3The orchestrator: the four shared tables only, through credentials Unity Catalog vends per
--    table for an hour (EXTERNAL USE SCHEMA allows the vending; SELECT and MODIFY decide READ or
--    READ_WRITE), and the landing volume through the Files API. Running the job is a permission
--    on the job itself (CAN_MANAGE_RUN, set by the bundle).
GRANT USE CATALOG ON CATALOG prod TO `sp-crm-orchestrator`;
GRANT USE SCHEMA, EXTERNAL USE SCHEMA ON SCHEMA prod.bronze TO `sp-crm-orchestrator`;
GRANT USE SCHEMA, EXTERNAL USE SCHEMA ON SCHEMA prod.silver TO `sp-crm-orchestrator`;
GRANT USE SCHEMA, EXTERNAL USE SCHEMA ON SCHEMA prod.gold TO `sp-crm-orchestrator`;
GRANT SELECT, MODIFY ON TABLE prod.bronze.web_events TO `sp-crm-orchestrator`;
GRANT SELECT, MODIFY ON TABLE prod.bronze.web_event_batches TO `sp-crm-orchestrator`;
GRANT SELECT, MODIFY ON TABLE prod.silver.web_events TO `sp-crm-orchestrator`;
GRANT SELECT, MODIFY ON TABLE prod.gold.web_daily_funnel TO `sp-crm-orchestrator`;
GRANT READ VOLUME ON VOLUME prod.bronze.landing TO `sp-crm-orchestrator`;

-- For the readers: the funnel, not the events (which carry client ids and user agents)
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `analysts`;

-- Outside SQL, once, in the account console or with the account CLI:
--    * sp-lakehouse-deployer: a federation policy accepting GitHub's OIDC tokens only from this
--      repository's prod-plan and prod environments:
--        issuer   https://token.actions.githubusercontent.com
--        subject  repo:<owner>/retail_sales_repo:environment:prod      (and :environment:prod-plan)
--        audience the Databricks account id
--    * sp-crm-orchestrator: a federation policy for the Airflow workers' Kubernetes service
--      account (or an OAuth secret, rotated, if the workers cannot present an identity token)
--    * the workspace's GitHub environments: prod requires a reviewer; prod-plan only main
