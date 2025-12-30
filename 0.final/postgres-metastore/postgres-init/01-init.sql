-- =====================================================
-- INIT DATABASES FOR HIVE METASTORE AND AIRFLOW (PG15)
-- =====================================================

-- Create hive role if not exists
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'hive') THEN
      CREATE ROLE hive LOGIN PASSWORD 'hive';
   END IF;
END
$$;


-- Create airflow database if not exists
      CREATE DATABASE airflow
        WITH ENCODING='UTF8'
        OWNER=hive
        CONNECTION LIMIT=-1;


-- Grant DB privileges
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
GRANT ALL PRIVILEGES ON DATABASE airflow TO hive;
