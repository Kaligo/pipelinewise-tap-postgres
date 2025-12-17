-- Create test_user (non-superuser for regular operations)
CREATE USER test_user WITH PASSWORD 'my-secret-passwd';
GRANT CONNECT ON DATABASE tap_postgres_test TO test_user;
GRANT USAGE ON SCHEMA public TO test_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO test_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO test_user;

-- Create replication user for logical replication
CREATE USER repl_user WITH REPLICATION PASSWORD 'repl_password';
GRANT CONNECT ON DATABASE tap_postgres_test TO repl_user;
GRANT USAGE ON SCHEMA public TO repl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl_user;
