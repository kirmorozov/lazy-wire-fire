-- The pgsrc service of docker-compose.test.yml: the SOURCE PostgreSQL of the pgoutput tests
-- (tests/e2e.rs, `pg_*`). Run once on first start against POSTGRES_DB=lwfsrc.
--
-- The role the tests connect as: REPLICATION for the slot functions and, once the engine can
-- read a PG source, the walsender itself; CREATE on the database (CREATE PUBLICATION) and on
-- the public schema for the test tables it creates and publishes (it owns them and their
-- publications).
CREATE ROLE lwfsrc LOGIN REPLICATION PASSWORD 'lwfsrcpw';
GRANT CREATE ON DATABASE lwfsrc TO lwfsrc;
GRANT CREATE ON SCHEMA public TO lwfsrc;
