-- A binding a deploy took from the service's declaration, beside those the
-- platform made (managed) and those an administrator set. The default keeps
-- the writes of an earlier binary valid while both run. A role carries one
-- service across every tenant, which the unique index holds against two
-- bindings written at once.

SET LOCAL lock_timeout = '10s';
ALTER TABLE service_db_roles ADD COLUMN IF NOT EXISTS declared BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE service_db_roles DROP CONSTRAINT IF EXISTS service_db_roles_origin;
ALTER TABLE service_db_roles ADD CONSTRAINT service_db_roles_origin CHECK (NOT (managed AND declared));
CREATE UNIQUE INDEX IF NOT EXISTS service_db_roles_role_name_key ON service_db_roles (role_name);
