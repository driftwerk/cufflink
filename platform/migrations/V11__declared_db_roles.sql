-- A binding a deploy took from the service's declaration, beside those the
-- platform made (managed) and those an administrator set. The default keeps
-- the writes of an earlier binary valid while both run. A role carries one
-- service across every tenant, which the unique index holds against two
-- bindings written at once.

SET LOCAL lock_timeout = '10s';
ALTER TABLE service_db_roles ADD COLUMN IF NOT EXISTS declared BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE service_db_roles DROP CONSTRAINT IF EXISTS service_db_roles_origin;
ALTER TABLE service_db_roles ADD CONSTRAINT service_db_roles_origin CHECK (NOT (managed AND declared));
DO $$
DECLARE
    shared TEXT;
BEGIN
    SELECT string_agg(role_name, ', ' ORDER BY role_name) INTO shared
    FROM (SELECT role_name FROM service_db_roles GROUP BY role_name HAVING count(*) > 1) bound;
    IF shared IS NOT NULL THEN
        RAISE EXCEPTION 'roles bound to more than one service: %', shared
            USING HINT = 'a role carries one service: bind every service but one to a role of its own with cufflink services db-role set, then upgrade';
    END IF;
END
$$;
CREATE UNIQUE INDEX IF NOT EXISTS service_db_roles_role_name_key ON service_db_roles (role_name);
