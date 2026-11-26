//! What the platform hands a service's database role, read the same by
//! its provisioning and by the native test host.

/// What of the schema `$1` the session's user owns, as `(kind, name)` in
/// the order it is handed to a service's role: tables before what depends
/// on them. A sequence a column owns is left out: it goes with its table.
pub const OWNED_BY_THE_USER: &str = "\
    SELECT kind, name FROM ( \
        SELECT CASE c.relkind WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW' \
                              WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'f' THEN 'FOREIGN TABLE' \
                              ELSE 'TABLE' END AS kind, \
               quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name, \
               CASE WHEN c.relkind IN ('r', 'p', 'f') THEN 1 ELSE 2 END AS turn \
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
        WHERE n.nspname = $1 AND c.relowner = current_user::regrole \
          AND c.relkind IN ('r', 'p', 'S', 'v', 'm', 'f') \
          AND NOT (c.relkind = 'S' AND EXISTS ( \
              SELECT 1 FROM pg_depend d \
              WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid \
                AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i'))) \
        UNION ALL \
        SELECT CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, \
               quote_ident(n.nspname) || '.' || quote_ident(p.proname) \
                   || '(' || pg_get_function_identity_arguments(p.oid) || ')', \
               4 \
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
        WHERE n.nspname = $1 AND p.proowner = current_user::regrole AND p.prokind IN ('f', 'p') \
        UNION ALL \
        SELECT CASE t.typtype WHEN 'd' THEN 'DOMAIN' ELSE 'TYPE' END, \
               quote_ident(n.nspname) || '.' || quote_ident(t.typname), \
               3 \
        FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace \
        WHERE n.nspname = $1 AND t.typowner = current_user::regrole AND t.typtype IN ('e', 'd', 'r') \
    ) o ORDER BY turn, kind, name";

/// What the role `$1` reaches by an object it owns, a privilege granted to
/// it or a policy naming it, beyond schema `$2`, as `(here, place, kind,
/// identity, received)`: `here` is whether the object is in the session's
/// database, where `kind` and `identity` name it; `place` is the database it
/// is in, or for a shared object the object itself; `received` is whether
/// the role only holds a privilege on it, which the object's owner granted.
/// Read across every database from `pg_shdepend`. Left out: the schema `$2`
/// and what it holds, default privileges the role sets for every schema,
/// `USAGE` alone on schema `public` and on schema `$5`, `EXECUTE` alone on
/// the functions `$3` and `$4`, and `CONNECT` or `TEMPORARY` on the
/// session's database, each without grant option.
pub const REACH_OF_THE_ROLE: &str = "\
    WITH target AS (SELECT oid FROM pg_roles WHERE rolname = $1), \
    here AS (SELECT oid FROM pg_database WHERE datname = current_database()), \
    schema AS (SELECT oid FROM pg_namespace WHERE nspname = $2), \
    dependent AS ( \
        SELECT d.dbid, d.classid, d.objid, d.objsubid, d.deptype \
        FROM pg_shdepend d JOIN target t ON d.refobjid = t.oid \
        WHERE d.refclassid = 'pg_authid'::regclass), \
    local AS ( \
        SELECT d.classid, d.objid, d.deptype, o.type, o.identity, \
               CASE d.classid \
                   WHEN 'pg_policy'::regclass THEN ( \
                       SELECT c.relnamespace FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid \
                       WHERE p.oid = d.objid) \
                   WHEN 'pg_default_acl'::regclass THEN ( \
                       SELECT NULLIF(a.defaclnamespace, 0) FROM pg_default_acl a WHERE a.oid = d.objid) \
                   ELSE (SELECT n.oid FROM pg_namespace n WHERE n.nspname = o.schema) \
               END AS namespace \
        FROM dependent d, LATERAL pg_identify_object(d.classid, d.objid, d.objsubid) o \
        WHERE d.dbid = (SELECT oid FROM here)), \
    held_alone AS ( \
        SELECT l.classid, l.objid FROM local l \
        WHERE l.deptype = 'a' AND ( \
            (l.classid = 'pg_namespace'::regclass AND EXISTS ( \
                SELECT 1 FROM pg_namespace n WHERE n.oid = l.objid AND n.nspname IN ('public', $5) \
                  AND NOT EXISTS (SELECT 1 FROM aclexplode(n.nspacl) a, target t \
                                  WHERE a.grantee = t.oid \
                                    AND (a.privilege_type <> 'USAGE' OR a.is_grantable)))) \
            OR (l.classid = 'pg_proc'::regclass \
                AND l.objid IN (to_regprocedure($3), to_regprocedure($4)) \
                AND NOT EXISTS (SELECT 1 FROM pg_proc p, aclexplode(p.proacl) a, target t \
                                WHERE p.oid = l.objid AND a.grantee = t.oid \
                                  AND (a.privilege_type <> 'EXECUTE' OR a.is_grantable))))) \
    SELECT true AS here, current_database()::text AS place, l.type, l.identity, \
           l.deptype = 'a' AS received FROM local l \
    WHERE NOT COALESCE(l.namespace = (SELECT oid FROM schema), false) \
      AND NOT COALESCE(l.classid = 'pg_namespace'::regclass AND l.objid = (SELECT oid FROM schema), false) \
      AND NOT (l.classid = 'pg_default_acl'::regclass AND l.deptype = 'o' AND l.namespace IS NULL) \
      AND (l.classid, l.objid) NOT IN (SELECT classid, objid FROM held_alone) \
    UNION ALL \
    SELECT false, o.identity, o.type, o.identity, d.deptype = 'a' \
    FROM dependent d, LATERAL pg_identify_object(d.classid, d.objid, 0) o \
    WHERE d.dbid = 0 AND NOT ( \
        d.classid = 'pg_database'::regclass AND d.objid = (SELECT oid FROM here) AND d.deptype = 'a' \
        AND NOT EXISTS (SELECT 1 FROM pg_database db, aclexplode(db.datacl) a, target t \
                        WHERE db.oid = d.objid AND a.grantee = t.oid \
                          AND (a.privilege_type NOT IN ('CONNECT', 'TEMPORARY') OR a.is_grantable))) \
    UNION ALL \
    SELECT false, db.datname::text, NULL, NULL, d.deptype = 'a' \
    FROM dependent d JOIN pg_database db ON db.oid = d.dbid \
    WHERE d.dbid NOT IN (0, (SELECT oid FROM here)) \
    ORDER BY 1 DESC, 2, 3, 4";
