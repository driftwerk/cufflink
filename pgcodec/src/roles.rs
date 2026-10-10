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
