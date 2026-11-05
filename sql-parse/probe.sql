SELECT 1
SELECT 1 AS i, 1.5 AS f, true AS b, 'text' AS s, B'101' AS bits, X'1F' AS hex, NULL AS n
SELECT lo.order_id, lo.store_id FROM reporting.order_overview lo WHERE lo.order_status = 'New'
SELECT count(*), sum(lo.total) FILTER (WHERE lo.is_priority) FROM reporting.order_overview lo GROUP BY lo.store_id HAVING count(*) > 1 ORDER BY 1 DESC NULLS LAST LIMIT 10 OFFSET 5
WITH recent AS (SELECT order_id FROM reporting.order_overview WHERE created_at > now() - interval '30 days') SELECT count(*) FROM recent
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10) SELECT i FROM n
SELECT current_date, current_time, current_time(2), current_timestamp, current_timestamp(3), localtime, localtime(1), localtimestamp, localtimestamp(4)
SELECT current_user, session_user, current_role, user, current_catalog, current_schema
SELECT (lo).order_id, (array_agg(lo.order_id))[1] FROM reporting.order_overview lo GROUP BY lo
SELECT 'pg_class'::regclass, CAST('1' AS integer), '{1,2}'::int[], '1'::_regtype
SELECT pg_catalog.lower('A'), pg_sleep(1), public.f(1), reporting.request_scope()
SELECT * FROM loan_service.loans l JOIN catalog_service.items q ON q.loan_id = l.id
SELECT * FROM other.reporting.order_overview
WITH d AS (DELETE FROM reporting.order_overview RETURNING 1) SELECT * FROM d
WITH u AS (UPDATE reporting.order_overview SET order_status = 'x' RETURNING 1) SELECT * FROM u
WITH i AS (INSERT INTO reporting.order_overview (order_id) VALUES (gen_random_uuid()) RETURNING 1) SELECT * FROM i
MERGE INTO reporting.order_overview t USING reporting.order_lines s ON t.order_id = s.order_id WHEN MATCHED THEN DELETE
SELECT 1 INTO TEMP t
SELECT 1; SELECT 2
SELECT a.x, b.y FROM (VALUES (1), (2)) a(x) CROSS JOIN LATERAL (SELECT a.x * 2 AS y) b
SELECT x FROM generate_series(1, 3) x UNION SELECT 4 INTERSECT SELECT 4 EXCEPT SELECT 5
SELECT row_number() OVER w, sum(total) OVER (PARTITION BY store_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM reporting.order_overview WINDOW w AS (ORDER BY created_at)
SELECT store_id, order_status, count(*) FROM reporting.order_overview GROUP BY GROUPING SETS ((store_id), (order_status), ())
SELECT CASE WHEN is_priority THEN 'hot' WHEN NOT is_priority THEN 'cold' ELSE 'unknown' END, coalesce(order_status, ''), nullif(order_status, ''), greatest(1, 2), least(1, 2) FROM reporting.order_overview
SELECT ARRAY[1, 2] @> ARRAY[1], '{"a": 1}'::jsonb -> 'a', '{"a": 1}'::jsonb ->> 'a', '{"a": {"b": 1}}'::jsonb #> '{a,b}', 1 = ANY (ARRAY[1, 2]), 1 IN (1, 2), 1 BETWEEN 0 AND 2, 'a' LIKE 'a%', 'a' ILIKE 'A%', 'a' ~ 'a', NOT EXISTS (SELECT 1)
SELECT order_id FROM reporting.order_overview WHERE store_id IN (SELECT store_id FROM reporting.store_panel) AND created_at IS NOT NULL AND is_priority IS TRUE
SELECT extract(year FROM now()), date_trunc('month', now()) AT TIME ZONE 'Europe/London', position('a' IN 'abc'), substring('abc' FROM 1 FOR 2), trim(both ' ' FROM ' a '), overlay('abc' PLACING 'x' FROM 2 FOR 1)
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY total), string_agg(DISTINCT order_status, ',' ORDER BY order_status), array_agg(order_id ORDER BY created_at) FROM reporting.order_overview
SELECT U&"set\005fconfig"('app.scope', 'global', true)
SELECT $1
SELECT ROW(1, 'a'), (1, 2) = (1, 2), 1::numeric(10, 2), interval '1 day' * 2, -1, 2 ^ 3, 'a' || 'b', 5 % 2
SELECT 1 FROM reporting.order_overview FOR UPDATE
TABLE reporting.order_overview
SELECT DISTINCT ON (store_id) store_id, order_id FROM reporting.order_overview ORDER BY store_id, created_at DESC
DROP TYPE regclass
