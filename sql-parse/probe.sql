SELECT 1
SELECT * FROM books
SELECT b.id, b.title AS name FROM catalog.books b WHERE b.id = $1
SELECT DISTINCT author_id FROM books ORDER BY author_id DESC NULLS LAST LIMIT 10 OFFSET 5
SELECT DISTINCT ON (author_id) author_id, title FROM books ORDER BY author_id, published DESC
SELECT count(*), sum(pages)::bigint, avg(pages) FILTER (WHERE pages > 100) FROM books GROUP BY author_id HAVING count(*) > 1
SELECT author_id, genre, count(*) FROM books GROUP BY GROUPING SETS ((author_id), (genre), ())
SELECT author_id, count(*) FROM books GROUP BY ROLLUP (author_id, genre)
SELECT author_id, count(*) FROM books GROUP BY CUBE (author_id, genre)
SELECT title, rank() OVER (PARTITION BY author_id ORDER BY pages DESC) FROM books
SELECT title, sum(pages) OVER w FROM books WINDOW w AS (ORDER BY published ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
SELECT a.name, b.title FROM authors a JOIN books b ON b.author_id = a.id LEFT JOIN loans l ON l.book_id = b.id AND l.returned IS NULL
SELECT * FROM books b RIGHT OUTER JOIN authors a USING (author_id) FULL JOIN members m ON true CROSS JOIN genres g
SELECT * FROM books NATURAL JOIN authors
SELECT * FROM authors a, LATERAL (SELECT title FROM books b WHERE b.author_id = a.id LIMIT 3) recent
WITH recent AS (SELECT * FROM loans WHERE taken > now() - interval '30 days') SELECT count(*) FROM recent
WITH RECURSIVE chain(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM chain WHERE n < 10) SELECT n FROM chain
WITH moved AS MATERIALIZED (DELETE FROM loans WHERE returned IS NOT NULL RETURNING *) INSERT INTO loan_history SELECT * FROM moved
SELECT id FROM books UNION SELECT id FROM archive INTERSECT SELECT id FROM shelved EXCEPT ALL SELECT id FROM lost
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(n, letter)
SELECT CASE WHEN pages > 500 THEN 'long' WHEN pages > 100 THEN 'medium' ELSE 'short' END FROM books
SELECT CASE genre WHEN 'poetry' THEN 1 ELSE 0 END FROM books
SELECT coalesce(subtitle, title), nullif(pages, 0), greatest(1, 2), least(3, 4) FROM books
SELECT title FROM books WHERE author_id IN (SELECT id FROM authors WHERE country = 'NZ')
SELECT title FROM books b WHERE EXISTS (SELECT 1 FROM loans l WHERE l.book_id = b.id)
SELECT title FROM books WHERE pages > ALL (SELECT pages FROM archive) OR pages = ANY ('{1,2,3}'::int[])
SELECT title FROM books WHERE title LIKE 'A%' AND title NOT ILIKE '%z' AND title SIMILAR TO '(a|b)%' ESCAPE '\'
SELECT title FROM books WHERE pages BETWEEN 10 AND 20 AND published NOT BETWEEN SYMMETRIC '2020-01-01' AND '2010-01-01'
SELECT title FROM books WHERE subtitle IS NULL AND isbn IS NOT NULL AND flag IS TRUE AND other IS DISTINCT FROM 1
SELECT 42, -7, 3.14, 1e10, 0x1F, 0o17, 0b101, 1_000_000, 'text', E'tab\there', B'1010', X'1F', U&'d\0061t', $$dollar$$, $tag$quoted$tag$, true, false, NULL
SELECT interval '1 day', date '2026-01-01', timestamp with time zone '2026-01-01 00:00Z', '1'::numeric(10, 2), CAST('2' AS int), int '3'
SELECT title COLLATE "C", now() AT TIME ZONE 'UTC', current_date, current_timestamp(3), localtime, current_user, session_user
SELECT ARRAY[1, 2, 3], ARRAY[[1, 2], [3, 4]], tags[1], tags[1:2], ARRAY(SELECT id FROM books), (row(1, 'a')).f1
SELECT doc -> 'author' ->> 'name', doc #> '{a,b}', doc @> '{"x": 1}', doc ? 'key', doc ?| array['a'], tags && array['x'] FROM books
SELECT json_build_object('id', id), jsonb_agg(title ORDER BY title), string_agg(title, ', ') WITHIN GROUP (ORDER BY title) FROM books
SELECT JSON_OBJECT('a': 1), JSON_ARRAY(1, 2), doc IS JSON OBJECT, JSON_VALUE(doc, '$.a' RETURNING int) FROM books
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY pages) FROM books
SELECT extract(year FROM published), date_trunc('month', published), substring(title FROM 1 FOR 3), trim(BOTH ' ' FROM title), position('a' IN title), overlay(title PLACING 'x' FROM 1 FOR 1) FROM books
SELECT 1 OPERATOR(pg_catalog.+) 2, 2 ^ 3, 7 % 3, |/ 16, @ -5, 5 & 3, 5 | 3, 5 # 3, ~5, 1 << 2, 8 >> 1, 'a' || 'b'
SELECT * FROM books TABLESAMPLE SYSTEM (10) REPEATABLE (1)
SELECT * FROM ONLY books FOR UPDATE OF books SKIP LOCKED
SELECT * FROM books FOR SHARE NOWAIT
SELECT * FROM books ORDER BY title FETCH FIRST 5 ROWS WITH TIES
SELECT * FROM generate_series(1, 10) WITH ORDINALITY AS g(n, i)
SELECT * FROM ROWS FROM (generate_series(1, 2), unnest(ARRAY['a', 'b'])) AS t(n, s)
SELECT xmlelement(name book, xmlattributes(id AS id), title) FROM books
SELECT * FROM books WHERE (author_id, genre) = (1, 'poetry') AND (a, b) OVERLAPS (c, d)
SELECT row_to_json(b.*) FROM books b
TABLE books
VALUES (1), (2)
INSERT INTO loans (book_id, member_id) VALUES ($1, $2) RETURNING id
INSERT INTO loans (book_id, member_id) SELECT id, 7 FROM books WHERE genre = 'poetry' ON CONFLICT (book_id) DO UPDATE SET member_id = EXCLUDED.member_id WHERE loans.returned IS NULL
INSERT INTO members DEFAULT VALUES ON CONFLICT DO NOTHING
UPDATE books SET pages = pages + 1, title = upper(title) FROM authors WHERE books.author_id = authors.id RETURNING books.*
UPDATE books SET (pages, title) = (SELECT 1, 'x') WHERE CURRENT OF reader
DELETE FROM loans USING members WHERE loans.member_id = members.id AND members.active = false
MERGE INTO stock s USING deliveries d ON s.book_id = d.book_id WHEN MATCHED THEN UPDATE SET count = s.count + d.count WHEN NOT MATCHED THEN INSERT (book_id, count) VALUES (d.book_id, d.count)
CREATE TABLE loans (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), book_id int NOT NULL REFERENCES books (id) ON DELETE CASCADE, taken timestamptz DEFAULT now(), memo text CHECK (length(memo) < 200), UNIQUE (book_id, taken))
CREATE INDEX CONCURRENTLY IF NOT EXISTS loans_taken ON loans USING btree (taken DESC) INCLUDE (memo) WHERE returned IS NULL
CREATE VIEW open_loans AS SELECT * FROM loans WHERE returned IS NULL
CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT a + b $$
CREATE FUNCTION twice(n int) RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT n * 2; END
ALTER TABLE loans ADD COLUMN due date, ALTER COLUMN memo SET NOT NULL, DROP CONSTRAINT IF EXISTS loans_memo_check
DROP TABLE IF EXISTS loans, holds CASCADE
GRANT SELECT, INSERT ON loans TO librarian
COMMENT ON TABLE loans IS 'who has which book'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM loans
COPY loans (id, memo) FROM STDIN WITH (FORMAT csv, HEADER true)
SET search_path TO catalog, public
SET LOCAL statement_timeout = '5s'
SHOW work_mem
BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY
SAVEPOINT sp_0
ROLLBACK TO SAVEPOINT sp_0
COMMIT
PREPARE find (int) AS SELECT * FROM books WHERE id = $1
EXECUTE find (1)
LISTEN loans_changed
NOTIFY loans_changed, 'payload'
VACUUM (VERBOSE, ANALYZE) loans
SELECT 1; SELECT 2
