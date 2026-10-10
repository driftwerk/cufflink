CREATE TABLE IF NOT EXISTS public.schema_history_deferred(
    version INT4 PRIMARY KEY,
    name VARCHAR(255),
    applied_on VARCHAR(255),
    checksum VARCHAR(255));

INSERT INTO public.schema_history_deferred (version, name, applied_on, checksum) VALUES
    (1, 'reporting_schema_and_views', '2026-01-02T09:01:11.482913Z', '4107228856013377190'),
    (2, 'book_overview_counts_loans_once', '2026-01-03T09:02:11.482913Z', '9930154471820066253'),
    (3, 'shelf_search_views', '2026-01-04T09:03:11.482913Z', '1288405539917246604'),
    (4, 'withdrawn_books_in_overview', '2026-01-05T09:04:11.482913Z', '15520391847730112985'),
    (5, 'returned_at_in_book_overview', '2026-01-06T09:05:11.482913Z', '7745120936655018342'),
    (6, 'kiosk_scoped_views', '2026-02-07T09:06:11.482913Z', '3361909284457120078'),
    (7, 'loan_activity_views', '2026-02-08T09:07:11.482913Z', '11872034495610273356'),
    (8, 'branch_panel_views', '2026-02-09T09:08:11.482913Z', '6054418829103347721');
