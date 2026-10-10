-- The renewals of a book, newest first: what the renewal list reads.
CREATE INDEX idx_loans_renewals ON loan_service.loans(book_id, created_at DESC)
    WHERE loan_kind = 'Renewal';
