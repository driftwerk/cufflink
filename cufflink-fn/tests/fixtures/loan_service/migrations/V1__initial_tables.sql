CREATE TABLE loan_service.loans (
    id UUID PRIMARY KEY,
    book_id UUID NOT NULL,
    memo TEXT NOT NULL,
    created_by UUID NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_loans_book_id ON loan_service.loans(book_id);
CREATE INDEX idx_loans_created_at ON loan_service.loans(created_at);
