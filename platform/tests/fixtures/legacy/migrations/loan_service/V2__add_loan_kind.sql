ALTER TABLE loan_service.loans
ADD COLUMN loan_kind VARCHAR(50) NOT NULL DEFAULT 'Standard';

CREATE INDEX idx_loans_loan_kind ON loan_service.loans(loan_kind);

-- Loans made before the column existed say in their memo what they were.
UPDATE loan_service.loans
SET loan_kind = 'Renewal'
WHERE memo LIKE 'Renewed until %';
