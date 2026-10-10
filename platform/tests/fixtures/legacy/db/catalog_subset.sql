SET ROLE catalog_service_user;

CREATE TYPE catalog_service.book_format AS ENUM ('Hardback', 'Paperback', 'Audio');

CREATE TABLE catalog_service.book (
    id uuid PRIMARY KEY,
    status VARCHAR(50) NOT NULL DEFAULT 'Available',
    format catalog_service.book_format NOT NULL DEFAULT 'Hardback',
    tags TEXT[] NOT NULL DEFAULT '{}',
    replacement_cost NUMERIC(12,2),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT book_cost_check CHECK (replacement_cost IS NULL OR replacement_cost >= 0),
    CONSTRAINT book_tags_check CHECK (tags <@ ARRAY['Fiction', 'Reference', 'Children', 'Local']::TEXT[])
);

CREATE INDEX idx_book_status ON catalog_service.book(status);
CREATE INDEX idx_book_created_at ON catalog_service.book(created_at);
CREATE INDEX idx_book_tags ON catalog_service.book USING GIN (tags);

CREATE OR REPLACE FUNCTION catalog_service.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER touch_book_updated_at BEFORE UPDATE ON catalog_service.book
    FOR EACH ROW EXECUTE FUNCTION catalog_service.touch_updated_at();

CREATE TABLE catalog_service.shelf_index (
    id uuid PRIMARY KEY,
    status TEXT NOT NULL,
    tags TEXT[] NOT NULL,
    replacement_cost NUMERIC(12,2)
);

CREATE OR REPLACE FUNCTION catalog_service.refresh_shelf_index(ids uuid[])
RETURNS void AS $$
BEGIN
    DELETE FROM catalog_service.shelf_index WHERE id = ANY(ids);
    INSERT INTO catalog_service.shelf_index (id, status, tags, replacement_cost)
    SELECT b.id, b.status, b.tags, b.replacement_cost
    FROM catalog_service.book b
    WHERE b.id = ANY(ids);
END;
$$ LANGUAGE plpgsql;

CREATE TABLE catalog_service.settings (
    id INTEGER DEFAULT 1 PRIMARY KEY CHECK (id = 1),
    loan_days INTEGER NOT NULL DEFAULT 21,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE catalog_service.shelf_section (
    key VARCHAR(50) PRIMARY KEY,
    label TEXT NOT NULL
);

INSERT INTO catalog_service.shelf_section (key, label) VALUES
    ('fiction', 'Fiction'),
    ('reference', 'Reference'),
    ('children', 'Children');

RESET ROLE;
