ALTER TABLE advertisements
ADD COLUMN is_closed BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX idx_advertisements_is_closed ON advertisements(is_closed);
