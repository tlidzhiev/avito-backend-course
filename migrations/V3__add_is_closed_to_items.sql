ALTER TABLE ads
ADD COLUMN is_closed BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX idx_ads_is_closed ON ads(is_closed);
