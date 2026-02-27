CREATE TABLE moderation_results (
    id SERIAL PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES ads(id),
    status VARCHAR(20) NOT NULL,
    is_violation BOOLEAN,
    probability FLOAT,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP
);

CREATE INDEX idx_moderation_results_item_id ON moderation_results(item_id);
CREATE INDEX idx_moderation_results_status ON moderation_results(status);
