CREATE TABLE sellers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    is_verified BOOLEAN NOT NULL
);
CREATE TABLE ads (
    id SERIAL PRIMARY KEY,
    seller_id INTEGER NOT NULL REFERENCES sellers(id),
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    category INT NOT NULL,
    images_qty INT NOT NULL
);
