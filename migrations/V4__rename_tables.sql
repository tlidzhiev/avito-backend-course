ALTER TABLE sellers RENAME TO users;
ALTER TABLE users DROP COLUMN name;
ALTER TABLE users RENAME COLUMN is_verified TO is_verified_seller;

ALTER TABLE ads RENAME TO advertisements;
