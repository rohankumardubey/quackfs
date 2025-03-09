-- Create files table
CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Create versions table
CREATE TABLE IF NOT EXISTS versions (
    id SERIAL PRIMARY KEY,
    tag TEXT NOT NULL
);

-- Create layers table
CREATE TABLE IF NOT EXISTS layers (
    id SERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sealed INTEGER DEFAULT 0,
    version_id INTEGER DEFAULT NULL REFERENCES versions(id),
    CHECK ((sealed = 0 AND version_id IS NULL) OR (sealed = 1 AND version_id IS NOT NULL)),
    UNIQUE (file_id, version_id)
);

-- Create entries table with proper index creation and range columns
CREATE TABLE IF NOT EXISTS entries (
    id SERIAL PRIMARY KEY,
    layer_id INTEGER REFERENCES layers(id),
    offset_value BIGINT NOT NULL,
    data BYTEA NOT NULL,
    layer_range INT8RANGE NOT NULL,
    file_range INT8RANGE NOT NULL
); 