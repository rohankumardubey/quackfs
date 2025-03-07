-- Create files table
CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Create layers table
CREATE TABLE IF NOT EXISTS layers (
    id SERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sealed INTEGER DEFAULT 0
);

-- Create entries table with proper index creation and range columns
CREATE TABLE IF NOT EXISTS entries (
    layer_id INTEGER REFERENCES layers(id),
    offset_value BIGINT NOT NULL,
    data BYTEA NOT NULL,
    layer_range INT8RANGE NOT NULL,
    file_range INT8RANGE NOT NULL,
    PRIMARY KEY (layer_id, offset_value)
); 