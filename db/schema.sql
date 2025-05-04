CREATE EXTENSION btree_gist;

-- Create files table
CREATE TABLE IF NOT EXISTS files (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Create versions table
CREATE TABLE IF NOT EXISTS versions (
    id BIGSERIAL PRIMARY KEY,
    tag TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create snapshot_layers table
CREATE TABLE IF NOT EXISTS snapshot_layers (
    id BIGSERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active INTEGER DEFAULT 0,
    version_id INTEGER DEFAULT NULL REFERENCES versions(id),
    object_key VARCHAR(255) NOT NULL,
    CHECK ((active = 1 AND version_id IS NULL) OR (active = 0 AND version_id IS NOT NULL)), -- version_id is NULL for the active snapshot layer
    UNIQUE (file_id, version_id)
);

-- Create chunks table with proper index creation and range columns
CREATE TABLE IF NOT EXISTS chunks (
    id BIGSERIAL PRIMARY KEY,
    snapshot_layer_id INTEGER REFERENCES snapshot_layers(id),
    layer_range INT8RANGE NOT NULL,
    file_range INT8RANGE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- for any given snapshot_layer_id, there should be no overlapping layer_ranges
    EXCLUDE USING GIST (snapshot_layer_id WITH =, layer_range WITH &&)
); 

CREATE INDEX IF NOT EXISTS idx_files_name ON files(name);
CREATE INDEX IF NOT EXISTS idx_versions_tag ON versions(tag);
CREATE INDEX IF NOT EXISTS idx_snapshot_layers_file_version ON snapshot_layers(file_id, version_id);
CREATE INDEX IF NOT EXISTS idx_chunks_layer_range ON chunks USING GIST(snapshot_layer_id, file_range);