-- name: CalcFileSize :one
SELECT 
    UPPER(e.file_range)::BIGINT as file_size
FROM 
    chunks e
INNER JOIN 
    snapshot_layers l ON e.snapshot_layer_id = l.id
WHERE 
    l.file_id = $1
ORDER BY 
    UPPER(e.file_range) DESC
LIMIT 1;

-- name: InsertChunk :exec
INSERT INTO 
    chunks (snapshot_layer_id, layer_range, file_range) 
VALUES 
    ($1, $2, $3);

-- name: GetLayerChunks :many
SELECT 
    layer_range, 
    file_range
FROM 
    chunks
WHERE 
    snapshot_layer_id = $1
ORDER BY 
    id ASC;

-- name: GetOverlappingChunksWithVersion :many
SELECT 
    c.snapshot_layer_id, 
    c.layer_range, 
    c.file_range
FROM 
    chunks c
INNER JOIN 
    snapshot_layers l ON c.snapshot_layer_id = l.id
WHERE
    -- if versionedLayerID is 0, then we don't filter by layer ID
    (sqlc.arg('versionedLayerID') = 0 OR l.id <= sqlc.arg('versionedLayerID')) AND
    l.file_id = sqlc.arg('fileID') AND c.file_range && sqlc.arg('range')::INT8RANGE
ORDER BY 
    l.id ASC, c.id ASC; 