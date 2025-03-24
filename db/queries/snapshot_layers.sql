-- name: GetLayersByFileID :many
SELECT 
    snapshot_layers.id, 
    snapshot_layers.file_id, 
    snapshot_layers.version_id, 
    versions.tag, 
    snapshot_layers.object_key
FROM 
    snapshot_layers
LEFT JOIN 
    versions ON snapshot_layers.version_id = versions.id
WHERE 
    snapshot_layers.file_id = $1 
ORDER BY 
    snapshot_layers.id ASC;

-- name: InsertLayer :one
INSERT INTO 
    snapshot_layers (file_id, version_id, object_key) 
VALUES 
    ($1, $2, $3) 
RETURNING id;

-- name: GetObjectKey :one
SELECT 
    object_key
FROM 
    snapshot_layers
WHERE 
    id = $1;

-- name: GetLayerByVersion :one
SELECT 
    snapshot_layers.id, 
    snapshot_layers.file_id, 
    snapshot_layers.version_id, 
    versions.tag, 
    snapshot_layers.object_key
FROM 
    snapshot_layers
INNER JOIN 
    versions ON versions.id = snapshot_layers.version_id
WHERE 
    snapshot_layers.file_id = $1 AND versions.tag = $2; 