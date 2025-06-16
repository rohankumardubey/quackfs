-- name: SetHead :exec
INSERT INTO heads (file_id, version_id)
VALUES ($1, $2)
ON CONFLICT (file_id)
DO UPDATE SET version_id = $2, created_at = CURRENT_TIMESTAMP;

-- name: GetHeadVersion :one
SELECT v.id as version_id, v.tag as version_tag
FROM heads h
JOIN versions v ON h.version_id = v.id
WHERE h.file_id = $1;

-- name: DeleteHead :exec
DELETE FROM heads
WHERE file_id = $1;

-- name: GetAllHeads :many
SELECT h.file_id, f.name as file_name, v.id as version_id, v.tag as version_tag
FROM heads h
JOIN files f ON h.file_id = f.id
JOIN versions v ON h.version_id = v.id; 