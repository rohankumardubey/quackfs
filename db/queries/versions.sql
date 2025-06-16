-- name: InsertVersion :one
INSERT INTO versions (tag) VALUES ($1) RETURNING id;

-- name: GetVersionIDByTag :one
SELECT id FROM versions WHERE tag = $1;

-- name: GetFileVersions :many
SELECT
    v.id,
    v.tag,
    v.created_at
FROM
    versions v
JOIN
    snapshot_layers sl ON v.id = sl.version_id
WHERE
    sl.file_id = $1
ORDER BY
    v.created_at DESC; 