-- name: GetFileIDByName :one
SELECT id FROM files WHERE name = $1;

-- name: InsertFile :one
INSERT INTO files (name) VALUES ($1) RETURNING id;

-- name: GetAllFiles :many
SELECT id, name FROM files; 