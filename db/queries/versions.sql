-- name: InsertVersion :one
INSERT INTO versions (tag) VALUES ($1) RETURNING id; 