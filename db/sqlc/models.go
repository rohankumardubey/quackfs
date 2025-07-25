// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.28.0

package sqlc

import (
	"database/sql"

	"github.com/vinimdocarmo/quackfs/db/types"
)

type Chunk struct {
	ID              int64        `json:"id"`
	SnapshotLayerID uint64       `json:"snapshotLayerId"`
	LayerRange      types.Range  `json:"layerRange"`
	FileRange       types.Range  `json:"fileRange"`
	CreatedAt       sql.NullTime `json:"createdAt"`
}

type File struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`
}

type Head struct {
	ID        int64        `json:"id"`
	FileID    uint64       `json:"fileId"`
	VersionID uint64       `json:"versionId"`
	CreatedAt sql.NullTime `json:"createdAt"`
}

type SnapshotLayer struct {
	ID        uint64        `json:"id"`
	FileID    uint64        `json:"fileId"`
	CreatedAt sql.NullTime  `json:"createdAt"`
	Active    sql.NullInt32 `json:"active"`
	VersionID sql.NullInt64 `json:"versionId"`
	ObjectKey string        `json:"objectKey"`
}

type Version struct {
	ID        uint64       `json:"id"`
	Tag       string       `json:"tag"`
	CreatedAt sql.NullTime `json:"createdAt"`
}
