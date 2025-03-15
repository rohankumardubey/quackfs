#!/bin/bash


# wget if files doesn't exist
if [ ! -f /tmp/spotify.csv ]; then
    unzip data/spotify.zip -d /tmp/
fi

echo "Create table for spotify data"
duckdb -c "CREATE TABLE IF NOT EXISTS spotify_data AS SELECT * FROM read_csv_auto('/tmp/spotify.csv');" /tmp/fuse/db.duckdb

echo "Unique spotify_id"
duckdb -c "SELECT COUNT(DISTINCT spotify_id) FROM spotify_data;" /tmp/fuse/db.duckdb

echo "Most popular artist"
duckdb -c "SELECT artists, COUNT(*) as mention_count FROM spotify_data GROUP BY artists ORDER BY mention_count DESC LIMIT 1;" /tmp/fuse/db.duckdb

echo "Creating table"
duckdb -c "CREATE TABLE IF NOT EXISTS test (id INTEGER, data TEXT);" /tmp/fuse/db.duckdb


echo "Inserting data"
duckdb -c "INSERT INTO test (id, data) VALUES (1, 'data1'), (2, 'data2');" /tmp/fuse/db.duckdb

echo "Selecting data"
duckdb -c "SELECT * FROM test;" /tmp/fuse/db.duckdb

echo "Checking point"
duckdb -c "CHECKPOINT;" /tmp/fuse/db.duckdb

echo "More inserts"
duckdb -c "INSERT INTO test (id, data) VALUES (3, 'data3'), (4, 'data4');" /tmp/fuse/db.duckdb

echo "Selecting data"
duckdb -c "SELECT * FROM test;" /tmp/fuse/db.duckdb

echo "Adding only one row"
duckdb -c "INSERT INTO test (id, data) VALUES (5, 'data5');" /tmp/fuse/db.duckdb

echo "Selecting data"
duckdb -c "SELECT * FROM test;" /tmp/fuse/db.duckdb
