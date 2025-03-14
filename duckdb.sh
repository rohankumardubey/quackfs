#!/bin/bash

echo "Creating table"
duckdb -c "CREATE TABLE test (id INTEGER, data TEXT);" /tmp/fuse/db.duckdb

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
