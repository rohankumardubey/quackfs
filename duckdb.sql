CREATE TABLE IF NOT EXISTS test (id INTEGER, data TEXT);

-- Initial data insertion
INSERT INTO test (id, data) VALUES (1, 'data1'), (2, 'data2');
SELECT * FROM test;

-- First checkpoint
CHECKPOINT;

-- More inserts
INSERT INTO test (id, data) VALUES (3, 'data3'), (4, 'data4');
SELECT * FROM test;

-- Batch insert
INSERT INTO test (id, data) VALUES 
  (5, 'data5'),
  (6, 'data6'),
  (7, 'data7'),
  (8, 'data8'),
  (9, 'data9'),
  (10, 'data10');
SELECT * FROM test;

-- Update some existing records
UPDATE test SET data = 'updated_data1' WHERE id = 1;
UPDATE test SET data = 'updated_data3' WHERE id = 3;
SELECT * FROM test;

-- Second checkpoint
CHECKPOINT;

-- Delete some records
DELETE FROM test WHERE id = 2;
DELETE FROM test WHERE id > 8;
SELECT * FROM test;

-- Insert after deletion
INSERT INTO test (id, data) VALUES 
  (11, 'data11'),
  (12, 'data12'),
  (13, 'data13');
SELECT * FROM test;

-- Update multiple records at once
UPDATE test SET data = 'batch_updated' WHERE id BETWEEN 6 AND 8;
SELECT * FROM test;

-- Third checkpoint
CHECKPOINT;

-- Complex operations
INSERT INTO test (id, data) VALUES (14, 'data14'), (15, 'data15');
UPDATE test SET data = 'final_' || data WHERE id > 10;
DELETE FROM test WHERE id = 7;
SELECT * FROM test;

-- Fourth checkpoint
CHECKPOINT;

-- Large batch insert for stress testing
INSERT INTO test (id, data) 
SELECT 
  i + 20, 
  'stress_test_' || i 
FROM 
  range(1, 51) t(i);
SELECT COUNT(*) FROM test;
SELECT * FROM test WHERE id BETWEEN 20 AND 30;

-- Final checkpoint
CHECKPOINT;

-- Statistics
SELECT COUNT(*) AS total_rows FROM test;
SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM test;
SELECT COUNT(DISTINCT data) AS unique_data_values FROM test;