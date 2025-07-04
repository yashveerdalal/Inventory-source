
CREATE DATABASE IF NOT EXISTS InventoryManagementSystem;

USE InventoryManagementSystem;

CREATE TABLE IF NOT EXISTS inventory_source (
    item_id INT,
    item_name VARCHAR(100),
    batch_no VARCHAR(20),
    purchase_date DATE,
    expiry_date DATE,
    quantity INT,
    store_location VARCHAR(100),
    sold_units INT,
    returned_units INT
);

SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE '/Users/yashdalal/Desktop/inventorySource.csv'
INTO TABLE inventory_source
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '
'
IGNORE 1 ROWS;

SELECT * FROM inventory_source;

CREATE TABLE inventory_stageA AS
SELECT
  item_id,
  item_name,
  batch_no,
  CAST(purchase_date AS CHAR) AS purchase_date,
  CAST(expiry_date AS CHAR) AS expiry_date,
  quantity,
  store_location,
  sold_units,
  returned_units
FROM inventory_source;

SELECT * FROM inventory_stageA WHERE item_name IS NULL OR TRIM(item_name) = '';
UPDATE inventory_stageA
SET item_name = 'Unknown'
WHERE item_name IS NULL OR TRIM(item_name) = '';

SELECT COUNT(*) AS negativeQuant FROM inventory_stageA WHERE quantity < 0;

SELECT COUNT(*) AS missing_store_locations
FROM inventory_stageA
WHERE store_location IS NULL OR TRIM(store_location) = '';

SELECT COUNT(*) AS negativeSold FROM inventory_stageA WHERE sold_units < 0;
SELECT COUNT(*) AS negativeReturned FROM inventory_stageA WHERE returned_units < 0;

SELECT item_id, COUNT(*) AS count
FROM inventory_stageA
GROUP BY item_id
HAVING count > 1;

SELECT
  item_name,
  batch_no,
  store_location,
  COUNT(*) AS duplicate_count
FROM inventory_stageA
GROUP BY item_name, batch_no, store_location
HAVING duplicate_count > 1;

WITH RankedDuplicates AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY item_id, item_name, batch_no, store_location, purchase_date, expiry_date
           ORDER BY item_id
         ) AS rn
  FROM inventory_stageA
)
DELETE FROM inventory_stageA
WHERE item_id IN (
  SELECT item_id FROM RankedDuplicates WHERE rn > 1
);

SELECT *
FROM inventory_stageA
WHERE item_id IN (
  SELECT item_id
  FROM inventory_stageA
  GROUP BY item_id
  HAVING COUNT(*) > 1
)
ORDER BY item_id;

UPDATE inventory_stageA
SET item_name = CASE
  WHEN item_name IN ('Amoxicilin', 'Amoxycillin') THEN 'Amoxicillin'
  WHEN item_name IN ('Ibuprofenn', 'Ibuprofin') THEN 'Ibuprofen'
  WHEN item_name IN ('Paracetamoll', 'Paracetomol') THEN 'Paracetamol'
  WHEN item_name IN ('Syring_5ml', 'Syringe_5mll') THEN 'Syringe_5ml'
  ELSE item_name
END;

SELECT * FROM inventory_stageA WHERE quantity = 0;

SELECT *
FROM inventory_stageA
WHERE (quantity IS NULL OR quantity = 0)
  AND (purchase_date IS NULL OR purchase_date = '0000-00-00' OR expiry_date IS NULL OR expiry_date = '0000-00-00');

SELECT *
FROM inventory_stageA
WHERE (purchase_date IS NULL OR purchase_date = '0000-00-00' OR expiry_date IS NULL OR expiry_date = '0000-00-00');

SELECT COUNT(*) AS invalid_expiry_count
FROM inventory_stageA
WHERE expiry_date = '0000-00-00' OR expiry_date IS NULL;

UPDATE inventory_stageA
SET expiry_date = NULL
WHERE expiry_date = '0000-00-00';

UPDATE inventory_stageA SET sold_units = 0 WHERE sold_units < 0;
UPDATE inventory_stageA SET returned_units = 0 WHERE returned_units < 0;

SELECT * FROM inventory_stageA WHERE purchase_date > expiry_date;

WITH to_swap AS (
  SELECT item_id, batch_no, store_location, purchase_date, expiry_date
  FROM inventory_stageA
  WHERE purchase_date > expiry_date
)
UPDATE inventory_stageA s
JOIN to_swap t
  ON s.item_id = t.item_id
  AND s.batch_no = t.batch_no
  AND s.store_location = t.store_location
  AND s.purchase_date = t.purchase_date
  AND s.expiry_date = t.expiry_date
SET
  s.purchase_date = t.expiry_date,
  s.expiry_date = t.purchase_date;

SELECT * FROM inventory_stageA WHERE quantity = 0;

SELECT DISTINCT(store_location) FROM inventory_stageA;

UPDATE inventory_stageA
SET store_location = 'Central Distribution Center'
WHERE store_location LIKE '%Central Distribution Center%';

UPDATE inventory_stageA
SET store_location = 'Northside Medical Depot'
WHERE store_location LIKE '%Northside%Depot%';

UPDATE inventory_stageA
SET store_location = 'Greenfield Pharmacy Outlet'
WHERE store_location LIKE '%Greenfield%Pharmacy%Outlet%';

UPDATE inventory_stageA
SET store_location = 'Metro Health Supply Unit'
WHERE store_location LIKE '%Metro%Health%Supply%';

UPDATE inventory_stageA
SET store_location = 'Downtown Clinical Storehouse'
WHERE store_location LIKE '%Downtown%Clinical%Storehouse%';

UPDATE inventory_stageA
SET store_location = LOWER(store_location);

UPDATE inventory_stageA
SET store_location = 'northside medical depot'
WHERE store_location = 'northside medical deport';

ALTER TABLE inventory_stageA
MODIFY purchase_date DATE,
MODIFY expiry_date DATE;

SELECT * FROM inventory_stageA;

CREATE TABLE inventory_stageB AS
SELECT * FROM inventory_stageA;

DELETE FROM inventory_stageB
WHERE sold_units > quantity;

DELETE FROM inventory_stageB
WHERE returned_units > sold_units;

SELECT * FROM inventory_stageB WHERE purchase_date > CURDATE();
DELETE FROM inventory_stageB WHERE purchase_date > CURDATE();

SELECT * FROM inventory_stageB
WHERE TRIM(item_name) = '';

UPDATE inventory_stageB
SET item_name = TRIM(item_name);

SELECT * FROM inventory_stageB
WHERE item_name IS NULL OR item_name = '' OR item_name = 'unknown';

CREATE TABLE IF NOT EXISTS batch_item_map (
  batch_no VARCHAR(20) PRIMARY KEY,
  item_name VARCHAR(100)
);

INSERT INTO batch_item_map (batch_no, item_name) VALUES
('SYR-1533', 'Syringe_5ml'),
('OME-1120', 'Omeprazole'),
('OME-4603', 'Omeprazole'),
('GLO-4170', 'Gloves_Latex'),
('GLO-5016', 'Gloves_Latex'),
('MET-4492', 'Metformin'),
('AMX-9185', 'Amoxicillin'),
('LIS-2867', 'Lisinopril'),
('IVC-2166', 'IV_Catheter')
ON DUPLICATE KEY UPDATE item_name = VALUES(item_name);

UPDATE inventory_stageB b
JOIN batch_item_map m ON b.batch_no = m.batch_no
SET b.item_name = m.item_name
WHERE b.item_name IS NULL OR b.item_name = '' OR b.item_name = 'unknown';

SELECT * FROM inventory_stageB
WHERE item_name IS NULL OR item_name = '' OR item_name = 'unknown';

DROP TABLE IF EXISTS batch_item_map;

DELETE FROM inventory_stageB
WHERE expiry_date IS NULL
   OR TRIM(expiry_date) = '';

UPDATE `inventory_stageB`
SET item_name = TRIM(item_name);

SELECT * FROM `inventory_stageB`
WHERE item_name = "unknown";

SELECT COUNT(*) FROM inventory_source;
SELECT COUNT(*) FROM inventory_stageA;
SELECT COUNT(*) FROM inventory_stageB;
