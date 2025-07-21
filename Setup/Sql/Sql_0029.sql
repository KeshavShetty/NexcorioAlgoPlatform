ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeHybridCEAvgGamma REAL DEFAULT 0,
ADD COLUMN deltaRangeHybridPEAvgGamma REAL DEFAULT 0,
ADD COLUMN deltaRangeCEOutlierRatio REAL DEFAULT 0,
ADD COLUMN deltaRangePEOutlierRatio REAL DEFAULT 0;

ALTER FOREIGN TABLE fdw_nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeHybridCEAvgGamma REAL DEFAULT 0,
ADD COLUMN deltaRangeHybridPEAvgGamma REAL DEFAULT 0,
ADD COLUMN deltaRangeCEOutlierRatio REAL DEFAULT 0,
ADD COLUMN deltaRangePEOutlierRatio REAL DEFAULT 0;

INSERT INTO db_versions VALUES ('0029', now(), 'Keshav', 'Additional ATM columns', 'Schema');