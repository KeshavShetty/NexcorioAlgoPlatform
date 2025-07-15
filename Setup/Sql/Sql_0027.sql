ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeHybridCEAvgIv REAL DEFAULT 0,
ADD COLUMN deltaRangeHybridPEAvgIv REAL DEFAULT 0;

INSERT INTO db_versions VALUES ('0027', now(), 'Keshav', 'Additional option OI columns', 'Schema');
