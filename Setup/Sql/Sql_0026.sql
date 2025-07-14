ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeCEFullAvgIv REAL DEFAULT 0,
ADD COLUMN deltaRangePEFullAvgIv REAL DEFAULT 0;

INSERT INTO db_versions VALUES ('0026', now(), 'Keshav', 'Additional option OI columns', 'Schema');