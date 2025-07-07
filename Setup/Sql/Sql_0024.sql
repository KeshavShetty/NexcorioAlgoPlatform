ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeCEOI REAL DEFAULT 0,
ADD COLUMN deltaRangePEOI REAL DEFAULT 0;

ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeCEDeltaOI REAL DEFAULT 0,
ADD COLUMN deltaRangePEDeltaOI REAL DEFAULT 0;

INSERT INTO db_versions VALUES ('0024', now(), 'Keshav', 'Additional option OI columns', 'Schema');