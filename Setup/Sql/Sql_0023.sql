ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeCEWorth REAL DEFAULT 0,
ADD COLUMN deltaRangePEWorth REAL DEFAULT 0;

INSERT INTO db_versions VALUES ('0023', now(), 'Keshav', 'Additional option worth columns', 'Schema');