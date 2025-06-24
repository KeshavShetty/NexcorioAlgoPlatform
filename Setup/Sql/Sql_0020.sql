ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN selectiveStrike_AvgCeIv REAL DEFAULT 0,
ADD COLUMN selectiveStrike_AvgPeIv REAL DEFAULT 0;

INSERT INTO db_versions VALUES('0020', now(), 'Keshav', 'New column for ATM','Data' );