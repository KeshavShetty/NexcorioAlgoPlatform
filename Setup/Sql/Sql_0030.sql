ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN dr4_9CEAvgIv REAL DEFAULT 0,
ADD COLUMN dr4_9PEAvgIv REAL DEFAULT 0;

INSERT INTO db_versions VALUES ('0030', now(), 'Keshav', 'Additional ATM columns', 'Schema');