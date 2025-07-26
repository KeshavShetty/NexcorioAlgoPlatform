ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN countCETotal INTEGER DEFAULT 0,
ADD COLUMN countCEOutlier INTEGER DEFAULT 0,
ADD COLUMN countPETotal INTEGER DEFAULT 0,
ADD COLUMN countPEOutlier INTEGER DEFAULT 0;

INSERT INTO db_versions VALUES ('0031', now(), 'Keshav', 'Additional ATM columns', 'Schema');