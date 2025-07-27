alter table nexcorio_option_atm_movement_data
ADD COLUMN dr1_6CEAvgIv REAL DEFAULT 0,
ADD COLUMN dr1_6PEAvgIv REAL DEFAULT 0;

INSERT INTO db_versions VALUES ('0032', now(), 'Keshav', 'Additional ATM columns', 'Schema');