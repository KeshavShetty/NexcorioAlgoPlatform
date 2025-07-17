alter table nexcorio_option_snapshot
add column volume1min real default 0;

ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeCEvolume1min REAL DEFAULT 0,
ADD COLUMN deltaRangePEvolume1min REAL DEFAULT 0;

INSERT INTO db_versions VALUES ('0028', now(), 'Keshav', 'Additional 1M volume info', 'Schema');
