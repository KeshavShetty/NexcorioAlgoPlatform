alter table fdw_nexcorio_option_atm_movement_data
add column cumulativeCEAvgIVDiff real default 0,
add column cumulativePEAvgIVDiff real default 0;

INSERT INTO db_versions VALUES ('0044', now(), 'Keshav', 'Additional column gamma exposure with strike distance', 'Schema');