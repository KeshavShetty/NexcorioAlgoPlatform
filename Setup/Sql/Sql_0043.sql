alter table nexcorio_option_atm_movement_data
add column minGammaExposureTopN real default 0,
add column maxGammaExposureTopN real default 0,
add column netGammaExposureTopN real default 0;

INSERT INTO db_versions VALUES ('0043', now(), 'Keshav', 'Additional column gamma exposure with strike distance', 'Schema');