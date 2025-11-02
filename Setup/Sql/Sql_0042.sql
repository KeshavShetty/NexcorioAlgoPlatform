alter table nexcorio_option_atm_movement_data
add column minGammaExposureWithStrike real default 0,
add column maxGammaExposureWithStrike real default 0,
add column netGammaExposureWithStrike real default 0;

INSERT INTO db_versions VALUES ('0042', now(), 'Keshav', 'Additional column gamma exposure with strike distance', 'Schema');