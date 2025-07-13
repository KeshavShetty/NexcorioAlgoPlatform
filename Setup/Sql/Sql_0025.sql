alter table nexcorio_option_atm_movement_data
add column deltaRangeCEFullDeltaOI real default 0,
add column deltaRangePEFullDeltaOI real default 0;

alter table nexcorio_option_atm_movement_data
add column deltaRangeCEGammaOI real default 0,
add column deltaRangePEGammaOI real default 0;

INSERT INTO db_versions VALUES ('0025', now(), 'Keshav', 'Additional option OI columns', 'Schema');