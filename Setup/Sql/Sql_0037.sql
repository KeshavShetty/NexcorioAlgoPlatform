alter table nexcorio_option_atm_movement_data
add column adjustedCEATMLtp real default 0,
add column adjustedPEATMLtp real default 0,
add column adjustedCEATMIV real default 0,
add column adjustedPEATMIV real default 0,
add column adjustedCEATMGamma real default 0, 
add column adjustedPEATMGamma real default 0,
add column adjustedCEATMVega real default 0, 
add column adjustedPEATMVega real default 0,
add column adjustedCEATMTheta real default 0,
add column adjustedPEATMTheta real default 0;

INSERT INTO db_versions VALUES ('0037', now(), 'Keshav', 'Additional column for ATM data', 'Schema');