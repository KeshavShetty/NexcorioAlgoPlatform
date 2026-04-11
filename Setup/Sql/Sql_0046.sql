alter table nexcorio_option_atm_movement_data
add column otm250_750accmlcetheta real default 0,
add column otm250_750accmlpetheta real default 0,
add column lowerstrikeceavgiv real default 0,
add column lowerstrikepeavgiv real default 0,
add column otm200_400accmlcetheta real default 0,
add column otm200_400accmlpetheta real default 0,
add column altabove5whlstrkceavgtimevalue real default 0,
add column altabove5whlstrkpeavgtimevalue real default 0,
add column fullotm0x600cegreeks real default 0,
add column fullotm0x600pegreeks real default 0,
add column lowerotm0x300cegreeks real default 0,
add column lowerotm0x300pegreeks real default 0,
add column upperotm300x600cegreeks real default 0,
add column upperotm300x600pegreeks real default 0,
add column upperotm150x300cegreeks real default 0,
add column upperotm150x300pegreeks real default 0;


INSERT INTO db_versions VALUES ('0046', now(), 'Keshav', 'Additional columns', 'Schema');