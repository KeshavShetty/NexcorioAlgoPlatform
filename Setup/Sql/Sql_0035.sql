alter TABLE nexcorio_option_atm_movement_data

add column fullrangecetotaliv real default 0,
add column fullrangepetotaliv real default 0,

add column dr16CETotalIV real default 0,
add column dr16PETotalIV real default 0,
add column dr49CETotalIV real default 0,
add column dr49PETotalIV real default 0,
add column dr46CETotalIV real default 0,
add column dr46PETotalIV real default 0,
add column dr4PlusCETotalIV real default 0,
add column dr4PlusPETotalIV real default 0;

INSERT INTO db_versions VALUES ('0035', now(), 'Keshav', 'Additional column for ATM data', 'Schema');