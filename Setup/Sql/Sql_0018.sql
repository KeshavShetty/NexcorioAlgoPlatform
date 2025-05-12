alter table nexcorio_option_atm_movement_data
add column ceoi real default 0,
add column peoi real default 0;

INSERT INTO db_versions VALUES('0018', now(), 'Keshav', 'Spike Aversion buy and mix algo','Data' );