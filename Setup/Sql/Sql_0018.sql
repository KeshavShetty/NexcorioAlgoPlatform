alter table nexcorio_option_atm_movement_data
add column ceoi real default 0,
add column peoi real default 0;

alter table nexcorio_option_atm_movement_data
add column totalCEOI real,
add column totalPEOI real default 0;

alter table nexcorio_option_atm_movement_data
add column totalCEIV real default 0,
add column totalPEIV real default 0;

alter table nexcorio_option_atm_movement_data
add column totalCEGamma real default 0,
add column totalPEGamma real default 0,
add column totalCEVega real default 0,
add column totalPEVega real default 0;

alter table nexcorio_option_atm_movement_data
add column avgcegamma real default 0,
add column avgpegamma real default 0;

alter table nexcorio_option_atm_movement_data
add column selectiveStrike_AvgCeGamma real default 0,
add column selectiveStrike_AvgPeGamma real default 0,
add column futures_Ltp real default 0;

INSERT INTO db_versions VALUES('0018', now(), 'Keshav', 'Additional columns for ATM','Data' );