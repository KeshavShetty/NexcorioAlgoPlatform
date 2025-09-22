ALTER TABLE nexcorio_option_atm_movement_data
ADD COLUMN totalChangeInCEIV REAl DEFAULT 0,
ADD COLUMN totalChangeInPEIV REAl DEFAULT 0;

INSERT INTO db_versions VALUES ('0039', now(), 'Keshav', 'Additional column for ATM data', 'Schema');