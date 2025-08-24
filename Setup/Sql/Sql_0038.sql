ALTER TABLE nexcorio_option_atm_movement_data
ADD COLUMN dr19WholeStrikeCEAvgIV REAl DEFAULT 0,
ADD COLUMN dr19WholeStrikePEAvgIV REAl DEFAULT 0;

INSERT INTO db_versions VALUES ('0038', now(), 'Keshav', 'Additional column for ATM data', 'Schema');