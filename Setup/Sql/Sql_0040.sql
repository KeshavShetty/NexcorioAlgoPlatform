ALTER TABLE nexcorio_option_atm_movement_data
ADD COLUMN changein5secCeIV REAl DEFAULT 0,
ADD COLUMN changein5secPeIV REAl DEFAULT 0;

INSERT INTO db_versions VALUES ('0040', now(), 'Keshav', 'Additional column changein5secCeIV and PeIV', 'Schema');