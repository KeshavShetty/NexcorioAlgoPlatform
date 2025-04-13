CREATE SEQUENCE nexcorio_option_atm_movement_data_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_option_atm_movement_data
(
  id bigint NOT NULL,
  f_main_instrument bigint,
  record_time timestamp without time zone DEFAULT now(),
  ceOptionname character varying(25),
  peOptionname character varying(25),
  instrumentltp real,
  base_delta real,
  cedelta real,
  pedelta real,
  cegamma real,
  pegamma real,
  cevega real,
  pevega real,
  cetheta real,
  petheta real,
  ceiv real,
  peiv real,
  celtp real,
  peltp real,
  CONSTRAINT nexcorio_option_atm_movement_data_pkey PRIMARY KEY (id),
  CONSTRAINT fk_nexcorio_option_atm_movement_data_f_main_instrument FOREIGN KEY (f_main_instrument) REFERENCES nexcorio_main_instruments(id) MATCH FULL DEFERRABLE
)
WITH (
  OIDS=FALSE
);

CREATE INDEX nexcorio_option_atm_movement_data_idx1
  ON nexcorio_option_atm_movement_data
  USING btree
  (f_main_instrument, base_delta, record_time);
  
INSERT INTO db_versions VALUES('0007', now(), 'Keshav', 'ATM movement extracted info table','Schema' );
