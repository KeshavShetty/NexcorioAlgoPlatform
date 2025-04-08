CREATE SEQUENCE nexcorio_fno_expiry_dates_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_fno_expiry_dates
(
  id bigint NOT NULL,
  expiry_date date,
  f_main_instrument bigint,
  fno_segment character varying(15),
  fno_prefix character varying(25),
  CONSTRAINT nexcorio_fno_expiry_dates_id_pkey PRIMARY KEY (id),
  CONSTRAINT fk_nexcorio_fno_expiry_dates_f_main_instrument FOREIGN KEY (f_main_instrument) REFERENCES nexcorio_main_instruments(id) MATCH FULL DEFERRABLE,
  CONSTRAINT uk_nexcorio_fno_expiry_dates UNIQUE (f_main_instrument, fno_segment, expiry_date)
)
WITH (
  OIDS=FALSE
);

CREATE SEQUENCE nexcorio_fno_instruments_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_fno_instruments
(
  id bigint NOT NULL,
  trading_symbol character varying(25),
  zerodha_instrument_token bigint,
  f_main_instrument bigint,
  exchange character varying(10),
  strike smallint,
  expiry_date date,
  CONSTRAINT nexcorio_fno_instruments_id_pkey PRIMARY KEY (id),
  CONSTRAINT fk_nexcorio_fno_instruments_f_main_instrument FOREIGN KEY (f_main_instrument) REFERENCES nexcorio_main_instruments(id) MATCH FULL DEFERRABLE,
  CONSTRAINT uk_nexcorio_fno_instruments UNIQUE (trading_symbol)
)
WITH (
  OIDS=FALSE
);

INSERT INTO db_versions VALUES('0003', now(), 'Keshav', 'FnO instrumenst and Expriry Dates','Schema' );
