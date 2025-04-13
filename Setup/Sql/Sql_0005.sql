CREATE TABLE nexcorio_option_greeks
(
  id bigint NOT NULL,
  trading_symbol character varying(25),
  quote_time timestamp(3) without time zone,
  record_time timestamp(3) without time zone DEFAULT now(),  
  ltp real,
  oi real,
  underlying_value real,
  iv real,
  delta real,
  vega real,
  theta real,
  gamma real,
  CONSTRAINT nexcorio_option_greeks_id_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX nexcorio_option_greeks_idx1
ON nexcorio_option_greeks
USING btree
(trading_symbol, quote_time);

CREATE INDEX nexcorio_option_greeks_idx2
ON nexcorio_option_greeks
USING btree
(quote_time);

CREATE SEQUENCE nexcorio_option_snapshot_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_option_snapshot
(
  id bigint NOT NULL,
  trading_symbol character varying(25),
  strike integer,
  last_updated_time timestamp(3) with time zone,
  record_date date,  
  ltp real,
  oi real,
  iv real,
  delta real,
  vega real,
  theta real,
  gamma real,
  CONSTRAINT nexcorio_option_snapshot_id_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX nexcorio_option_snapshot_idx1
  ON nexcorio_option_snapshot
  USING btree
  (oi, record_date, trading_symbol COLLATE pg_catalog."default");

INSERT INTO db_versions VALUES('0005', now(), 'Keshav', 'Option Greeks table and Snapshot','Schema' );
