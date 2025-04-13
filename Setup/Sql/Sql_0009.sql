CREATE SEQUENCE nexcorio_option_algo_orders_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_option_algo_orders
(
  id bigint NOT NULL,
  f_strategy bigint,
  option_name character varying(25),
  entry_time timestamp without time zone DEFAULT now(),
  exit_time timestamp without time zone,
  sell_price real,
  buy_price real,
  short_date date DEFAULT now(),
  exit_reason character varying(100),
  place_actual_order boolean,
  quantity integer,
  days_to_expiry smallint,
  status character varying(15) DEFAULT 'OPEN'::character varying,
  CONSTRAINT nexcorio_option_algo_orders_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX nexcorio_option_algo_orders_idx1 ON nexcorio_option_algo_orders
  USING btree
  (f_strategy, short_date);
  
INSERT INTO db_versions VALUES('0009', now(), 'Keshav', 'Option algo order simulated','Schema' );