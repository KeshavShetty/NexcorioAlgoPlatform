CREATE SEQUENCE nexcorio_tick_data_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_tick_data
(
  id bigint NOT NULL,
  f_main_instrument bigint,
  trading_symbol character varying(25),
  quote_time timestamp(3) with time zone,
  record_time timestamp(3) with time zone DEFAULT now(),  
  last_traded_price real, 
  last_traded_qty real, 
  open_interest real, 
  total_buy_qty real, 
  total_sell_qty real, 
  volume_traded_today real,
  avg_traded_price real,
  CONSTRAINT nexcorio_tick_data_id_pkey PRIMARY KEY (id),
  CONSTRAINT fk_nexcorio_tick_data_f_main_instrument FOREIGN KEY (f_main_instrument) REFERENCES nexcorio_main_instruments(id) MATCH FULL DEFERRABLE
)
WITH (
  OIDS=FALSE
);

CREATE INDEX nexcorio_tick_data_idx1
  ON nexcorio_tick_data
  USING btree
  (trading_symbol, quote_time);

INSERT INTO db_versions VALUES('0004', now(), 'Keshav', 'Tick data table','Schema' );
