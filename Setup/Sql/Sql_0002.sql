CREATE SEQUENCE nexcorio_main_instruments_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_main_instruments
(
  id bigint NOT NULL,
  name character varying(25),
  short_name character varying(25),
  instrument_type character varying(10),
  exchange character varying(5),
  lot_size integer DEFAULT 0,
  order_freezing_quantity integer DEFAULT 0,
  expiry_day smallint DEFAULT 0,
  zerodha_instrument_token bigint,
  gap_between_strikes integer DEFAULT 0,
  no_of_future_expiry_data smallint default 1,
  no_of_options_expiry_data smallint default 1,
  no_of_options_strike_points smallint default 500,
  straddle_margin real DEFAULT 0,
  IS_ACTIVE BOOLEAN DEFAULT true,
  CONSTRAINT nexcorio_main_instruments_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'INDIA VIX', 'VIX', 'INDEX', 'NSE', 0, 0, 5, 264969, 0, 0);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'NIFTY 50', 'NIFTY', 'INDEX', 'NSE', 75, 1800, 5, 256265, 50, 127000);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'NIFTY BANK', 'BANKNIFTY', 'INDEX', 'NSE', 30, 900, 4, 260105, 100, 127000);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'SENSEX', 'SENSEX', 'INDEX', 'BSE', 60, 1800, 5, 265, 100, 127000);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'RELIANCE INDUSTRIES', 'RELIANCE', 'EQ', 'NSE', 500, 1800, 5, 738561, 10, 127000);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'HDFC BANK', 'HDFCBANK', 'EQ', 'NSE', 550, 1800, 5, 341249, 20, 127000);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'ICICI BANK', 'ICICIBANK', 'EQ', 'NSE', 700, 1800, 5, 1270529, 10, 127000);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'INFOSYS', 'INFY', 'EQ', 'NSE', 400, 1800, 5, 408065, 20, 127000);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'TATA CONSULTANCY SERV', 'TCS', 'EQ', 'NSE', 175, 1800, 5, 2953217, 20, 127000);

INSERT INTO nexcorio_main_instruments 
(id, name, short_name, instrument_type, exchange, lot_size , order_freezing_quantity, expiry_day, zerodha_instrument_token, gap_between_strikes, straddle_margin)
VALUES
(nextval('nexcorio_main_instruments_id_seq'), 'BHARTI AIRTEL', 'BHARTIARTL', 'EQ', 'NSE', 175, 1800, 5, 2714625, 20, 127000);

INSERT INTO db_versions VALUES('0002', now(), 'Keshav', 'nexcorio_main_instruments table','Schema' );

