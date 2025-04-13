
CREATE SEQUENCE nexcorio_options_algo_strategy_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE SEQUENCE nexcorio_options_algo_strategy_test_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 9000 CACHE 1;

CREATE TABLE nexcorio_options_algo_strategy
(
  id bigint NOT NULL,
  f_user bigint DEFAULT 1,
  f_main_instrument bigint,
  algoname character varying(125),
  entry_time character varying(10),
  exit_time character varying(10),
  no_of_lots integer DEFAULT 1,
  hedge_distance integer DEFAULT 500,
  max_fund_allocated real DEFAULT 0,
  target integer DEFAULT (-1),
  stoploss integer DEFAULT (-1),
  trailing_stoploss integer DEFAULT (-1),
  max_allowed_nooforders integer DEFAULT (-1),
  order_enabled_monday boolean DEFAULT false,
  order_enabled_tuesday boolean DEFAULT false,
  order_enabled_wednesday boolean DEFAULT false,
  order_enabled_thursday boolean DEFAULT false,
  order_enabled_friday boolean DEFAULT false,
  manual_exit_enabled boolean DEFAULT false,
  status character varying(15) DEFAULT 'Exited'::character varying,
  favourite boolean DEFAULT false,
  isactive boolean DEFAULT false,
  algo_class_name character varying(150),
  f_parent bigint,
  CONSTRAINT nexcorio_options_algo_strategy_pkey PRIMARY KEY (id),
  CONSTRAINT fk_nexcorio_options_algo_strategy_f_user FOREIGN KEY (f_user) REFERENCES nexcorio_users(id) MATCH FULL DEFERRABLE,
  CONSTRAINT fk_nexcorio_options_algo_strategy_f_main_instrument FOREIGN KEY (f_main_instrument) REFERENCES nexcorio_main_instruments(id) MATCH FULL DEFERRABLE,
  CONSTRAINT fk_nexcorio_options_algo_strategy_f_parent FOREIGN KEY (f_parent) REFERENCES nexcorio_options_algo_strategy(id) ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);

CREATE SEQUENCE nexcorio_options_algo_strategy_parameters_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_options_algo_strategy_parameters
(
  id bigint NOT NULL,
  f_strategy bigint,
  name character varying(50),
  data_type character varying(10),
  value character varying(20),
  CONSTRAINT nexcorio_options_algo_strategy_parameters_pkey PRIMARY KEY (id),
  CONSTRAINT fk_nexcorio_options_algo_strategy_parameters_f_strategy FOREIGN KEY (f_strategy) REFERENCES nexcorio_options_algo_strategy(id) ON DELETE CASCADE,
  CONSTRAINT nexcorio_options_algo_strategy_parameters_uk UNIQUE (f_strategy, name)
)
WITH (
  OIDS=FALSE
);
  
INSERT INTO db_versions VALUES('0008', now(), 'Keshav', 'FnO Algo strategies','Schema' );