CREATE SEQUENCE nexcorio_option_algo_orders_daily_summary_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_option_algo_orders_daily_summary
(
  id bigint NOT NULL,
  f_strategy bigint,
  exit_profit real,
  best_profit real,
  worst_profit real,
  short_date date DEFAULT now(),
  max_profit_reached_at timestamp without time zone,
  worst_profit_reached_at timestamp without time zone,
  nooforders integer,
  maxtrailingprofit real DEFAULT 0,
  exit_reason character varying(100),
  last_updated_at timestamp without time zone,
  CONSTRAINT nexcorio_option_algo_orders_daily_summary_pkey PRIMARY KEY (id),
  CONSTRAINT fk_nexcorio_option_algo_orders_daily_summary_f_strategy FOREIGN KEY (f_strategy) REFERENCES nexcorio_options_algo_strategy(id) ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);

INSERT INTO db_versions VALUES('0010', now(), 'Keshav', 'Option algo order Daily summary','Schema' );