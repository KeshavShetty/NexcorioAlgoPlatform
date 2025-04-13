CREATE SEQUENCE nexcorio_real_orders_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_real_orders
(
  id bigint NOT NULL,
  algo_order_id bigint,
  f_user bigint,
  algo_tag character varying(10),
  option_name character varying(25),
  quantity integer,
  transaction_type character varying(5),
  waitforpositionfill boolean,
  status character varying(15) DEFAULT 'PENDING'::character varying,
  record_time timestamp without time zone DEFAULT now(),
  executed_time timestamp without time zone,
  CONSTRAINT nexcorio_real_orders_pkey PRIMARY KEY (id),
  CONSTRAINT fk_nexcorio_real_orders_f_user FOREIGN KEY (f_user) REFERENCES nexcorio_users(id) MATCH FULL DEFERRABLE
)
WITH (
  OIDS=FALSE
);

INSERT INTO db_versions VALUES('0006', now(), 'Keshav', 'Real orders','Schema' );
