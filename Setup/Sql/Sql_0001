CREATE SEQUENCE nexcorio_users_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

CREATE TABLE nexcorio_users
(
  id bigint NOT NULL,
  firstname character varying(25),
  lastname character varying(25),
  email_id character varying(50),
  username character varying(12),
  password character varying(50),
  usertype character varying(10),
  zerodha_user_id character varying(10),
  zerodha_user_id_pin character varying(10),
  zerodha_api_key character varying(100),
  zerodha_api_secret_key character varying(100),
  zerodha_service_token character varying(100),
  zerodha_access_token character varying(100),
  zerodha_public_token character varying(100),
  zerodha_last_login_time timestamp without time zone,
  enablerealtimeorder boolean DEFAULT false,
  CONSTRAINT nexcorio_users_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

INSERT INTO db_versions VALUES('0001', now(), 'Keshav', 'nexcorio_users table','Schema' );