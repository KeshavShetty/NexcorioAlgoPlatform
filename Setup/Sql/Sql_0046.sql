alter table nexcorio_option_atm_movement_data
add column otm250_750accmlcetheta real default 0,
add column otm250_750accmlpetheta real default 0,
add column lowerstrikeceavgiv real default 0,
add column lowerstrikepeavgiv real default 0,
add column otm200_400accmlcetheta real default 0,
add column otm200_400accmlpetheta real default 0,
add column altabove5whlstrkceavgtimevalue real default 0,
add column altabove5whlstrkpeavgtimevalue real default 0,
add column fullotm0x600cegreeks real default 0,
add column fullotm0x600pegreeks real default 0,
add column lowerotm0x300cegreeks real default 0,
add column lowerotm0x300pegreeks real default 0,
add column upperotm300x600cegreeks real default 0,
add column upperotm300x600pegreeks real default 0,
add column upperotm150x300cegreeks real default 0,
add column upperotm150x300pegreeks real default 0,
add column otm0x400cegreeks real default 0,
add column otm400x800cegreeks real default 0,
add column otm0x400pegreeks real default 0,
add column otm400x800pegreeks real default 0;

CREATE FOREIGN TABLE "public"."fdw_nexcorio_option_greeks" (
  "id" int8 NOT NULL,
  "trading_symbol" varchar(25) COLLATE "pg_catalog"."default",
  "quote_time" timestamp(3) NOT NULL,
  "record_time" timestamp(3) DEFAULT now(),
  "ltp" float4,
  "oi" float4,
  "underlying_value" float4,
  "iv" float4,
  "delta" float4,
  "vega" float4,
  "theta" float4,
  "gamma" float4,
  "f_main_instrument" int8
)
SERVER "nexcorio_foreign_server"
OPTIONS ("schema_name" 'public', "table_name" 'nexcorio_option_greeks')
;

CREATE FOREIGN TABLE "public"."fdw_nexcorio_option_snapshot" (
  "id" int8 NOT NULL,
  "trading_symbol" varchar(25) COLLATE "pg_catalog"."default",
  "strike" int4,
  "last_updated_time" timestamptz(3),
  "record_date" date,
  "ltp" float4,
  "oi" float4,
  "iv" float4,
  "delta" float4,
  "vega" float4,
  "theta" float4,
  "gamma" float4,
  "volume1min" float4 DEFAULT 0
)
SERVER "nexcorio_foreign_server"
OPTIONS ("schema_name" 'public', "table_name" 'nexcorio_option_snapshot')
;

INSERT INTO db_versions VALUES ('0046', now(), 'Keshav', 'Additional columns', 'Schema');