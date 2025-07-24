-- Excute in Stock Poratl DB

CREATE SERVER nexcorio_foreign_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (hostaddr '127.0.0.1', port '5432', dbname 'nexcorio_db');

CREATE USER MAPPING FOR postgres SERVER nexcorio_foreign_server OPTIONS (USER 'postgres' , password 'jijikos');

DROP FOREIGN TABLE IF EXISTS fdw_nexcorio_daily_summary;

CREATE FOREIGN TABLE fdw_nexcorio_daily_summary (
    id BIGINT,
    f_strategy BIGINT,
    short_name CHARACTER VARYING (25),
    algoname CHARACTER VARYING (125),
    short_date DATE,
    exit_profit REAL,
    best_profit REAL,
    worst_profit REAL,
    maxtrailingprofit REAL,
    max_profit_reached_at TIMESTAMP WITHOUT TIME ZONE,
    worst_profit_reached_at TIMESTAMP WITHOUT TIME ZONE,
    nooforders INTEGER,
    exit_time TIMESTAMP WITHOUT TIME ZONE,
    exit_reason CHARACTER VARYING (100)
) SERVER nexcorio_foreign_server OPTIONS (SCHEMA_NAME'public', TABLE_NAME'nexcorio_daily_summary');


DROP FOREIGN TABLE IF EXISTS fdw_nexcorio_option_atm_movement_data;

CREATE FOREIGN TABLE  fdw_nexcorio_option_atm_movement_data
(
    id bigint NOT NULL,
    f_main_instrument bigint,
    record_time timestamp without time zone DEFAULT now(),
    ceoptionname character varying(25) COLLATE pg_catalog."default",
    peoptionname character varying(25) COLLATE pg_catalog."default",
    instrumentltp real,
    cedelta real,
    pedelta real,
    cegamma real,
    pegamma real,
    cevega real,
    pevega real,
    cetheta real,
    petheta real,
    ceiv real,
    peiv real,
    celtp real,
    peltp real,
    totalfuturepoints integer,
    bullishfuturepoints integer,
    ceoi real DEFAULT 0,
    peoi real DEFAULT 0,
    totalceoi real,
    totalpeoi real DEFAULT 0,
    totalceiv real DEFAULT 0,
    totalpeiv real DEFAULT 0,
    totalcegamma real DEFAULT 0,
    totalpegamma real DEFAULT 0,
    totalcevega real DEFAULT 0,
    totalpevega real DEFAULT 0,
    avgcegamma real DEFAULT 0,
    avgpegamma real DEFAULT 0,
    selectivestrike_avgcegamma real DEFAULT 0,
    selectivestrike_avgpegamma real DEFAULT 0,
    futures_ltp real DEFAULT 0,
    selectivestrike_avgceiv real DEFAULT 0,
    selectivestrike_avgpeiv real DEFAULT 0,
    selective10strike_avgcegamma real DEFAULT 0,
    selective10strike_avgpegamma real DEFAULT 0,
    selective10strike_avgceiv real DEFAULT 0,
    selective10strike_avgpeiv real DEFAULT 0,
    selective20strike_avgcegamma real DEFAULT 0,
    selective20strike_avgpegamma real DEFAULT 0,
    selective20strike_avgceiv real DEFAULT 0,
    selective20strike_avgpeiv real DEFAULT 0,
    deltarangeceavgltp real DEFAULT 0,
    deltarangeceavgiv real DEFAULT 0,
    deltarangeceavgdelta real DEFAULT 0,
    deltarangeceavggamma real DEFAULT 0,
    deltarangeceavgvega real DEFAULT 0,
    deltarangepeavgltp real DEFAULT 0,
    deltarangepeavgiv real DEFAULT 0,
    deltarangepeavgdelta real DEFAULT 0,
    deltarangepeavggamma real DEFAULT 0,
    deltarangepeavgvega real DEFAULT 0,
    deltarangeceworth real DEFAULT 0,
    deltarangepeworth real DEFAULT 0,
    deltarangeceoi real DEFAULT 0,
    deltarangepeoi real DEFAULT 0,
    deltarangecedeltaoi real DEFAULT 0,
    deltarangepedeltaoi real DEFAULT 0,
    deltarangecefulldeltaoi real DEFAULT 0,
    deltarangepefulldeltaoi real DEFAULT 0,
    deltarangecegammaoi real DEFAULT 0,
    deltarangepegammaoi real DEFAULT 0,
    deltarangecefullavgiv real DEFAULT 0,
    deltarangepefullavgiv real DEFAULT 0,
    deltarangehybridceavgiv real DEFAULT 0,
    deltarangehybridpeavgiv real DEFAULT 0,
    deltarangecevolume1min real DEFAULT 0,
    deltarangepevolume1min real DEFAULT 0
) SERVER nexcorio_foreign_server OPTIONS (schema_name 'public' , table_name 'nexcorio_option_atm_movement_data');

ALTER FOREIGN TABLE fdw_nexcorio_option_atm_movement_data 
ADD COLUMN deltaRangeHybridCEAvgGamma REAL DEFAULT 0,
ADD COLUMN deltaRangeHybridPEAvgGamma REAL DEFAULT 0,
ADD COLUMN deltaRangeCEOutlierRatio REAL DEFAULT 0,
ADD COLUMN deltaRangePEOutlierRatio REAL DEFAULT 0;


alter table fdw_nexcorio_option_atm_movement_data
ADD COLUMN dr4_9CEAvgIv REAL DEFAULT 0,
ADD COLUMN dr4_9PEAvgIv REAL DEFAULT 0;