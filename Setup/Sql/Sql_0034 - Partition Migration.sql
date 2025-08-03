-- 1. nexcorio_option_greeks

CREATE TABLE public.nexcorio_option_greeks_partitioned
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
    f_main_instrument bigint,
	CONSTRAINT fk_nexcorio_option_greeks_partitioned_f_main_instrument FOREIGN KEY (f_main_instrument)
        REFERENCES public.nexcorio_main_instruments (id)
) PARTITION BY RANGE (quote_time);

ALTER TABLE public.nexcorio_option_greeks_partitioned 
ADD CONSTRAINT pk_nexcorio_greeks PRIMARY KEY (id, quote_time);

CREATE TABLE nexcorio_option_greeks_202506 
    PARTITION OF nexcorio_option_greeks_partitioned
    FOR VALUES FROM ('2025-06-01 00:00:00+00') TO ('2025-07-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);

CREATE TABLE nexcorio_option_greeks_202507 
    PARTITION OF nexcorio_option_greeks_partitioned
    FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2025-08-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);
	
CREATE TABLE nexcorio_option_greeks_202508 
    PARTITION OF nexcorio_option_greeks_partitioned
    FOR VALUES FROM ('2025-08-01 00:00:00+00') TO ('2025-09-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);
	
-- Index on each partition
CREATE INDEX ON nexcorio_option_greeks_202506 (trading_symbol, quote_time);
CREATE INDEX ON nexcorio_option_greeks_202507 (trading_symbol, quote_time);
CREATE INDEX ON nexcorio_option_greeks_202508 (trading_symbol, quote_time);

-- Migrate Data
INSERT INTO nexcorio_option_greeks_partitioned
SELECT * FROM nexcorio_option_greeks;

-- Rename old table
ALTER TABLE nexcorio_option_greeks RENAME TO nexcorio_option_greeks_old;

-- Rename partitioned table
ALTER TABLE nexcorio_option_greeks_partitioned RENAME TO nexcorio_option_greeks;


-- 2. nexcorio_tick_data

CREATE TABLE IF NOT EXISTS public.nexcorio_tick_data_partitioned
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
    CONSTRAINT fk_nexcorio_tick_data_f_main_instrument FOREIGN KEY (f_main_instrument)
        REFERENCES public.nexcorio_main_instruments (id)
) PARTITION BY RANGE (quote_time);

ALTER TABLE public.nexcorio_tick_data_partitioned 
ADD CONSTRAINT pk_nexcorio_tick_data_partitioned PRIMARY KEY (id, quote_time);

CREATE TABLE nexcorio_tick_data_202506 
    PARTITION OF nexcorio_tick_data_partitioned
    FOR VALUES FROM ('2025-06-01 00:00:00+00') TO ('2025-07-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);
	
CREATE TABLE nexcorio_tick_data_202507 
    PARTITION OF nexcorio_tick_data_partitioned
    FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2025-08-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);

CREATE TABLE nexcorio_tick_data_202508 
    PARTITION OF nexcorio_tick_data_partitioned
    FOR VALUES FROM ('2025-08-01 00:00:00+00') TO ('2025-09-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);
	
-- Index on each partition
CREATE INDEX ON nexcorio_tick_data_202506 (quote_time);
CREATE INDEX ON nexcorio_tick_data_202507 (quote_time);
CREATE INDEX ON nexcorio_tick_data_202508 (quote_time);

-- Migrate Data
INSERT INTO nexcorio_tick_data_partitioned
SELECT * FROM nexcorio_tick_data;

-- Rename old table
ALTER TABLE nexcorio_tick_data RENAME TO nexcorio_tick_data_old;

-- Rename partitioned table
ALTER TABLE nexcorio_tick_data_partitioned RENAME TO nexcorio_tick_data;




-- 3.0 nexcorio_option_atm_movement_data


CREATE TABLE public.nexcorio_option_atm_movement_data_partitioned
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
    deltarangecegammaoi real DEFAULT 0,
    deltarangepegammaoi real DEFAULT 0,
    deltarangecefulldeltaoi real DEFAULT 0,
    deltarangepefulldeltaoi real DEFAULT 0,
    deltarangecefullavgiv real DEFAULT 0,
    deltarangepefullavgiv real DEFAULT 0,
    deltarangehybridceavgiv real DEFAULT 0,
    deltarangehybridpeavgiv real DEFAULT 0,
    deltarangecevolume1min real DEFAULT 0,
    deltarangepevolume1min real DEFAULT 0,
    deltarangehybridceavggamma real DEFAULT 0,
    deltarangehybridpeavggamma real DEFAULT 0,
    deltarangeceoutlierratio real DEFAULT 0,
    deltarangepeoutlierratio real DEFAULT 0,
    dr4_9ceavgiv real DEFAULT 0,
    dr4_9peavgiv real DEFAULT 0,
    countcetotal integer DEFAULT 0,
    countceoutlier integer DEFAULT 0,
    countpetotal integer DEFAULT 0,
    countpeoutlier integer DEFAULT 0,
    dr1_6ceavgiv real DEFAULT 0,
    dr1_6peavgiv real DEFAULT 0,
    cedeltaoiworth real DEFAULT 0,
    pedeltaoiworth real DEFAULT 0,
    CONSTRAINT fk_nexcorio_option_atm_movement_data_f_main_instrument FOREIGN KEY (f_main_instrument)
        REFERENCES public.nexcorio_main_instruments (id)
) PARTITION BY RANGE (record_time);


ALTER TABLE public.nexcorio_option_atm_movement_data_partitioned 
ADD CONSTRAINT pk_nexcorio_option_atm_movement_data_partitioned PRIMARY KEY (id, record_time);

CREATE TABLE nexcorio_option_atm_movement_data_202506 
    PARTITION OF nexcorio_option_atm_movement_data_partitioned
    FOR VALUES FROM ('2025-06-01 00:00:00+00') TO ('2025-07-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);
	
CREATE TABLE nexcorio_option_atm_movement_data_202507 
    PARTITION OF nexcorio_option_atm_movement_data_partitioned
    FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2025-08-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);
	
CREATE TABLE nexcorio_option_atm_movement_data_202508
    PARTITION OF nexcorio_option_atm_movement_data_partitioned
    FOR VALUES FROM ('2025-08-01 00:00:00+00') TO ('2025-09-01 00:00:00+00')
    WITH (autovacuum_vacuum_scale_factor = 0, toast.autovacuum_vacuum_scale_factor = 0);
	
-- Index on each partition
CREATE INDEX ON nexcorio_option_atm_movement_data_202506 (f_main_instrument, record_time);
CREATE INDEX ON nexcorio_option_atm_movement_data_202507 (f_main_instrument, record_time);
CREATE INDEX ON nexcorio_option_atm_movement_data_202508 (f_main_instrument, record_time);


-- Migrate Data
INSERT INTO nexcorio_option_atm_movement_data_partitioned
SELECT * FROM nexcorio_option_atm_movement_data;

-- Rename old table
ALTER TABLE nexcorio_option_atm_movement_data RENAME TO nexcorio_option_atm_movement_data_old;

-- Rename partitioned table
ALTER TABLE nexcorio_option_atm_movement_data_partitioned RENAME TO nexcorio_option_atm_movement_data;

INSERT INTO db_versions VALUES ('0034', now(), 'Keshav', 'Migration to partition', 'Schema');
