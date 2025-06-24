DROP INDEX IF EXISTS public.nexcorio_option_atm_movement_data_idx1;

CREATE INDEX IF NOT EXISTS nexcorio_option_atm_movement_data_idx1
    ON public.nexcorio_option_atm_movement_data USING btree
    (record_time)
    TABLESPACE pg_default;
	
DROP INDEX IF EXISTS public.nexcorio_option_greeks_idx1;
DROP INDEX IF EXISTS public.nexcorio_option_greeks_idx2;

CREATE INDEX IF NOT EXISTS nexcorio_option_greeks_idx1
    ON public.nexcorio_option_greeks USING btree
    (quote_time)
    TABLESPACE pg_default;
	
INSERT INTO db_versions VALUES('0021', now(), 'Keshav', 'Rearrnged the indexes for performance','Schema' );
