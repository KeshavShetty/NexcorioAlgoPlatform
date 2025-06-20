delete from nexcorio_option_atm_movement_data where base_delta < 0.49 or base_delta > 0.51;

alter table nexcorio_option_atm_movement_data drop column base_delta;

DROP INDEX IF EXISTS public.nexcorio_option_atm_movement_data_idx1;

CREATE INDEX IF NOT EXISTS nexcorio_option_atm_movement_data_idx1
    ON public.nexcorio_option_atm_movement_data USING btree
    (f_main_instrument, record_time)
    TABLESPACE pg_default;
	
INSERT INTO db_versions VALUES('0019', now(), 'Keshav', 'Drop base_elta columns from ATM','Data' );