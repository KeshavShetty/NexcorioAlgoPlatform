alter table nexcorio_option_greeks add column f_main_instrument bigint;

alter table nexcorio_option_greeks add CONSTRAINT fk_nexcorio_option_greeks_f_main_instrument FOREIGN KEY (f_main_instrument) REFERENCES public.nexcorio_main_instruments (id);

update nexcorio_option_greeks as nog set f_main_instrument = (select f_main_instrument from nexcorio_tick_data where id = nog.id);

INSERT INTO db_versions VALUES ('0033', now(), 'Keshav', 'f_main instrument in nexcorio_option_greeks', 'Schema');