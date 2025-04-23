alter table nexcorio_options_algo_strategy add column max_hedge_cost_per_leg real default 1.5;
alter table nexcorio_options_algo_strategy add column non_directional boolean default true;

INSERT INTO db_versions VALUES('0013', now(), 'Keshav', 'MaxHedgeCostPerLeg and directional indication','Schema' );
