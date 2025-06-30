ALTER TABLE nexcorio_option_atm_movement_data 
ADD COLUMN selective10strike_avgcegamma REAL DEFAULT 0,
ADD COLUMN selective10strike_avgpegamma REAL DEFAULT 0,
ADD COLUMN selective10strike_avgceiv REAL DEFAULT 0,
ADD COLUMN selective10strike_avgpeiv REAL DEFAULT 0,
ADD COLUMN selective20strike_avgcegamma REAL DEFAULT 0,
ADD COLUMN selective20strike_avgpegamma REAL DEFAULT 0,
ADD COLUMN selective20strike_avgceiv REAL DEFAULT 0,
ADD COLUMN selective20strike_avgpeiv REAL DEFAULT 0;

alter table nexcorio_option_atm_movement_data
add column deltaRangeCEAvgLtp real default 0, 
add column deltaRangeCEAvgIv real default 0, 
add column deltaRangeCEAvgDelta real default 0, 
add column deltaRangeCEAvgGamma real default 0, 
add column deltaRangeCEAvgVega real default 0,
add column deltaRangePEAvgLtp real default 0, 
add column deltaRangePEAvgIv real default 0, 
add column deltaRangePEAvgDelta real default 0, 
add column deltaRangePEAvgGamma real default 0, 
add column deltaRangePEAvgVega real default 0;

INSERT INTO db_versions VALUES ('0022', now(), 'Keshav', 'Additional selective strike columns', 'Schema');