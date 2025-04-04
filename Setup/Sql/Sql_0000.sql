CREATE TABLE db_versions (
  dbversion VARCHAR(6) NOT NULL,
  record_date DATE default now(),
  author VARCHAR(20) NULL,
  reason VARCHAR(200) NULL,
  type   VARCHAR(50) NULL, 
  PRIMARY KEY(dbversion,type)
);

INSERT INTO db_versions VALUES('DBVER', now(), 'Keshav', 'Setup db version table','Schema' );