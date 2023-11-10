CREATE TABLE t1 (id INT, first_name TEXT NOT NULL, last_name VARCHAR(40), hours DOUBLE PRECISION, PRIMARY KEY (id ASC)) SPLIT AT VALUES ((10), (20), (30));

CREATE TABLE all_types (id serial PRIMARY KEY, bigintcol bigint, bitcol bit(5), varbitcol varbit(5), booleanval boolean, byteaval bytea, ch char(5), vchar varchar(25),
cidrval cidr, dt date, dp double precision, inetval inet, intervalval interval, jsonval json, jsonbval jsonb, mc macaddr, mc8 macaddr8, mn money, nm numeric, rl real,
si smallint, i4r int4range, i8r int8range, nr numrange, tsr tsrange, tstzr tstzrange, dr daterange, txt text, tm time, tmtz timetz, ts timestamp, tstz timestamptz,
uuidval uuid) WITH (COLOCATION = false);

DROP DATABASE IF EXISTS secondary_database;
CREATE DATABASE secondary_database;

CREATE TYPE enum_type AS ENUM ('ZERO', 'ONE', 'TWO');
CREATE TABLE test_enum (id INT PRIMARY KEY, enum_col enum_type);

DROP DATABASE IF EXISTS colocated_database;
CREATE DATABASE colocated_database WITH COLOCATED = true;
