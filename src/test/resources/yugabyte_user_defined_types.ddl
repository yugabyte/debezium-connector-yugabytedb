CREATE TYPE my_name_type AS (first_name text, last_name varchar(40));

CREATE TABLE udt_table (id INT PRIMARY KEY, name_col my_name_type);