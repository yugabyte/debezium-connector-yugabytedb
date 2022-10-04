-- all the tables in the below database will be colocated on a single tablet
CREATE DATABASE colocated_database WITH colocated=true;

-- Connect to the database
\c colocated_database;

CREATE TABLE test_1 (id INT PRIMARY KEY, name TEXT) WITH (colocated=true);
CREATE TABLE test_2 (text_key TEXT PRIMARY KEY) WITH (colocated=true);
CREATE TABLE test_3 (hours FLOAT PRIMARY KEY, hours_in_text VARCHAR(40)) WITH (colocated=true);

-- Create a non colocated table as well
CREATE TABLE test_no_colocated (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS WITH (colocated=false);