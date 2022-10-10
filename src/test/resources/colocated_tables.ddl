-- all the tables in the below database will be colocated on a single tablet

CREATE TABLE test_1 (id INT PRIMARY KEY, name TEXT) WITH (COLOCATED = true);
CREATE TABLE test_2 (text_key TEXT PRIMARY KEY) WITH (COLOCATED = true);
CREATE TABLE test_3 (hours FLOAT PRIMARY KEY, hours_in_text VARCHAR(40)) WITH (COLOCATED = true);

-- Create a non colocated table as well
CREATE TABLE test_no_colocated (id INT PRIMARY KEY, name TEXT) WITH (COLOCATED = false) SPLIT INTO 3 TABLETS;