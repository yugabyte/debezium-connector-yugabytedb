CREATE OR REPLACE FUNCTION randomint(low INT, high INT) RETURNS INT AS $$ BEGIN RETURN floor(random()* (high - low + 1) + low); END; $$ language 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION randomstring(INT) RETURNS VARCHAR AS $$ DECLARE passedValue INT:=$1; rstring VARCHAR:=''; BEGIN SELECT string_agg(substring('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', ((RANDOM() * 51) + 1)::INT, 1), '') INTO rstring FROM generate_series(1, passedValue); RETURN rstring; END; $$ language 'plpgsql';

CREATE OR REPLACE FUNCTION randomdate(DATE, DATE) RETURNS DATE AS $$ DECLARE date1 DATE:=$1; date2 DATE:=$2; BEGIN RETURN date1 + (floor(random() * ((date2 - date1) + 1))::INT); END; $$ language 'plpgsql';

CREATE OR REPLACE FUNCTION randomdec(low INT, high INT)
  RETURNS NUMERIC AS
$$
BEGIN
  RETURN random()*(high - low) + low;

END;
$$ language 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION randomjsonb(keys_in TEXT, keys_types_in TEXT)
   RETURNS JSONB AS
$$
DECLARE
  keys TEXT[] := string_to_array(keys_in, ',');
  keys_types TEXT[] := string_to_array(keys_types_in, ',');
  val TEXT := '';
  r TEXT;
  c INT := 1;
BEGIN
  FOREACH r IN ARRAY keys
  LOOP
    val = val || '''' || r || ''', ''' || CASE keys_types[c]
                                            WHEN 'TEXT' THEN randomstring(randomint(5, 10))::TEXT
                                            WHEN 'INT' THEN randomint(1, 1000)::TEXT
                                            WHEN 'DATE' THEN randomdate(current_date-1000, current_date+1000)::TEXT
                                            WHEN 'DEC' THEN randomdec(100, 1000)::TEXT
                                            ELSE ''
                                           END || ''', ';
    c = c + 1;
  END LOOP;
  val = RTRIM(val, ', ');
  val = 'SELECT jsonb_build_object(' || val || ');';
  EXECUTE val INTO val;
  RETURN val;
END;
$$ language 'plpgsql';

CREATE SEQUENCE req_seq;
