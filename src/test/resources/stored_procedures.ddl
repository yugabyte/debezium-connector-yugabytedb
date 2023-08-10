CREATE OR REPLACE PROCEDURE rs_req_dbo.insert_rs_req_dbo_req_ty()
 AS
$$
DECLARE
  l_req_ty_c VARCHAR;
  l_prdt_c VARCHAR;
  insert_date TIMESTAMP := NOW();
BEGIN
INSERT INTO rs_req_dbo.req_ty (
  prdt_c,        -- character varying(2)
  req_ty_c,      -- character varying(5)
  dmn_nm,        -- character varying(32)
  rsrc_nm,       -- character varying(32)
  req_ty_ds,     -- character varying(64)
  root_cntx_x,   -- character varying(64)
  logc_del_i,    -- character varying(1)
  rec_insr_tmst, -- timestamp with time zone
  rec_upd_tmst   -- timestamp with time zone
)
VALUES (
  randomstring(randomint(1, 2)),  -- prdt_c
  randomstring(5),                -- req_ty_c
  randomstring(randomint(1, 32)), -- dmn_nm
  randomstring(randomint(1, 32)), -- rsrc_nm
  randomstring(randomint(1, 64)), -- req_ty_ds
  randomstring(randomint(1, 64)), -- root_cntx_x
  randomstring(1),                -- logc_del_i
  insert_date,                    -- rec_insr_tmst
  insert_date                     -- rec_upd_tmst
  ) RETURNING prdt_c, req_ty_c INTO l_prdt_c, l_req_ty_c;

INSERT INTO rs_req_dbo.req (
   wi_req_id,        -- numeric(20,0)
   prdt_c,           -- character varying(2)
   req_ty_c,         -- character varying(5)
   clnt_id,          -- numeric(28,0)
   per_id,           -- numeric(28,0)
   pln_id,           -- character varying(28)
   req_stat_c,       -- character varying(1)
   exp_tmst,         -- timestamp with time zone
   logc_del_i,       -- character varying(1)
   rec_insr_tmst,    -- timestamp with time zone
   rec_upd_tmst,     -- timestamp with time zone
   req_src_x,        -- jsonb
   mock_upd_usr_id,  -- character varying(8)
   prcs_sys_trk_id,  -- character varying(30)
   prcs_sys_appl_id, -- character varying(8)
   lgcy_ctl_id,      -- character varying(20)
   acknow_tmst       -- timestamp with time zone
   )
SELECT
   nextval('req_seq'),                          -- wi_req_id
   l_prdt_c,                                    -- prdt_c
   l_req_ty_c,                                  -- req_ty_c
   randomint(1, 1000000),                       -- clnt_id
   randomint(1, 1000000),                       -- per_id
   randomstring(randomint(1, 28)),              -- pln_id
   randomstring(1),                             -- req_stat_c
   randomdate('01/01/2015', '12/31/2025'),      -- exp_tmst
   randomstring(1),                             -- logc_del_i
   insert_date,                                 -- rec_insr_tmst
   insert_date,                                 -- rec_upd_tmst
   randomjsonb('a,b,c,d', 'TEXT,INT,DATE,DEC'), -- req_src_x
   randomstring(randomint(1, 8)),               -- mock_upd_usr_id
   randomstring(randomint(1, 30)),              -- prcs_sys_trk_id
   randomstring(randomint(1, 8)),               -- prcs_sys_appl_id
   randomstring(randomint(1, 20)),              -- lgcy_ctl_id
   randomdate('01/01/2015', '12/31/2025')       -- acknow_tmst
FROM generate_series(1, 3);

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE rs_req_dbo.delete_rs_req_dbo_req_ty() AS
$$
BEGIN
  DELETE FROM rs_req_dbo.req WHERE wi_req_id = (SELECT wi_req_id FROM rs_req_dbo.req ORDER BY random() LIMIT 1);

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE rs_req_dbo.update_rs_req_dbo_req_ty() AS
$$
BEGIN
  UPDATE rs_req_dbo.req SET req_src_x = randomjsonb('a,b,c,d', 'TEXT,INT,DATE,DEC') WHERE wi_req_id = (SELECT wi_req_id FROM rs_req_dbo.req ORDER BY random() LIMIT 1);

END;
$$ LANGUAGE plpgsql;
