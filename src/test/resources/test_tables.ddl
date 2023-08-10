CREATE SCHEMA rs_req_dbo;

CREATE TABLE IF NOT EXISTS rs_req_dbo.req_ty (
    prdt_c varchar(2) NOT NULL,
    req_ty_c varchar(5) NOT NULL,
    dmn_nm varchar(32) NOT NULL,
    rsrc_nm varchar(32) NOT NULL,
    req_ty_ds varchar(64),
    root_cntx_x varchar(64),
    logc_del_i varchar(1),
    rec_insr_tmst timestamp with time zone NOT NULL,
    rec_upd_tmst timestamp with time zone NOT NULL,
    CONSTRAINT req_ty_pk PRIMARY KEY (req_ty_c));

CREATE TABLE IF NOT EXISTS rs_req_dbo.req (
    wi_req_id numeric(20,0) NOT NULL,
    prdt_c varchar(2) NOT NULL,
    req_ty_c varchar(5) NOT NULL,
    clnt_id numeric(28,0),
    per_id numeric(28,0) NOT NULL,
    pln_id varchar(28),
    req_stat_c varchar(1) NOT NULL,
    exp_tmst timestamp with time zone NOT NULL,
    logc_del_i varchar(1),
    rec_insr_tmst timestamp with time zone NOT NULL,
    rec_upd_tmst timestamp with time zone,
    req_src_x jsonb,
    mock_upd_usr_id varchar(8),
    prcs_sys_trk_id varchar(30),
    prcs_sys_appl_id varchar(8),
    lgcy_ctl_id varchar(20),
    acknow_tmst timestamp with time zone,
    CONSTRAINT req_pk PRIMARY KEY (wi_req_id),
    CONSTRAINT req_req_ty_fk FOREIGN KEY (req_ty_c)
        REFERENCES rs_req_dbo.req_ty (req_ty_c) MATCH SIMPLE
        ON UPDATE RESTRICT
        ON DELETE RESTRICT);

CREATE INDEX req_idx ON rs_req_dbo.req (
    COALESCE(pln_id, '~'::varchar) text_ops,
	per_id numeric_ops);

CREATE INDEX req_pln_per_ix ON rs_req_dbo.req (
    wi_req_id numeric_ops,
	pln_id text_ops,
	per_id numeric_ops);
