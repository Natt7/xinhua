create job 2_2_result_sum_job(2_2_result)
begin

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/prod/data_bk/JT_J_FXFL_RJFL.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_x_xsdmx_sb_dataset
(
  filename:/home/prod/data_bk/JT_X_XSDMX_SB.csv,
  serverid:0,
  schema:jt_x_xsdmx_sb_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_x_xsd_sb_dataset
(
  filename:/home/prod/data_bk/JT_X_XSD_SB.csv,
  serverid:0,
  schema:jt_x_xsd_sb_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file ls_jt_x_xtdmx_dataset
(
  filename:/home/prod/data_bk/LS_JT_X_XTDMX.csv,
  serverid:0,
  schema:ls_jt_x_xtdmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file ls_jt_x_xtd_dataset
(
  filename:/home/prod/data_bk/LS_JT_X_XTD.csv,
  serverid:0,
  schema:ls_jt_x_xtd_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_spxx_dataset
(
  filename:/home/prod/data_bk/JT_J_SPXX.csv,
  serverid:0,
  schema:jt_j_spxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_cwdl_dataset
(
  filename:/home/prod/data_bk/JT_J_CWDL.csv,
  serverid:0,
  schema:jt_j_cwdl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_dwxx_dataset
(
  filename:/home/prod/data_bk/JT_J_DWXX.csv,
  serverid:0,
  schema:jt_j_dwxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_x_xsdmx_sb_select
(
fields: [ 
(fname:"xsdid"),
(fname:"ztid"),
(fname:"spxxid"),
(fname:"xssl"),
(fname:"xsmy"),
(fname:"xssy")
],
inputs: "jt_x_xsdmx_sb_dataset",
order_by:("xsdid","ztid")
);

dataproc select jt_x_xsd_sb_select
(
fields: [ 
(fname:"xsdid",alias:"xsdid1"),
(fname:"ztid",alias:"ztid1"),
(fname:"xslx"),
(fname:"xsrq")
],
inputs: "jt_x_xsd_sb_dataset",
order_by:("xsdid1","ztid1")
);

dataproc leftjoin xs_xd_join
(
inputs: (left_input: jt_x_xsdmx_sb_select, right_input: jt_x_xsd_sb_select),
join_keys: (("left_input.xsdid","right_input.xsdid1"),("left_input.ztid","right_input.ztid1"))
);

dataproc select xs_xd_join_select
(
fields: [ 
(fname:"ztid"),
(fname:"spxxid"),
(fname:"xslx"),
(fname:"xssl"),
(fname:"xsmy"),
(fname:"xssy"),
(fname:"xsrq")
],
inputs: "xs_xd_join"
);

dataproc select ls_jt_x_xtdmx_select
(
fields: [ 
(fname:"xtdid"),
(fname:"ztid"),
(fname:"spxxid"),
(fname:"0-xtsl",type:"double",alias:"xtsl"),
(fname:"0-xtmy",type:"double",alias:"xtmy"),
(fname:"0-xtsy",type:"double",alias:"xtsy")
],
inputs: "ls_jt_x_xtdmx_dataset",
order_by:("xtdid","ztid")
);

dataproc select ls_jt_x_xtd_select
(
fields: [ 
(fname:"xtdid",alias:"xtdid1"),
(fname:"ztid",alias:"ztid1"),
(fname:"xslx"),
(fname:"xsrq")
],
inputs: "ls_jt_x_xtd_dataset",
order_by:("xtdid1","ztid1")
);

dataproc leftjoin xtmx_xt_join
(
inputs: (left_input: ls_jt_x_xtdmx_select, right_input: ls_jt_x_xtd_select),
join_keys: (("left_input.xtdid","right_input.xtdid1"),("left_input.ztid","right_input.ztid1"))
);

dataproc select xtmx_xt_join_select
(
fields: [ 
(fname:"ztid"),
(fname:"spxxid"),
(fname:"xslx"),
(fname:"xtsl"),
(fname:"xtmy"),
(fname:"xtsy"),
(fname:"xsrq")
],
inputs: "xtmx_xt_join"
);

dataproc union xs_xd_xtmx_xt_union
(
    inputs: (xs_xd_join_select, xtmx_xt_join_select),
	fields:
	(
		(fname:"ztid"),
		(fname:"spxxid"),
		(fname:"xslx"),
		(fname:"xssl"),
		(fname:"xsmy"),
		(fname:"xssy"),
		(fname:"xsrq")
	) 
);

dataproc select xs_xd_xtmx_xt_union_select
(
fields: ( 
		(fname:"ztid"),
		(fname:"spxxid"),
		(fname:"xslx"),
		(fname:"xssl"),
		(fname:"xsmy"),
		(fname:"xssy"),
		(fname:"xsrq")
),
inputs: "xs_xd_xtmx_xt_union",
order_by:("spxxid")
);



dataproc select jt_j_spxx_select
(
fields: [ 
(fname:"spxxid",alias:"spxxid1"),
(fname:"fxflid")
],
inputs: "jt_j_spxx_dataset",
order_by:("spxxid1")
);

dataproc leftjoin mx_sp_join
(
inputs: (left_input: xs_xd_xtmx_xt_union_select, right_input: jt_j_spxx_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_sp_join_select
(
fields: [ 
(fname:"ztid"),
(fname:"spxxid"),
(fname:"xslx"),
(fname:"xssl"),
(fname:"xsmy"),
(fname:"xssy"),
(fname:"xsrq"),
(fname:"fxflid")
],
inputs: "mx_sp_join",
order_by:("fxflid")
);

dataproc select jt_j_fxfl_rjfl_select
(
fields: [ 
(fname:"fxflid",alias:"fxflid1"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
],
inputs: "jt_j_fxfl_rjfl_dataset",
order_by:("fxflid1")
);

dataproc leftjoin mx_sp_fxfl_join
(
inputs: (left_input: mx_sp_join_select, right_input: jt_j_fxfl_rjfl_select),
join_keys: (("left_input.fxflid","right_input.fxflid1"))
);

dataproc select mx_sp_fxfl_join_select
(
fields: [ 
(fname:"ztid"),
(fname:"spxxid"),
(fname:"xslx"),
(fname:"xssl"),
(fname:"xsmy"),
(fname:"xssy"),
(fname:"xsrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
],
inputs: "mx_sp_fxfl_join",
order_by:("cwdlid")
);

dataproc select jt_j_cwdl_select
(
fields: [ 
(fname:"cwdlid",alias:"cwdlid1"),
(fname:"cwfl")
],
inputs: "jt_j_cwdl_dataset",
order_by:("cwdlid1")
);

dataproc leftjoin mx_sp_fxfl_cw_join
(
inputs: (left_input: mx_sp_fxfl_join_select, right_input: jt_j_cwdl_select),
join_keys: (("left_input.cwdlid","right_input.cwdlid1"))
);

dataproc select mx_sp_fxfl_cw_join_select
(
fields: [ 
(fname:"ztid"),
(fname:"spxxid"),
(fname:"xslx"),
(fname:"xssl"),
(fname:"xsmy"),
(fname:"xssy"),
(fname:"xsrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl")
],
inputs: "mx_sp_fxfl_cw_join",
order_by:("ztid")
);

dataproc select jt_j_dwxx_select
(
fields: [ 
(fname:"dwid",alias:"dwid1"),
(fname:"dwmc")
],
inputs: "jt_j_dwxx_dataset",
order_by:("dwid1")
);

dataproc leftjoin mx_sp_fxfl_cw_dw_join
(
inputs: (left_input: mx_sp_fxfl_cw_join_select, right_input: jt_j_dwxx_select),
join_keys: (("left_input.ztid","right_input.dwid1"))
);


dataproc index 2_2_result_index1
(
  inputs:"mx_sp_fxfl_cw_dw_join",
  table:"2_2_result",
  indexes:(2_2_result_index)
 );
 dataproc doc 1_2_result_doc
(  
  inputs:"mx_sp_fxfl_cw_dw_join",
  table:"2_2_result",
  format:"2_2_result_parser",
  fields:("ztid","dwmc","cwdlid","cwfl","rjfxid","rjflmc","xslx","xsmy","spxxid","xsrq")
); 
dataproc statistics 2_2_result_sum1
(
	stat_model:"2_2_result_sum",
  	table:"2_2_result",
  	inputs:"mx_sp_fxfl_cw_dw_join"
);
end;
run job 2_2_result_sum_job(threads:8);
