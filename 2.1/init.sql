create job 2_1_result_sum_job(2_1_result)
begin

dataset table jt_j_fxfl_rjfl_dataset
(
	table:jt_j_fxfl_rjfl;
);

dataset file jt_x_xsdmx_sb_dataset
(
  filename:/home/natt/xinhuadata/JT_X_XSDMX_SB.csv,
  serverid:0,
  schema:jt_x_xsdmx_sb_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_x_xsd_sb_dataset
(
  filename:/home/natt/xinhuadata/JT_X_XSD_SB.csv,
  serverid:0,
  schema:jt_x_xsd_sb_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file ls_jt_x_xtdmx_dataset
(
  filename:/home/natt/xinhuadata/LS_JT_X_XTDMX.csv,
  serverid:0,
  schema:ls_jt_x_xtdmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file ls_jt_x_xtd_dataset
(
  filename:/home/natt/xinhuadata/LS_JT_X_XTD.csv,
  serverid:0,
  schema:ls_jt_x_xtd_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_spxx_dataset
(
  filename:/home/natt/xinhuadata/JT_J_SPXX.csv,
  serverid:0,
  schema:jt_j_spxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_cwdl_dataset
(
  filename:/home/natt/xinhuadata/JT_J_CWDL.csv,
  serverid:0,
  schema:jt_j_cwdl_schema,
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
(fname:"xsdid",alais:"xsdid1"),
(fname:"ztid",alais:"ztid1"),
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
(fname:"-xtsl"),
(fname:"-xtmy"),
(fname:"-xtsy")
],
inputs: "ls_jt_x_xtdmx_dataset",
order_by:("xsdid","ztid")
);

dataproc select ls_jt_x_xtd_select
(
fields: [ 
(fname:"xtdid",alais:"xtdid1"),
(fname:"ztid",alais:"ztid1"),
(fname:"xslx"),
(fname:"xsrq")
],
inputs: "ls_jt_x_xtd_dataset",
order_by:("xsdid1","ztid1")
);

dataproc leftjoin xtmx_xt_join
(
inputs: (left_input: ls_jt_x_xtdmx_select, right_input: ls_jt_x_xtd_select),
join_keys: (("left_input.xsdid","right_input.xsdid1"),("left_input.ztid","right_input.ztid1"))
);

dataproc select xtmx_xt_join_select
(
fields: [ 
(fname:"ztid"),
(fname:"spxxid"),
(fname:"xslx"),
(fname:"-xtsl"),
(fname:"-xtmy"),
(fname:"-xtsy"),
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


dataproc select jt_j_spxx_select
(
fields: [ 
(fname:"spxxid",alais:"spxxid1"),
(fname:"fxflid")
],
inputs: "jt_j_spxx_dataset",
order_by:("spxxid1")
);

dataproc leftjoin mx_sp_join
(
inputs: (left_input: xs_xd_xtmx_xt_union, right_input: jt_j_spxx_select),
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
(fname:"fxflid",alais:"fxflid1"),
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
(fname:"cwdlid",alais:"cwdlid1"),
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


dataproc index 2_1_result_index1
(
  inputs:"mx_sp_fxfl_cw_join",
  table:"2_1_result",
  indexes:(2_1_result_index)
 );
 dataproc doc 1_2_result_doc
(  
  inputs:"mx_sp_fxfl_cw_join",
  table:"2_1_result",
  format:"2_1_result_parser"
); 
dataproc statistics 2_1_result_sum1
(
	stat_model:"2_1_result_sum",
  	table:"2_1_result",
  	inputs:"mx_sp_fxfl_cw_join"
);
end;
run job 2_1_result_sum_job(threads:1);
