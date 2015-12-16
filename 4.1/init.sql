create job 4_1_result_sum_job(4_1_result)
begin

dataset table jt_j_fxfl_rjfl_dataset
(
	table:jt_j_fxfl_rjfl;
);

dataset table jt_c_gysys_dataset
(
	table:jt_c_gysys;
);

dataset table jt_j_gys_dataset
(
	table:jt_j_gys;
);

dataset file jt_g_jtdmx_dataset
(
  filename:/home/natt/xinhuadata/JT_G_JTDMX.csv,
  serverid:0,
  schema:jt_g_jtdmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_g_jtd_dataset
(
  filename:/home/natt/xinhuadata/JT_G_JTD.csv,
  serverid:0,
  schema:jt_g_jtd_schema,
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

dataproc select jt_g_jtdmx_select
(
fields: [ 
(fname:"jtdid"),
(fname:"spxxid"),
(fname:"sdmy")
],
inputs: "jt_g_jtdmx_dataset",
order_by:("jtdid")
);

dataproc select jt_g_jtd_select
(
fields: [ 
(fname:"jtdid",alais:"jtdid1"),
(fname:"gysid"),
(fname:"jzrq")
],
inputs: "jt_g_jtd_dataset",
order_by:("jtdid1")
);

dataproc leftjoin mx_jt_join
(
inputs: (left_input: jt_g_jtdmx_select, right_input: jt_g_jtd_select),
join_keys: (("left_input.jtdid","right_input.jtdid1"))
);

dataproc select mx_jt_join_select
(
fields: [ 
(fname:"jtdid"),
(fname:"spxxid"),
(fname:"sdmy"),
(fname:"gysid"),
(fname:"jzrq")
],
inputs: "mx_jt_join",
order_by:("spxxid")
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

dataproc leftjoin mx_jt_sp_join
(
inputs: (left_input: mx_jt_join_select, right_input: jt_j_spxx_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_jt_sp_join_select
(
fields: [ 
(fname:"jtdid"),
(fname:"spxxid"),
(fname:"sdmy"),
(fname:"gysid"),
(fname:"jzrq"),
(fname:"fxflid")
],
inputs: "mx_jt_sp_join",
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

dataproc leftjoin mx_jt_sp_fxfl_join
(
inputs: (left_input: mx_jt_sp_join_select, right_input: jt_j_fxfl_rjfl_select),
join_keys: (("left_input.fxflid","right_input.fxflid1"))
);

dataproc select mx_jt_sp_fxfl_join_select
(
fields: [ 
(fname:"jtdid"),
(fname:"spxxid"),
(fname:"sdmy"),
(fname:"gysid"),
(fname:"jzrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
],
inputs: "mx_jt_sp_fxfl_join",
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

dataproc leftjoin mx_jt_sp_fxfl_cw_join
(
inputs: (left_input: mx_jt_sp_fxfl_join_select, right_input: jt_j_cwdl_select),
join_keys: (("left_input.cwdlid","right_input.cwdlid1"))
);

dataproc select mx_jt_sp_fxfl_cw_join_select
(
fields: [ 
(fname:"jtdid"),
(fname:"spxxid"),
(fname:"sdmy"),
(fname:"gysid"),
(fname:"jzrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl")
],
inputs: "mx_jt_sp_fxfl_cw_join",
order_by:("gysid")
);

dataproc select jt_c_gysys_select
(
fields: [ 
(fname:"gysid",alais:"gysid1"),
(fname:"dgysid")
],
inputs: "jt_c_gysys_dataset",
order_by:("gysid1")
);

dataproc leftjoin mx_jt_sp_fxfl_cw_ys_join
(
inputs: (left_input: mx_jt_sp_fxfl_cw_join_select, right_input: jt_c_gysys_select),
join_keys: (("left_input.gysid","right_input.gysid1"))
);

dataproc select mx_jt_sp_fxfl_cw_ys_join_select
(
fields: [ 
(fname:"jtdid"),
(fname:"spxxid"),
(fname:"sdmy"),
(fname:"gysid"),
(fname:"jzrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl"),
(fname:"dgysid")
],
inputs: "mx_jt_sp_fxfl_cw_ys_join",
order_by:("dgysid")
);

dataproc select jt_j_gys_select
(
fields: [ 
(fname:"gysid",alais:"gysid1"),
(fname:"gysmc")
],
inputs: "jt_j_gys_dataset",
order_by:("gysid1")
);

dataproc leftjoin mx_jt_sp_fxfl_cw_ys_join
(
inputs: (left_input: mx_jt_sp_fxfl_cw_ys_join_select, right_input: jt_j_gys_select),
join_keys: (("left_input.dgysid","right_input.gysid1"))
);

dataproc index 4_1_result_index1
(
  inputs:"mx_jt_sp_fxfl_cw_ys_join",
  table:"4_1_result",
  indexes:(4_1_result_index)
 );
 dataproc doc 4_1_result_doc
(  
  inputs:"mx_jt_sp_fxfl_cw_ys_join",
  table:"4_1_result",
  format:"4_1_result_parser"
); 
dataproc statistics 4_1_result_sum1
(
	stat_model:"4_1_result_sum",
  	table:"4_1_result",
  	inputs:"mx_jt_sp_fxfl_cw_ys_join"
);
end;
run job 4_1_result_sum_job(threads:1);
