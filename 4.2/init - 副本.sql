create job 4_2_result_sum_job(4_2_result)
begin

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/natt/xinhuadata/jt_j_fxfl_rjfl.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_x_xtdmx_dataset
(
  filename:/home/natt/xinhuadata/jt_x_xtdmx.csv,
  serverid:0,
  schema:jt_x_xtdmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_x_xtd_dataset
(
  filename:/home/natt/xinhuadata/jt_x_xtd.csv,
  serverid:0,
  schema:jt_x_xtd_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_spxx_dataset
(
  filename:/home/natt/xinhuadata/jt_j_spxx.csv,
  serverid:0,
  schema:jt_j_spxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_cwdl_dataset
(
  filename:/home/natt/xinhuadata/jt_j_cwdl.csv,
  serverid:0,
  schema:jt_j_cwdl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_dwxx_dataset
(
  filename:/home/natt/xinhuadata/jt_j_dwxx.csv,
  serverid:0,
  schema:jt_j_dwxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_x_xtdmx_select
(
fields: [ 
(fname:"xtdid"),
(fname:"spxxid"),
(fname:"xtmy")
],
inputs: "jt_x_xtdmx_dataset",
order_by:("xtdid")
);

dataproc select jt_x_xtd_select
(
fields: [ 
(fname:"xtdid",alias:"xtdid1"),
(fname:"khid"),
(fname:"wlshrq")
],
inputs: "jt_x_xtd_dataset",
order_by:("xtdid1")
);

dataproc leftjoin mx_xt_join
(
inputs: (left_input: jt_x_xtdmx_select, right_input: jt_x_xtd_select),
join_keys: (("left_input.xtdid","right_input.xtdid1"))
);

dataproc select mx_xt_join_select
(
fields: [ 
(fname:"xtdid"),
(fname:"spxxid"),
(fname:"xtmy"),
(fname:"khid"),
(fname:"wlshrq")
],
inputs: "mx_xt_join",
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

dataproc leftjoin mx_xt_sp_join
(
inputs: (left_input: mx_xt_join_select, right_input: jt_j_spxx_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_xt_sp_join_select
(
fields: [ 
(fname:"xtdid"),
(fname:"spxxid"),
(fname:"xtmy"),
(fname:"khid"),
(fname:"wlshrq"),
(fname:"fxflid")
],
inputs: "mx_xt_sp_join",
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

dataproc leftjoin mx_xt_sp_fxfl_join
(
inputs: (left_input: mx_xt_sp_join_select, right_input: jt_j_fxfl_rjfl_select),
join_keys: (("left_input.fxflid","right_input.fxflid1"))
);

dataproc select mx_xt_sp_fxfl_join_select
(
fields: [ 
(fname:"xtdid"),
(fname:"spxxid"),
(fname:"xtmy"),
(fname:"khid"),
(fname:"wlshrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
],
inputs: "mx_xt_sp_fxfl_join",
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

dataproc leftjoin mx_xt_sp_fxfl_cw_join
(
inputs: (left_input: mx_xt_sp_fxfl_join_select, right_input: jt_j_cwdl_select),
join_keys: (("left_input.cwdlid","right_input.cwdlid1"))
);

dataproc select mx_xt_sp_fxfl_cw_join_select
(
fields: [ 
(fname:"xtdid"),
(fname:"spxxid"),
(fname:"xtmy"),
(fname:"khid"),
(fname:"wlshrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl")
],
inputs: "mx_xt_sp_fxfl_cw_join",
order_by:("khid")
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

dataproc leftjoin mx_xt_sp_fxfl_cw_dw_join
(
inputs: (left_input: mx_xt_sp_fxfl_cw_join_select, right_input: jt_j_dwxx_select),
join_keys: (("left_input.khid","right_input.dwid1"))
);

dataproc index 4_2_result_index1
(
  inputs:"mx_xt_sp_fxfl_cw_dw_join",
  table:"4_2_result",
  indexes:(4_2_result_index)
 );
 dataproc doc 4_2_result_doc
(  
  inputs:"mx_xt_sp_fxfl_cw_dw_join",
  table:"4_2_result",
  format:"4_2_result_parser",
  fields:("cwdlid","cwfl","rjfxid","rjflmc","khid","dwmc","xtmy","wlshrq")  
); 
dataproc statistics 4_2_result_sum1
(
	stat_model:"4_2_result_sum",
  	table:"4_2_result",
  	inputs:"mx_xt_sp_fxfl_cw_dw_join"
);
end;
run job 4_2_result_sum_job(threads:8);
