create job 3_2_result_sum_job(3_2_result)
begin

dataset table jt_j_fxfl_rjfl_dataset
(
	table:jt_j_fxfl_rjfl;
);

dataset file jt_c_bmspkfmx_dataset
(
  filename:/home/natt/xinhuadata/JT_C_BMSPKFMX.csv,
  serverid:0,
  schema:jt_c_bmspkfmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_c_khspmx_dataset
(
  filename:/home/natt/xinhuadata/JT_C_KHSPMX.csv,
  serverid:0,
  schema:jt_c_khspmx_schema,
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

dataset file jt_j_dwxx_dataset
(
  filename:/home/natt/xinhuadata/JT_J_DWXX.csv,
  serverid:0,
  schema:jt_j_dwxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_c_khspmx_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy")
],
inputs: "jt_c_khspmx_dataset",
order_by:("bmspkfmxid")
);

dataproc select jt_c_bmspkfmx_select
(
fields: [ 
(fname:"bmspkfmxid",alais:"bmspkfmxid1"),
(fname:"jzrq")
],
inputs: "jt_c_bmspkfmx_dataset",
order_by:("bmspkfmxid1")
);

dataproc leftjoin mx_kf_join
(
inputs: (left_input: jt_c_khspmx_select, right_input: jt_c_bmspkfmx_select),
join_keys: (("left_input.bmspkfmxid","right_input.bmspkfmxid1"))
);

dataproc select mx_kf_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"jzrq")
],
inputs: "mx_kf_join",
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

dataproc leftjoin mx_kf_sp_join
(
inputs: (left_input: zb_kh_join_select, right_input: jt_j_spxx_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_kf_sp_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"jzrq"),
(fname:"fxflid")
],
inputs: "mx_kf_sp_join",
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

dataproc leftjoin mx_kf_sp_fxfl_join
(
inputs: (left_input: mx_kf_sp_join_select, right_input: jt_j_fxfl_rjfl_select),
join_keys: (("left_input.fxflid","right_input.fxflid1"))
);

dataproc select mx_kf_sp_fxfl_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"jzrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
],
inputs: "mx_kf_sp_fxfl_join",
order_by:("cwdlid")
);

dataproc select jt_j_cwdl_select
(
fields: [ 
(fname:"cwdlid",alais:"cwdlid1"),
(fname:"dwmc")
],
inputs: "jt_j_cwdl_dataset",
order_by:("cwdlid1")
);

dataproc leftjoin mx_kf_sp_fxfl_cw_join
(
inputs: (left_input: mx_kf_sp_fxfl_join_select, right_input: jt_j_cwdl_select),
join_keys: (("left_input.cwdlid","right_input.cwdlid1"))
);

dataproc index 3_2_result_index1
(
  inputs:"mx_kf_sp_fxfl_cw_join",
  table:"3_2_result",
  indexes:(3_2_result_index)
 );
 dataproc doc 3_2_result_doc
(  
  inputs:"mx_kf_sp_fxfl_cw_join",
  table:"3_2_result",
  format:"3_2_result_parser"
); 
dataproc statistics 3_2_result_sum1
(
	stat_model:"3_2_result_sum",
  	table:"3_2_result",
  	inputs:"mx_kf_sp_fxfl_cw_join"
);
end;
run job 3_2_result_sum_job(threads:1);