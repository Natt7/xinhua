create job 3_1_result_sum_job(3_1_result)
begin

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/natt/xinhuadata/jt_j_fxfl_rjfl.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_c_bmspkfmx_dataset
(
  filename:/home/natt/xinhuadata/jt_c_bmspkfmx.csv,
  serverid:0,
  schema:jt_c_bmspkfmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_c_khspmx_dataset
(
  filename:/home/natt/xinhuadata/jt_c_khspmx.csv,
  serverid:0,
  schema:jt_c_khspmx_schema,
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

dataproc select jt_c_bmspkfmx_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"qmmy",alias:"zbqmmy"),
(fname:"jzrq")
],
inputs: "jt_c_bmspkfmx_dataset",
order_by:("bmspkfmxid")
);

dataproc select jt_c_khspmx_select
(
fields: [ 
(fname:"bmspkfmxid",alias:"bmspkfmxid1"),
(fname:"qmmy",alias:"khqmmy")
],
inputs: "jt_c_khspmx_dataset",
order_by:("bmspkfmxid1"),
group_by:("bmspkfmxid1")
);

dataproc leftjoin zb_kh_join
(
inputs: (left_input: jt_c_bmspkfmx_select, right_input: jt_c_khspmx_select),
join_keys: (("left_input.bmspkfmxid","right_input.bmspkfmxid1"))
);

dataproc select zb_kh_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"zbqmmy"),
(fname:"jzrq"),
(fname:"khqmmy")
],
inputs: "zb_kh_join",
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

dataproc leftjoin zb_kh_sp_join
(
inputs: (left_input: zb_kh_join_select, right_input: jt_j_spxx_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select zb_kh_sp_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"zbqmmy"),
(fname:"jzrq"),
(fname:"khqmmy"),
(fname:"fxflid")
],
inputs: "zb_kh_sp_join",
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

dataproc leftjoin zb_kh_sp_fxfl_join
(
inputs: (left_input: zb_kh_sp_join_select, right_input: jt_j_fxfl_rjfl_select),
join_keys: (("left_input.fxflid","right_input.fxflid1"))
);

dataproc select zb_kh_sp_fxfl_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"Nvl(zbqmmy, 0)+Nvl(khqmmy, 0)",type:double,alias:"qmmy"),
(fname:"jzrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
],
inputs: "zb_kh_sp_fxfl_join",
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

dataproc leftjoin zb_kh_sp_fxfl_cw_join
(
inputs: (left_input: zb_kh_sp_fxfl_join_select, right_input: jt_j_cwdl_select),
join_keys: (("left_input.cwdlid","right_input.cwdlid1"))
);

dataproc index 3_1_result_index1
(
  inputs:"zb_kh_sp_fxfl_cw_join",
  table:"3_1_result",
  indexes:(3_1_result_index)
 );
 dataproc doc 3_1_result_doc
(  
  inputs:"zb_kh_sp_fxfl_cw_join",
  table:"3_1_result",
  format:"3_1_result_parser",
  fields:("cwdlid","cwfl","rjfxid","rjflmc","qmmy","jzrq")
); 
dataproc statistics 3_1_result_sum1
(
	stat_model:"3_1_result_sum",
  	table:"3_1_result",
  	inputs:"zb_kh_sp_fxfl_cw_join"
);
end;
run job 3_1_result_sum_job(threads:8);
