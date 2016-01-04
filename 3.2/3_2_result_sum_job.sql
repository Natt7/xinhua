create job 3_2_result_sum_job(3_2_result)
begin

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/prod/data_bk/JT_J_FXFL_RJFL.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_c_bmspkfmx_dataset
(
  filename:/home/prod/data_bk/JT_C_BMSPKFMX.csv,
  serverid:0,
  schema:jt_c_bmspkfmx_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_c_khspmx_dataset
(
  filename:/home/prod/data_bk/JT_C_KHSPMX.csv,
  serverid:0,
  schema:jt_c_khspmx_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_j_spxx_dataset
(
  filename:/home/prod/data_bk/JT_J_SPXX.csv,
  serverid:0,
  schema:jt_j_spxx_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_j_cwdl_dataset
(
  filename:/home/prod/data_bk/JT_J_CWDL.csv,
  serverid:0,
  schema:jt_j_cwdl_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_j_dwxx_dataset
(
  filename:/home/prod/data_bk/JT_J_DWXX.csv,
  serverid:0,
  schema:jt_j_dwxx_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_j_dqbm_dataset
(
  filename:/home/prod/data_bk/JT_J_DQBM.csv,
  serverid:0,
  schema:jt_j_dqbm_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_g_fxflywydz_dataset
(
  filename:/home/prod/data_bk/JT_G_FXFLYWYDZ.csv,
  serverid:0,
  schema:jt_g_fxflywydz_schema,
  charset:utf-8,
  splitter:(block_size:200)
);

dataset file base_operator_dataset
(
  filename:/home/prod/data_bk/BASE_OPERATOR.csv,
  serverid:0,
  schema:base_operator_schema,
  charset:utf-8,
  splitter:(block_size:200)
);

dataproc select jt_c_khspmx_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"qmsy"),
(fname:"qmkc")
],
inputs: "jt_c_khspmx_dataset",
order_by:("bmspkfmxid")
);

dataproc select jt_c_bmspkfmx_select
(
fields: [ 
(fname:"bmspkfmxid",alias:"bmspkfmxid1"),
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
(fname:"qmsy"),
(fname:"qmkc"),
(fname:"jzrq")
],
inputs: "mx_kf_join",
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

dataproc leftjoin mx_kf_sp_join
(
inputs: (left_input: mx_kf_join_select, right_input: jt_j_spxx_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_kf_sp_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"qmsy"),
(fname:"qmkc"),
(fname:"jzrq"),
(fname:"fxflid")
],
inputs: "mx_kf_sp_join",
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
(fname:"qmsy"),
(fname:"qmkc"),
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
(fname:"cwdlid",alias:"cwdlid1"),
(fname:"cwfl")
],
inputs: "jt_j_cwdl_dataset",
order_by:("cwdlid1")
);

dataproc leftjoin mx_kf_sp_fxfl_cw_join
(
inputs: (left_input: mx_kf_sp_fxfl_join_select, right_input: jt_j_cwdl_select),
join_keys: (("left_input.cwdlid","right_input.cwdlid1"))
);

dataproc select mx_kf_sp_fxfl_cw_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"qmsy"),
(fname:"qmkc"),
(fname:"jzrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl")
],
inputs: "mx_kf_sp_fxfl_cw_join",
order_by:("khid")
);

dataproc select jt_j_dwxx_select
(
fields: [ 
(fname:"dwid",alias:"dwid1"),
(fname:"dwmc"),
(fname:"dqid")
],
inputs: "jt_j_dwxx_dataset",
order_by:("dwid1")
);

dataproc leftjoin mx_kf_sp_fxfl_cw_dw_join
(
inputs: (left_input: mx_kf_sp_fxfl_cw_join_select, right_input: jt_j_dwxx_select),
join_keys: (("left_input.khid","right_input.dwid1"))
);

dataproc select mx_kf_sp_fxfl_cw_dw_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"qmsy"),
(fname:"qmkc"),
(fname:"jzrq"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl"),
(fname:"dwmc"),
(fname:"dqid")
],
inputs: "mx_kf_sp_fxfl_cw_dw_join",
order_by:("dqid")
);

dataproc select jt_j_dqbm_select
(
fields: ( 
(fname:"dqid",alias:"dqid1",type:string),
(fname:"dqmc")
),
inputs: "jt_j_dqbm_dataset",
order_by:("dqid1")
);

dataproc leftjoin mx_kf_sp_fxfl_cw_dw_dq_join
(
inputs: (left_input: mx_kf_sp_fxfl_cw_dw_join_select, right_input: jt_j_dqbm_select),
join_keys: (("left_input.dqid","right_input.dqid1"))
);

dataproc select mx_kf_sp_fxfl_cw_dw_dq_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"qmsy"),
(fname:"qmkc"),
(fname:"jzrq"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl"),
(fname:"dwmc"),
(fname:"dqid"),
(fname:"dqmc")
],
inputs: "mx_kf_sp_fxfl_cw_dw_dq_join",
order_by:("rjfxid")
);

dataproc select jt_g_fxflywydz_select
(
fields: (
(fname:"fxflid"),
(fname:"ywyid")
),
inputs: "jt_g_fxflywydz_dataset",
order_by:("fxflid")
);

dataproc leftjoin mx_kf_sp_fxfl_cw_dw_dq_g_join
(
inputs: (left_input: mx_kf_sp_fxfl_cw_dw_dq_join_select, right_input: jt_g_fxflywydz_select),
join_keys: (("left_input.rjfxid","right_input.fxflid"))
);

dataproc select mx_kf_sp_fxfl_cw_dw_dq_g_join_select
(
fields: [ 
(fname:"bmspkfmxid"),
(fname:"spxxid"),
(fname:"khid"),
(fname:"qmmy"),
(fname:"qmsy"),
(fname:"qmkc"),
(fname:"jzrq"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl"),
(fname:"dwmc"),
(fname:"dqid"),
(fname:"dqmc"),
(fname:"ywyid")
],
inputs: "mx_kf_sp_fxfl_cw_dw_dq_g_join",
order_by:("ywyid")
);

dataproc select base_operator_select
(
fields: (
(fname:"operatorid"),
(fname:"operatorname",alias:"fxfloperatorname",type:string)
),
inputs: "base_operator_dataset",
order_by:("operatorid")
);

dataproc leftjoin mx_kf_sp_fxfl_cw_dw_dq_g_p_join
(
inputs: (left_input: mx_kf_sp_fxfl_cw_dw_dq_g_join_select, right_input: base_operator_select),
join_keys: (("left_input.ywyid","right_input.operatorid"))
);



dataproc index 3_2_result_index1
(
  inputs:"mx_kf_sp_fxfl_cw_dw_dq_g_p_join",
  table:"3_2_result",
  indexes:(3_2_result_index)
 );
 dataproc doc 3_2_result_doc
(  
  inputs:"mx_kf_sp_fxfl_cw_dw_dq_g_p_join",
  table:"3_2_result",
  format:"3_2_result_parser",
  fields:("khid","dwmc","cwdlid","cwfl","rjfxid","rjflmc","dqmc","fxfloperatorname","qmmy","qmsy","qmkc","jzrq")
); 
dataproc statistics 3_2_result_sum1
(
  	table:"3_2_result",
  	inputs:"mx_kf_sp_fxfl_cw_dw_dq_g_p_join"
);
end;
run job 3_2_result_sum_job(threads:8);
