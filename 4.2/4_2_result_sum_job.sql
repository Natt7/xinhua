create job 4_2_result_sum_job(4_2_result)
begin

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/prod/data_bk/JT_J_FXFL_RJFL.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_x_xtdmx_dataset
(
  filename:/home/prod/data_bk/JT_X_XTDMX.csv,
  serverid:0,
  schema:jt_x_xtdmx_schema,
  charset:utf-8,
  splitter:(block_size:400)
);

dataset file jt_x_xtd_dataset
(
  filename:/home/prod/data_bk/JT_X_XTD.csv,
  serverid:0,
  schema:jt_x_xtd_schema,
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

dataproc select jt_x_xtd_select
(
fields: [ 
(fname:"xtdid"),
(fname:"khid"),
(fname:"wlshrq")
],
inputs: "jt_x_xtd_dataset",
order_by:("khid")
);

dataproc select jt_j_dwxx_select
(
fields: [ 
(fname:"dwid"),
(fname:"dwmc"),
(fname:"dqid")
],
inputs: "jt_j_dwxx_dataset",
order_by:("dwid")
);

dataproc leftjoin xt_dw_join
(
inputs: (left_input: jt_x_xtd_select, right_input: jt_j_dwxx_select),
join_keys: (("left_input.khid","right_input.dwid"))
);

dataproc select xt_dw_join_select
(
fields: [ 
(fname:"xtdid",alias:"xtdid1"),
(fname:"khid"),
(fname:"wlshrq"),
(fname:"dwmc"),
(fname:"dqid")
],
inputs: "xt_dw_join",
order_by:("xtdid1")
);




dataproc select jt_j_spxx_select
(
fields: ( 
(fname:"spxxid"),
(fname:"fxflid",type:"string")
),
inputs: "jt_j_spxx_dataset",
order_by:("fxflid")
);

dataproc select jt_j_fxfl_rjfl_select
(
fields: ( 
(fname:"fxflid",type:"string",alias:"fxflid1"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
),
inputs: "jt_j_fxfl_rjfl_dataset",
order_by:("fxflid1")
);

dataproc leftjoin sp_fxfl_join
(
inputs: (left_input: jt_j_spxx_select, right_input: jt_j_fxfl_rjfl_select),
join_keys: (("left_input.fxflid","right_input.fxflid1"))
);

dataproc select sp_fxfl_join_select
(
fields: ( 
(fname:"spxxid"),
(fname:"fxflid"),
(fname:"cwdlid",type:"string"),
(fname:"rjfxid"),
(fname:"rjflmc")
),
inputs: "sp_fxfl_join",
order_by:("cwdlid")
);

dataproc select jt_j_cwdl_select
(
fields: ( 
(fname:"cwdlid",type:"string",alias:"cwdlid1"),
(fname:"cwfl")
),
inputs: "jt_j_cwdl_dataset",
order_by:("cwdlid1")
);

dataproc leftjoin sp_fxfl_cw_join
(
inputs: (left_input: sp_fxfl_join_select, right_input: jt_j_cwdl_select),
join_keys: (("left_input.cwdlid","right_input.cwdlid1"))
);

dataproc select sp_fxfl_cw_join_select
(
fields: ( 
(fname:"spxxid",type:"string",alias:"spxxid1"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl")
),
inputs: "sp_fxfl_cw_join",
order_by:("spxxid1")
);








dataproc select jt_x_xtdmx_select
(
fields: [ 
(fname:"xtdid"),
(fname:"spxxid"),
(fname:"xtmy"),
(fname:"xtsy"),
(fname:"xtsl")
],
inputs: "jt_x_xtdmx_dataset",
order_by:("xtdid")
);

dataproc leftjoin mx_xt_dw_join
(
inputs: (left_input: jt_x_xtdmx_select, right_input: xt_dw_join_select),
join_keys: (("left_input.xtdid","right_input.xtdid1"))
);

dataproc select mx_xt_dw_join_select
(
fields: [ 
(fname:"xtdid"),
(fname:"spxxid"),
(fname:"xtmy"),
(fname:"xtsy"),
(fname:"xtsl"),
(fname:"khid"),
(fname:"wlshrq"),
(fname:"dwmc"),
(fname:"dqid")
],
inputs: "mx_xt_dw_join",
order_by:("spxxid")
);

dataproc leftjoin mx_xt_dw_sp_fxfl_cw_join
(
inputs: (left_input: mx_xt_dw_join_select, right_input: sp_fxfl_cw_join_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_xt_dw_sp_fxfl_cw_join_select
(
fields: [ 
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"khid"),
(fname:"dwmc"),
(fname:"dqid"),
(fname:"xtmy"),
(fname:"xtsy"),
(fname:"xtsl"),
(fname:"wlshrq")
],
inputs: "mx_xt_dw_sp_fxfl_cw_join",
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

dataproc leftjoin mx_xt_dw_sp_fxfl_cw_dq_join
(
inputs: (left_input: mx_xt_dw_sp_fxfl_cw_join_select, right_input: jt_j_dqbm_select),
join_keys: (("left_input.dqid","right_input.dqid1"))
);

dataproc select mx_xt_dw_sp_fxfl_cw_dq_join_select
(
fields: [ 
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"khid"),
(fname:"dwmc"),
(fname:"dqid"),
(fname:"xtmy"),
(fname:"xtsy"),
(fname:"xtsl"),
(fname:"wlshrq"),
(fname:"dqmc")
],
inputs: "mx_xt_dw_sp_fxfl_cw_dq_join",
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

dataproc leftjoin mx_xt_dw_sp_fxfl_cw_dq_g_join
(
inputs: (left_input: mx_xt_dw_sp_fxfl_cw_dq_join_select, right_input: jt_g_fxflywydz_select),
join_keys: (("left_input.rjfxid","right_input.fxflid"))
);

dataproc select mx_xt_dw_sp_fxfl_cw_dq_g_join_select
(
fields: [ 
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"khid"),
(fname:"dwmc"),
(fname:"dqid"),
(fname:"xtmy"),
(fname:"xtsy"),
(fname:"xtsl"),
(fname:"wlshrq"),
(fname:"dqmc"),
(fname:"ywyid")
],
inputs: "mx_xt_dw_sp_fxfl_cw_dq_g_join",
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

dataproc leftjoin mx_xt_dw_sp_fxfl_cw_dq_g_p_join
(
inputs: (left_input: mx_xt_dw_sp_fxfl_cw_dq_g_join_select, right_input: base_operator_select),
join_keys: (("left_input.ywyid","right_input.operatorid"))
);



dataproc index 4_2_result_index1
(
  inputs:"mx_xt_dw_sp_fxfl_cw_dq_g_p_join",
  table:"4_2_result",
  indexes:(4_2_result_index)
 );
 dataproc doc 4_2_result_doc
(  
  inputs:"mx_xt_dw_sp_fxfl_cw_dq_g_p_join",
  table:"4_2_result",
  format:"4_2_result_parser",
  fields:("cwdlid","cwfl","rjfxid","rjflmc","khid","dwmc","dqmc","fxfloperatorname","xtmy","xtsy","xtsl","wlshrq")  
); 
dataproc statistics 4_2_result_sum1
(
  	table:"4_2_result",
  	inputs:"mx_xt_dw_sp_fxfl_cw_dq_g_p_join"
);
end;
run job 4_2_result_sum_job(threads:8);
