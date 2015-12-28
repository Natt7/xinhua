create job 1_1_result_sum_job(1_1_result)
begin
dataset file jt_j_gys_dataset
(
  filename:/home/prod/xinhuadata/jt_j_gys.csv,
  serverid:0,
  schema:jt_j_gys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_c_gysys_dataset
(
  filename:/home/prod/xinhuadata/jt_c_gysys.csv,
  serverid:0,
  schema:jt_c_gysys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/prod/xinhuadata/jt_j_fxfl_rjfl.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_w_cgsh_dataset
(
  filename:/home/prod/xinhuadata/jt_w_cgsh.csv,
  serverid:0,
  schema:jt_w_cgsh_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_cwdl_dataset
(
  filename:/home/prod/xinhuadata/jt_j_cwdl.csv,
  serverid:0,
  schema:jt_j_cwdl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_spxx_dataset
(
  filename:/home/prod/xinhuadata/jt_j_spxx.csv,
  serverid:0,
  schema:jt_j_spxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_w_cgshmx_dataset
(
  filename:/home/prod/xinhuadata/jt_w_cgshmx.csv,
  serverid:0,
  schema:jt_w_cgshmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_g_bmhyrygx_dataset
(
  filename:/home/prod/xinhuadata/jt_g_bmhyrygx.csv,
  serverid:0,
  schema:jt_g_bmhyrygx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_g_fxflywydz_dataset
(
  filename:/home/prod/xinhuadata/jt_g_fxflywydz.csv,
  serverid:0,
  schema:jt_g_fxflywydz_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file base_operator_dataset
(
  filename:/home/prod/xinhuadata/base_operator.csv,
  serverid:0,
  schema:base_operator_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_w_cgsh_select
(
fields: ( 
(fname:"cgshid"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid")
),
inputs: "jt_w_cgsh_dataset",
order_by:("gysid")
);

dataproc select jt_c_gysys_select
(
fields: ( 
(fname:"ygysid"),
(fname:"dgysid")
),
inputs: "jt_c_gysys_dataset",
order_by:("ygysid")
);

dataproc leftjoin sh_ys_join
(
inputs: (left_input: jt_w_cgsh_select, right_input: jt_c_gysys_select),
join_keys: (("left_input.gysid","right_input.ygysid"))
);

dataproc select sh_ys_join_select
(
fields: ( 
(fname:"cgshid"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid"),
(fname:"dgysid")
),
inputs: "sh_ys_join",
order_by:("dgysid")
);

dataproc select jt_j_gys_select
(
fields: ( 
(fname:"gysid",alias:"gysid1",type:"string"),
(fname:"gysmc")
),
inputs: "jt_j_gys_dataset",
order_by:("gysid1")
);

dataproc leftjoin sh_ys_gys_join
(
inputs: (left_input: sh_ys_join_select, right_input: jt_j_gys_select),
join_keys: (("left_input.dgysid","right_input.gysid1"))
);

dataproc select sh_ys_gys_join_select
(
fields: ( 
(fname:"cgshid",alias:"cgshid1",type:"string"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid"),
(fname:"dgysid"),
(fname:"gysmc")
),
inputs: "sh_ys_gys_join",
order_by:("cgshid1")
);



dataproc select jt_j_spxx_select
(
fields: ( 
(fname:"spxxid"),
(fname:"fxflid")
),
inputs: "jt_j_spxx_dataset",
order_by:("fxflid")
);

dataproc select jt_j_fxfl_rjfl_select
(
fields: ( 
(fname:"fxflid",alias:"fxflid1",type:"string"),
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
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
),
inputs: "sp_fxfl_join",
order_by:("cwdlid")
);

dataproc select jt_j_cwdl_select
(
fields: ( 
(fname:"cwdlid",alias:"cwdlid1",type:"string"),
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
(fname:"spxxid",alias:"spxxid1",type:"string"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl")
),
inputs: "sp_fxfl_cw_join",
order_by:("spxxid1")
);




dataproc select jt_w_cgshmx_select
(
fields: ( 
(fname:"cgshid"),
(fname:"spxxid"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy")
),
inputs: "jt_w_cgshmx_dataset",
order_by:("cgshid")
);

dataproc leftjoin mx_sh_ys_gys_join
(
inputs: (left_input: jt_w_cgshmx_select, right_input: sh_ys_gys_join_select),
join_keys: (("left_input.cgshid","right_input.cgshid1"))
);

dataproc select mx_sh_ys_gys_join_select
(
fields: ( 
(fname:"cgshid"),
(fname:"spxxid"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid"),
(fname:"dgysid"),
(fname:"gysmc")
),
inputs: "mx_sh_ys_gys_join",
order_by:("spxxid")
);

dataproc leftjoin mx_sh_ys_gys_sp_fxfl_cw_join
(
inputs: (left_input: mx_sh_ys_gys_join_select, right_input: sp_fxfl_cw_join_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_sh_ys_gys_sp_fxfl_cw_join_select
(
fields: ( 
(fname:"cgshdh"),
(fname:"dgysid"),
(fname:"gysmc"),
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"dhrq")
),
inputs: "mx_sh_ys_gys_sp_fxfl_cw_join",
order_by:("dgysid")
);

dataproc select jt_g_bmhyrygx_select
(
fields: ( 
(fname:"gysid"),
(fname:"ywyid")
),
inputs: "jt_g_bmhyrygx_dataset",
order_by:("gysid")
);

dataproc leftjoin mx_sh_ys_gys_sp_fxfl_cw_g_join
(
inputs: (left_input: mx_sh_ys_gys_sp_fxfl_cw_join_select, right_input: jt_g_bmhyrygx_select),
join_keys: (("left_input.dgysid","right_input.gysid"))
);

dataproc select mx_sh_ys_gys_sp_fxfl_cw_g_join_select
(
fields: ( 
(fname:"cgshdh"),
(fname:"dgysid"),
(fname:"gysmc"),
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"dhrq"),
(fname:"ywyid")
),
inputs: "mx_sh_ys_gys_sp_fxfl_cw_g_join",
order_by:("ywyid")
);

dataproc select base_operator_select
(
fields: ( 
(fname:"operatorid"),
(fname:"operatorname")
),
inputs: "base_operator_dataset",
order_by:("operatorid")
);

dataproc leftjoin mx_sh_ys_gys_sp_fxfl_cw_g_p_join
(
inputs: (left_input: mx_sh_ys_gys_sp_fxfl_cw_g_join_select, right_input: base_operator_select),
join_keys: (("left_input.ywyid","right_input.operatorid"))
);

dataproc select mx_sh_ys_gys_sp_fxfl_cw_g_p_join_select
(
fields: ( 
(fname:"cgshdh"),
(fname:"dgysid"),
(fname:"gysmc"),
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"dhrq"),
(fname:"ywyid"),
(fname:"operatorname")
),
inputs: "mx_sh_ys_gys_sp_fxfl_cw_g_p_join",
order_by:("rjfxid")
);

dataproc select jt_g_fxflywydz_select
(
fields: ( 
(fname:"fxflid"),
(fname:"ywyid",alias:"ywyid1",type:string)
),
inputs: "jt_g_fxflywydz_dataset",
order_by:("fxflid")
);

dataproc leftjoin mx_sh_ys_gys_sp_fxfl_cw_g_p_g_join
(
inputs: (left_input: mx_sh_ys_gys_sp_fxfl_cw_g_p_join_select, right_input: jt_g_fxflywydz_select),
join_keys: (("left_input.rjfxid","right_input.fxflid"))
);

dataproc select mx_sh_ys_gys_sp_fxfl_cw_g_p_g_join_select
(
fields: ( 
(fname:"cgshdh"),
(fname:"dgysid"),
(fname:"gysmc"),
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"dhrq"),
(fname:"ywyid"),
(fname:"operatorname"),
(fname:"ywyid1")
),
inputs: "mx_sh_ys_gys_sp_fxfl_cw_g_p_g_join",
order_by:("ywyid1")
);

dataproc select base_operator_select1
(
fields: ( 
(fname:"operatorid",alias:"operatorid1",type:string),
(fname:"operatorname",alias:"fxfloperatorname",type:string)
),
inputs: "base_operator_dataset",
order_by:("operatorid1")
);

dataproc leftjoin mx_sh_ys_gys_sp_fxfl_cw_g_p_g_p_join
(
inputs: (left_input: mx_sh_ys_gys_sp_fxfl_cw_g_p_g_join_select, right_input: base_operator_select1),
join_keys: (("left_input.ywyid1","right_input.operatorid1"))
);

dataproc select mx_sh_ys_gys_sp_fxfl_cw_g_p_g_p_join_select
(
fields: ( 
(fname:"cgshdh"),
(fname:"dgysid"),
(fname:"gysmc"),
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"dhrq"),
(fname:"ywyid"),
(fname:"operatorname"),
(fname:"ywyid1"),
(fname:"fxfloperatorname")
),
inputs: "mx_sh_ys_gys_sp_fxfl_cw_g_p_g_p_join"
);



dataproc index 1_1_result_index1
(
  inputs:"mx_sh_ys_gys_sp_fxfl_cw_g_p_g_p_join_select",
  table:"1_1_result",
  indexes:(1_1_result_index)
 );
 dataproc doc 1_1_result_doc
(  
  inputs:"mx_sh_ys_gys_sp_fxfl_cw_g_p_g_p_join_select",
  table:"1_1_result",
  format:"1_1_result_parser",
  fields:("cgshdh","dgysid","gysmc","cwdlid","cwfl","rjfxid","rjflmc","sssl","ssmy","sssy","dhrq","operatorname","fxfloperatorname")
); 
dataproc statistics 1_1_result_sum1
(
	stat_model:"1_1_result_sum",
  	table:"1_1_result",
  	inputs:"mx_sh_ys_gys_sp_fxfl_cw_g_p_g_p_join_select"
);
end;
run job 1_1_result_sum_job(threads:8);
