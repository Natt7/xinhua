create job 1_1_result_sum_job(1_1_result)
begin
dataset file jt_j_gys_dataset
(
  filename:/home/natt/xinhuadata/jt_j_gys.csv,
  serverid:0,
  schema:jt_j_gys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_c_gysys_dataset
(
  filename:/home/natt/xinhuadata/jt_c_gysys.csv,
  serverid:0,
  schema:jt_c_gysys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/natt/xinhuadata/jt_j_fxfl_rjfl.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_w_cgsh_dataset
(
  filename:/home/natt/xinhuadata/jt_w_cgsh.csv,
  serverid:0,
  schema:jt_w_cgsh_schema,
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

dataset file jt_j_spxx_dataset
(
  filename:/home/natt/xinhuadata/jt_j_spxx.csv,
  serverid:0,
  schema:jt_j_spxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_w_cgshmx_dataset
(
  filename:/home/natt/xinhuadata/jt_w_cgshmx.csv,
  serverid:0,
  schema:jt_w_cgshmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_w_cgsh_select
(
fields: ( 
(fname:"cgshid"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid",type:"string")
),
inputs: "jt_w_cgsh_dataset",
order_by:("gysid")
);

dataproc select jt_c_gysys_select
(
fields: ( 
(fname:"ygysid",type:"string"),
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
(fname:"dgysid",type:"string")
),
inputs: "sh_ys_join",
order_by:("dgysid")
);

dataproc select jt_j_gys_select
(
fields: ( 
(fname:"gysid",type:"string",alias:"gysid1"),
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
(fname:"cgshid",type:"string",alias:"cgshid1"),
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




dataproc select jt_w_cgshmx_select
(
fields: ( 
(fname:"cgshid",type:"string"),
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
(fname:"spxxid",type:"string"),
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
inputs: "mx_sh_ys_gys_sp_fxfl_cw_join"
);


dataproc index 1_1_result_index1
(
  inputs:"mx_sh_ys_gys_sp_fxfl_cw_join_select",
  table:"1_1_result",
  indexes:(1_1_result_index)
 );
 dataproc doc 1_1_result_doc
(  
  inputs:"mx_sh_ys_gys_sp_fxfl_cw_join_select",
  table:"1_1_result",
  format:"1_1_result_parser",
  fields:("cgshdh","dgysid","gysmc","cwdlid","cwfl","rjfxid","rjflmc","sssl","ssmy","sssy","dhrq")
); 
dataproc statistics 1_1_result_sum1
(
	stat_model:"1_1_result_sum",
  	table:"1_1_result",
  	inputs:"mx_sh_ys_gys_sp_fxfl_cw_join_select"
);
end;
run job 1_1_result_sum_job(threads:8);
