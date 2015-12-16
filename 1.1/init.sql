create job 1_1_result_sum_job(1_1_result)
begin
dataset table jt_j_gys_dataset
(
	table:jt_j_gys
);

dataset table jt_c_gysys_dataset
(
	table:jt_c_gysys
);

dataset table jt_j_fxfl_rjfl_dataset
(
	table:jt_j_fxfl_rjfl;
);

dataset file jt_w_cgsh_dataset
(
  filename:/home/natt/xinhuadata/JT_W_CGSH.csv,
  serverid:0,
  schema:jt_w_cgsh_schema,
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

dataset file jt_j_spxx_dataset
(
  filename:/home/natt/xinhuadata/JT_J_SPXX.csv,
  serverid:0,
  schema:jt_j_spxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_w_cgshmx_dataset
(
  filename:/home/natt/xinhuadata/JT_W_CGSHMX.csv,
  serverid:0,
  schema:jt_w_cgshmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_w_cgshmx_select
(
fields: [ 
(fname:"cgshid"),
(fname:"spxxid"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy")
],
inputs: "jt_w_cgshmx_dataset",
order_by:("cgshid")
);

dataproc select jt_w_cgsh_select
(
fields: [ 
(fname:"cgshid",alais:"cgshid1"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid")
],
inputs: "jt_w_cgsh_dataset",
order_by:("cgshid1")
);

dataproc leftjoin mx_sh_join
(
inputs: (left_input: jt_w_cgshmx_select, right_input: jt_w_cgsh_select),
join_keys: (("left_input.cgshid","right_input.cgshid1"))
);

dataproc select mx_sh_join_select
(
fields: [ 
(fname:"cgshid"),
(fname:"spxxid"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid")
],
inputs: "mx_sh_join",
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

dataproc leftjoin mx_sh_sp_join
(
inputs: (left_input: mx_sh_join_select, right_input: jt_j_spxx_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_sh_sp_join_select
(
fields: [ 
(fname:"cgshid"),
(fname:"spxxid"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid"),
(fname:"fxflid")
],
inputs: "mx_sh_sp_join",
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

dataproc leftjoin mx_sh_sp_fxfl_join
(
inputs: (left_input: mx_sh_sp_join_select, right_input: jt_j_fxfl_rjfl_select),
join_keys: (("left_input.fxflid","right_input.fxflid1"))
);

dataproc select mx_sh_sp_fxfl_join_select
(
fields: [ 
(fname:"cgshid"),
(fname:"spxxid"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc")
],
inputs: "mx_sh_sp_fxfl_join",
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

dataproc leftjoin mx_sh_sp_fxfl_cw_join
(
inputs: (left_input: mx_sh_sp_fxfl_join_select, right_input: jt_j_cwdl_select),
join_keys: (("left_input.cwdlid","right_input.cwdlid1"))
);

dataproc select mx_sh_sp_fxfl_cw_join_select
(
fields: [ 
(fname:"cgshid"),
(fname:"spxxid"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl")
],
inputs: "mx_sh_sp_fxfl_cw_join",
order_by:("gysid")
);

dataproc select jt_c_gysys_select
(
fields: [ 
(fname:"ygysid",alais:"ygysid1"),
(fname:"dgysid")
],
inputs: "jt_c_gysys_dataset",
order_by:("ygysid1")
);

dataproc leftjoin mx_sh_sp_fxfl_cw_ys_join
(
inputs: (left_input: mx_sh_sp_fxfl_cw_join_select, right_input: jt_c_gysys_select),
join_keys: (("left_input.gysid","right_input.ygysid1"))
);

dataproc select mx_sh_sp_fxfl_cw_ys_join_select
(
fields: [ 
(fname:"cgshid"),
(fname:"spxxid"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"cgshdh"),
(fname:"dhrq"),
(fname:"gysid"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl"),
(fname:"dgysid")
],
inputs: "mx_sh_sp_fxfl_cw_ys_join",
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

dataproc leftjoin mx_sh_sp_fxfl_cw_ys_gys_join
(
inputs: (left_input: mx_sh_sp_fxfl_cw_ys_join_select, right_input: jt_j_gys_select),
join_keys: (("left_input.dgysid","right_input.gysid1"))
);


dataproc index 1_1_result_index1
(
  inputs:"mx_sh_sp_fxfl_cw_ys_gys_join",
  table:"1_1_result",
  indexes:(1_1_result_index)
 );
 dataproc doc 1_1_result_doc
(  
  inputs:"mx_sh_sp_fxfl_cw_ys_gys_join",
  table:"1_1_result",
  format:"1_1_result_parser"
); 
dataproc statistics 1_1_result_sum1
(
	stat_model:"1_1_result_sum",
  	table:"1_1_result",
  	inputs:"mx_sh_sp_fxfl_cw_ys_gys_join"
);
end;
run job 1_1_result_sum_job(threads:1);
