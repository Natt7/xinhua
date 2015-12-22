create job 4_1_result_sum_job(4_1_result)
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

dataset file jt_g_jtdmx_dataset
(
  filename:/home/natt/xinhuadata/jt_g_jtdmx.csv,
  serverid:0,
  schema:jt_g_jtdmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_g_jtd_dataset
(
  filename:/home/natt/xinhuadata/jt_g_jtd.csv,
  serverid:0,
  schema:jt_g_jtd_schema,
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

dataproc select jt_g_jtd_select
(
fields: [ 
(fname:"jtdid"),
(fname:"gysid"),
(fname:"jtrq")
],
inputs: "jt_g_jtd_dataset",
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

dataproc leftjoin jt_ys_join
(
inputs: (left_input: jt_g_jtd_select, right_input: jt_c_gysys_select),
join_keys: (("left_input.gysid","right_input.ygysid"))
);

dataproc select jt_ys_join_select
(
fields: ( 
(fname:"jtdid"),
(fname:"gysid"),
(fname:"jtrq"),
(fname:"dgysid")
),
inputs: "jt_ys_join",
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

dataproc leftjoin jt_ys_gys_join
(
inputs: (left_input: jt_ys_join_select, right_input: jt_j_gys_select),
join_keys: (("left_input.dgysid","right_input.gysid1"))
);

dataproc select jt_ys_gys_join_select
(
fields: ( 
(fname:"jtdid",type:"string",alias:"jtdid1"),
(fname:"gysid"),
(fname:"jtrq"),
(fname:"dgysid"),
(fname:"gysmc")
),
inputs: "jt_ys_gys_join",
order_by:("jtdid1")
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

dataproc leftjoin mx_jt_ys_gys_join
(
inputs: (left_input: jt_g_jtdmx_select, right_input: jt_ys_gys_join_select),
join_keys: (("left_input.jtdid","right_input.jtdid1"))
);

dataproc select mx_jt_ys_gys_join_select
(
fields: ( 
(fname:"jtdid"),
(fname:"spxxid"),
(fname:"sdmy"),
(fname:"gysid"),
(fname:"jtrq"),
(fname:"dgysid"),
(fname:"gysmc")
),
inputs: "mx_jt_ys_gys_join",
order_by:("spxxid")
);

dataproc leftjoin mx_jt_ys_gys_sp_fxfl_cw_join
(
inputs: (left_input: mx_jt_ys_gys_join_select, right_input: sp_fxfl_cw_join_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select mx_jt_ys_gys_sp_fxfl_cw_join_select
(
fields: ( 
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"dgysid"),
(fname:"gysmc"),
(fname:"sdmy"),
(fname:"jtrq")
),
inputs: "mx_jt_ys_gys_sp_fxfl_cw_join"
);

dataproc index 4_1_result_index1
(
  inputs:"mx_jt_ys_gys_sp_fxfl_cw_join_select",
  table:"4_1_result",
  indexes:(4_1_result_index)
 );
 dataproc doc 4_1_result_doc
(  
  inputs:"mx_jt_ys_gys_sp_fxfl_cw_join_select",
  table:"4_1_result",
  format:"4_1_result_parser",
  fields:("cwdlid","cwfl","rjfxid","rjflmc","dgysid","gysmc","sdmy","jtrq")
); 
dataproc statistics 4_1_result_sum1
(
	stat_model:"4_1_result_sum",
  	table:"4_1_result",
  	inputs:"mx_jt_ys_gys_sp_fxfl_cw_join_select"
);
end;
run job 4_1_result_sum_job(threads:8);
