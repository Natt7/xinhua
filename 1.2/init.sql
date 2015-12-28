create job 1_2_result_sum_job(1_2_result)
begin

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/prod/xinhuadata/jt_j_fxfl_rjfl.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_x_xsdmx_dataset
(
  filename:/home/prod/xinhuadata/jt_x_xsdmx.csv,
  serverid:0,
  schema:jt_x_xsdmx_schema,
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

dataset file jt_x_xsd_dataset
(
  filename:/home/prod/xinhuadata/jt_x_xsd.csv,
  serverid:0,
  schema:jt_x_xsd_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_dwxx_dataset
(
  filename:/home/prod/xinhuadata/jt_j_dwxx.csv,
  serverid:0,
  schema:jt_j_dwxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset file jt_j_dqbm_dataset
(
  filename:/home/prod/xinhuadata/jt_j_dqbm.csv,
  serverid:0,
  schema:jt_j_dqbm_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_x_xsd_select
(
fields: ( 
(fname:"xsdid"),
(fname:"xsdh"),
(fname:"khid"),
(fname:"ykbz"),
(fname:"zpfh"),
(fname:"mdjsrq")
),
inputs: "jt_x_xsd_dataset",
order_by:("khid")
);

dataproc select jt_j_dwxx_select
(
fields: ( 
(fname:"dwid",alias:"dwid1"),
(fname:"dwmc"),
(fname:"dqid")
),
inputs: "jt_j_dwxx_dataset",
order_by:("dwid1")
);

dataproc leftjoin xd_dw_join
(
inputs: (left_input: jt_x_xsd_select, right_input: jt_j_dwxx_select),
join_keys: (("left_input.khid","right_input.dwid1"))
);

dataproc select xd_dw_join_select
(
fields: ( 
(fname:"xsdid",alias:"xsdid1"),
(fname:"xsdh"),
(fname:"khid"),
(fname:"ykbz"),
(fname:"zpfh"),
(fname:"mdjsrq"),
(fname:"dwmc"),
(fname:"dqid")
),
inputs: "xd_dw_join",
order_by:("xsdid1")
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
(fname:"fxflid",alias:"fxflid1"),
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
(fname:"cwdlid",alias:"cwdlid1"),
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
(fname:"spxxid",alias:"spxxid1"),
(fname:"fxflid"),
(fname:"cwdlid"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwfl")
),
inputs: "sp_fxfl_cw_join",
order_by:("spxxid1")
);






dataproc select jt_x_xsdmx_select
(
fields: ( 
(fname:"xsdid"),
(fname:"spxxid"),
(fname:"sdsl"),
(fname:"sdmy"),
(fname:"sdsy")
),
inputs: "jt_x_xsdmx_dataset",
order_by:("xsdid")
);

dataproc leftjoin xs_xd_dw_join
(
inputs: (left_input: jt_x_xsdmx_select, right_input: xd_dw_join_select),
join_keys: (("left_input.xsdid","right_input.xsdid1"))
);

dataproc select xs_xd_dw_join_select
(
fields: ( 
(fname:"xsdid"),
(fname:"spxxid"),
(fname:"sdsl"),
(fname:"sdmy"),
(fname:"sdsy"),
(fname:"xsdh"),
(fname:"khid"),
(fname:"ykbz"),
(fname:"zpfh"),
(fname:"mdjsrq"),
(fname:"dwmc"),
(fname:"dqid")
),
inputs: "xs_xd_dw_join",
order_by:("spxxid")
);

dataproc leftjoin xs_xd_dw_sp_fxfl_cw_join
(
inputs: (left_input: xs_xd_dw_join_select, right_input: sp_fxfl_cw_join_select),
join_keys: (("left_input.spxxid","right_input.spxxid1"))
);

dataproc select xs_xd_dw_sp_fxfl_cw_join_select
(
fields: ( 
(fname:"xsdh"),
(fname:"khid"),
(fname:"dwmc"),
(fname:"dqid"),
(fname:"ykbz"),
(fname:"zpfh"),
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"sdsl"),
(fname:"sdmy"),
(fname:"sdsy"),
(fname:"mdjsrq")
),
inputs: "xs_xd_dw_sp_fxfl_cw_join",
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

dataproc leftjoin xs_xd_dw_sp_fxfl_cw_b_join
(
inputs: (left_input: xs_xd_dw_sp_fxfl_cw_join_select, right_input: jt_j_dqbm_select),
join_keys: (("left_input.dqid","right_input.dqid1"))
);

dataproc select xs_xd_dw_sp_fxfl_cw_b_join_select
(
fields: ( 
(fname:"xsdh"),
(fname:"khid"),
(fname:"dwmc"),
(fname:"dqid"),
(fname:"ykbz"),
(fname:"zpfh"),
(fname:"cwdlid"),
(fname:"cwfl"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"sdsl"),
(fname:"sdmy"),
(fname:"sdsy"),
(fname:"mdjsrq"),
(fname:"dqmc")
),
inputs: "xs_xd_dw_sp_fxfl_cw_b_join"
);






dataproc index 1_2_result_index1
(
  inputs:"xs_xd_dw_sp_fxfl_cw_b_join_select",
  table:"1_2_result",
  indexes:(1_2_result_index)
 );
 dataproc doc 1_2_result_doc
(  
  inputs:"xs_xd_dw_sp_fxfl_cw_b_join_select",
  table:"1_2_result",
  format:"1_2_result_parser",
  fields:("xsdh","khid","dwmc","ykbz","zpfh","cwdlid","cwfl","rjfxid","rjflmc","sdsl","sdmy","sdsy","mdjsrq","dqid","dqmc")
); 
dataproc statistics 1_2_result_sum1
(
	stat_model:"1_2_result_sum",
  	table:"1_2_result",
  	inputs:"xs_xd_dw_sp_fxfl_cw_b_join_select"
);
end;
run job 1_2_result_sum_job(threads:8);
