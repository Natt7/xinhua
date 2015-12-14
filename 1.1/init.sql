create job map_job11(t1)
begin
dataset file jt_j_gys_dataset
(
  filename:/home/natt/xinhua/jt_j_gys.csv,
  serverid:0,
  schema:jt_j_gys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index jt_j_gys_index1
(
  inputs:"jt_j_gys_dataset",
  table:"jt_j_gys",
  indexes:(jt_j_gys_index)
 );
 dataproc doc jt_j_gys_doc
(  
  inputs:"jt_j_gys_dataset",
  table:"jt_j_gys",
  format:"jt_j_gys_parser"
  
); 
dataproc map jt_j_gys_map
(  
   inputs:"jt_j_gys_dataset",
   table:"jt_j_gys"
);

dataset file jt_c_gysys_dataset
(
  filename:/home/natt/xinhua/jt_c_gysys.csv,
  serverid:0,
  schema:jt_c_gysys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index jt_c_gysys_index1
(
  inputs:"jt_c_gysys_dataset",
  table:"jt_c_gysys",
  indexes:(jt_c_gysys_index)
 );
 dataproc doc jt_c_gysys_doc
(  
  inputs:"jt_c_gysys_dataset",
  table:"jt_c_gysys",
  format:"jt_c_gysys_parser"
  
); 
dataproc map jt_c_gysys_map
(  
   inputs:"jt_c_gysys_dataset",
   table:"jt_c_gysys"
);
end;

dataset file jt_w_cgsh_dataset
(
  filename:/home/natt/xinhua/jt_w_cgsh.csv,
  serverid:0,
  schema:jt_w_cgsh_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index jt_w_cgsh_index1
(
  inputs:"jt_w_cgsh_dataset",
  table:"jt_w_cgsh",
  indexes:(jt_w_cgsh_index)
 );
 dataproc doc jt_w_cgsh_doc
(  
  inputs:"jt_w_cgsh_dataset",
  table:"jt_w_cgsh",
  format:"jt_w_cgsh_parser"
  
); 
dataproc map jt_w_cgsh_map
(  
   inputs:"jt_w_cgsh_dataset",
   table:"jt_w_cgsh"
);

dataset file jt_j_cwdl_dataset
(
  filename:/home/natt/xinhua/jt_j_cwdl.csv,
  serverid:0,
  schema:jt_j_cwdl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index jt_j_cwdl_index1
(
  inputs:"jt_j_cwdl_dataset",
  table:"jt_j_cwdl",
  indexes:(jt_j_cwdl_index)
 );
 dataproc doc jt_j_cwdl_doc
(  
  inputs:"jt_j_cwdl_dataset",
  table:"jt_j_cwdl",
  format:"jt_j_cwdl_parser"
  
); 
dataproc map jt_j_cwdl_map
(  
   inputs:"jt_j_cwdl_dataset",
   table:"jt_j_cwdl"
);

dataset file jt_j_fxfl_rjfl_dataset
(
  filename:/home/natt/xinhua/jt_j_fxfl_rjfl.csv,
  serverid:0,
  schema:jt_j_fxfl_rjfl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index jt_j_fxfl_rjfl_index1
(
  inputs:"jt_j_fxfl_rjfl_dataset",
  table:"jt_j_fxfl_rjfl",
  indexes:(jt_j_fxfl_rjfl_index)
 );
 dataproc doc jt_j_fxfl_rjfl_doc
(  
  inputs:"jt_j_fxfl_rjfl_dataset",
  table:"jt_j_fxfl_rjfl",
  format:"jt_j_fxfl_rjfl_parser"
  
); 
dataproc map jt_j_fxfl_rjfl_map
(  
   inputs:"jt_j_fxfl_rjfl_dataset",
   table:"jt_j_fxfl_rjfl"
);

dataset file jt_j_spxx_dataset
(
  filename:/home/natt/xinhua/jt_j_spxx.csv,
  serverid:0,
  schema:jt_j_spxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index jt_j_spxx_index1
(
  inputs:"jt_j_spxx_dataset",
  table:"jt_j_spxx",
  indexes:(jt_j_fxfl_rjfl_index)
 );
 dataproc doc jt_j_spxx_doc
(  
  inputs:"jt_j_spxx_dataset",
  table:"jt_j_spxx",
  format:"jt_j_spxx_parser"
  
); 
dataproc map jt_j_spxx_map
(  
   inputs:"jt_j_spxx_dataset",
   table:"jt_j_spxx"
);
end;
run job map_job11(threads:8);


create job jt_w_cgshmx_job(jt_w_cgshmx)
begin
dataset file jt_w_cgshmx_dataset
(
  filename:/home/natt/xinhua/jt_w_cgshmx.csv,
  serverid:0,
  schema:jt_w_cgshmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_w_cgshmx_select
(
fields: [ 
(fname:"vf_cgshdh"),
(fname:"vf_dgysid"),
(fname:"vf_gysmc"),
(fname:"vf_cwdlid"),
(fname:"vf_cwfl"),
(fname:"vf_rjfxid"),
(fname:"vf_rjflmc"),
(fname:"sssl"),
(fname:"ssmy"),
(fname:"sssy"),
(fname:"vf_dhrq")
],
inputs: "jt_w_cgshmx_dataset",
group_by:("vf_cgshdh","vf_dgysid","vf_gysmc","vf_cwdlid","vf_cwfl","vf_rjfxid","vf_rjflmc","vf_dhrq")
);

dataproc index jt_w_cgshmx_index1
(
  inputs:"jt_w_cgshmx_select",
  table:"jt_w_cgshmx",
  indexes:(jt_w_cgshmx_index)
 );
 dataproc doc jt_w_cgshmx_doc
(  
  inputs:"jt_w_cgshmx_select",
  table:"jt_w_cgshmx",
  format:"jt_w_cgshmx_parser"
); 
dataproc statistics jt_w_cgshmx_sum1
(
	stat_model:"jt_w_cgshmx_sum",
  	table:"jt_w_cgshmx",
  	inputs:"jt_w_cgshmx_select"
);
end;
run job jt_w_cgshmx_job(threads:8);
