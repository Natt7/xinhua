create job map_job11(t1)
begin
dataset file Jt_j_Gys_dataset
(
  filename:/home/natt/xinhua/Jt_j_Gys.csv,
  serverid:0,
  schema:Jt_j_Gys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index Jt_j_Gys_index1
(
  inputs:"Jt_j_Gys_dataset",
  table:"Jt_j_Gys",
  indexes:(Jt_j_Gys_index)
 );
 dataproc doc Jt_j_Gys_doc
(  
  inputs:"Jt_j_Gys_dataset",
  table:"Jt_j_Gys",
  format:"Jt_j_Gys_parser"
  
); 
dataproc map Jt_j_Gys_map
(  
   inputs:"Jt_j_Gys_dataset",
   table:"Jt_j_Gys"
);

dataset file Jt_c_Gysys_dataset
(
  filename:/home/natt/xinhua/Jt_c_Gysys.csv,
  serverid:0,
  schema:Jt_c_Gysys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index Jt_c_Gysys_index1
(
  inputs:"Jt_c_Gysys_dataset",
  table:"Jt_c_Gysys",
  indexes:(Jt_c_Gysys_index)
 );
 dataproc doc Jt_c_Gysys_doc
(  
  inputs:"Jt_c_Gysys_dataset",
  table:"Jt_c_Gysys",
  format:"Jt_c_Gysys_parser"
  
); 
dataproc map Jt_c_Gysys_map
(  
   inputs:"Jt_c_Gysys_dataset",
   table:"Jt_c_Gysys"
);
end;

dataset file Jt_w_Cgsh_dataset
(
  filename:/home/natt/xinhua/Jt_w_Cgsh.csv,
  serverid:0,
  schema:Jt_w_Cgsh_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index Jt_w_Cgsh_index1
(
  inputs:"Jt_w_Cgsh_dataset",
  table:"Jt_w_Cgsh",
  indexes:(Jt_w_Cgsh_index)
 );
 dataproc doc Jt_w_Cgsh_doc
(  
  inputs:"Jt_w_Cgsh_dataset",
  table:"Jt_w_Cgsh",
  format:"Jt_w_Cgsh_parser"
  
); 
dataproc map Jt_w_Cgsh_map
(  
   inputs:"Jt_w_Cgsh_dataset",
   table:"Jt_w_Cgsh"
);

dataset file Jt_j_Cwdl_dataset
(
  filename:/home/natt/xinhua/Jt_j_Cwdl.csv,
  serverid:0,
  schema:Jt_j_Cwdl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index Jt_j_Cwdl_index1
(
  inputs:"Jt_j_Cwdl_dataset",
  table:"Jt_j_Cwdl",
  indexes:(Jt_j_Cwdl_index)
 );
 dataproc doc Jt_j_Cwdl_doc
(  
  inputs:"Jt_j_Cwdl_dataset",
  table:"Jt_j_Cwdl",
  format:"Jt_j_Cwdl_parser"
  
); 
dataproc map Jt_j_Cwdl_map
(  
   inputs:"Jt_j_Cwdl_dataset",
   table:"Jt_j_Cwdl"
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


create job Jt_w_Cgshmx_job(Jt_w_Cgshmx)
begin
dataset file Jt_w_Cgshmx_dataset
(
  filename:/home/natt/xinhua/Jt_w_Cgshmx.csv,
  serverid:0,
  schema:Jt_w_Cgshmx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select Jt_w_Cgshmx_select
(
fields: [ 
(fname:"vf_Cgshdh"),
(fname:"vf_Dgysid"),
(fname:"vf_Gysmc"),
(fname:"vf_cwdlid"),
(fname:"vf_Cwfl"),
(fname:"vf_rjfxid"),
(fname:"vf_rjflmc"),
(fname:"Sssl"),
(fname:"Ssmy"),
(fname:"Sssy"),
(fname:"vf_Dhrq")
],
inputs: "Jt_w_Cgshmx_dataset",
group_by:("vf_Cgshdh","vf_Dgysid","vf_Gysmc","vf_cwdlid","vf_Cwfl","vf_rjfxid","vf_rjflmc","vf_Dhrq")
);

dataproc index Jt_w_Cgshmx_index1
(
  inputs:"Jt_w_Cgshmx_select",
  table:"Jt_w_Cgshmx",
  indexes:(Jt_w_Cgshmx_index)
 );
 dataproc doc Jt_w_Cgshmx_doc
(  
  inputs:"Jt_w_Cgshmx_select",
  table:"Jt_w_Cgshmx",
  format:"Jt_w_Cgshmx_parser"
); 
dataproc statistics Jt_w_Cgshmx_sum1
(
	stat_model:"Jt_w_Cgshmx_sum",
  	table:"Jt_w_Cgshmx",
  	inputs:"Jt_w_Cgshmx_select"
);
end;
run job Jt_w_Cgshmx_job(threads:8);

