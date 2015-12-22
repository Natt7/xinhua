create job 0_2_jt_c_gysys_view_map_job(jt_c_gysys)
begin
dataset file jt_j_gysys_dataset
(
  filename:/home/natt/xinhuadata/jt_j_gysys.csv,
  serverid:0,
  schema:jt_j_gysys_schema,
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
dataproc index jt_j_gysys_index1
(
  inputs:"jt_j_gysys_dataset",
  table:"jt_j_gysys",
  indexes:(jt_j_gysys_index)
 );
 dataproc doc jt_j_gysys_doc
(  
  inputs:"jt_j_gysys_dataset",
  table:"jt_j_gysys",
  format:"jt_j_gysys_parser"
); 
dataproc map jt_j_gysys_map
(  
   inputs:"jt_j_gysys_dataset",
   table:"jt_j_gysys"
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
end;
run job 0_2_jt_c_gysys_view_map_job(threads:8);




create job 0_2_jt_c_gysys_view_job(t1)
begin
dataset file jt_j_gys_dataset
(
  filename:/home/natt/xinhuadata/jt_j_gys.csv,
  serverid:0,
  schema:jt_j_gys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);


dataproc select jt_j_gysys_select
(
fields: ( 
(fname:"ygysid"),
(fname:"dgysid"),
(fname:"zt"),
(fname:"'0'",alias:"v1",type:"u64"),
(fname:"'0'",alias:"v2",type:"u64")
),
inputs: "jt_j_gysys_dataset"
);


dataproc select jt_j_gys_select1
(
fields: ( 
(fname:"gysid"),
(fname:"zt"),
(fname:"case when iilmap('jt_w_cgsh_map',gysid) then 1 else 2 end",alias:"vf_tru",type:"u64"),
(fname:"case when iilmap('jt_j_gysys_map',gysid) then 1 else 2 end",alias:"vf_fals",type:"u64")
),
inputs: "jt_j_gys_dataset"
);


dataproc select jt_j_gys_select
(
fields: ( 
(fname:"gysid",alias:"gysid1"),
(fname:"gysid"),
(fname:"'确认'",alias:"zt",type:"string"),
(fname:"vf_tru",type:"u64"),
(fname:"vf_fals",type:"u64")
),
inputs: "jt_j_gys_select1",
conditions:"(zt='启用' or vf_tru=1 ) and vf_fals=2"
);

dataproc union t_s_union
(
fields: ( 
(fname:"ygysid"),
(fname:"dgysid"),
(fname:"zt"),
(fname:"v1"),
(fname:"v2")
),
inputs: (jt_j_gysys_select,jt_j_gys_select)
);


dataproc index jt_c_gysys_index1
(
  inputs:"t_s_union",
  table:"jt_c_gysys",
  indexes:(jt_c_gysys_index)
 );
 dataproc doc jt_c_gysys_doc
(  
  inputs:"t_s_union",
  table:"jt_c_gysys",
  format:"jt_c_gysys_parser",
  fields:("ygysid","dgysid","zt","v1","v2")
);
end;
run job 0_2_jt_c_gysys_view_job(threads:8);
