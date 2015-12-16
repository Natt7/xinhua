create job jt_j_gys_map_job(t1)
begin
dataset file jt_j_dwxx_dataset
(
  filename:/home/natt/xinhua/jt_j_dwxx.csv,
  serverid:0,
  schema:jt_j_dwxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc index jt_j_dwxx_index1
(
  inputs:"jt_j_dwxx_dataset",
  table:"jt_j_dwxx",
  indexes:(jt_j_dwxx_index)
 );
 dataproc doc jt_j_dwxx_doc
(  
  inputs:"jt_j_dwxx_dataset",
  table:"jt_j_dwxx",
  format:"jt_j_dwxx_parser"
  
); 
dataproc map jt_j_dwxx_map
(  
   inputs:"jt_j_dwxx_dataset",
   table:"jt_j_dwxx"
);

end;
run job jt_j_gys_map_job(threads:8);


create job jt_j_gys_job(jt_j_gys)
begin
dataset file jt_j_dwxx1_dataset
(
  filename:/home/natt/xinhua/jt_j_dwxx.csv,
  serverid:0,
  schema:jt_j_dwxx1_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_j_dwxx1_select
(
fields: [ 
(fname:"dwid", alias:"gysid"),
(fname:"dwbh", alias:"gysbh"),
(fname:"dwmc", alias:"gysmc"),
(fname:"dwjc", alias:"gysjc"),
(fname:"djsdbz"),
(fname:"jsdwid", alias:"djsdid"),
(fname:"zjm"),
(fname:"dwtjbh", alias:"wxtbh"),
(fname:"null", alias:"ywyid",type:"string"),
(fname:"sfid"),
(fname:"dqid"),
(fname:"yzbm"),
(fname:"txdz", alias:"dz"),
(fname:"dh"),
(fname:"cz"),
(fname:"lxr"),
(fname:"khyh"),
(fname:"zh"),
(fname:"sh"),
(fname:"email"),
(fname:"wz"),
(fname:"dwid",alias:"gysfwptid"),
(fname:"null", alias:"zdytjfl1",type:"string"),
(fname:"null", alias:"zdytjfl2",type:"string"),
(fname:"null", alias:"zdytjfl3",type:"string"),
(fname:"zt"),
(fname:"cjr"),
(fname:"tyr"),
(fname:"czrq"),
(fname:"gyslxid"),
(fname:"dwid"),
(fname:"dwjb"),
(fname:"yxzf"),
(fname:"sjdwid",alias:"sjgysid"),
(fname:"dwsxid"),
(fname:"kpsx"),
(fname:"cgjszq"),
(fname:"yyzz"),
(fname:"zzjgdm"),
(fname:"vf_jsdwmc",alias:"jsdwmc")
],
inputs: "jt_j_dwxx1_dataset",
conditions:"gyslxid>'0'"
);

dataproc index jt_j_gys_index1
(
  inputs:"jt_j_dwxx1_select",
  table:"jt_j_gys",
  indexes:(jt_j_gys_index)
 );
 dataproc doc jt_j_gys_doc
(  
  inputs:"jt_j_dwxx1_select",
  table:"jt_j_gys",
  format:"jt_j_gys_parser",
  fields:("gysid","gysbh","gysmc","gysjc","djsdbz","djsdbz","zjm","wxtbh","ywyid","sfid","dqid","yzbm","dz","dh","cz","lxr","khyh","zh","sh","email","wz","gysfwptid","zdytjfl1","zdytjfl2","zdytjfl3","zt","cjr","tyr","czrq","gyslxid","dwid","dwjb","yxzf","sjgysid","dwsxid","kpsx","cgjszq","yyzz","zzjgdm","jsdwmc")
); 
end;
run job jt_j_gys_job(threads:8);
