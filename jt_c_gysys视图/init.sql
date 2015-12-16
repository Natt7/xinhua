create 0_2_jt_c_gysys_view_job(jt_c_gysys)
begin
dataset file jt_j_gysys_dataset
(
  filename:/home/natt/xinhuadata/JT_J_GYSYS.csv,
  serverid:0,
  schema:jt_j_gysys_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataset table jt_j_gys_dataset
(
	table:jt_j_gys
);

dataset file jt_w_cgsh_dataset
(
  filename:/home/natt/xinhuadata/JT_W_CGSH.csv,
  serverid:0,
  schema:Jt_w_Cgsh_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_j_gysys_select
(
fields: [ 
(fname:"ygysid"),
(fname:"dgysid"),
(fname:"'确认'", alias:"zt")
],
inputs: "jt_j_gysys_dataset",
conditions:"zt='确认' and ygysid!=dgysid"
);

dataproc select jt_j_gys_select
(
fields: [ 
(fname:"zt"),
(fname:"gysid"),
(fname:"'确认'", alias:"zt")
],
inputs: "jt_j_gys_dataset",
conditions:"zt='确认' and ygysid!=dgysid"
);

dataproc select jt_w_cgsh_select
(
fields: [ 
(fname:"gysid")
],
inputs: "jt_w_cgsh_dataset"
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
run job 0_2_jt_c_gysys_view_job(threads:8);
