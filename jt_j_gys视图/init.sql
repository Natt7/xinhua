create job 0_1_jt_j_gys_view_job(jt_j_gys)
begin
dataset file jt_j_dwxx_dataset
(
  filename:/home/natt/xinhuadata/jt_j_dwxx.csv,
  serverid:0,
  schema:jt_j_dwxx_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);

dataproc select jt_j_dwxx_select
(
fields: (
(fname:"dwid"),
(fname:"dwbh"),
(fname:"dwmc"),
(fname:"dwjc"),
(fname:"djsdbz"),
(fname:"jsdwid"),
(fname:"zjm"),
(fname:"dwtjbh"),
(fname:"sfid"),
(fname:"dqid"),
(fname:"yzbm"),
(fname:"txdz"),
(fname:"dh"),
(fname:"cz"),
(fname:"lxr"),
(fname:"khyh"),
(fname:"zh"),
(fname:"sh"),
(fname:"email"),
(fname:"wz"),
(fname:"zt"),
(fname:"cjr"),
(fname:"tyr"),
(fname:"czrq"),
(fname:"gyslxid"),
(fname:"dwjb"),
(fname:"yxzf"),
(fname:"sjdwid"),
(fname:"dwsxid"),
(fname:"kpsx"),
(fname:"cgjszq"),
(fname:"yyzz"),
(fname:"zzjgdm")
),
inputs: "jt_j_dwxx_dataset",
order_by:("jsdwid")
);

dataproc select jt_j_dwxx_select1
(
fields: (
(fname:"dwid",alias:"dwid1"),
(fname:"dwmc",alias:"jsdwmc")
),
inputs: "jt_j_dwxx_dataset",
order_by:("dwid1")
);

dataproc leftjoin s_jt_j_dwxx_join
(
inputs: (left_input: jt_j_dwxx_select, right_input: jt_j_dwxx_select1),
join_keys: (("left_input.jsdwid","right_input.dwid1"))
);

dataproc select s_jt_j_dwxx_select
(
fields: (
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
(fname:"jsdwmc")
),
inputs: "s_jt_j_dwxx_join",
conditions:"gyslxid>'0'"
);

dataproc index jt_j_gys_index1
(
  inputs:"s_jt_j_dwxx_select",
  table:"jt_j_gys",
  indexes:(jt_j_gys_index)
 );
 dataproc doc jt_j_gys_doc
(  
  inputs:"s_jt_j_dwxx_select",
  table:"jt_j_gys",
  format:"jt_j_gys_parser",
  fields:("gysid","gysbh","gysmc","gysjc","djsdbz","djsdbz","zjm","wxtbh","ywyid","sfid","dqid","yzbm","dz","dh","cz","lxr","khyh","zh","sh","email","wz","gysfwptid","zdytjfl1","zdytjfl2","zdytjfl3","zt","cjr","tyr","czrq","gyslxid","dwid","dwjb","yxzf","sjgysid","dwsxid","kpsx","cgjszq","yyzz","zzjgdm","jsdwmc")
); 
end;
run job 0_1_jt_j_gys_view_job(threads:8);
