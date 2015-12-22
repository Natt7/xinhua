create job jt_j_fxfl_rjfl_job(jt_j_fxfl_rjfl)
begin
dataset file jt_j_fxfl_dataset
(
  filename:/home/natt/xinhuadata/jt_j_fxfl.csv,
  serverid:0,
  schema:jt_j_fxfl_schema,
  charset:utf-8,
  splitter:(block_size:1000)
);
dataproc select jt_j_fxfl_select
(
fields: [ 
(fname:"fxflid"),
(fname:"fxflmc"),
(fname:"fxflfid")
],
inputs: "jt_j_fxfl_dataset",
order_by:("fxflfid"),
group_by:("fxflfid"),
conditions:"fxfljs=4"
);

dataproc select jt_j_fxfl_select1
(
fields: [ 
(fname:"fxflid",alias:"fxflid1"),
(fname:"fxflfid",alias:"fxflfid1")
],
inputs: "jt_j_fxfl_dataset",
order_by:("fxflid1"),
group_by:("fxflid1")
);

dataproc leftjoin dj_cj_join
(
inputs: (left_input: jt_j_fxfl_select, right_input: jt_j_fxfl_select1),
join_keys: (("left_input.fxflfid","right_input.fxflid1"))
);

dataproc select dj_cj_join_select
(
fields: [ 
(fname:"fxflid"),
(fname:"fxflmc"),
(fname:"fxflfid"),
(fname:"fxflfid1")
],
inputs: "dj_cj_join",
order_by:("fxflfid1"),
group_by:("fxflfid1")
);

dataproc select jt_j_fxfl_select2
(
fields: [ 
(fname:"fxflid",alias:"rjfxid"),
(fname:"fxflmc",alias:"rjflmc"),
(fname:"cwdlid")
],
inputs: "jt_j_fxfl_dataset",
order_by:("rjfxid"),
group_by:("rjfxid")
);

dataproc leftjoin bj_cj_join
(
inputs: (left_input: dj_cj_join_select, right_input: jt_j_fxfl_select2),
join_keys: (("left_input.fxflfid1","right_input.rjfxid"))
);

dataproc select bj_cj_join_select
(
fields: [ 
(fname:"fxflid"),
(fname:"fxflmc"),
(fname:"rjfxid"),
(fname:"rjflmc"),
(fname:"cwdlid")
],
inputs: "bj_cj_join"
);

dataproc index result_index
(
  inputs:"bj_cj_join_select",
  table:"jt_j_fxfl_rjfl",
  indexes:(jt_j_fxfl_rjfl_index)
 );
 dataproc doc result_doc
(  
  inputs:"bj_cj_join_select",
  table:"jt_j_fxfl_rjfl",
  format:"jt_j_fxfl_rjfl_parser",
  fields: ("fxflid","fxflmc","rjfxid","rjflmc","cwdlid")
); 

end;
run job jt_j_fxfl_rjfl_job(threads:8);