Create view jt_j_fxfl_rjfl as 
select dj.fxflid,dj.fxflmc,bj.fxflid rjfxid,bj.fxflmc rjflmc,bj.cwdlid
  from jt_j_fxfl dj
  left join jt_j_fxfl cj
    on dj.fxflfid = cj.fxflid
  left join jt_j_fxfl bj
    on cj.fxflfid = bj.fxflid
  where dj.fxfljs = 4
union all
select dj.fxflid,dj.fxflmc,bj.fxflid rjfxid,bj.fxflmc rjflmc,bj.cwdlid
  from jt_j_fxfl dj
  left join jt_j_fxfl bj
    on dj.fxflfid = bj.fxflid
  where dj.fxfljs = 3
union all
select dj.fxflid,dj.fxflmc,dj.fxflid rjfxid,dj.fxflmc rjflmc,dj.cwdlid
  from jt_j_fxfl dj
  where dj.fxfljs = 2


  select fxflid,fxflmc,rjfxid,rjflmc,cwdlid
  from jt_j_fxfl_rjfl;