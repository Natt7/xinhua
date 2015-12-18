select cgshdh,dgysid,gysmc,cwdlid,cwfl,rjfxid,rjflmc,sum(sssl),sum(ssmy),sum(sssy),trunc(dhrq) from 1_1_result;


select xsdh,khid,dwmc,case ykbz when 'true' then '越库' else (case zpfh when 'true' then '直配' else '报订' end) end,cwdlid,rjfxid,sum(sdsl),sum(sdmy),sum(sdsy),trunc(mdjsrq)from 1_2_result;
 select case ykbz when 'true' then '越库' else (case zpfh when 'true' then '直配' else '报订' end) end from 1_2_result;

select cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(xsmy),
       count(spxxid),
       trunc(xsrq) from 2_1_result;
	   
	   select ztid,dwmc,cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
	   xslx,
       sum(xsmy),
       count(spxxid),
       trunc(xsrq) from 2_2_result;
	   
	   
	   select cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(qmmy),
       trunc(jzrq) from 3_1_result;
	   
	   select khid,
       dwmc,
       cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(qmmy),
       trunc(jzrq) from 3_2_result;
	   
	   select cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       dgysid,
       gysmc,
       sum(sdmy),
       trunc(jtrq) from 4_1_result;
	   
	   select cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       khid,
       dwmc,
       sum(xtmy),
       trunc(wlshrq) from 4_2_result;

	   
	   