select cgshdh,dgysid,gysmc,cwdlid,cwfl,rjfxid,rjflmc,sum(sssl),sum(ssmy),sum(sssy),trunc(dhrq) from 1_1_result;


select xsdh,khid,dwmc,case ykbz when 'true' then '越库' else (case zpfh when 'true' then '直配' else '报订' end) end,cwdlid,rjfxid,sum(sdsl),sum(sdmy),sum(sdsy),trunc(mdjsrq)from 1_2_result;
 
select case ykbz when 'true' then '越库' else (case zpfh when 'true' then '直配' else '报订' end) end from 1_2_result;