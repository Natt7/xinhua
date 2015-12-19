1.1
SELECT cgshdh,
       dgysid,
       gysmc,
       cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(sssl),
       sum(ssmy),
       sum(sssy),
       trunc(dhrq)
FROM 1_1_result
GROUP BY cgshdh,
         dgysid,
         gysmc,
         cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         trunc(dhrq);

1.2
SELECT xsdh,
       khid,
       dwmc,
       CASE ykbz
           WHEN 'true' THEN '越库'
           ELSE (CASE zpfh
                     WHEN 'true' THEN '直配'
                     ELSE '报订'
                 END)
       END,
       cwdlid,
       rjfxid,
       sum(sdsl),
       sum(sdmy),
       sum(sdsy),
       trunc(mdjsrq)
FROM 1_2_result
GROUP BY xsdh,
         khid,
         dwmc,
         CASE ykbz
             WHEN 'true' THEN '越库'
             ELSE (CASE zpfh
                       WHEN 'true' THEN '直配'
                       ELSE '报订'
                   END)
         END,
         cwdlid,
         rjfxid,
         trunc(mdjsrq)
ORDER BY khid,
         CASE ykbz
             WHEN 'true' THEN '越库'
             ELSE (CASE zpfh
                       WHEN 'true' THEN '直配'
                       ELSE '报订'
                   END)
         END,
         cwdlid,
         rjfxid,
         xsdh;

2.1
SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(xsmy),
       count(DISTINCT spxxid),
       trunc(xsrq)
FROM 2_1_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         trunc(xsrq);
	   
2.2
SELECT ztid,
       dwmc,
       cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       xslx,
       sum(xsmy),
       count(DISTINCT spxxid),
       trunc(xsrq)
FROM 2_2_result
GROUP BY ztid,
         dwmc,
         cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         xslx,
         trunc(xsrq);
	   
3.1	   
SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(qmmy),
       trunc(jzrq)
FROM 3_1_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         trunc(jzrq);

3.2	   
SELECT khid,
       dwmc,
       cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(qmmy),
       trunc(jzrq)
FROM 3_2_result
GROUP BY khid,
         dwmc,
         cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         trunc(jzrq);

4.1	   
SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       dgysid,
       gysmc,
       sum(sdmy),
       trunc(jtrq)
FROM 4_1_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         dgysid,
         gysmc,
         trunc(jtrq);

4.2	   
SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       khid,
       dwmc,
       sum(xtmy),
       trunc(wlshrq)
FROM 4_2_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         khid,
         dwmc,
         trunc(wlshrq);

	   
	   