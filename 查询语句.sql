jt_c_gysys视图

select ygysid,dgysid,zt from jt_c_gysys where zt="确认"  limit 100 ;

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
	   dhrq
FROM 1_1_result
where rjflmc is not null
GROUP BY cgshdh,
         dgysid,
         gysmc,
         cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
		 dhrq;
		 
		 SELECT cgshdh,
       dgysid,
       gysmc,
       cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sssl,
       ssmy,
       sssy,
	   dhrq
FROM 1_1_result
		 
		 SELECT cgshdh,sum(sssl),
       sum(ssmy),
       sum(sssy)from 1_1_result group by cgshdh;
		 
		 
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
		 
		 
SELECT xsdh,
       khid,
       dwmc,
	       CASE ykbz
             WHEN 'true' THEN '越库'
             ELSE (CASE zpfh
                       WHEN 'true' THEN '直配'
                       ELSE '报订'
                   END)
         END yy,
sum(sdsl),
       sum(sdmy),
       sum(sdsy)
	   FROM 1_2_result
GROUP BY xsdh,
         khid,
         dwmc,
		 cwdlid,
         rjfxid,
	mdjsrq;

SELECT      
sum(sdsl),
       sum(sdmy),
       sum(sdsy)
	   FROM 1_2_result;

mdjsrq;

2.1
SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(xsmy),
       count(DISTINCT spxxid),
       xsrq
FROM 2_1_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         xsrq;
		 
SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(xsmy),
       xsrq
FROM 2_1_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         xsrq;


销售总量（按时间分类）折线图
SELECT xsrq,sum(xsmy) FROM 2_1_result GROUP BY xsrq;

财务大类销售量，饼图
SELECT cwfl,sum(xsmy) FROM 2_1_result GROUP BY cwfl;

品类销售量，柱状图
SELECT rjflmc,sum(xsmy) FROM 2_1_result GROUP BY rjflmc;
	   
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
		 
SELECT ztid,
       dwmc,
       cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       xslx,
       sum(xsmy),
       xsrq
FROM 2_2_result
GROUP BY ztid,
         dwmc,
         cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         xslx,
         xsrq;
		 
门店总销量
SELECT xsrq,sum(xsmy) FROM 2_2_result GROUP BY xsrq;

门店品类销量
SELECT rjflmc,sum(xsmy) FROM 2_2_result GROUP BY rjflmc;

门店批发零售销量
SELECT xslx,sum(xsmy) FROM 2_2_result GROUP BY xslx;
	   
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
		 
SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(qmmy),
       jzrq
FROM 3_1_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         jzrq;
		 
总部存货总量（按时间分类）折线图
SELECT sum(qmmy),jzrq FROM 3_1_result GROUP BY jzrq;

总部品类存货量 饼图
SELECT sum(qmmy),rjflmc FROM 3_1_result GROUP BY rjflmc;

总部财务大类存货量 柱状图
SELECT sum(qmmy),cwfl FROM 3_1_result GROUP BY cwfl;

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
		 
SELECT khid,
       dwmc,
       cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       sum(qmmy),
       jzrq
FROM 3_2_result
GROUP BY khid,
         dwmc,
         cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         jzrq;
		 
门店采购总量（按时间分类）折线图
SELECT sum(qmmy),jzrq FROM 3_2_result GROUP BY jzrq;

门店品类存货量 饼图
SELECT sum(qmmy),rjflmc FROM 3_2_result GROUP BY rjflmc;

门店财务大类存货量 柱状图
SELECT sum(qmmy),cwfl FROM 3_2_result GROUP BY cwfl;
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
		 
SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       dgysid,
       gysmc,
       sum(sdmy),
       jtrq
FROM 4_1_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         dgysid,
         gysmc,
         jtrq;

总部退货总量（按时间分类）折线图
SELECT sum(sdmy),jtrq FROM 4_1_result GROUP BY jtrq;

供应商退货量，饼图
SELECT sum(sdmy),gysmc FROM 4_1_result GROUP BY gysmc;

财务大类退货量，柱状图
SELECT sum(sdmy),cwfl FROM 4_1_result GROUP BY cwfl;

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

SELECT cwdlid,
       cwfl,
       rjfxid,
       rjflmc,
       khid,
       dwmc,
       sum(xtmy),
       wlshrq
FROM 4_2_result
GROUP BY cwdlid,
         cwfl,
         rjfxid,
         rjflmc,
         khid,
         dwmc,
         wlshrq;

门店退货总量 折线图
SELECT sum(xtmy),wlshrq FROM 4_2_result GROUP BY wlshrq;

门店品类退货量 饼图
SELECT sum(xtmy),rjflmc FROM 4_2_result GROUP BY rjflmc;

门店财务大类退货量 柱状图
SELECT sum(xtmy),cwfl FROM 4_2_result GROUP BY cwfl;

	   
	   