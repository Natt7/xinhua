SELECT Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Ys.Dgysid,
       Gys.Gysmc,
       Sum(Nvl(Mx.Sdmy, 0)),
       sdsy
       sdsl
       (select p.operatorname from base_operator p where p.operatorid=(select g.ywyid from JT_G_BMHYRYGX g where g.gysid=Ys.Dgysid)) operatornme,
       (select p.operatorname from base_operator p where p.operatorid=(select g.ywyid from jt_g_fxflywydz g where g.Fxflid=Fxfl.Rjfxid)) fxfloperatorname
       Trunc(Jt.Jtrq)
FROM Jt_g_Jtdmx Mx
LEFT JOIN Jt_g_Jtd Jt ON Mx.Jtdid = Jt.Jtdid
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN Jt_c_Gysys Ys ON Jt.Gysid = Ys.Ygysid
LEFT JOIN Jt_j_Gys Gys ON Ys.Dgysid = Gys.Gysid
GROUP BY Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Ys.Dgysid,
         Gys.Gysmc,
         Trunc(Jt.Jtrq)


总部退货总量 码洋 实洋 数量 表格
SELECT sum(sdmy) sdmy,sum(sdsy) sdsy,sum(sdsl) sdsl FROM 4_1_result;
总部退货总量（按月展示）折线图
SELECT _month(jtrq),sum(sdsl) sdsl,sum(sdmy) sdmy,sum(sdsy) sdsy FROM 4_1_result group by _month(jtrq);
总部退货折扣率（按月展示）柱状图
SELECT _month(jtrq),sum(sdsy)/sum(sdmy) zkl FROM 4_1_result group by _month(jtrq);


采购供应商业务员 码洋 饼图
SELECT operatorname,sum(sdmy) sdmy FROM 4_1_result group by operatorname;
采购供应商业务员 码洋 实洋 数量 折扣率 表格
SELECT operatorname,sum(sdsl) sdsl,sum(sdmy) sdmy,sum(sdsy) sdsy,sum(sdsy)/sum(sdmy) zkl FROM 4_1_result GROUP BY operatorname;
采购供应商业务员 折扣率 柱状图
SELECT operatorname,sum(sdsy)/sum(sdmy) zkl FROM 4_1_result GROUP BY operatorname;
采购供应商业务员名称 供应商 码洋 饼图
SELECT operatorname,gysmc,sum(sdmy) sdmy FROM 4_1_result where operatorname = "陆悦" GROUP BY operatorname,gysmc;
采购供应商业务员名称 供应商 折扣率 柱状图
SELECT operatorname,gysmc,sum(sdsy)/sum(sdmy) zkl FROM 4_1_result where operatorname = "陆悦" GROUP BY operatorname,gysmc;


总部退货分类业务员 码洋 饼图
SELECT fxfloperatorname,sum(sdmy) sdmy FROM 4_1_result group by fxfloperatorname;
总部退货分类业务员 码洋 实洋 数量 折扣率 表格
SELECT fxfloperatorname,sum(sdsl) sdsl,sum(sdmy) sdmy,sum(sdsy) sdsy,sum(sdsy)/sum(sdmy) zkl FROM 4_1_result GROUP BY fxfloperatorname;
总部退货分类业务员 折扣率 柱状图
SELECT fxfloperatorname,sum(sdsy)/sum(sdmy) zkl FROM 4_1_result GROUP BY fxfloperatorname;
总部退货分类业务员名称 分类 码洋 饼图
SELECT fxfloperatorname,rjfxid,rjflmc,sum(sdmy) sdmy FROM 4_1_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;
总部退货分类业务员名称 分类 折扣率 柱状图
SELECT fxfloperatorname,rjfxid,rjflmc,sum(sdsy)/sum(sdmy) zkl FROM 4_1_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;


总部退货财务大类 码洋 饼图
SELECT cwdlid,cwfl,sum(sdmy) sdmy FROM 4_1_result group by cwdlid,cwfl;
总部退货财务大类 码洋 实洋 数量 表格
SELECT cwdlid,cwfl,sum(sdsl) sdsl,sum(sdmy) sdmy,sum(sdsy) sdsy FROM 4_1_result group by cwdlid,cwfl;
总部退货财务大类 折扣率 柱状图
SELECT cwdlid,cwfl,sum(sdsy)/sum(sdmy) zkl FROM 4_1_result group by cwdlid,cwfl;
总部退货财务大类 分类 码洋 饼图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(sdmy) sdmy FROM 4_1_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;
总部退货财务大类 分类 折扣率 柱状图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(sdsy)/sum(sdmy) zkl FROM 4_1_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;