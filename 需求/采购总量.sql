SELECT Sh.Cgshdh,
       Ys.Dgysid,
       Gys.Gysmc,
       Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Sum(Mx.Sssl),
       Sum(Mx.Ssmy),
       Sum(Mx.Sssy),
       Trunc(Sh.Dhrq),
       (select p.operatorname from base_operator p where p.operatorid=(select g.ywyid from JT_G_BMHYRYGX g where g.gysid=Ys.Dgysid)) operatornme,
       (select p.operatorname from base_operator p where p.operatorid=(select g.ywyid from jt_g_fxflywydz g where g.Fxflid=Fxfl.Rjfxid)) fxfloperatorname
FROM Jt_w_Cgshmx Mx
LEFT JOIN Jt_w_Cgsh Sh ON Mx.Cgshid = Sh.Cgshid
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN Jt_c_Gysys Ys ON Sh.Gysid = Ys.Ygysid
LEFT JOIN Jt_j_Gys Gys ON Ys.Dgysid = Gys.Gysid
GROUP BY Sh.Cgshdh,
         Ys.Dgysid,
         Gys.Gysmc,
         Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         operatornme,
         fxfloperatorname,
         Trunc(Sh.Dhrq)


采购总量 总表 表格
SELECT sum(sssl) sssl,sum(ssmy) ssmy,sum(sssy) sssy FROM 1_1_result;
采购总量（按月展示）折线图
SELECT _month(dhrq),sum(sssl) sssl,sum(ssmy) ssmy,sum(sssy) sssy FROM 1_1_result group by _month(dhrq);
采购折扣率（按月展示）柱状图
SELECT _month(dhrq),sum(sssy)/sum(ssmy) zkl FROM 1_1_result group by _month(dhrq);


采购供应商业务员 码洋 饼图
SELECT operatorname,sum(ssmy) ssmy FROM 1_1_result group by operatorname;
采购供应商业务员 码洋 实洋 数量 折扣率 表格
SELECT operatorname,sum(sssl) sssl,sum(ssmy) ssmy,sum(sssy) sssy,sum(sssy)/sum(ssmy) zkl FROM 1_1_result GROUP BY operatorname;
采购供应商业务员 折扣率 柱状图
SELECT operatorname,gysmc,sum(sssy)/sum(ssmy) zkl FROM 1_1_result GROUP BY operatorname,gysmc;
采购供应商业务员名称 供应商 码洋 饼图
SELECT operatorname,gysmc,sum(ssmy) ssmy FROM 1_1_result where operatorname = "陆悦" GROUP BY operatorname,gysmc;
采购供应商业务员名称 供应商 折扣率 柱状图
SELECT operatorname,gysmc,sum(sssy)/sum(ssmy) zkl FROM 1_1_result where operatorname = "陆悦" GROUP BY operatorname,gysmc;


财务大类 码洋 饼图
SELECT cwdlid,cwfl,sum(ssmy) ssmy FROM 1_1_result group by cwdlid,cwfl;
财务大类 码洋 实洋 数量 表格
SELECT cwdlid,cwfl,sum(sssl) sssl,sum(ssmy) ssmy,sum(sssy) sssy FROM 1_1_result group by cwdlid,cwfl;
财务大类 折扣率 柱状图
SELECT cwdlid,cwfl,sum(sssy)/sum(ssmy) zkl FROM 1_1_result group by cwdlid,cwfl;
财务大类 分类 码洋 饼图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(ssmy) ssmy FROM 1_1_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;
财务大类 分类 折扣率 柱状图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(sssy)/sum(ssmy) zkl FROM 1_1_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;


品类业务员 码洋 饼图
SELECT fxfloperatorname,sum(ssmy) ssmy FROM 1_1_result group by fxfloperatorname;
品类业务员 码洋 实洋 数量 表格
SELECT fxfloperatorname,sum(sssl) sssl,sum(ssmy) ssmy,sum(sssy) sssy FROM 1_1_result group by fxfloperatorname;
品类 折扣率 柱状图
SELECT fxfloperatorname,sum(sssy)/sum(ssmy) zkl FROM 1_1_result group by fxfloperatorname;
品类业务员名称 品类 码洋
SELECT fxfloperatorname,rjfxid,rjflmc,sum(ssmy) ssmy FROM 1_1_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;
品类业务员名称 品类 折扣率
SELECT fxfloperatorname,rjfxid,rjflmc,sum(sssy)/sum(ssmy) zkl FROM 1_1_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;