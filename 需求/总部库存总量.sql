SELECT Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Sum(Nvl(Zb.Qmmy, 0)),
       Sum(Nvl(Zb.Qmsy, 0)),
       Sum(Nvl(Zb.Qmkc, 0)),
       Trunc(Zb.Jzrq),
      (select p.operatorname from base_operator p where p.operatorid=(select g.ywyid from jt_g_fxflywydz g where g.Fxflid=Fxfl.Rjfxid)) fxfloperatorname
FROM Jt_c_Bmspkfmx Zb
LEFT JOIN Jt_j_Spxx Sp ON Zb.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
GROUP BY Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Trunc(Zb.Jzrq),
         fxfloperatorname



门店存货总量 码洋 实洋 数量 表格
SELECT sum(qmmy) qmmy,sum(qmsy) qmsy,sum(qmkc) qmkc FROM 3_1_result;
门店存货总量（按月展示）折线图
SELECT _month(jzrq),sum(qmkc) qmkc,sum(qmmy) qmmy,sum(qmsy) qmsy FROM 3_1_result group by _month(jzrq);
门店存货折扣率（按月展示）柱状图
SELECT _month(jzrq),sum(qmsy)/sum(qmmy) zkl FROM 3_1_result group by _month(jzrq);


门店存货财务大类 码洋 饼图
SELECT cwdlid,cwfl,sum(qmmy) qmmy FROM 3_1_result group by cwdlid,cwfl;
门店存货财务大类 码洋 实洋 数量 表格
SELECT cwdlid,cwfl,sum(qmkc) qmkc,sum(qmmy) qmmy,sum(qmsy) qmsy FROM 3_1_result group by cwdlid,cwfl;
门店存货财务大类 折扣率 柱状图
SELECT cwdlid,cwfl,sum(qmsy)/sum(qmmy) zkl FROM 3_1_result group by cwdlid,cwfl;
门店存货财务大类 分类 码洋 饼图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(qmmy) qmmy FROM 3_1_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;
门店存货财务大类 分类 折扣率 柱状图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(qmsy)/sum(qmmy) zkl FROM 3_1_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;


门店存货分类业务员 码洋 饼图
SELECT fxfloperatorname,sum(qmmy) qmmy FROM 3_1_result group by fxfloperatorname;
门店存货分类业务员 码洋 实洋 数量 折扣率 表格
SELECT fxfloperatorname,sum(qmkc) qmkc,sum(qmmy) qmmy,sum(qmsy) qmsy,sum(qmsy)/sum(qmmy) zkl FROM 3_1_result GROUP BY fxfloperatorname;
门店存货分类业务员 折扣率 柱状图
SELECT fxfloperatorname,sum(qmsy)/sum(qmmy) zkl FROM 3_1_result GROUP BY fxfloperatorname;
门店存货分类业务员名称 分类 码洋 饼图
SELECT fxfloperatorname,rjfxid,rjflmc,sum(qmmy) qmmy FROM 3_1_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;
门店存货分类业务员名称 分类 折扣率 柱状图
SELECT fxfloperatorname,rjfxid,rjflmc,sum(qmsy)/sum(qmmy) zkl FROM 3_1_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;