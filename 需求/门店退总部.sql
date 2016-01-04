SELECT Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Xt.Khid,
       Dw.Dwmc,
       dqmc
       Sum(Nvl(Mx.Xtmy, 0)),
       xtsl
       xtsy
       (select p.operatorname from base_operator p where p.operatorid=(select g.ywyid from jt_g_fxflywydz g where g.Fxflid=Fxfl.Rjfxid)) fxfloperatorname
       Trunc(Xt.Wlshrq)
FROM Jt_x_Xtdmx Mx
LEFT JOIN Jt_x_Xtd Xt ON Mx.Xtdid = Xt.Xtdid
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN Jt_j_Dwxx Dw ON Xt.Khid = Dw.Dwid
LEFT JOIN jt_j_dqbm b ON Dw.dqid = b.dqid
GROUP BY Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Xt.Khid,
         Dw.Dwmc,
         Trunc(Xt.Wlshrq)

门店退货总量 码洋 实洋 数量 表格
SELECT sum(xtmy) xtmy,sum(xtsy) xtsy,sum(xtsl) xtsl FROM 4_2_result;
门店退货总量（按月展示）折线图
SELECT _month(wlshrq),sum(xtsl) xtsl,sum(xtmy) xtmy,sum(xtsy) xtsy FROM 4_2_result group by _month(wlshrq);
门店退货折扣率（按月展示）柱状图
SELECT _month(wlshrq),sum(xtsy)/sum(xtmy) zkl FROM 4_2_result group by _month(wlshrq);


门店退货分类业务员 码洋 饼图
SELECT fxfloperatorname,sum(xtmy) xtmy FROM 4_2_result group by fxfloperatorname;
门店退货分类业务员 码洋 实洋 数量 折扣率 表格
SELECT fxfloperatorname,sum(xtsl) xtsl,sum(xtmy) xtmy,sum(xtsy) xtsy,sum(xtsy)/sum(xtmy) zkl FROM 4_2_result GROUP BY fxfloperatorname;
门店退货分类业务员 折扣率 柱状图
SELECT fxfloperatorname,sum(xtsy)/sum(xtmy) zkl FROM 4_2_result GROUP BY fxfloperatorname;
门店退货分类业务员名称 分类 码洋 饼图
SELECT fxfloperatorname,rjfxid,rjflmc,sum(xtmy) xtmy FROM 4_2_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;
门店退货分类业务员名称 分类 折扣率 柱状图
SELECT fxfloperatorname,rjfxid,rjflmc,sum(xtsy)/sum(xtmy) zkl FROM 4_2_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;


门店退货财务大类 码洋 饼图
SELECT cwdlid,cwfl,sum(xtmy) xtmy FROM 4_2_result group by cwdlid,cwfl;
门店退货财务大类 码洋 实洋 数量 表格
SELECT cwdlid,cwfl,sum(xtsl) xtsl,sum(xtmy) xtmy,sum(xtsy) xtsy FROM 4_2_result group by cwdlid,cwfl;
门店退货财务大类 折扣率 柱状图
SELECT cwdlid,cwfl,sum(xtsy)/sum(xtmy) zkl FROM 4_2_result group by cwdlid,cwfl;
门店退货财务大类 分类 码洋 饼图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(xtmy) xtmy FROM 4_2_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;
门店退货财务大类 分类 折扣率 柱状图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(xtsy)/sum(xtmy) zkl FROM 4_2_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;

门店退货地区 码洋 饼图
SELECT dqmc,sum(xtmy) xtmy from 4_2_result GROUP BY dqmc;
门店退货地区 码洋 实洋 数量 表格
SELECT dqmc,sum(xtmy) xtmy,sum(xtsy) xtsy,sum(xtsl) xtsl from 4_2_result GROUP BY dqmc;
门店退货地区 折扣率 柱状图
SELECT dqmc,sum(xtsy)/sum(xtmy) zkl from 4_2_result GROUP BY dqmc;
门店退货地区 门店 码洋 饼图
SELECT dqmc,khid,dwmc,sum(xtmy) xtmy from 4_2_result where dqmc = "长沙" GROUP BY dqmc,khid,dwmc;
门店退货地区 门店 码洋 柱状图
SELECT dqmc,khid,dwmc,sum(xtsy)/sum(xtmy) zkl from 4_2_result where dqmc = "长沙" GROUP BY dqmc,khid,dwmc;