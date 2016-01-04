SELECT Mx.Khid,
       Dw.Dwmc,
       Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Sum(Mx.Qmmy),
       Sum(Mx.Qmsy),
       Sum(Mx.Qmkc),
       b.dqmc,
       (select p.operatorname from base_operator p where p.operatorid=(select g.ywyid from jt_g_fxflywydz g where g.Fxflid=Fxfl.Rjfxid)) fxfloperatorname
       Trunc(Kf.Jzrq),
FROM Jt_c_Khspmx Mx
LEFT JOIN Jt_c_Bmspkfmx Kf ON Mx.Bmspkfmxid = Kf.Bmspkfmxid
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN Jt_j_Dwxx Dw ON Mx.Khid = Dw.Dwid
LEFT JOIN jt_j_dqbm b ON Dw.dqid = b.dqid
GROUP BY Mx.Khid,
         Dw.Dwmc,
         Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         b.dqmc,
         fxfloperatorname,
         Trunc(Kf.Jzrq)


门店存货总量 码洋 实洋 数量 表格
SELECT sum(qmmy) qmmy,sum(qmsy) qmsy,sum(qmkc) qmkc FROM 3_2_result;
门店存货总量（按月展示）折线图
SELECT _month(jzrq),sum(qmkc) qmkc,sum(qmmy) qmmy,sum(qmsy) qmsy FROM 3_2_result group by _month(jzrq);
门店存货折扣率（按月展示）柱状图
SELECT _month(jzrq),sum(qmsy)/sum(qmmy) zkl FROM 3_2_result group by _month(jzrq);


门店存货分类业务员 码洋 饼图
SELECT fxfloperatorname,sum(qmmy) qmmy FROM 3_2_result group by fxfloperatorname;
门店存货分类业务员 码洋 实洋 数量 折扣率 表格
SELECT fxfloperatorname,sum(qmkc) qmkc,sum(qmmy) qmmy,sum(qmsy) qmsy,sum(qmsy)/sum(qmmy) zkl FROM 3_2_result GROUP BY fxfloperatorname;
门店存货分类业务员 折扣率 柱状图
SELECT fxfloperatorname,sum(qmsy)/sum(qmmy) zkl FROM 3_2_result GROUP BY fxfloperatorname;
门店存货分类业务员名称 分类 码洋 饼图
SELECT fxfloperatorname,rjfxid,rjflmc,sum(qmmy) qmmy FROM 3_2_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;
门店存货分类业务员名称 分类 折扣率 柱状图
SELECT fxfloperatorname,rjfxid,rjflmc,sum(qmsy)/sum(qmmy) zkl FROM 3_2_result where fxfloperatorname = "陆悦" GROUP BY fxfloperatorname,rjfxid,rjflmc;


门店存货财务大类 码洋 饼图
SELECT cwdlid,cwfl,sum(qmmy) qmmy FROM 3_2_result group by cwdlid,cwfl;
门店存货财务大类 码洋 实洋 数量 表格
SELECT cwdlid,cwfl,sum(qmkc) qmkc,sum(qmmy) qmmy,sum(qmsy) qmsy FROM 3_2_result group by cwdlid,cwfl;
门店存货财务大类 折扣率 柱状图
SELECT cwdlid,cwfl,sum(qmsy)/sum(qmmy) zkl FROM 3_2_result group by cwdlid,cwfl;
门店存货财务大类 分类 码洋 饼图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(qmmy) qmmy FROM 3_2_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;
门店存货财务大类 分类 折扣率 柱状图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(qmsy)/sum(qmmy) zkl FROM 3_2_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;


门店采购地区 码洋 饼图
SELECT dqmc,sum(qmmy) qmmy from 3_2_result GROUP BY dqmc;
门店采购地区 码洋 实洋 数量 表格
SELECT dqmc,sum(qmmy) qmmy,sum(qmsy) qmsy,sum(qmkc) qmkc from 3_2_result GROUP BY dqmc;
门店采购地区 折扣率 柱状图
SELECT dqmc,sum(qmsy)/sum(qmmy) zkl from 3_2_result GROUP BY dqmc;
门店采购地区 门店 码洋 饼图
SELECT dqmc,khid,dwmc,sum(qmmy) qmmy from 3_2_result where dqmc = "长沙" GROUP BY dqmc,khid,dwmc;
门店采购地区 门店 码洋 柱状图
SELECT dqmc,khid,dwmc,sum(qmsy)/sum(qmmy) zkl from 3_2_result where dqmc = "长沙" GROUP BY dqmc,khid,dwmc;