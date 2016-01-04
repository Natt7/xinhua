SELECT Xd.Xsdh,
       Xd.Khid,
       Dw.Dwmc,
       Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Sum(Xs.Sdsl),
       Sum(Xs.Sdmy),
       Sum(Xs.Sdsy),
       Xd.Mdjsrq,
       Dw.dqid,
       b.dqmc
FROM Jt_x_Xsdmx Xs
LEFT JOIN Jt_x_Xsd Xd ON Xs.Xsdid = Xd.Xsdid
LEFT JOIN Jt_j_Dwxx Dw ON Xd.Khid = Dw.Dwid
LEFT JOIN Jt_j_Spxx Sp ON Xs.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN jt_j_dqbm b ON Dw.dqid = b.dqid
WHERE Xd.Mdjsrq IS NOT NULL
GROUP BY Xd.Xsdh,
         Xd.Khid,
         Dw.Dwmc,
         Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Xd.Mdjsrq,
         dw.dqid,
         b.dqmc


门店采购地区总量 表格
SELECT sum(sdmy) sdmy,sum(sdsy) sdsy,sum(sdsl) sdsl from 1_2_result;
门店采购地区 码洋 饼图
SELECT dqid,dqmc,sum(sdmy) sdmy from 1_2_result GROUP BY dqid,dqmc;
门店采购地区 码洋 实洋 数量 表格
SELECT dqid,dqmc,sum(sdmy) sdmy,sum(sdsy) sdsy,sum(sdsl) sdsl from 1_2_result GROUP BY dqid,dqmc;
门店采购地区 折扣率 柱状图
SELECT dqid,dqmc,sum(sdsy)/sum(sdmy) zkl from 1_2_result GROUP BY dqid,dqmc;
门店采购地区 门店 码洋 饼图
SELECT dqid,dqmc,khid,dwmc,sum(sdmy) sdmy from 1_2_result where dqmc = "长沙" GROUP BY dqid,dqmc,khid,dwmc;
门店采购地区 门店 码洋 柱状图
SELECT dqid,dqmc,khid,dwmc,sum(sdsy)/sum(sdmy) zkl from 1_2_result where dqmc = "长沙" GROUP BY dqid,dqmc,khid,dwmc;