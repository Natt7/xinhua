SELECT Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Mx.xslx,
      dq.dqmc,
      dw.dwmc,
       Sum(Mx.Xsmy),
      Sum(Mx.Xssy),
      Sum(Mx.Xssl),
       Count(DISTINCT Mx.Spxxid),
       Trunc(Mx.Xsrq)
FROM
  (SELECT Xs.Ztid,
          Xs.Spxxid,
          Xd.Xslx,
          Xs.Xssl,
          Xs.Xsmy,
          Xs.Xssy,
          Xd.Xsrq
   FROM Jt_x_Xsdmx_Sb Xs
   LEFT JOIN Jt_x_Xsd_Sb Xd ON Xs.Xsdid = Xd.Xsdid
   AND Xs.Ztid = Xd.Ztid
   UNION ALL SELECT Xtmx.Ztid, Xtmx.Spxxid, Xt.Xslx, -xtmx.Xtsl, -xtmx.Xtmy, -xtmx.Xtsy,
                                                                              Xt.Xsrq
   FROM Ls_Jt_x_Xtdmx Xtmx
   LEFT JOIN Ls_Jt_x_Xtd Xt ON Xtmx.Xtdid = Xt.Xtdid
   AND Xtmx.Ztid = Xt.Ztid) Mx
LEFT JOIN jt_j_dwxx dw ON mx.ztid=dw.dwid
LEFT JOIN jt_j_dqbm dq ON dw.dqid=dq.dqid
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
GROUP BY Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Mx.xslx,
         dq.dqmc,
         dw.dwmc,
         Trunc(Mx.Xsrq)


销售总量 总表 表格
SELECT sum(xssl) xssl,sum(xsmy) xsmy,sum(xssy) xssy FROM 2_1_result;
销售总量（按月展示）折线图
SELECT _month(xsrq),sum(xssl) xssl,sum(xsmy) xsmy,sum(xssy) xssy FROM 2_1_result group by _month(xsrq);
销售折扣率（按月展示）柱状图
SELECT _month(xsrq),sum(xssy)/sum(xsmy) zkl FROM 2_1_result group by _month(xsrq);


销售财务大类 码洋 饼图
SELECT cwdlid,cwfl,sum(xsmy) xsmy FROM 2_1_result group by cwdlid,cwfl;
销售财务大类 码洋 实洋 数量 表格
SELECT cwdlid,cwfl,sum(xssl) xssl,sum(xsmy) xsmy,sum(xssy) xssy FROM 2_1_result group by cwdlid,cwfl;
销售财务大类 折扣率 柱状图
SELECT cwdlid,cwfl,sum(sssy)/sum(ssmy) zkl FROM 2_1_result group by cwdlid,cwfl;
销售财务大类 分类 码洋 饼图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(xsmy) xsmy FROM 2_1_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;
销售财务大类 分类 折扣率 柱状图
SELECT cwdlid,cwfl,rjfxid,rjflmc,sum(xssy)/sum(xsmy) zkl FROM 2_1_result where cwfl = "动漫读物" GROUP BY cwdlid,cwfl,rjfxid,rjflmc;


销售地区 码洋 饼图
SELECT dqmc,sum(xsmy) xsmy from 2_1_result GROUP BY dqmc;
销售地区 码洋 实洋 数量 表格
SELECT dqmc,sum(xssl) xssl,sum(xsmy) xsmy,sum(xssy) xssy from 2_1_result GROUP BY dqmc;
销售地区 折扣率 柱状图
SELECT dqmc,sum(xssy)/sum(xsmy) zkl from 2_1_result GROUP BY dqmc;
销售地区 门店 码洋 饼图
SELECT dqmc,dwmc,sum(xsmy) xsmy from 2_1_result where dqmc = "长沙" GROUP BY dqmc,dwmc;
销售地区 门店 折扣率 柱状图
SELECT dqmc,dwmc,sum(xssy)/sum(xsmy) zkl from 2_1_result where dqmc = "长沙" GROUP BY dqmc,dwmc;


销售类型 码洋 饼图
SELECT xslx,sum(xsmy) xsmy from 2_1_result GROUP BY xslx;
销售类型 码洋 实洋 数量 表格
SELECT xslx,sum(xssl) xssl,sum(xsmy) xsmy,sum(xssy) xssy from 2_1_result GROUP BY xslx;
销售类型 折扣率 柱状图
SELECT xslx,sum(xssy)/sum(xsmy) zkl from 2_1_result GROUP BY xslx;