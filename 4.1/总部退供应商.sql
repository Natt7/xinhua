SELECT Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Ys.Dgysid,
       Gys.Gysmc,
       Sum(Nvl(Mx.Sdmy, 0)),
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