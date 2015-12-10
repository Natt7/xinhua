SELECT Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Xt.Khid,
       Dw.Dwmc,
       Sum(Nvl(Mx.Xtmy, 0)),
       Trunc(Xt.Wlshrq)
FROM Jt_x_Xtdmx Mx
LEFT JOIN Jt_x_Xtd Xt ON Mx.Xtdid = Xt.Xtdid
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN Jt_j_Dwxx Dw ON Xt.Khid = Dw.Dwid
GROUP BY Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Xt.Khid,
         Dw.Dwmc,
         Trunc(Xt.Wlshrq)