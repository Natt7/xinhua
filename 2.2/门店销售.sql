SELECT Mx.Ztid,
       Dw.Dwmc,
       Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Mx.Xslx,
       Sum(Mx.Xsmy),
       Count(DISTINCT Mx.Spxxid),
       Trunc(Mx.Xsrq)
FROM
  (SELECT Xs.Ztid,
          Xs.Spxxid,
          Xs.Xslx,
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
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN Jt_j_Dwxx Dw ON Mx.Ztid = Dw.Dwid
GROUP BY Mx.Ztid,
         Dw.Dwmc,
         Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Mx.Xslx,
         Trunc(Mx.Xsrq)