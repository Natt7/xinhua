SELECT Mx.Khid,
       Dw.Dwmc,
       Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Sum(Mx.Qmmy),
       Trunc(Kf.Jzrq)
FROM Jt_c_Khspmx Mx
LEFT JOIN Jt_c_Bmspkfmx Kf ON Mx.Bmspkfmxid = Kf.Bmspkfmxid
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN Jt_j_Dwxx Dw ON Mx.Khid = Dw.Dwid
GROUP BY Mx.Khid,
         Dw.Dwmc,
         Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Trunc(Kf.Jzrq)