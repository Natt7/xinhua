SELECT Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Sum(Nvl(Zb.Qmmy, 0) + Nvl(Kh.Qmmy, 0)),
       Trunc(Zb.Jzrq)
FROM Jt_c_Bmspkfmx Zb
LEFT JOIN
  (SELECT Bmspkfmxid,
          Sum(Nvl(Qmmy, 0)) Qmmy
   FROM Jt_c_Khspmx
   GROUP BY Bmspkfmxid) Kh ON Zb.Bmspkfmxid = Kh.Bmspkfmxid
LEFT JOIN Jt_j_Spxx Sp ON Zb.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
GROUP BY Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Trunc(Zb.Jzrq)