SELECT Sh.Cgshdh,
       Ys.Dgysid,
       Gys.Gysmc,
       Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Sum(Mx.Sssl),
       Sum(Mx.Ssmy),
       Sum(Mx.Sssy),
       Trunc(Sh.Dhrq),
       (select p.operatorname from base_operator p where p.operatorid=
(select g.ywyid from JT_G_BMHYRYGX g where g.gysid=Ys.Dgysid)) operatornme,

       (select p.operatorname from base_operator p where p.operatorid=
(select g.ywyid from jt_g_fxflywydz g where g.Fxflid=Fxfl.Rjfxid)) fxfloperatorname

FROM Jt_w_Cgshmx Mx
LEFT JOIN Jt_w_Cgsh Sh ON Mx.Cgshid = Sh.Cgshid
LEFT JOIN Jt_j_Spxx Sp ON Mx.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN Jt_c_Gysys Ys ON Sh.Gysid = Ys.Ygysid
LEFT JOIN Jt_j_Gys Gys ON Ys.Dgysid = Gys.Gysid
GROUP BY Sh.Cgshdh,
         Ys.Dgysid,
         Gys.Gysmc,
         Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Trunc(Sh.Dhrq)