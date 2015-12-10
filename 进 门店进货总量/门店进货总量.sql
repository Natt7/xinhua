SELECT Xd.Xsdh,
       Xd.Khid,
       Dw.Dwmc,
       Decode(Xd.Ykbz, 'true', '越库', Decode(Xd.Zpfh, 'true', '直配', '报订')),
       Fxfl.Cwdlid,
       Cw.Cwfl,
       Fxfl.Rjfxid,
       Fxfl.Rjflmc,
       Sum(Xs.Sdsl),
       Sum(Xs.Sdmy),
       Sum(Xs.Sdsy),
       Trunc(Xd.Mdjsrq)
FROM Jt_x_Xsdmx Xs
LEFT JOIN Jt_x_Xsd Xd ON Xs.Xsdid = Xd.Xsdid
LEFT JOIN Jt_j_Dwxx Dw ON Xd.Khid = Dw.Dwid
LEFT JOIN Jt_j_Spxx Sp ON Xs.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
WHERE Xd.Mdjsrq IS NOT NULL
GROUP BY Xd.Xsdh,
         Xd.Khid,
         Dw.Dwmc,
         Decode(Xd.Ykbz, 'true', '越库', Decode(Xd.Zpfh, 'true', '直配', '报订')),
         Fxfl.Cwdlid,
         Cw.Cwfl,
         Fxfl.Rjfxid,
         Fxfl.Rjflmc,
         Trunc(Xd.Mdjsrq)
ORDER BY Xd.Khid,
         Decode(Xd.Ykbz, 'true', '越库', Decode(Xd.Zpfh, 'true', '直配', '报订')),
         Fxfl.Cwdlid,
         Fxfl.Rjfxid,
         Xd.Xsdh
         