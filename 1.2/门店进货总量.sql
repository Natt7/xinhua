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

select a.dqid,b.dqmc from jt_j_dwxx a
left join jt_j_dqbm b on a.dqid=b.dqid


SELECT
  Xd.Xsdh,
  Xd.Khid,
  Dw.Dwmc,
  CASE
WHEN Xd.Ykbz = 'true' THEN
  '越库'
ELSE
  (
    CASE
    WHEN Xd.Zpfh = 'true' THEN
      '直配'
    ELSE
      '报订'
    END
  )
END,
 Fxfl.Cwdlid,
 Cw.Cwfl,
 Fxfl.Rjfxid,
 Fxfl.Rjflmc,
 Sum(Xs.Sdsl),
 Sum(Xs.Sdmy),
 Sum(Xs.Sdsy),
 Xd.Mdjsrq,
 dw.dqid,
 b.dqmc
FROM
  Jt_x_Xsdmx Xs
LEFT JOIN Jt_x_Xsd Xd ON Xs.Xsdid = Xd.Xsdid
LEFT JOIN Jt_j_Dwxx Dw ON Xd.Khid = Dw.Dwid
LEFT JOIN Jt_j_Spxx Sp ON Xs.Spxxid = Sp.Spxxid
LEFT JOIN Jt_j_Fxfl_Rjfl Fxfl ON Sp.Fxflid = Fxfl.Fxflid
LEFT JOIN Jt_j_Cwdl Cw ON Fxfl.Cwdlid = Cw.Cwdlid
LEFT JOIN jt_j_dqbm b ON dw.dqid = b.dqid
WHERE
  Xd.Mdjsrq IS NOT NULL
GROUP BY
  Xd.Xsdh,
  Xd.Khid,
  Dw.Dwmc,
  CASE
WHEN Xd.Ykbz = 'true' THEN
  '越库'
ELSE
  (
    CASE
    WHEN Xd.Zpfh = 'true' THEN
      '直配'
    ELSE
      '报订'
    END
  )
END,
 Fxfl.Cwdlid,
 Cw.Cwfl,
 Fxfl.Rjfxid,
 Fxfl.Rjflmc,
 Xd.Mdjsrq,
 dw.dqid,
 b.dqmc
ORDER BY
  Xd.Khid,
  CASE
WHEN Xd.Ykbz = 'true' THEN
  '越库'
ELSE
  (
    CASE
    WHEN Xd.Zpfh = 'true' THEN
      '直配'
    ELSE
      '报订'
    END
  )
END,
 Fxfl.Cwdlid,
 Fxfl.Rjfxid,
 Xd.Xsdh
         