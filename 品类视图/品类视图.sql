CREATE VIEW jt_j_fxfl_rjfl AS
SELECT dj.fxflid,
       dj.fxflmc,
       bj.fxflid rjfxid,
       bj.fxflmc rjflmc,
       bj.cwdlid
FROM jt_j_fxfl dj
LEFT JOIN jt_j_fxfl cj ON dj.fxflfid = cj.fxflid
LEFT JOIN jt_j_fxfl bj ON cj.fxflfid = bj.fxflid
WHERE dj.fxfljs = 4