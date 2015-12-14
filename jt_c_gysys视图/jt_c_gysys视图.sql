CREATE OR REPLACE VIEW jt_c_gysys AS
SELECT t.ygysid,
       t.dgysid,
       '确认' zt
FROM jt_j_gysys t
WHERE t.zt = '确认'
  AND t.ygysid <> t.dgysid
UNION
SELECT DISTINCT s.gysid,
                s.gysid,
                '确认' zt
FROM jt_j_gys s
WHERE (s.zt = '启用'
       OR s.gysid IN
         (SELECT DISTINCT gysid
          FROM jt_w_cgsh))
  AND gysid NOT IN
    (SELECT ygysid
     FROM jt_j_gysys
     WHERE ygysid <> dgysid
       AND zt = '确认' );