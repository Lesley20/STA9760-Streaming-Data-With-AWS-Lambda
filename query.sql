SELECT DISTINCT t1.Name, t1.Hour, t1.High, t2.ts
FROM (
  SELECT 
      name as Name, 
      EXTRACT(HOUR FROM (CAST(ts as TIMESTAMP))) AS Hour,
      MAX(high) as High
  FROM "sta9760-proj3-4"."data17" 
  GROUP BY 
  1,2) AS t1,
  
"sta9760-proj3-4"."data17" AS t2
  
WHERE t1.Name = t2.name AND t1.High = t2.high AND t1.Hour = HOUR(CAST(t2.ts as TIMESTAMP))

ORDER BY
1,2;