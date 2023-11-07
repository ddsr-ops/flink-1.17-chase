-- GROUPING SETS: The GROUPING SETS operator allows you to specify multiple grouping sets explicitly. It produces a result set that includes the specified grouping sets. For example, if you have columns A, B, and C, and you use GROUPING SETS on (A, B) and (C), it will produce grouping sets (A, B), (C).

SELECT A, B, C, SUM(D)
FROM table
GROUP BY GROUPING SETS ((A, B), (C))

-- This query will generate the grouping sets:
-- (A, B), (C)
-- The result will include only the specified grouping sets.