-- CUBE: The CUBE operator generates all possible grouping sets for a given set of columns.
-- It produces a result set that includes all combinations of the specified columns.
-- For example, if you have columns A, B, and C, and you use CUBE on them,
-- it will produce grouping sets (A, B, C), (A, B), (A, C), (B, C), (A), (B), (C), ().

SELECT A, B, C, SUM(D)
FROM table
GROUP BY CUBE (A, B, C)

-- This query will generate the following grouping sets:
-- (A, B, C), (A, B), (A, C), (B, C), (A), (B), (C), ()
-- The result will include all possible combinations of the specified columns.