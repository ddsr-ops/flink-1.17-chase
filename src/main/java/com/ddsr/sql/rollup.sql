-- ROLLUP: The ROLLUP operator generates all possible grouping sets in a hierarchical manner.
-- It produces a result set that includes not only the individual groups but also the subtotal and grand total rows. For example, if you have columns A, B, and C, and you use ROLLUP on them,
-- it will produce grouping sets (A, B, C), (A, B), (A), ().

SELECT A, B, C, SUM(D)
FROM table
GROUP BY ROLLUP (A, B, C)

-- This query will generate the following grouping sets:
-- (A, B, C), (A, B), (A), ()
-- The result will include subtotals and grand total rows.