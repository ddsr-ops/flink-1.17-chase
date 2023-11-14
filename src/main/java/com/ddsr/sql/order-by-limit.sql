-- order by
-- 支持 Batch\Streaming，但在实时任务中一般用的非常少。
-- 实时任务中，Order By 子句中必须要有时间属性字段，并且必须写在最前面且为升序。
SELECT *
FROM ws
ORDER BY et, id desc;


-- LIMIT clause constrains the number of rows returned by the SELECT statement. In general, this clause is used in conjunction with ORDER BY to ensure that the results are deterministic.
--
-- The following example selects the first 3 rows in Orders table.
-- Only supports Batch mode
SELECT *
FROM Orders
ORDER BY orderTime
LIMIT 3;


