-- In 子查询的结果集只能有一列
SELECT id, vc
FROM ws
WHERE id IN (
    SELECT id FROM ws1
)
-- 上述 SQL 的 In 子句和之前介绍到的 Inner Join 类似。并且 In 子查询也会涉及到大状态问题，要注意设置 State 的 TTL。