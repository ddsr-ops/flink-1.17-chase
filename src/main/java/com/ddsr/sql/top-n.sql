SELECT [column_list]
FROM (
    SELECT [column_list],
    ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
    ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
    FROM table_name)
WHERE rownum <= N [AND conditions];
-- 不能指定范围：range/rows between
--     ROW_NUMBER() ：标识 TopN 排序子句
--     PARTITION BY col1[, col2...] ：标识分区字段，代表按照这个 col 字段作为分区粒度对数据进行排序取 topN，比如下述案例中的 partition by key ，就是根据需求中的搜索关键词（key）做为分区
--     ORDER BY col1 [asc|desc][, col2 [asc|desc]...] ：标识 TopN 的排序规则，是按照哪些字段、顺序或逆序进行排序，可以不是时间字段，也可以降序（TopN特殊支持）
-- WHERE rownum <= N ：这个子句是一定需要的，只有加上了这个子句，Flink 才能将其识别为一个TopN 的查询，其中 N 代表 TopN 的条目数
-- [AND conditions] ：其他的限制条件也可以加上


select
    id,
    et,
    vc,
    rownum
from
    (
        select
            id,
            et,
            vc,
            row_number() over(
            partition by id
            order by vc desc
        ) as rownum
        from ws
    )
where rownum<=3;