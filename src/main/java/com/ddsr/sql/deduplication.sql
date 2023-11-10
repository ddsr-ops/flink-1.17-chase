-- 去重，也即上文介绍到的TopN 中 row_number = 1 的场景，但是这里有一点不一样在于其排序字段一定是时间属性列（事件时间或处理时间），可以降序，不能是其他非时间属性的普通列。
-- 在 row_number = 1 时，如果排序字段是普通列 planner 会翻译成 TopN 算子，如果是时间属性列 planner 会翻译成 Deduplication，这两者最终的执行算子是不一样的，Deduplication 相比 TopN 算子专门做了对应的优化，性能会有很大提升。可以从webui看出是翻译成哪种算子。

-- 如果是按照时间属性字段降序，表示取最新一条，会造成不断的更新保存最新的一条。如果是升序，表示取最早的一条，不用去更新，性能更好。

SELECT [column_list]
FROM (
    SELECT [column_list],
    ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
    ORDER BY time_attr [asc|desc]) AS rownum
    FROM table_name)
WHERE rownum = 1;

-- Here, time_attr must be event time or process time as deduplication, otherwise, top-one if time_attr is not time attribute

-- 对每个传感器的水位值去重
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
            partition by id,vc
            order by et -- et as time attribute
        ) as rownum
        from ws
    )
where rownum=1;