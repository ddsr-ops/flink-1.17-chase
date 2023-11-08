SELECT
    agg_func(agg_col) OVER (
    [PARTITION BY col1[, col2, ...]]
    ORDER BY time_col
    range_definition),
  ...
FROM ...
--     ORDER BY：必须是时间戳列（事件时间、处理时间），只能升序
--     PARTITION BY：标识了聚合窗口的聚合粒度
--     range_definition：这个标识聚合窗口的聚合数据范围，在 Flink 中有两种指定数据范围的方式。第一种为按照行数聚合，第二种为按照时间区间聚合