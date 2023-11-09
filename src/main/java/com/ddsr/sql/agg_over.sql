SELECT agg_func(agg_col) OVER (
    [PARTITION BY col1[, col2, ...]]
    ORDER BY time_col
    range_definition),
  ...
FROM ...;
--     ORDER BY：必须是时间戳列（事件时间、处理时间），只能升序
--     PARTITION BY：标识了聚合窗口的聚合粒度
--     range_definition：这个标识聚合窗口的聚合数据范围，在 Flink 中有两种指定数据范围的方式。
--     第一种为按照行数(rows)聚合，第二种为按照时间区间(range)聚合


-- range interval
-- Note 2 things： the unit is second, the '5' is string literal
select id,
       et,
       vc,
       count(vc) over(partition by id order by et range between interval '5' second preceding and current row) as vc2
from ws;

-- rows interval
select id,
       et,
       vc,
       count(vc) over(partition by id order by et rows between 5 preceding and current row) as vc2
from ws;

-- reuse the window utilizing alias name
select id,
       et,
       vc,
       count(vc) over w as vc1,
       sum(vc) over w as vc2
from ws window w as (partition by id order by et rows between 5 preceding and current row);