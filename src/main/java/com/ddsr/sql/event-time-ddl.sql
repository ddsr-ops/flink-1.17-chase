CREATE TABLE EventTable(
                           user STRING,
                           url STRING,
                           ts TIMESTAMP(3),
                           WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
      ...
      );

-- 这里我们把ts字段定义为事件时间属性，而且基于ts设置了5秒的水位线延迟。
-- “WATERMARK FOR ts AS ts - INTERVAL '5' SECOND”，第一个ts为前面出现的数据类型为TIMESTAMP的ts字段，第二个ts为同第一个ts， '5' 必须使用单引号，为固定语法。
-- 时间戳类型必须是 TIMESTAMP 或者TIMESTAMP_LTZ 类型。但是时间戳一般都是秒或者是毫秒（BIGINT 类型），这种情况可以通过如下方式转换
-- ts BIGINT,
-- time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),


CREATE TABLE EventTable(
                           user STRING,
                           url STRING,
                           ts BIGINT, -- in seconds
                           time_ltz AS TO_TIMESTAMP_LTZ(ts, 3), -- 3 means the granularity down to milliseconds
                           WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
      ...
      );

-- Converts a epoch seconds or epoch milliseconds to a TIMESTAMP_LTZ,
-- the valid precision is 0 or 3, the 0 represents TO_TIMESTAMP_LTZ(epochSeconds, 0),
-- the 3 represents TO_TIMESTAMP_LTZ(epochMilliseconds, 3).

-- Flink SQL 提供了几种 WATERMARK 生产策略：
-- 严格升序：WATERMARK FOR rowtime_column AS rowtime_column。
-- Flink 任务认为时间戳只会越来越大，也不存在相等的情况，只要相等或者小于之前的，就认为是迟到的数据。

-- 递增：WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND 。
-- 一般基本不用这种方式。如果设置此类，则允许有相同的时间戳出现。

-- 有界无序： WATERMARK FOR rowtime_column AS rowtime_column – INTERVAL 'string' timeUnit 。
-- 此类策略就可以用于设置最大乱序时间，假如设置为 WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '5' SECOND ，
-- 则生成的是运行 5s 延迟的Watermark。一般都用这种 Watermark 生成策略，此类 Watermark 生成策略通常用于有数据乱序的场景中，
-- 而对应到实际的场景中，数据都是会存在乱序的，所以基本都使用此类策略。