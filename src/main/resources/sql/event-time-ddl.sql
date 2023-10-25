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