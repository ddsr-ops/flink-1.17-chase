-- 在定义处理时间属性时，必须要额外声明一个字段，专门用来保存当前的处理时间。
-- 在创建表的DDL（CREATE TABLE语句）中，可以增加一个额外的字段，通过调用系统内置的PROCTIME()函数来指定当前的处理时间属性。
CREATE TABLE EventTable(
                           user STRING,
                           url STRING,
                           ts AS PROCTIME()
) WITH (
      ...
      );