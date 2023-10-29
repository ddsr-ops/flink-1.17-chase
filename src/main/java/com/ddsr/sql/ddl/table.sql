CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
(
    { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
)
    [COMMENT table_comment]
    [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
    WITH (key1=val1, key2=val2, ...)
    [ LIKE source_table [( <like_options> )] | AS select_query ]


-- metadata_column_definition
-- 元数据列是 SQL 标准的扩展，允许访问数据源本身具有的一些元数据。元数据列由 METADATA 关键字标识。
-- 具体示例，参考table-kafka.sql

-- computed_column_definition
CREATE TABLE MyTable (
                         `user_id` BIGINT,
                         `price` DOUBLE,
                         `quantity` DOUBLE,
                         `cost` AS price * quanitity
) WITH (
      'connector' = 'kafka'
      ...
      );