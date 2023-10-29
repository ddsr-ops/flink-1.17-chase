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

-- 主键约束表明表中的一列或一组列是唯一的，并且它们不包含NULL值。主键唯一地标识表中的一行，只支持 not enforced。
CREATE TABLE MyTable (
                         `user_id` BIGINT,
                         `name` STRING,
                         PARYMARY KEY(user_id) not enforced -- PRIMARY KEY must not be nullable
) WITH (
      'connector' = 'kafka'
      ...
      );

-- PRIMARY KEY #
-- Primary key constraint is a hint for Flink to leverage for optimizations. It tells that a column or a set of columns of a table or a view are unique and they do not contain null. Neither of columns in a primary can be nullable. Primary key therefore uniquely identify a row in a table.
--
-- Primary key constraint can be either declared along with a column definition (a column constraint) or as a single line (a table constraint). For both cases, it should only be declared as a singleton. If you define multiple primary key constraints at the same time, an exception would be thrown.
--
-- Validity Check
--
-- SQL standard specifies that a constraint can either be ENFORCED or NOT ENFORCED. This controls if the constraint checks are performed on the incoming/outgoing data. Flink does not own the data therefore the only mode we want to support is the NOT ENFORCED mode. It is up to the user to ensure that the query enforces key integrity.
--
-- Flink will assume correctness of the primary key by assuming that the columns nullability is aligned with the columns in primary key. Connectors should ensure those are aligned.
--
-- Notes: In a CREATE TABLE statement, creating a primary key constraint will alter the columns nullability, that means, a column with primary key constraint is not nullable.


CREATE TABLE Orders (
                        `user` BIGINT,
                        product STRING,
                        order_time TIMESTAMP(3)
) WITH (
      'connector' = 'kafka',
      'scan.startup.mode' = 'earliest-offset'
      );

CREATE TABLE Orders_with_watermark (
    -- Add watermark definition
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
      -- Overwrite the startup-mode
      'scan.startup.mode' = 'latest-offset'
      )
    LIKE Orders;

-- 用于基于现有表的定义创建表。此外，用户可以扩展原始表或排除表的某些部分。
-- 可以使用该子句重用(可能还会覆盖)某些连接器属性，或者向外部定义的表添加水印。
-- 可增加属性（原表没有的属性），覆盖属性（原表有的属性）。
