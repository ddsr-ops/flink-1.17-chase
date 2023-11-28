--  Env
-- default_catalog
    -- default_database
        -- t1

-- mysql_jdbc_catalog
    -- mysql_jdbc_database
        -- t4

-- JdbcCatalog：JdbcCatalog 使得用户可以将 Flink 通过 JDBC 协议连接到关系数据库。
-- Postgres Catalog和MySQL Catalog是目前仅有的两种JDBC Catalog实现，将元数据存储在数据库中。
-- JDBC catalog 仅支持读写 MySQL表，不支持另建新表（任何表）
-- 使用该特性，可不用Connector去连接MySQL表。

select * from default_catalog.default_database.t1 t
join mysql_jdbc_catalog.mysql_jdbc_database.t4 t2
on t.id = t2.id;

-- View all catalogs
show catalogs;

-- View all databases in the current catalog
show databases;

-- View all tables in the current database
show tables;

-- View used catalog
show current catalog;

use catalog mysql_jdbc_catalog;

use  mysql_jdbc_database;