CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name
  [COMMENT database_comment]
  WITH (key1=val1, key2=val2, ...);

-- catalog_name is optional, default is current catalog if not specified.

-- 修改数据库
ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)

-- 删除数据库
DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]
-- RESTRICT：删除非空数据库会触发异常。默认启用
-- CASCADE：删除非空数据库也会删除所有相关的表和函数。