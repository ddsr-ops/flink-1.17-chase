-- 1）上传所需jar包到lib下
-- cp flink-sql-connector-hive-3.1.3_2.12-1.17.0.jar /opt/module/flink-1.17.0/lib/

-- cp mysql-connector-j-8.0.31.jar /opt/module/flink-1.17.0/lib/

-- 2）更换planner依赖
-- 只有在使用Hive方言或HiveServer2时才需要这样额外的计划器jar移动，但这是Hive集成的推荐设置。
-- mv /opt/module/flink-1.17.0/opt/flink-table-planner_2.12-1.17.0.jar /opt/module/flink-1.17.0/lib/flink-table-planner_2.12-1.17.0.jar

-- mv /opt/module/flink-1.17.0/lib/flink-table-planner-loader-1.17.0.jar /opt/module/flink-1.17.0/opt/flink-table-planner-loader-1.17.0.jar

-- 3）重启flink集群和sql-client

-- 4）启动外置的hive metastore服务
-- Hive metastore必须作为独立服务运行，也就是hive-site中必须配置hive.metastore.uris
-- hive --service metastore &

-- 5）创建Catalog
-- 配置项	必需	默认值	类型	说明
-- type	Yes	(none)	String	Catalog类型，创建HiveCatalog时必须设置为'hive'。
-- name	Yes	(none)	String	Catalog的唯一名称
-- hive-conf-dir	No	(none)	String	包含hive -site.xml的目录,需要Hadoop文件系统支持。如果没指定hdfs协议，则认为是本地文件系统。如果不指定该选项，则在类路径中搜索hive-site.xml。
-- default-database	No	default	String	Hive Catalog使用的默认数据库
-- hive-version	No	(none)	String	HiveCatalog能够自动检测正在使用的Hive版本。建议不要指定Hive版本，除非自动检测失败。
-- hadoop-conf-dir	No	(none)	String	Hadoop conf目录的路径。只支持本地文件系统路径。设置Hadoop conf的推荐方法是通过HADOOP_CONF_DIR环境变量。只有当环境变量不适合你时才使用该选项，例如，如果你想分别配置每个HiveCatalog。
CREATE CATALOG myhive WITH (
    'type' = 'hive',
    'default-database' = 'default',
    'hive-conf-dir' = '/opt/module/hive/conf'
);
-- 虽然hive catalog可存储Flink创建的元数据，但手动创建hive catalog，并use catalog catalog_name是必须的


-- 6）查看Catalog
SHOW CATALOGS;

--查看当前的CATALOG
SHOW CURRENT CATALOG;
-- 7）使用指定Catalog
USE CATALOG myhive;

--查看当前的CATALOG
SHOW CURRENT CATALOG;
-- 建表，退出sql-client重进，查看catalog和表还在。
-- 8）读写Hive表
SHOW DATABASES; -- 可以看到hive的数据库

USE test;  -- 可以切换到hive的数据库

SHOW TABLES; -- 可以看到hive的表

SELECT * from ws; --可以读取hive表

INSERT INTO ws VALUES(1,1,1); -- 可以写入hive表