SET sql-client.execution.result-mode=changelog;
CREATE DATABASE mydatabase;
-- /opt/module/flink-1.17.0/bin/sql-client.sh embedded -s yarn-session -i conf/sql-client-init.sql
-- This Sql file is run as launching script of sql-client.sh