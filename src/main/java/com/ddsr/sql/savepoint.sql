-- 1）提交一个insert作业，可以给作业设置名称
INSERT INTO sink select  * from source;
-- 2）查看job列表
SHOW JOBS;
-- 3）停止作业，触发savepoint
SET state.checkpoints.dir='hdfs://hadoop102:8020/chk';
SET state.savepoints.dir='hdfs://hadoop102:8020/sp';

STOP JOB '228d70913eab60dda85c5e7f78b5782c' WITH SAVEPOINT;
-- 4）从savepoint恢复
-- 设置从savepoint恢复的路径
SET execution.savepoint.path='hdfs://hadoop102:8020/sp/savepoint-37f5e6-0013a2874f0a';

-- 之后直接提交sql，就会从savepoint恢复
--允许跳过无法还原的保存点状态
set 'execution.savepoint.ignore-unclaimed-state' = 'true';

-- 5）恢复后重置路径
-- 指定execution.savepoint.path后，将影响后面执行的所有DML语句，可以使用RESET命令重置这个配置选项。
RESET execution.savepoint.path;
-- 如果出现reset没生效的问题，可能是个bug，我们可以退出sql-client，再重新进，不需要重启flink的集群。