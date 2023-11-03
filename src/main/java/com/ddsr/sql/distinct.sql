-- 用作根据 key 进行数据去重
SELECT DISTINCT vc FROM source
-- 对于流查询，计算查询结果所需的状态可能无限增长。状态大小取决于不同行数。
-- 可以设置适当的状态生存时间(TTL)的查询配置，以防止状态过大。
-- 但是，这可能会影响查询结果的正确性。如某个 key 的数据过期从状态中删除了，
-- 那么下次再来这么一个 key，由于在状态中找不到，就又会输出一遍。

-- table.exec.state.ttl?
-- How to set ttl of state in Flink SQL?