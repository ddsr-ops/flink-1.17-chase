/**
  我们曾经学习过DataStream API中的双流Join，包括窗口联结（window join）和间隔联结（interval join）。两条流的Join就对应着SQL中两个表的Join，这是流处理中特有的联结方式。目前Flink SQL还不支持窗口联结，而间隔联结则已经实现。
间隔联结（Interval Join）返回的，同样是符合约束条件的两条中数据的笛卡尔积。只不过这里的“约束条件”除了常规的联结条件外，还多了一个时间间隔的限制。具体语法有以下要点：

两表的联结
    间隔联结不需要用JOIN关键字，直接在FROM后将要联结的两表列出来就可以，用逗号分隔。这与标准SQL中的语法一致，表示一个“交叉联结”（Cross Join），会返回两表中所有行的笛卡尔积。
联结条件
    联结条件用WHERE子句来定义，用一个等值表达式描述。交叉联结之后再用WHERE进行条件筛选，效果跟内联结INNER JOIN ... ON ...非常类似。
时间间隔限制
    我们可以在WHERE子句中，联结条件后用AND追加一个时间间隔的限制条件；做法是提取左右两侧表中的时间字段，然后用一个表达式来指明两者需要满足的间隔限制。具体定义方式有下面三种，这里分别用ltime和rtime表示左右表中的时间字段：
    （1）ltime = rtime
    （2）ltime >= rtime AND ltime < rtime + INTERVAL '10' MINUTE
    （3）ltime BETWEEN rtime - INTERVAL '10' SECOND AND rtime + INTERVAL '5' SECOND
 */


SELECT *
FROM ws,ws1
WHERE ws.id = ws1. id
  AND ws.et BETWEEN ws1.et - INTERVAL '2' SECOND AND ws1.et + INTERVAL '2' SECOND;