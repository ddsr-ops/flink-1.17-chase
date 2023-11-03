-- 自定义 Source 的数据， t为表名
SELECT order_id, price FROM (VALUES (1, 2.0), (2, 3.1)) AS t (order_id, price)