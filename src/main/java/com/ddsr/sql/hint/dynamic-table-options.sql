-- 在执行查询时，可以在表名后面添加SQL Hints来临时修改表属性，对当前job生效。
select * from ws1/*+ OPTIONS('rows-per-second'='10')*/;
select * from ws1/*+ OPTIONS('rows-per-second'='10', 'key'='value', 'key2'='value2')*/;