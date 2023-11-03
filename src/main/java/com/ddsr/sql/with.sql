WITH <with_item_definition> [ , ... ]
SELECT ... FROM ...;

-- with_item_definition may appear multiple times

with temp1 as (select * from table1),
    temp2 as (select * from table2),
    temp3 as (select * from temp1 where col1 = 'x') -- use temp1 as subquery
select * from temp1 join temp2 on temp1.col1 = temp2.col2
    join temp3 on temp1.col1 = temp3.col3;

WITH source_with_total AS (
    SELECT id, vc+10 AS total
    FROM source
)

SELECT id, SUM(total)
FROM source_with_total
GROUP BY id;