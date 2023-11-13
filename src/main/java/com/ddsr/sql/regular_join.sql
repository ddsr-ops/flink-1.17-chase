SELECT *
FROM ws
         INNER JOIN ws1
                    ON ws.id = ws1.id;

SELECT *
FROM ws
         LEFT JOIN ws1
                   ON ws.id = ws1.id;

SELECT *
FROM ws
         RIGHT JOIN ws1
                    ON ws.id = ws1.id;

SELECT *
FROM ws
         FULL OUTER JOIN ws1
                         ON ws.id = ws.id;

-- The states(left table or right table) are hold in the memory, will be cleared if they exceed the TTL
-- Therefore, the data might be lost after TTL is expired, setting a suitable TTL is recommended