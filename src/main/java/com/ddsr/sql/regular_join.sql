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