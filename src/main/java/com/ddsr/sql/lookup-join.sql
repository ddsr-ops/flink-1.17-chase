-- A lookup join is typically used to enrich a table with data that is queried from an external system. The join requires one table to have a processing time attribute and the other table to be backed by a lookup source connector.
--
-- The lookup join uses the above Processing Time Temporal Join syntax with the right table to be backed by a lookup source connector.
--
-- The following example shows the syntax to specify a lookup join.

-- Customers is backed by the JDBC connector and can be used for lookup joins
CREATE TEMPORARY TABLE Customers (
  id INT,
  name STRING,
  country STRING,
  zip STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysqlhost:3306/customerdb',
  'table-name' = 'customers'
);

-- enrich each order with customer information
-- Note: Every one row of orders is coming, query action will be performed in MySQL database
SELECT o.order_id, o.total, c.country, c.zip
FROM Orders AS o
         JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
              ON o.customer_id = c.id;
-- "FOR SYSTEM_TIME AS OF alias.proc_time", the section is solid for lookup join

-- In the example above, the Orders table is enriched with data from the Customers table which resides in a MySQL database. The FOR SYSTEM_TIME AS OF clause with the subsequent processing time attribute ensures that each row of the Orders table is joined with those Customers rows that match the join predicate at the point in time when the Orders row is processed by the join operator. It also prevents that the join result is updated when a joined Customer row is updated in the future. The lookup join also requires a mandatory equality join predicate, in the example above o.customer_id = c.id.