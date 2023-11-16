
SELECT user, amount
FROM Orders
WHERE product EXISTS (
    SELECT product FROM NewProducts
)
-- Returns true if the sub-query returns at least one row. Only supported if the operation can be rewritten in a join and group operation.
--
--     The optimizer rewrites the EXISTS operation into a join and group operation. For streaming queries, the required state for computing the query result might grow infinitely depending on the number of distinct input rows. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See query configuration for details.