(SELECT s FROM t1) INTERSECT (SELECT s FROM t2);
(SELECT s FROM t1) INTERSECT ALL (SELECT s FROM t2);

-- INTERSECT and INTERSECT ALL return the rows that are found in both tables. INTERSECT takes only distinct rows while INTERSECT ALL does not remove duplicates from the result rows.
-- Support batch and streaming