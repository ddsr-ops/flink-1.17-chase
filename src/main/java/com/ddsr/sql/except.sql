(SELECT s FROM t1) EXCEPT (SELECT s FROM t2);

(SELECT s FROM t1) EXCEPT ALL (SELECT s FROM t2);

-- EXCEPT and EXCEPT ALL return the rows that are found in one table but not the other. EXCEPT takes only distinct rows while EXCEPT ALL does not remove duplicates from the result rows.
-- Support batch and streaming