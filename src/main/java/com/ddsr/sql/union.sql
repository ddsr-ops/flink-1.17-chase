(SELECT id FROM ws) UNION (SELECT id FROM ws1);
(SELECT id FROM ws) UNION ALL (SELECT id FROM ws1);

-- UNION and UNION ALL return the rows that are found in either table. UNION takes only distinct rows while UNION ALL does not remove duplicates from the result rows.
