SELECT
    window_start,
    window_end,
    id ,
    SUM(vc) sumVC
FROM TABLE(
        CUMULATE(TABLE ws, DESCRIPTOR(et), INTERVAL '2' SECONDS , INTERVAL '6' SECONDS))
GROUP BY window_start, window_end, id;
--  rollup( (id) )
--  cube( (id) )
--  grouping sets( (id),()  )