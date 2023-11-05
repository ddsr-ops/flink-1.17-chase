SELECT
    window_start,
    window_end,
    id , SUM(vc)
        sumVC
FROM TABLE(
        TUMBLE(TABLE ws, DESCRIPTOR(et), INTERVAL '5' SECONDS))
GROUP BY window_start, window_end, id;

-- et as process time or event time