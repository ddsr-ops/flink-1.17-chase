SELECT window_start, window_end, id , SUM(vc) sumVC
FROM TABLE(
        HOP(TABLE ws, DESCRIPTOR(et), INTERVAL '5' SECONDS , INTERVAL '10' SECONDS))
GROUP BY window_start, window_end, id;

-- et as process time or event time
-- '5' as sliding stride length, '10' as sliding window length
-- Note: the window length is multiple times of stride length