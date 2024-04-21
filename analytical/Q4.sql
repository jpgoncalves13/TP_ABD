EXPLAIN ANALYZE 
SELECT date_bin('1 minute', date, '2008-01-01 00:00:00'), count(*)
FROM badges
WHERE NOT tagbased
    AND name NOT IN (
        'Analytical',
        'Census',
        'Documentation Beta',
        'Documentation Pioneer',
        'Documentation User',
        'Reversal',
        'Tumbleweed'
    )
    AND class in (1, 2, 3)
    AND userid <> -1
GROUP BY 1
ORDER BY 1;


------------------------------------------------------------------------------------------------------------

EXPLAIN ANALYZE
SELECT date_trunc('minute', date) AS minute, count(*)
FROM badges
WHERE NOT tagbased
    AND name NOT IN (
        'Analytical',
        'Census',
        'Documentation Beta',
        'Documentation Pioneer',
        'Documentation User',
        'Reversal',
        'Tumbleweed'
    )
    AND class in (1, 2, 3)
    AND userid <> -1
GROUP BY minute
ORDER BY minute;


CREATE INDEX idx_badges_date ON badges (date);
CREATE INDEX idx_badges_tagbased ON badges (tagbased);
CREATE INDEX idx_badges_name ON badges (name);
CREATE INDEX idx_badges_class ON badges (class);
CREATE INDEX idx_badges_userid ON badges (userid);



