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
