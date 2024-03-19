WITH buckets AS (
    SELECT year,
        generate_series(0, (
            SELECT cast(max(reputation) as int)
            FROM users
            WHERE extract(year FROM creationdate) = year
        ), 5000) AS reputation_range
    FROM (
        SELECT generate_series(2008, extract(year FROM NOW())) AS year
    ) years
    GROUP BY 1, 2
)
SELECT year, reputation_range, count(u.id) total
FROM buckets
LEFT JOIN (
    SELECT id, creationdate, reputation
    FROM users
    WHERE id in (
        SELECT a.owneruserid
        FROM answers a
        WHERE a.id IN (
            SELECT postid
            FROM votes v
            JOIN votestypes vt ON vt.id = v.votetypeid
            WHERE vt.name = 'AcceptedByOriginator'
                AND v.creationdate >= NOW() - INTERVAL '5 year'
        )
    )
) u ON extract(year FROM u.creationdate) = year
    AND floor(u.reputation / 5000) * 5000 = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;
