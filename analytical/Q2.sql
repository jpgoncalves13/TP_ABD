EXPLAIN ANALYZE 
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

------------------------------------------------------------------------------------------------------------------------

EXPLAIN ANALYZE 
WITH years AS (
    SELECT generate_series(2008, extract(year FROM NOW())) AS year
),
max_reputation AS (
    SELECT extract(year FROM creationdate) AS year, max(reputation) as max_rep
    FROM users
    GROUP BY extract(year FROM creationdate)
),
buckets AS (
    SELECT y.year, generate_series(0, COALESCE(mr.max_rep, 0), 5000) AS reputation_range
    FROM years y
    LEFT JOIN max_reputation mr ON y.year = mr.year
),
recent_accepted_answers AS (
    SELECT a.owneruserid
    FROM answers a
    JOIN votes v ON a.id = v.postid
    JOIN votestypes vt ON vt.id = v.votetypeid
    WHERE vt.name = 'AcceptedByOriginator' AND v.creationdate >= NOW() - INTERVAL '5 year'
    GROUP BY a.owneruserid
),
filtered_users AS (
    SELECT id, extract(year FROM creationdate) AS year, floor(reputation / 5000) * 5000 AS rep_range
    FROM users
    WHERE id IN (SELECT owneruserid FROM recent_accepted_answers)
)
SELECT b.year, b.reputation_range, count(fu.id) as total
FROM buckets b
LEFT JOIN filtered_users fu ON fu.year = b.year AND fu.rep_range = b.reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;


CREATE INDEX idx_users_creationdate ON users (date_trunc('year', creationdate));
CREATE INDEX idx_users_id ON users (id);
CREATE INDEX idx_users_reputation_creationdate ON users (reputation, date_trunc('year', creationdate));


CREATE INDEX idx_answers_owneruserid ON answers (owneruserid);
CREATE INDEX idx_answers_id ON answers (id);


CREATE INDEX idx_votes_postid_votetypeid ON votes (postid, votetypeid);
CREATE INDEX idx_votes_creationdate ON votes (creationdate);

CREATE INDEX idx_votestypes_name ON votestypes (name);
