EXPLAIN ANALYZE 
SELECT id, displayname,
    count(DISTINCT q_id) + count(DISTINCT a_id) + count(DISTINCT c_id) total
FROM (
    SELECT u.id, u.displayname, q.id q_id, a.id a_id, c.id c_id
    FROM users u
    LEFT JOIN (
        SELECT owneruserid, id
        FROM questions
        WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    ) q ON q.owneruserid = u.id
    LEFT JOIN (
        SELECT owneruserid, id
        FROM answers
        WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    ) a ON a.owneruserid = u.id
    LEFT JOIN (
        SELECT userid, id
        FROM comments
        WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    ) c ON c.userid = u.id
) t
GROUP BY id, displayname
ORDER BY total DESC
LIMIT 100;

-----------------------------------------------------------------------------------------------------------------------------

EXPLAIN ANALYZE
SELECT u.id, u.displayname, COALESCE(total_activities, 0) AS total
FROM users u
LEFT JOIN (
    SELECT owneruserid, COUNT(*) AS total_activities
    FROM (
        SELECT owneruserid FROM questions WHERE creationdate BETWEEN NOW() - INTERVAL '6 months' AND NOW()
        UNION ALL
        SELECT owneruserid FROM answers WHERE creationdate BETWEEN NOW() - INTERVAL '6 months' AND NOW()
        UNION ALL
        SELECT userid FROM comments WHERE creationdate BETWEEN NOW() - INTERVAL '6 months' AND NOW()
    )
    GROUP BY owneruserid
) activity ON activity.owneruserid = u.id
ORDER BY total DESC
LIMIT 100;

-- 3200 m

CREATE INDEX idx_questions_owneruserid ON questions(owneruserid);
CREATE INDEX idx_questions_creationdate ON questions(creationdate);

CREATE INDEX idx_answers_owneruserid ON answers(owneruserid);
CREATE INDEX idx_answers_creationdate ON answers(creationdate);

CREATE INDEX idx_comments_userid ON comments(userid);
CREATE INDEX idx_comments_creationdate ON comments(creationdate);

CREATE INDEX idx_users_id ON users(id);

-- 400 ms 