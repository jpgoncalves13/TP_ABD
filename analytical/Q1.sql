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


------------------------------------------------------------------------------------------------------------------------------

EXPLAIN ANALYZE 
SELECT u.id,
       u.displayname,
       COALESCE(q.total_questions, 0) + COALESCE(a.total_answers, 0) + COALESCE(c.total_comments, 0) AS total
FROM users u
LEFT JOIN (
    SELECT owneruserid, COUNT(*) AS total_questions
    FROM questions
    WHERE creationdate BETWEEN NOW() - INTERVAL '6 months' AND NOW()
    GROUP BY owneruserid
) q ON q.owneruserid = u.id
LEFT JOIN (
    SELECT owneruserid, COUNT(*) AS total_answers
    FROM answers
    WHERE creationdate BETWEEN NOW() - INTERVAL '6 months' AND NOW()
    GROUP BY owneruserid
) a ON a.owneruserid = u.id
LEFT JOIN (
    SELECT userid, COUNT(*) AS total_comments
    FROM comments
    WHERE creationdate BETWEEN NOW() - INTERVAL '6 months' AND NOW()
    GROUP BY userid
) c ON c.userid = u.id
ORDER BY total DESC
LIMIT 100;


CREATE INDEX idx_answers ON answers (owneruserid, creationdate);
CREATE INDEX idx_questions ON questions (owneruserid, creationdate);
CREATE INDEX idx_comments ON comments (userid, creationdate);
CREATE INDEX idx_users on users (id);


-- VISTAS MATERIALIZADAS ??