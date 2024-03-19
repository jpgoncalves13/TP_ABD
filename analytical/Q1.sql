SELECT id, displayname,
    count(DISTINCT q_id) + count(DISTINCT a_id) + count(DISTINCT c_id) total
FROM (
    SELECT u.*, q.id q_id, a.id a_id, c.id c_id
    FROM users u
    LEFT JOIN (
        SELECT *
        FROM questions
        WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    ) q ON q.owneruserid = u.id
    LEFT JOIN (
        SELECT *
        FROM answers
        WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    ) a ON a.owneruserid = u.id
    LEFT JOIN (
        SELECT *
        FROM comments
        WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    ) c ON c.userid = u.id
) t
GROUP BY id, displayname
ORDER BY total DESC
LIMIT 100;
