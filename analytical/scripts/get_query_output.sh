
#!/bin/bash

query="
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
"


FILENAME=${1:-"output.txt"}

psql -U postgres -d abd -c "$query" > $FILENAME

echo "Output written to $FILENAME successfully"
