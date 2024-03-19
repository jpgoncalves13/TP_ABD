SELECT tagname, round(avg(total), 3), count(*)
FROM (
    SELECT t.tagname, q.id, count(*) AS total
    FROM tags t
    JOIN questionstags qt ON qt.tagid = t.id
    JOIN questions q ON q.id = qt.questionid
    LEFT JOIN answers a ON a.parentid = q.id
    WHERE t.id IN (
        SELECT t.id
        FROM tags t
        JOIN questionstags qt ON qt.tagid = t.id
        JOIN questions q ON q.id = qt.questionid
        GROUP BY t.id
        HAVING count(*) > 10
    )
    GROUP BY t.tagname, q.id
)
GROUP BY tagname
ORDER BY 2 DESC, 3 DESC, tagname;
