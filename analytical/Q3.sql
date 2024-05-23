EXPLAIN ANALYZE
SELECT t.tagname, round(avg(t.total), 3), count(*)
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
) t
GROUP BY tagname
ORDER BY 2 DESC, 3 DESC, tagname;


--------------------------------------------------------------


EXPLAIN ANALYZE
WITH questions_answers AS (
    SELECT q.id, COUNT(*) AS total
    FROM questions q 
    LEFT JOIN answers a ON a.parentid = q.id
    GROUP BY q.id
),
relevant_tags AS (
    SELECT t.id, t.tagname
    FROM tags t
    JOIN questionstags qt ON qt.tagid = t.id
    JOIN questions q ON q.id = qt.questionid
    GROUP BY t.id
    HAVING COUNT(*) > 10
)
SELECT t.tagname, ROUND(AVG(t.total), 3) AS avg_total, COUNT(*) AS count
FROM (
    SELECT t.tagname, q.id, q.total
    FROM relevant_tags t
    JOIN questionstags qt ON qt.tagid = t.id
    JOIN questions_answers qa ON qa.id = qt.questionid
) t
GROUP BY t.tagname
ORDER BY avg_total DESC, count DESC, t.tagname;


CREATE INDEX idx_questions_id ON questions(id);

CREATE INDEX idx_answers_parentid ON answers(parentid);

CREATE INDEX idx_tags_id ON tags(id);

CREATE INDEX idx_questiontags_tagid ON questionstags(tagid);
CREATE INDEX idx_questiontags_questionid ON questionstags(questionid);




