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


-- QUESTIONS
-- QUESTIONSTAGS
-- TAGS
-- ANSWERS


-- 25247.67 ms
------------------------------------------------------------------------

EXPLAIN ANALYZE
SELECT t.tagname, round(avg(t.total), 3), count(*)
FROM (
    SELECT t.tagname, q.id, COUNT(*) AS total
    FROM RelevantTags t
    JOIN questionstags qt ON qt.tagid = t.id
    JOIN questions q ON q.id = qt.questionid
    LEFT JOIN answers a ON a.parentid = q.id
    GROUP BY t.tagname, q.id
) t
GROUP BY tagname
ORDER BY 2 DESC, 3 DESC, tagname;


------------------------------------------------------------------------


CREATE MATERIALIZED VIEW RelevantTags AS
SELECT t.id, t.tagname
FROM tags t
JOIN questionstags qt ON qt.tagid = t.id
JOIN questions q ON q.id = qt.questionid
GROUP BY t.id
HAVING COUNT(*) > 10;

SELECT rt.tagname, ROUND(AVG(a.total), 3) AS avg_total, COUNT(*) AS num_questions
FROM RelevantTags rt
JOIN questionstags qt ON qt.tagid = rt.id
JOIN questions q ON q.id = qt.questionid
LEFT JOIN 
    (SELECT parentid, COUNT(*) AS total
     FROM answers
     GROUP BY parentid) a ON q.id = a.parentid
GROUP BY rt.tagname
ORDER BY avg_total DESC, num_questions DESC, rt.tagname;


------------------------------------------------------------------------
WITH tag_question_occurrences AS (
    SELECT rt.tagname, COUNT(DISTINCT qt.questionid) AS nr_tags, COUNT(*) AS total
    FROM RelevantTags rt
    JOIN questionstags qt ON qt.tagid = rt.id
    LEFT JOIN answers a ON a.parentid = qt.questionid
    GROUP BY rt.tagname
)
SELECT t.tagname, ROUND((t.total :: NUMERIC/ t.nr_tags), 3) AS avg_total, t.nr_tags
FROM tag_question_occurrences t
ORDER BY avg_total DESC, t.nr_tags DESC, t.tagname;

------------------------------------------------------------------------

WITH tag_question_occurrences AS (
    SELECT a.tagname, COUNT(*) as nr_tags
    FROM (
        SELECT qt.questionid, rt.tagname
        FROM RelevantTags rt
        JOIN questionstags qt ON qt.tagid = rt.id 
        GROUP BY rt.tagname, qt.questionid
    ) a
    GROUP BY a.tagname
),
tag_occurrences AS (
    SELECT rt.tagname, COUNT(*) AS total
    FROM RelevantTags rt
    JOIN questionstags qt ON qt.tagid = rt.id 
    LEFT JOIN answers a ON a.parentid = qt.questionid
    GROUP BY rt.tagname
)
SELECT t.tagname, ROUND((t.total :: NUMERIC/ tq.nr_tags), 3) AS avg_total, tq.nr_tags
FROM tag_occurrences t
JOIN tag_question_occurrences tq ON t.tagname = tq.tagname 
ORDER BY avg_total DESC, tq.nr_tags DESC, t.tagname;
