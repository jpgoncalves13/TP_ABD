import psycopg2
from psycopg2 import sql
import time
import csv

conn = psycopg2.connect(
    dbname="abd",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432 "
)



query1_sql = """
SELECT u.id, u.displayname, COALESCE(total_activities, 0) AS total
FROM users u
LEFT JOIN (
    SELECT owneruserid, COUNT(*) AS total_activities
    FROM (
        SELECT owneruserid FROM questions WHERE creationdate BETWEEN NOW() - INTERVAL '%s months' AND NOW()
        UNION ALL
        SELECT owneruserid FROM answers WHERE creationdate BETWEEN NOW() - INTERVAL '%s months' AND NOW()
        UNION ALL
        SELECT userid FROM comments WHERE creationdate BETWEEN NOW() - INTERVAL '%s months' AND NOW()
    )
    GROUP BY owneruserid
) activity ON activity.owneruserid = u.id
ORDER BY total DESC
LIMIT 100;
"""

query1_indexes = """
CREATE INDEX idx_questions_owneruserid ON questions(owneruserid);
CREATE INDEX idx_questions_creationdate ON questions(creationdate);

CREATE INDEX idx_answers_owneruserid ON answers(owneruserid);
CREATE INDEX idx_answers_creationdate ON answers(creationdate);

CREATE INDEX idx_comments_userid ON comments(userid);
CREATE INDEX idx_comments_creationdate ON comments(creationdate);

CREATE INDEX idx_users_id ON users(id);
"""

query1_params = [
    (2, 2, 2),
    (4, 4, 4),
    (6, 6, 6),
    (8, 8, 8),
    (10, 10, 10),
    (12, 12, 12)
]


query2_sql = """
WITH buckets AS (
    SELECT y.year, generate_series(0, COALESCE(m.max_reputation, 0), 5000) AS reputation_range 
    FROM (SELECT generate_series(2008, extract(year FROM NOW())) AS year) AS y
    JOIN max_reputation_per_year m ON m.year = y.year
),
users_reputation AS (
    SELECT id, EXTRACT(year FROM creationdate) AS year, FLOOR(reputation / 5000) * 5000 AS reputation_interval
    FROM users
    WHERE id in (
        SELECT a.owneruserid
        FROM answers a
        JOIN votes v ON v.postid = a.id
        JOIN votestypes vt ON vt.id = v.votetypeid
        WHERE vt.name = 'AcceptedByOriginator' AND v.creationdate >= NOW() - INTERVAL '5 year'
    )
)
SELECT b.year, b.reputation_range, COUNT(ur.id) AS total
FROM buckets b
LEFT JOIN users_reputation ur ON ur.year = b.year AND ur.reputation_interval = b.reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;
"""

query2_params = [
    (5000, 1, 5000, 5000),
    (5000, 10, 5000, 5000),
    (5000, 5, 5000, 5000),
    (2000, 5, 2000, 2000),
    (8000, 5, 8000, 8000)
]

query2_indexes = """
CREATE INDEX idx_max_reputation_per_year_on_year ON max_reputation_per_year(year);

CREATE INDEX idx_users_id ON users(id);

CREATE INDEX idx_answers_id ON answers(id);

CREATE INDEX idx_votes_postid ON votes(postid);
CREATE INDEX idx_votes_creationdate ON votes(creationdate);
CREATE INDEX idx_votes_votetypeid ON votes(votetypeid);

CREATE INDEX idx_votestypes_id ON votestypes(id);
CREATE INDEX idx_votestypes_name ON votestypes(name);
"""

query3_sql = """
WITH questions_prepared AS (
    SELECT q.id, COUNT(*) AS total
    FROM questions q 
    LEFT JOIN answers a ON a.parentid = q.id
    GROUP BY q.id
),
tag_counts AS (
    SELECT t.id, t.tagname
    FROM tags t
    JOIN questionstags qt ON qt.tagid = t.id
    JOIN questions q ON q.id = qt.questionid
    GROUP BY t.id
    HAVING COUNT(*) > 10
)
SELECT t.tagname, round(avg(t.total), 3), count(*)
FROM (
    SELECT t.tagname, q.id, q.total
    FROM tag_counts t
    JOIN questionstags qt ON qt.tagid = t.id
    JOIN questions_prepared q ON q.id = qt.questionid
) t
GROUP BY tagname
ORDER BY 2 DESC, 3 DESC, tagname;
"""

query3_indexes = """
CREATE INDEX idx_questions_id ON questions(id);

CREATE INDEX idx_answers_parentid ON answers(parentid);

CREATE INDEX idx_tags_id ON tags(id);

CREATE INDEX idx_questiontags_tagid ON questionstags(tagid);
CREATE INDEX idx_questiontags_questionid ON questionstags(questionid);
"""

query3_params = [
    (5,),
    (10,),
    (15,),
    (20,)
]


query4_sql = """
SELECT date_bin('%s minute', date, '2008-01-01 00:00:00'), count(*)
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
"""

query4_indexes = """
CREATE INDEX idx_badges ON badges (tagbased, name, class, userid);
"""

query4_params = [
    (0.5,),
    (1,),
    (5,),
    (10,)
]


query1 = {"sql": query1_sql, "params": query1_params, "indexes": query1_indexes}
query2 = {"sql": query2_sql, "params": query2_params, "indexes": query2_indexes}
query3 = {"sql": query3_sql, "params": query3_params, "indexes": query3_indexes}
query4 = {"sql": query4_sql, "params": query4_params, "indexes": query4_indexes}

queries = [query1, query2, query3, query4]

# Nome do arquivo CSV
output_csv = 'query_execution_times.csv'

# Cabeçalho do CSV
csv_header = ['Query', 'Parameter', 'Average Time']

try:
    cursor = conn.cursor()

    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(csv_header)

        i = 1
    
        for query in queries:
            cursor.execute(query["indexes"])
            conn.commit()

            for param in query["params"]:
                times = []

                for _ in range(5):
                    start_time = time.time()  
                    cursor.execute(query["sql"], param)
                    end_time = time.time()

                    times.append(end_time - start_time)

                times.sort()
                excluded_times = times[1:-1]

                avg_time = sum(times) / len(times)
                print(f"Parameter: {param} : {avg_time}")

                writer.writerow(["Query " + str(i), param, avg_time])

            # DROPAR OS INDICES

            i+=1
            



except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    cursor.close()
    conn.close()
