import psycopg2
import csv

conn = psycopg2.connect(
    dbname="abd",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432 "
)

def fetch_query_results(query):
    cursor = conn.cursor()

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()

    return sorted(results)

def write_results_to_file(results, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(results)

try:
    query1 = """
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
    """
    
    query2 = """
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
    
    results1 = fetch_query_results(query1)
    results2 = fetch_query_results(query2)
    
    #write_results_to_file(results1, 'results1.csv')
    #write_results_to_file(results2, 'results2.csv')

    if results1 == results2:
        print("Os resultados das queries são iguais.")
    else:
        print("Os resultados das queries são diferentes.")

except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    conn.close()
