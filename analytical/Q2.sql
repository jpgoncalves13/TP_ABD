VERIFICAR RESULTADOSVERIFICAR RESULTADOSVERIFICAR RESULTADOS
VERIFICAR RESULTADOS
VERIFICAR RESULTADOS

VERIFICAR RESULTADOS
VERIFICAR RESULTADOS
VERIFICAR RESULTADOS

VERIFICAR RESULTADOS
VERIFICAR RESULTADOS
VERIFICAR RESULTADOS

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


-----------------------------------------------------------------------------------------------

-- COALESCE PARA O CASO EM QUE HA UM ANO SEM REPUTAÇÕES
EXPLAIN ANALYZE 
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


-- CRIAR VISTA MATEARILIZADA PARA TER O MAXIMO DE REPUTAÇÃO POR ANO
CREATE MATERIALIZED VIEW max_reputation_per_year AS
SELECT extract(year FROM creationdate) AS year, MAX(reputation) AS max_reputation
FROM users
GROUP BY extract(year FROM creationdate);


CREATE INDEX idx_max_reputation_per_year_on_year ON max_reputation_per_year(year);

CREATE INDEX idx_users_id ON users(id);

CREATE INDEX idx_answers_id ON answers(id);

CREATE INDEX idx_votes_postid ON votes(postid);
CREATE INDEX idx_votes_creationdate ON votes(creationdate);
CREATE INDEX idx_votes_votetypeid ON votes(votetypeid);

CREATE INDEX idx_votestypes_id ON votestypes(id);
CREATE INDEX idx_votestypes_name ON votestypes(name);

