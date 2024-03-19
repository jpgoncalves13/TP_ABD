-- update the sequences
SELECT setval('answers_id_seq', (SELECT MAX(id) FROM Answers));
SELECT setval('comments_id_seq', (SELECT MAX(id) FROM Comments));
SELECT setval('questions_id_seq', (SELECT MAX(id) FROM Questions));
SELECT setval('tags_id_seq', (SELECT MAX(id) FROM Tags));
SELECT setval('users_id_seq', (SELECT MAX(id) FROM Users));
SELECT setval('votes_id_seq', (SELECT MAX(id) FROM Votes));
