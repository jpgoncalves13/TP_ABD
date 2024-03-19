import argparse
from multiprocessing import Pool
import os
import psycopg2
from psycopg2.extensions import connection
from pathlib import Path

# data directory with the CSVs
DATA_DIR = 'data'

# list of CSV files
FILES = [
    "Badges.csv",
    "Comments.csv",
    "Questions.csv",
    "QuestionsTags.csv",
    "QuestionsLinks.csv",
    "Answers.csv",
    "Tags.csv",
    "Users.csv",
    "Votes.csv",
    "VotesTypes.csv"
]

# creates a factory of databases connections
def createConnection(connStr: str) -> connection:
    return psycopg2.connect(connStr)


# loads a CSV file into some table
def loadFile(filename: str, tablename, connStr: str):
    print(f'Loading {tablename}')
    conn = createConnection(connStr)
    conn.autocommit = True
    cursor = conn.cursor()

    with open(filename, encoding='utf8') as f:
        cursor.copy_expert(f"COPY {tablename} FROM stdin WITH CSV HEADER DELIMITER ',' QUOTE '\"'", f)
    
    conn.close()
    print(f'Loading {tablename} completed')


# executes a sql script
def executeScript(filename, connStr: str):
    print(f'Executing {filename}')
    conn = createConnection(connStr)
    cursor = conn.cursor()

    with open(os.path.join(os.path.dirname(__file__), filename)) as f:
        cursor.execute(f.read())

    conn.commit()
    conn.close()


# runs vacuum + analyze
def vacuumAnalyze(connStr: str):
    print(f'Running VACUUM ANALYZE')
    conn = createConnection(connStr)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('VACUUM ANALYZE')
    conn.close()


# executes the schema.sql script in the provided database and loads each CSV file
def populateDatabase(connStr: str):
    executeScript('schema.sql', connStr)
    
    p = Pool(os.cpu_count())
    jobs = []
    for filename in [os.path.join(os.path.dirname(__file__), DATA_DIR, x) for x in FILES]:
        tablename = Path(filename).stem.lower()
        jobs.append(p.apply_async(loadFile, (filename, tablename, connStr)))
    [x.get() for x in jobs]

    executeScript('after_populate.sql', connStr)
    
    vacuumAnalyze(connStr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', help='PostgreSQL host', default='localhost')
    parser.add_argument('-P', '--port', help='PostgreSQL port', default=5432)
    parser.add_argument('-d', '--database', help='Database name', default='stack')
    parser.add_argument('-u', '--user', help='Username', default='postgres')
    parser.add_argument('-p', '--password', help='Password', default='postgres')
    args = parser.parse_args()

    connStr = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
    populateDatabase(connStr)


if __name__ == '__main__':
    main()
