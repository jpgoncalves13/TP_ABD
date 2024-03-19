# StackBench

### Load Data (PostgreSQL)

- Download the dataset (https://storage.googleapis.com/abd24/data.zip) and export it to `db/data`;

- Install the requirements (with `pip`):
```shell
pip3 install -r db/requirements.txt
```

- Create a database to store the data;

- Load:
```shell
# replace 'HOST', 'PORT', 'DBNAME', 'USER', and 'PASSWORD' with the
# respective connection variables.
python3 db/load.py -H HOST -P PORT -d DBNAME -u USER -p PASSWORD
```

### Transactional workload (Java 21)

- Install:
```shell
cd transactional
mvn package
```

- Run:
```shell
# replace the connection, warmup, runtime, and client variables with the respective values
java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://HOST:PORT/DBNAME -U USER -P PASSWORD -W WARMUP -R RUNTIME -c CLIENTS
# E.g.:
java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/stack -U postgres -P postgres -W 15 -R 180 -c 16
```

### Analytical workloads

The analytical queries can be found in the `analytical` folder.
