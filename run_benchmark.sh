#!/bin/bash

DB_NAME="stack_lite"
DURATION="60"
WARMUP_TIME="15"

java -jar stackBenck/transactional/target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/$DB_NAME -U postgres -P postgres -W $WARMUP_TIME -R $DURATION -c 16
