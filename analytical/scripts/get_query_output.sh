#!/bin/bash

db_name="stack_full"
filename=${1:-"output.txt"}

# Query to be executed
q="
"

psql -U postgres -d "$db_name" -c "$q" > $filename

echo "Output written to $filename successfully"
