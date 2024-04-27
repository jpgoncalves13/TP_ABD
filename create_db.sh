#!/bin/bash
DB_NAME="stack_full"
DB_FILE="stack_full.sql"

# Drop the database if it exists
dropdb --if-exists $DB_NAME

# Create the database
createdb $DB_NAME

# Populate the database
psql -d $DB_NAME -f $DB_FILE

# Add indexes to the database
psql -d $DB_NAME << EOF
CREATE INDEX idx_title_gin ON questions USING GIN(to_tsvector('english', title));
EOF
