#!/bin/bash

# Check if two arguments are given
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 file1.txt file2.txt"
    exit 1
fi

# Assign the arguments to variables
file1=$1
file2=$2

# Check if files exist
if [ ! -f "$file1" ]; then
    echo "Error: $file1 does not exist."
    exit 1
fi

if [ ! -f "$file2" ]; then
    echo "Error: $file2 does not exist."
    exit 1
fi

# Use diff to compare the files
diff $file1 $file2

# Check if files are same
if [ $? -eq 0 ]; then
    echo "No differences found."
else
    echo "Differences found."
fi