#!/bin/bash

num_runs=10
db_name="stack_full"

# Query to be executed
q="
"

# Timing and storing run times
times=()

# Run the query multiple times
for ((i = 1; i <= num_runs; i++)); do
  echo "Running query... ($i/$num_runs)"

  # Run the query and capture the output
  output=$(psql -d "$db_name" -c "EXPLAIN ANALYZE $q" 2>&1)
  if [ $? -ne 0 ]; then
    echo "Error running query on attempt $i: $output"
    exit 1
  fi

  # Extract the execution time from the output
  elapsed_time=$(echo "$output" | awk '/Execution Time:/ {print $3}')
  times+=($elapsed_time)

  # Write the output to a file
  echo "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------" >>query_benchmark_results.txt
  echo "RUN1 $i" >>query_benchmark_results.txt
  echo "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------" >>query_benchmark_results.txt
  echo "$output" >>query_benchmark_results.txt
  echo -e "\n\n" >> query_benchmark_results.txt
done

# Sort times and remove the best and worst
IFS=$'\n' sorted_times=($(sort -n <<<"${times[*]}"))
unset sorted_times[0]
unset sorted_times[-1]

# Calculate the average time from the remaining times
total_time=0
for t in "${sorted_times[@]}"; do
  total_time=$(echo "$total_time + $t" | bc)
done
average_time=$(echo "scale=2; $total_time / ${#sorted_times[@]}" | bc)

echo "Average time: $average_time ms"
echo "Average time: $average_time ms" >> query_benchmark_results.txt