#!/bin/bash
#
# Run CSV import benchmarks and save logs
#
# Usage:
#   ./run_benchmark_04_csv_import_documents.sh [size] [configs] [--export]
#
# Configs (comma-separated pX_bY format):
#   pX_bY format where X = parallel threads, Y = batch size
#   Examples: p4_b5000, p8_b10000, p1_b5000, p16_b15000
#
# Examples:
#   ./run_benchmark_04_csv_import_documents.sh small p4_b5000
#   ./run_benchmark_04_csv_import_documents.sh small "p4_b5000,p8_b10000"
#   ./run_benchmark_04_csv_import_documents.sh large "p4_b5000,p4_b10000,p8_b5000,p8_b10000"
#   ./run_benchmark_04_csv_import_documents.sh small "p1_b5000,p4_b5000,p8_b5000,p16_b5000"
#   ./run_benchmark_04_csv_import_documents.sh small p4_b5000 --export   # Export databases
#

# Start timing
SCRIPT_START=$(date +%s)

# Get parameters with defaults
SIZE=${1:-small}
CONFIGS=${2:-p4_b5000}

# Check for --export flag (can be in any position)
EXPORT_FLAG=""
if [[ "$@" == *"--export"* ]]; then
    EXPORT_FLAG="--export"
    echo "💾 Export enabled: Databases will be exported to JSONL after import"
    echo ""
fi

# Parse configs argument - expecting comma-separated pX_bY format
declare -A RUN_CONFIGS

IFS=',' read -ra CONFIG_ARRAY <<< "$CONFIGS"
for config in "${CONFIG_ARRAY[@]}"; do
    # Trim whitespace
    config=$(echo "$config" | xargs)

    # Validate format: pX_bY where X and Y are numbers
    if [[ ! "$config" =~ ^p[0-9]+_b[0-9]+$ ]]; then
        echo "❌ Invalid config format: $config"
        echo "   Expected format: pX_bY (e.g., p4_b5000, p8_b10000)"
        echo "   where X = parallel threads, Y = batch size"
        exit 1
    fi

    # Extract parallel and batch values
    PAR=$(echo "$config" | sed 's/p\([0-9]*\)_b.*/\1/')
    BATCH=$(echo "$config" | sed 's/p[0-9]*_b\([0-9]*\)/\1/')

    RUN_CONFIGS[$config]="$PAR,$BATCH"
done

# Count how many configs we're running
NUM_CONFIGS=${#RUN_CONFIGS[@]}

# Create log directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="./benchmark_logs/csv_import_${SIZE}_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

echo "========================================================================"
echo "CSV Import Benchmarks - $NUM_CONFIGS Configuration(s)"
echo "========================================================================"
echo "Dataset: $SIZE"
echo "Configurations: ${!RUN_CONFIGS[@]}"
echo "Log directory: $LOG_DIR"
echo ""

# Check if dataset exists
DATA_BASE="./data"
if [ "$SIZE" = "small" ]; then
    DATA_DIR="$DATA_BASE/ml-small"
else
    DATA_DIR="$DATA_BASE/ml-large"
fi

if [ ! -d "$DATA_DIR" ]; then
    echo "❌ Dataset not found: $DATA_DIR"
    echo "   The script will automatically download it on first run."
    echo ""
fi

echo "Dataset directory: $DATA_DIR"
if [ -d "$DATA_DIR" ]; then
    echo "  Size: $(du -sh "$DATA_DIR" 2> /dev/null | cut -f1)"
fi
echo ""

# Function to monitor memory usage of a process (Python + embedded Java JVM)
# Note: Java JVM is embedded within Python process (GraalVM), not a separate process
monitor_memory() {
    local PID=$1
    local NAME=$2
    local LOG_FILE="$LOG_DIR/${NAME}_memory.log"

    echo "Time,RSS_MB,VSZ_MB,CPU%" > "$LOG_FILE"

    while kill -0 $PID 2> /dev/null; do
        # Get Python process stats (includes embedded Java JVM)
        # RSS = Resident Set Size (actual physical memory used)
        # VSZ = Virtual Size (total virtual memory allocated)
        STATS=$(ps -o rss=,vsz=,%cpu= -p $PID 2> /dev/null || echo "0 0 0")
        RSS_KB=$(echo $STATS | awk '{print $1}')
        VSZ_KB=$(echo $STATS | awk '{print $2}')
        CPU=$(echo $STATS | awk '{print $3}')

        # Convert to MB
        RSS_MB=$(echo "scale=2; $RSS_KB/1024" | bc)
        VSZ_MB=$(echo "scale=2; $VSZ_KB/1024" | bc)

        TIMESTAMP=$(date +%s)
        echo "$TIMESTAMP,$RSS_MB,$VSZ_MB,$CPU" >> "$LOG_FILE"

        sleep 2
    done
}

echo "Starting $NUM_CONFIGS parallel run(s)..."
echo ""

# Track PIDs and statuses
declare -A PIDS
declare -A STATUSES
RUN_NUM=0

# Run selected configurations
for config in "${!RUN_CONFIGS[@]}"; do
    RUN_NUM=$((RUN_NUM + 1))

    # Parse parallel and batch-size from config value
    IFS=',' read -r PAR BATCH <<< "${RUN_CONFIGS[$config]}"

    # Create a unique working directory for this config to avoid log conflicts
    WORK_DIR="./benchmark_work/${config}"
    mkdir -p "$WORK_DIR"

    # Build command arguments with absolute paths
    SCRIPT_PATH="$(pwd)/04_csv_import_documents.py"
    ABS_SIZE_ARG="$SIZE"
    CMD_ARGS="--size $ABS_SIZE_ARG --batch-size $BATCH --parallel $PAR --db-name csv_benchmark_${config}_db"

    # Add export flag if enabled
    if [ ! -z "$EXPORT_FLAG" ]; then
        # Set export path to benchmark log directory for easy access (use absolute path)
        EXPORT_PATH="$(pwd)/$LOG_DIR/${config}.jsonl.tgz"
        CMD_ARGS="$CMD_ARGS $EXPORT_FLAG --export-path $EXPORT_PATH"
    fi

    echo "  [$RUN_NUM] $config (parallel=$PAR, batch=$BATCH) -> ${config}.log"

    # Run in the unique working directory to separate log files
    # Use exec to replace subshell with Python process so $! gives Python PID, not shell PID
    (cd "$WORK_DIR" && exec python -u "$SCRIPT_PATH" $CMD_ARGS) \
        > "$LOG_DIR/${config}.log" 2>&1 &

    PIDS[$config]=$!
    monitor_memory ${PIDS[$config]} "$config" &
done

echo ""
echo "All processes started. Waiting for completion..."
echo ""

# Wait for all processes and track completion
for config in "${!PIDS[@]}"; do
    wait ${PIDS[$config]}
    STATUSES[$config]=$?
    echo "  ✓ $config completed (exit code: ${STATUSES[$config]})"
done

echo ""
echo "========================================================================"
echo "All benchmarks completed!"
echo "========================================================================"
echo ""

# Extract and display memory usage summary
echo "MEMORY USAGE SUMMARY:"
echo "------------------------------------------------------------------------"
echo ""

for MEM_LOG in "$LOG_DIR"/*_memory.log; do
    if [ -f "$MEM_LOG" ]; then
        NAME=$(basename "$MEM_LOG" _memory.log)

        # Extract peak values (skip header)
        # RSS = Resident Set Size (actual physical memory used)
        # VSZ = Virtual Size (total virtual memory allocated)
        PEAK_RSS=$(tail -n +2 "$MEM_LOG" | cut -d',' -f2 | sort -n | tail -1)
        PEAK_VSZ=$(tail -n +2 "$MEM_LOG" | cut -d',' -f3 | sort -n | tail -1)
        PEAK_CPU=$(tail -n +2 "$MEM_LOG" | cut -d',' -f4 | sort -n | tail -1)

        # Calculate averages
        AVG_RSS=$(tail -n +2 "$MEM_LOG" | cut -d',' -f2 | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
        AVG_VSZ=$(tail -n +2 "$MEM_LOG" | cut -d',' -f3 | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')

        if [ ! -z "$PEAK_RSS" ] && [ "$PEAK_RSS" != "" ]; then
            printf "[$NAME]\n"
            printf "  Peak RSS: %8.2f MB (actual memory) | Peak VSZ: %8.2f MB (virtual) | Peak CPU: %6.1f%%\n" \
                $PEAK_RSS $PEAK_VSZ $PEAK_CPU
            printf "  Avg  RSS: %8.2f MB                | Avg  VSZ: %8.2f MB\n" \
                $AVG_RSS $AVG_VSZ
            echo ""
        fi
    fi
done

echo "------------------------------------------------------------------------"
echo ""

# Extract and display performance summary
echo "PERFORMANCE SUMMARY:"
echo "------------------------------------------------------------------------"
echo ""

for LOG in "$LOG_DIR"/*.log; do
    # Skip memory monitoring logs
    if [[ "$LOG" == *"_memory.log" ]]; then
        continue
    fi

    NAME=$(basename "$LOG" .log)
    echo "[$NAME]"
    echo "========================================================================"

    # Extract total Python script execution time
    SCRIPT_TIME_LINE=$(grep "TOTAL SCRIPT RUN TIME:" "$LOG" | tail -1)
    if [ ! -z "$SCRIPT_TIME_LINE" ]; then
        # Extract minutes and seconds from format "0m 4s"
        SCRIPT_MINUTES=$(echo "$SCRIPT_TIME_LINE" | grep -oP '\d+(?=m)')
        SCRIPT_SECONDS=$(echo "$SCRIPT_TIME_LINE" | grep -oP '\d+(?=s)')
        if [ ! -z "$SCRIPT_MINUTES" ] && [ ! -z "$SCRIPT_SECONDS" ]; then
            TOTAL_SECONDS=$((SCRIPT_MINUTES * 60 + SCRIPT_SECONDS))
            echo "  Total Python Script Runtime: ${SCRIPT_MINUTES}m ${SCRIPT_SECONDS}s (${TOTAL_SECONDS}s)"
            echo "  --------------------------------------------------------------------"
            echo ""
        fi
    fi

    # Extract import rates for each table
    echo "  Import Performance:"
    echo "  --------------------------------------------------------------------"

    # Movies
    MOVIES_COUNT=$(grep "Imported.*movies" "$LOG" | grep -oP '\d{1,3}(,\d{3})*(?= movies)' | head -1 | tr -d ',')
    MOVIES_TIME=$(grep -A 2 "Imported.*movies" "$LOG" | grep "Time:" | head -1 | grep -oP '\d+\.\d+(?=s)')
    MOVIES_RATE=$(grep -A 3 "Imported.*movies" "$LOG" | grep "Rate:" | head -1 | grep -oP '\d+(?= records/sec)')
    if [ ! -z "$MOVIES_COUNT" ] && [ ! -z "$MOVIES_TIME" ] && [ ! -z "$MOVIES_RATE" ]; then
        printf "    Movies:   %8s records in %6ss → %10s records/sec\n" \
            "$(printf "%'d" $MOVIES_COUNT)" "$MOVIES_TIME" "$(printf "%'d" $MOVIES_RATE)"
    fi

    # Ratings
    RATINGS_COUNT=$(grep "Imported.*ratings" "$LOG" | grep -oP '\d{1,3}(,\d{3})*(?= ratings)' | head -1 | tr -d ',')
    RATINGS_TIME=$(grep -A 2 "Imported.*ratings" "$LOG" | grep "Time:" | head -1 | grep -oP '\d+\.\d+(?=s)')
    RATINGS_RATE=$(grep -A 3 "Imported.*ratings" "$LOG" | grep "Rate:" | head -1 | grep -oP '\d+(?= records/sec)')
    if [ ! -z "$RATINGS_COUNT" ] && [ ! -z "$RATINGS_TIME" ] && [ ! -z "$RATINGS_RATE" ]; then
        printf "    Ratings:  %8s records in %6ss → %10s records/sec\n" \
            "$(printf "%'d" $RATINGS_COUNT)" "$RATINGS_TIME" "$(printf "%'d" $RATINGS_RATE)"
    fi

    # Links
    LINKS_COUNT=$(grep "Imported.*links" "$LOG" | grep -oP '\d{1,3}(,\d{3})*(?= links)' | head -1 | tr -d ',')
    LINKS_TIME=$(grep -A 2 "Imported.*links" "$LOG" | grep "Time:" | head -1 | grep -oP '\d+\.\d+(?=s)')
    LINKS_RATE=$(grep -A 3 "Imported.*links" "$LOG" | grep "Rate:" | head -1 | grep -oP '\d+(?= records/sec)')
    if [ ! -z "$LINKS_COUNT" ] && [ ! -z "$LINKS_TIME" ] && [ ! -z "$LINKS_RATE" ]; then
        printf "    Links:    %8s records in %6ss → %10s records/sec\n" \
            "$(printf "%'d" $LINKS_COUNT)" "$LINKS_TIME" "$(printf "%'d" $LINKS_RATE)"
    fi

    # Tags
    TAGS_COUNT=$(grep "Imported.*tags" "$LOG" | grep -oP '\d{1,3}(,\d{3})*(?= tags)' | head -1 | tr -d ',')
    TAGS_TIME=$(grep -A 2 "Imported.*tags" "$LOG" | grep "Time:" | head -1 | grep -oP '\d+\.\d+(?=s)')
    TAGS_RATE=$(grep -A 3 "Imported.*tags" "$LOG" | grep "Rate:" | head -1 | grep -oP '\d+(?= records/sec)')
    if [ ! -z "$TAGS_COUNT" ] && [ ! -z "$TAGS_TIME" ] && [ ! -z "$TAGS_RATE" ]; then
        printf "    Tags:     %8s records in %6ss → %10s records/sec\n" \
            "$(printf "%'d" $TAGS_COUNT)" "$TAGS_TIME" "$(printf "%'d" $TAGS_RATE)"
    fi

    # Calculate total import time and average rate
    if [ ! -z "$MOVIES_TIME" ] && [ ! -z "$RATINGS_TIME" ] && [ ! -z "$LINKS_TIME" ] && [ ! -z "$TAGS_TIME" ]; then
        TOTAL_IMPORT_TIME=$(echo "$MOVIES_TIME + $RATINGS_TIME + $LINKS_TIME + $TAGS_TIME" | bc)
        TOTAL_RECORDS=$(echo "$MOVIES_COUNT + $RATINGS_COUNT + $LINKS_COUNT + $TAGS_COUNT" | bc)
        AVG_RATE=$(echo "scale=0; $TOTAL_RECORDS / $TOTAL_IMPORT_TIME" | bc)
        printf "    %-10s%8s records in %6ss → %10s records/sec (avg)\n" \
            "TOTAL:" "$(printf "%'d" $TOTAL_RECORDS)" "$TOTAL_IMPORT_TIME" "$(printf "%'d" $AVG_RATE)"
    fi

    echo ""

    # Extract index creation time
    INDEX_TIME=$(grep "Total index creation time:" "$LOG" | grep -oP '\d+\.\d+')
    if [ ! -z "$INDEX_TIME" ]; then
        echo "  Index Creation:"
        echo "  --------------------------------------------------------------------"
        printf "    Total time: %ss\n" "$INDEX_TIME"
        echo ""
    fi

    # Extract query performance WITHOUT indexes
    echo "  Query Performance (WITHOUT indexes):"
    echo "  --------------------------------------------------------------------"

    # Find movie by ID
    QUERY_TIME_S=$(grep -A 1 "📊 Find movie by ID:" "$LOG" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
    if [ ! -z "$QUERY_TIME_S" ]; then
        printf "    Find movie by ID:              %9ss\n" "$QUERY_TIME_S"
    fi

    # Find user's ratings
    QUERY_TIME_S=$(grep -A 1 "📊 Find user's ratings:" "$LOG" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
    if [ ! -z "$QUERY_TIME_S" ]; then
        printf "    Find user's ratings:           %9ss\n" "$QUERY_TIME_S"
    fi

    # Find movie ratings
    QUERY_TIME_S=$(grep -A 1 "📊 Find movie ratings:" "$LOG" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
    if [ ! -z "$QUERY_TIME_S" ]; then
        printf "    Find movie ratings:            %9ss\n" "$QUERY_TIME_S"
    fi

    # Count user's ratings
    QUERY_TIME_S=$(grep -A 1 "📊 Count user's ratings:" "$LOG" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
    if [ ! -z "$QUERY_TIME_S" ]; then
        printf "    Count user's ratings:          %9ss\n" "$QUERY_TIME_S"
    fi

    # Find movies by genre
    QUERY_TIME_S=$(grep -A 1 "📊 Find movies by genre" "$LOG" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
    if [ ! -z "$QUERY_TIME_S" ]; then
        printf "    Find movies by genre (LIKE):   %9ss\n" "$QUERY_TIME_S"
    fi

    # Count ALL Action movies
    QUERY_TIME_S=$(grep -A 1 "📊 Count ALL Action movies" "$LOG" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
    if [ ! -z "$QUERY_TIME_S" ]; then
        printf "    Count ALL Action movies:       %9ss\n" "$QUERY_TIME_S"
    fi

    echo ""

    # Extract query performance WITH indexes
    echo "  Query Performance (WITH indexes):"
    echo "  --------------------------------------------------------------------"

    # Extract from Step 10 section
    STEP10_START=$(grep -n "Step 10: Testing query performance WITH indexes" "$LOG" | cut -d: -f1)
    if [ ! -z "$STEP10_START" ]; then
        # Find movie by ID
        QUERY_TIME_S=$(tail -n +$STEP10_START "$LOG" | grep -A 1 "📊 Find movie by ID:" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
        if [ ! -z "$QUERY_TIME_S" ]; then
            printf "    Find movie by ID:              %9ss\n" "$QUERY_TIME_S"
        fi

        # Find user's ratings
        QUERY_TIME_S=$(tail -n +$STEP10_START "$LOG" | grep -A 1 "📊 Find user's ratings:" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
        if [ ! -z "$QUERY_TIME_S" ]; then
            printf "    Find user's ratings:           %9ss\n" "$QUERY_TIME_S"
        fi

        # Find movie ratings
        QUERY_TIME_S=$(tail -n +$STEP10_START "$LOG" | grep -A 1 "📊 Find movie ratings:" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
        if [ ! -z "$QUERY_TIME_S" ]; then
            printf "    Find movie ratings:            %9ss\n" "$QUERY_TIME_S"
        fi

        # Count user's ratings
        QUERY_TIME_S=$(tail -n +$STEP10_START "$LOG" | grep -A 1 "📊 Count user's ratings:" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
        if [ ! -z "$QUERY_TIME_S" ]; then
            printf "    Count user's ratings:          %9ss\n" "$QUERY_TIME_S"
        fi

        # Find movies by genre
        QUERY_TIME_S=$(tail -n +$STEP10_START "$LOG" | grep -A 1 "📊 Find movies by genre" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
        if [ ! -z "$QUERY_TIME_S" ]; then
            printf "    Find movies by genre (LIKE):   %9ss\n" "$QUERY_TIME_S"
        fi

        # Count ALL Action movies
        QUERY_TIME_S=$(tail -n +$STEP10_START "$LOG" | grep -A 1 "📊 Count ALL Action movies" | head -2 | grep "Average:" | grep -oP '\d+\.\d+(?=s)' | head -1)
        if [ ! -z "$QUERY_TIME_S" ]; then
            printf "    Count ALL Action movies:       %9ss\n" "$QUERY_TIME_S"
        fi
    fi

    echo ""

    # Extract speedup summary
    echo "  Index Speedup Summary:"
    echo "  --------------------------------------------------------------------"

    # Extract key speedup values from the performance table
    grep -A 20 "Performance Improvement Summary:" "$LOG" | grep -E "Find movie by ID|Find user's ratings|Find movie ratings|Count user's ratings" | while read -r line; do
        QUERY_NAME=$(echo "$line" | awk '{print $1, $2, $3, $4}' | sed 's/  *$//')
        SPEEDUP=$(echo "$line" | grep -oP '\d+\.\d+x' | head -1)
        TIME_SAVED=$(echo "$line" | grep -oP '\(\d+\.\d+% time saved\)' | grep -oP '\d+\.\d+')
        if [ ! -z "$SPEEDUP" ] && [ ! -z "$TIME_SAVED" ]; then
            printf "    %-30s %6s (%5s%% faster)\n" "$QUERY_NAME:" "$SPEEDUP" "$TIME_SAVED"
        fi
    done

    echo ""

    # Extract baseline validation results
    echo "  Baseline Validation:"
    echo "  --------------------------------------------------------------------"

    # Check Step 8 (BEFORE indexes)
    STEP8_RESULT=$(grep "Step 8:.*baseline" "$LOG" | head -1)
    if [[ "$STEP8_RESULT" == *"All results match baseline!"* ]]; then
        echo "    Step  8 (BEFORE indexes):  ✅ PASS - All results match baseline"
    elif [[ "$STEP8_RESULT" == *"Some results differ from baseline!"* ]]; then
        echo "    Step  8 (BEFORE indexes):  ❌ FAIL - Some results differ from baseline"
    else
        echo "    Step  8 (BEFORE indexes):  ⚠️  No baseline check found"
    fi

    # Check Step 10 (AFTER indexes)
    STEP10_RESULT=$(grep "Step 10:.*baseline" "$LOG" | head -1)
    if [[ "$STEP10_RESULT" == *"All results match baseline!"* ]]; then
        echo "    Step 10 (AFTER indexes):   ✅ PASS - All results match baseline"
    elif [[ "$STEP10_RESULT" == *"Some results differ from baseline!"* ]]; then
        echo "    Step 10 (AFTER indexes):   ❌ FAIL - Some results differ from baseline"
    else
        echo "    Step 10 (AFTER indexes):   ⚠️  No baseline check found"
    fi

    # Check Step 14 (AFTER roundtrip) - only if export was enabled
    STEP14_RESULT=$(grep "Step 14:.*baseline" "$LOG" | head -1)
    if [[ "$STEP14_RESULT" == *"All results match baseline!"* ]]; then
        echo "    Step 14 (AFTER roundtrip): ✅ PASS - All results match baseline"
    elif [[ "$STEP14_RESULT" == *"Some results differ from baseline!"* ]]; then
        echo "    Step 14 (AFTER roundtrip): ❌ FAIL - Some results differ from baseline"
    elif [ ! -z "$EXPORT_FLAG" ]; then
        echo "    Step 14 (AFTER roundtrip): ⚠️  No baseline check found (export enabled)"
    else
        echo "    Step 14 (AFTER roundtrip): ⏭️  Skipped (export not enabled)"
    fi

    # Check final validation
    FINAL_RESULT=$(grep "SUCCESS: All query results are consistent!" "$LOG")
    if [ ! -z "$FINAL_RESULT" ]; then
        echo ""
        echo "    FINAL VALIDATION:          ✅ SUCCESS - All query runs consistent"
    else
        FINAL_FAIL=$(grep "FINAL VALIDATION:" "$LOG")
        if [ ! -z "$FINAL_FAIL" ]; then
            echo ""
            echo "    FINAL VALIDATION:          ❌ FAIL - Inconsistencies detected"
        fi
    fi

    echo ""
    echo "========================================================================"
    echo ""
done

echo "------------------------------------------------------------------------"
echo ""
echo "Full logs saved in: $LOG_DIR"
echo ""

# Show exported databases if export was enabled
if [ ! -z "$EXPORT_FLAG" ]; then
    echo "EXPORTED DATABASES:"
    echo "------------------------------------------------------------------------"
    echo ""

    EXPORT_COUNT=0
    TOTAL_EXPORT_SIZE=0

    for config in "${!RUN_CONFIGS[@]}"; do
        EXPORT_FILE="$LOG_DIR/${config}.jsonl.tgz"
        if [ -f "$EXPORT_FILE" ]; then
            EXPORT_COUNT=$((EXPORT_COUNT + 1))
            FILE_SIZE=$(stat -c%s "$EXPORT_FILE" 2> /dev/null || stat -f%z "$EXPORT_FILE" 2> /dev/null || echo "0")
            SIZE_MB=$(echo "scale=2; $FILE_SIZE / (1024 * 1024)" | bc)
            TOTAL_EXPORT_SIZE=$(echo "$TOTAL_EXPORT_SIZE + $SIZE_MB" | bc)

            printf "  [$config] %s (%.2f MB)\n" "$EXPORT_FILE" "$SIZE_MB"
        fi
    done

    echo ""
    if [ $EXPORT_COUNT -gt 0 ]; then
        printf "  Total: %d exported database(s), %.2f MB\n" $EXPORT_COUNT $TOTAL_EXPORT_SIZE
        echo ""
        echo "  💡 Use these exports to:"
        echo "     • Share reproducible benchmark databases"
        echo "     • Restore databases: arcadedb.import_database(db, 'file.jsonl.tgz')"
        echo "     • Create test fixtures from real data"
        echo "     • Skip CSV import step in future benchmarks"
    else
        echo "  ⚠️  No exports found (check logs for errors)"
    fi
    echo ""
    echo "------------------------------------------------------------------------"
    echo ""
fi

# Calculate total script run time
SCRIPT_END=$(date +%s)
SCRIPT_DURATION=$((SCRIPT_END - SCRIPT_START))
SCRIPT_MINUTES=$((SCRIPT_DURATION / 60))
SCRIPT_SECONDS=$((SCRIPT_DURATION % 60))

echo ""
echo "========================================================================"
echo "TOTAL SCRIPT RUN TIME: ${SCRIPT_MINUTES}m ${SCRIPT_SECONDS}s"
echo "========================================================================"
echo ""

# Move exports from working directories to log directory before cleanup
if [ ! -z "$EXPORT_FLAG" ]; then
    echo "Moving exports to log directory..."
    for config in "${!RUN_CONFIGS[@]}"; do
        # Find export files in working directory
        WORK_EXPORT=$(find "./benchmark_work/$config" -name "${config}.jsonl.tgz" 2> /dev/null | head -1)
        if [ ! -z "$WORK_EXPORT" ] && [ -f "$WORK_EXPORT" ]; then
            mv "$WORK_EXPORT" "$LOG_DIR/${config}.jsonl.tgz"
            echo "  ✓ Moved $config export to $LOG_DIR/"
        fi
    done
    echo ""
fi

# Clean up temporary working directories
if [ -d "./benchmark_work" ]; then
    rm -rf "./benchmark_work"
    echo "  ✓ Removed ./benchmark_work"
fi
echo ""

# Check for any failures
FAILED=0
for config in "${!STATUSES[@]}"; do
    if [ ${STATUSES[$config]} -ne 0 ]; then
        FAILED=1
        break
    fi
done

if [ $FAILED -ne 0 ]; then
    echo "⚠️  Some benchmarks failed. Check logs for details."
    exit 1
else
    echo "✅ All benchmarks completed successfully!"
    exit 0
fi
