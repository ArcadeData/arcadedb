#!/bin/bash
#
# Run graph creation benchmarks and save logs
#
# Usage:
#   ./run_benchmark_05_csv_import_graph.sh [size] [batch-size] [parallel-level] [methods] [--import-jsonl path] [--export]
#
# Methods (comma-separated, or "all"):
#   java                  - Java API with indexes + async (FASTEST, RECOMMENDED)
#   java_noasync          - Java API with indexes, no async (synchronous)
#   java_noindex          - Java API without indexes + async
#   java_noindex_noasync  - Java API without indexes, no async
#   sql                   - SQL with indexes (always synchronous)
#   sql_noindex           - SQL without indexes (always synchronous)
#   all                   - Both methods with indexes (java, sql)
#   all_noindex           - Both noindex methods (java_noindex, sql_noindex)
#   all_java              - All Java API variants (4 methods)
#   all_6                 - All 6 methods
#
# Options:
#   --import-jsonl PATH   - Import from JSONL export instead of using source DB
#                          (e.g., ./benchmark_logs/csv_import_small_*/p4_b5000.jsonl.tgz)
#   --export              - Export graph databases to JSONL after creation
#                          (enables roundtrip validation: export → import → verify)
#
# Examples:
#   ./run_benchmark_05_csv_import_graph.sh small 5000 4 all
#   ./run_benchmark_05_csv_import_graph.sh small 5000 4 "java,sql"
#   ./run_benchmark_05_csv_import_graph.sh small 5000 4 all_6
#   ./run_benchmark_05_csv_import_graph.sh small 5000 4 all_6 --export
#   ./run_benchmark_05_csv_import_graph.sh large 10000 8 java
#   ./run_benchmark_05_csv_import_graph.sh small 5000 4 all_java
#   ./run_benchmark_05_csv_import_graph.sh small 5000 4 java --import-jsonl ./exports/ml_small_db.jsonl.tgz
#   ./run_benchmark_05_csv_import_graph.sh small 5000 4 java --import-jsonl ./exports/ml_small_db.jsonl.tgz --export
#

# Start timing
SCRIPT_START=$(date +%s)

# Get parameters with defaults
SIZE=${1:-small}
BATCH_SIZE=${2:-5000}
PARALLEL=${3:-4}

# Get METHODS, but default to "all" if it's a flag or empty
if [[ -z "$4" ]] || [[ "$4" == --* ]]; then
    METHODS="all"
else
    METHODS="$4"
fi

# Check for --export flag (can be in any position after first 4 args)
EXPORT_FLAG=""
if [[ "$@" == *"--export"* ]]; then
    EXPORT_FLAG="--export"
    echo "💾 Export enabled: Graph databases will be exported to JSONL after creation"
    echo ""
fi

# Check for --import-jsonl flag (can be in any position after first 4 args)
IMPORT_JSONL=""
if [[ "$@" == *"--import-jsonl"* ]]; then
    # Find the argument after --import-jsonl
    for i in "${!@}"; do
        if [ "${!i}" = "--import-jsonl" ]; then
            next_i=$((i + 1))
            IMPORT_JSONL="${!next_i}"
            break
        fi
    done

    if [ -z "$IMPORT_JSONL" ]; then
        echo "❌ --import-jsonl requires a path argument"
        exit 1
    fi

    if [ ! -f "$IMPORT_JSONL" ]; then
        echo "❌ JSONL file not found: $IMPORT_JSONL"
        exit 1
    fi

    echo "📥 Import mode enabled: Will import from JSONL"
    echo "   File: $IMPORT_JSONL"
    echo "   Size: $(du -h "$IMPORT_JSONL" | cut -f1)"
    echo ""
fi

# Parse methods argument
declare -A RUN_METHODS
case "$METHODS" in
    all)
        RUN_METHODS[java]=1
        RUN_METHODS[sql]=1
        ;;
    all_noindex)
        RUN_METHODS[java_noindex]=1
        RUN_METHODS[sql_noindex]=1
        ;;
    all_java)
        RUN_METHODS[java]=1
        RUN_METHODS[java_noasync]=1
        RUN_METHODS[java_noindex]=1
        RUN_METHODS[java_noindex_noasync]=1
        ;;
    all_6)
        RUN_METHODS[java]=1
        RUN_METHODS[java_noasync]=1
        RUN_METHODS[java_noindex]=1
        RUN_METHODS[java_noindex_noasync]=1
        RUN_METHODS[sql]=1
        RUN_METHODS[sql_noindex]=1
        ;;
    *)
        # Parse comma-separated list
        IFS=',' read -ra METHOD_ARRAY <<< "$METHODS"
        for method in "${METHOD_ARRAY[@]}"; do
            # Trim whitespace
            method=$(echo "$method" | xargs)
            case "$method" in
                java | java_noasync | java_noindex | java_noindex_noasync | sql | sql_noindex)
                    RUN_METHODS[$method]=1
                    ;;
                *)
                    echo "❌ Unknown method: $method"
                    echo "Valid methods: java, java_noasync, java_noindex, java_noindex_noasync, sql, sql_noindex"
                    echo "Valid groups: all, all_noindex, all_java, all_6"
                    exit 1
                    ;;
            esac
        done
        ;;
esac

# Count how many methods we're running
NUM_METHODS=${#RUN_METHODS[@]}

# Create log directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="./benchmark_logs/graph_${SIZE}_${TIMESTAMP}"
mkdir -p "$LOG_DIR"

echo "========================================================================"
echo "Graph Creation Benchmarks - $NUM_METHODS Method(s)"
echo "========================================================================"
echo "Dataset: $SIZE"
echo "Batch size: $BATCH_SIZE"
echo "Parallel level: $PARALLEL"
echo "Methods: ${!RUN_METHODS[@]}"
echo "Log directory: $LOG_DIR"
if [ ! -z "$IMPORT_JSONL" ]; then
    echo "Import from: $IMPORT_JSONL"
fi
echo ""

# Check if source database exists (skip if using import mode)
SOURCE_DB="./my_test_databases/ml_${SIZE}_db"
if [ -z "$IMPORT_JSONL" ]; then
    if [ ! -d "$SOURCE_DB" ]; then
        echo "❌ Source database not found: $SOURCE_DB"
        echo "   Run: python 04_csv_import_documents.py --size $SIZE"
        echo "   OR use --import-jsonl to import from JSONL export"
        exit 1
    fi

    echo "Source database: $SOURCE_DB"
    echo "  Size: $(du -sh "$SOURCE_DB" 2> /dev/null | cut -f1)"
    echo ""
else
    echo "Source: JSONL import (no source database needed)"
    echo ""
fi

# Function to monitor memory usage of a process (Python + Java JVM)
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

# Clean up any existing copies from previous runs
echo "Cleaning up any existing database copies..."
for i in {1..6}; do
    rm -rf "./my_test_databases/ml_${SIZE}_db_copy${i}"
done

# Create temporary copies for parallel runs (skip if using import mode - each run will import independently)
if [ $NUM_METHODS -gt 1 ] && [ -z "$IMPORT_JSONL" ]; then
    echo "Creating temporary database copies for parallel runs..."
    echo "  (this may take a few minutes for large datasets)"
    echo ""

    COPY_NUM=0
    declare -A COPY_MAP

    for method in "${!RUN_METHODS[@]}"; do
        COPY_NUM=$((COPY_NUM + 1))
        COPY_MAP[$method]=$COPY_NUM
        cp -r "$SOURCE_DB" "./my_test_databases/ml_${SIZE}_db_copy${COPY_NUM}" &
        eval "CP_PID${COPY_NUM}=$!"
    done

    # Wait for all copies to complete
    for i in $(seq 1 $COPY_NUM); do
        eval "wait \$CP_PID${i} && echo '  ✓ Created copy ${i}'"
    done
    echo ""
fi

echo "Starting $NUM_METHODS parallel run(s)..."
echo ""

# Track PIDs and statuses
declare -A PIDS
declare -A STATUSES
RUN_NUM=0

# Function to get source DB path for a method
get_source_db() {
    local METHOD=$1
    if [ ! -z "$IMPORT_JSONL" ]; then
        echo "" # No source DB when using import
    elif [ $NUM_METHODS -gt 1 ]; then
        echo "$(pwd)/my_test_databases/ml_${SIZE}_db_copy${COPY_MAP[$METHOD]}"
    else
        echo "$(pwd)/$SOURCE_DB"
    fi
}

# Function to build common arguments
get_common_args() {
    local ARGS="--size $SIZE --batch-size $BATCH_SIZE --parallel $PARALLEL"
    if [ ! -z "$IMPORT_JSONL" ]; then
        ARGS="$ARGS --import-jsonl $(pwd)/$IMPORT_JSONL"
    fi
    if [ ! -z "$EXPORT_FLAG" ]; then
        ARGS="$ARGS --export"
    fi
    echo "$ARGS"
}

# Create working directories for each method
for method in "${!RUN_METHODS[@]}"; do
    mkdir -p "./benchmark_work/$method"
done

# Run selected methods
if [ -n "${RUN_METHODS[java]}" ]; then
    RUN_NUM=$((RUN_NUM + 1))
    SOURCE=$(get_source_db "java")
    COMMON_ARGS=$(get_common_args)
    SCRIPT_PATH="$(pwd)/05_csv_import_graph.py"
    CMD="python -u $SCRIPT_PATH $COMMON_ARGS --method java --db-name benchmark_java_db"
    if [ ! -z "$SOURCE" ]; then
        CMD="$CMD --source-db $SOURCE"
    fi
    # Run in working directory and use exec so $! gives Python PID, not shell PID
    (cd "./benchmark_work/java" && exec $CMD) > "$LOG_DIR/java.log" 2>&1 &
    PIDS[java]=$!
    monitor_memory ${PIDS[java]} "java" &
    echo "  [$RUN_NUM] java (Java API + async)        (PID: ${PIDS[java]}) -> java.log"
fi

if [ -n "${RUN_METHODS[java_noasync]}" ]; then
    RUN_NUM=$((RUN_NUM + 1))
    SOURCE=$(get_source_db "java_noasync")
    COMMON_ARGS=$(get_common_args)
    SCRIPT_PATH="$(pwd)/05_csv_import_graph.py"
    CMD="python -u $SCRIPT_PATH $COMMON_ARGS --method java --db-name benchmark_java_noasync_db --no-async"
    if [ ! -z "$SOURCE" ]; then
        CMD="$CMD --source-db $SOURCE"
    fi
    # Run in working directory and use exec so $! gives Python PID, not shell PID
    (cd "./benchmark_work/java_noasync" && exec $CMD) > "$LOG_DIR/java_noasync.log" 2>&1 &
    PIDS[java_noasync]=$!
    monitor_memory ${PIDS[java_noasync]} "java_noasync" &
    echo "  [$RUN_NUM] java_noasync (Java API, sync)  (PID: ${PIDS[java_noasync]}) -> java_noasync.log"
fi

if [ -n "${RUN_METHODS[sql]}" ]; then
    RUN_NUM=$((RUN_NUM + 1))
    SOURCE=$(get_source_db "sql")
    COMMON_ARGS=$(get_common_args)
    SCRIPT_PATH="$(pwd)/05_csv_import_graph.py"
    CMD="python -u $SCRIPT_PATH $COMMON_ARGS --method sql --db-name benchmark_sql_db"
    if [ ! -z "$SOURCE" ]; then
        CMD="$CMD --source-db $SOURCE"
    fi
    # Run in working directory and use exec so $! gives Python PID, not shell PID
    (cd "./benchmark_work/sql" && exec $CMD) > "$LOG_DIR/sql.log" 2>&1 &
    PIDS[sql]=$!
    monitor_memory ${PIDS[sql]} "sql" &
    echo "  [$RUN_NUM] sql (SQL, always sync)         (PID: ${PIDS[sql]}) -> sql.log"
fi

if [ -n "${RUN_METHODS[java_noindex]}" ]; then
    RUN_NUM=$((RUN_NUM + 1))
    SOURCE=$(get_source_db "java_noindex")
    COMMON_ARGS=$(get_common_args)
    SCRIPT_PATH="$(pwd)/05_csv_import_graph.py"
    CMD="python -u $SCRIPT_PATH $COMMON_ARGS --method java --db-name benchmark_java_noindex_db --no-index"
    if [ ! -z "$SOURCE" ]; then
        CMD="$CMD --source-db $SOURCE"
    fi
    # Run in working directory and use exec so $! gives Python PID, not shell PID
    (cd "./benchmark_work/java_noindex" && exec $CMD) > "$LOG_DIR/java_noindex.log" 2>&1 &
    PIDS[java_noindex]=$!
    monitor_memory ${PIDS[java_noindex]} "java_noindex" &
    echo "  [$RUN_NUM] java_noindex (no idx + async)  (PID: ${PIDS[java_noindex]}) -> java_noindex.log"
fi

if [ -n "${RUN_METHODS[java_noindex_noasync]}" ]; then
    RUN_NUM=$((RUN_NUM + 1))
    SOURCE=$(get_source_db "java_noindex_noasync")
    COMMON_ARGS=$(get_common_args)
    SCRIPT_PATH="$(pwd)/05_csv_import_graph.py"
    CMD="python -u $SCRIPT_PATH $COMMON_ARGS --method java --db-name benchmark_java_noindex_noasync_db --no-index --no-async"
    if [ ! -z "$SOURCE" ]; then
        CMD="$CMD --source-db $SOURCE"
    fi
    # Run in working directory and use exec so $! gives Python PID, not shell PID
    (cd "./benchmark_work/java_noindex_noasync" && exec $CMD) > "$LOG_DIR/java_noindex_noasync.log" 2>&1 &
    PIDS[java_noindex_noasync]=$!
    monitor_memory ${PIDS[java_noindex_noasync]} "java_noindex_noasync" &
    echo "  [$RUN_NUM] java_noindex_noasync (no idx, sync) (PID: ${PIDS[java_noindex_noasync]}) -> java_noindex_noasync.log"
fi

if [ -n "${RUN_METHODS[sql_noindex]}" ]; then
    RUN_NUM=$((RUN_NUM + 1))
    SOURCE=$(get_source_db "sql_noindex")
    COMMON_ARGS=$(get_common_args)
    SCRIPT_PATH="$(pwd)/05_csv_import_graph.py"
    CMD="python -u $SCRIPT_PATH $COMMON_ARGS --method sql --db-name benchmark_sql_noindex_db --no-index"
    if [ ! -z "$SOURCE" ]; then
        CMD="$CMD --source-db $SOURCE"
    fi
    # Run in working directory and use exec so $! gives Python PID, not shell PID
    (cd "./benchmark_work/sql_noindex" && exec $CMD) > "$LOG_DIR/sql_noindex.log" 2>&1 &
    PIDS[sql_noindex]=$!
    monitor_memory ${PIDS[sql_noindex]} "sql_noindex" &
    echo "  [$RUN_NUM] sql_noindex (no idx, sync)     (PID: ${PIDS[sql_noindex]}) -> sql_noindex.log"
fi

echo ""
echo "All processes started. Waiting for completion..."
echo ""

# Wait for all processes and track completion
for method in "${!PIDS[@]}"; do
    wait ${PIDS[$method]}
    STATUSES[$method]=$?
    echo "  ✓ $method completed (exit code: ${STATUSES[$method]})"
done

echo ""
echo "========================================================================"
echo "All benchmarks completed!"
echo "========================================================================"
echo ""

# Extract and display validation results (only for small dataset)
if [ "$SIZE" = "small" ]; then
    echo "VALIDATION RESULTS:"
    echo "------------------------------------------------------------------------"
    echo ""

    for LOG in "$LOG_DIR"/*.log; do
        # Skip memory monitoring logs
        if [[ "$LOG" == *"_memory.log" ]]; then
            continue
        fi

        NAME=$(basename "$LOG" .log)
        echo "[$NAME]"

        # Display basic validation summary (no more "Part 1" - now just numbered)
        grep -A 10 "Validation & Query Testing" "$LOG" | grep -E "(Users:|Movies:|RATED:|TAGGED:|Ratings:|Tags:|Title:|Genres:)" | sed 's/^/  /' | head -8

        # Check for basic validation pass
        if grep -q "✅ All expected baseline values match!" "$LOG"; then
            echo "  ✅ Basic Validation PASSED"
        else
            echo "  ❌ Basic Validation FAILED"
        fi
        echo ""

        # Check Step 5 (combined validation & queries) - overall status
        STEP5_RESULT=$(grep "✅ Step 5:.*passed" "$LOG")
        if [[ "$STEP5_RESULT" == *"All validation & queries passed!"* ]]; then
            echo "  Step 5 (Validation & Queries): ✅ PASS"
        elif grep -q "❌ Step 5:.*failed" "$LOG"; then
            echo "  Step 5 (Validation & Queries): ❌ FAIL"
        else
            echo "  Step 5 (Validation & Queries): ⚠️  Unknown"
        fi

        # Check Step 6 (Export) - only if export was enabled
        if [ ! -z "$EXPORT_FLAG" ]; then
            if grep -q "✅ Export complete!" "$LOG"; then
                echo "  Step 6 (Export):               ✅ PASS"
            elif grep -q "❌ Export failed:" "$LOG"; then
                echo "  Step 6 (Export):               ❌ FAIL"
            else
                echo "  Step 6 (Export):               ⚠️  Not found"
            fi
        else
            echo "  Step 6 (Export):               ⏭️  Skipped"
        fi

        # Check Step 7 (Roundtrip validation) - only if export was enabled
        if [ ! -z "$EXPORT_FLAG" ]; then
            if grep -q "✅ Step 7: All validation & queries passed!" "$LOG"; then
                echo "  Step 7 (Roundtrip):            ✅ PASS"
            elif grep -q "❌ Step 7: Some validations or queries failed!" "$LOG"; then
                echo "  Step 7 (Roundtrip):            ❌ FAIL"
            elif grep -q "❌ Roundtrip validation failed:" "$LOG"; then
                echo "  Step 7 (Roundtrip):            ❌ FAIL"
            elif grep -q "Step 7: Roundtrip validation skipped" "$LOG"; then
                echo "  Step 7 (Roundtrip):            ⏭️  Skipped"
            else
                echo "  Step 7 (Roundtrip):            ⚠️  Not found"
            fi
        else
            echo "  Step 7 (Roundtrip):            ⏭️  Skipped"
        fi

        echo ""
    done

    echo "------------------------------------------------------------------------"
    echo ""
fi

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

    # Check if JSONL import was used and extract import time
    IMPORT_TIME_LINE=$(grep "^JSONL Import:" "$LOG" | head -1)
    if [ ! -z "$IMPORT_TIME_LINE" ]; then
        echo "  $IMPORT_TIME_LINE"
        echo ""
    fi

    # Extract vertex and edge counts/rates from Summary section
    VERTEX_LINE=$(grep "^Vertices:" "$LOG" | head -1)
    EDGE_LINE=$(grep "^Edges:" "$LOG" | head -1)
    CREATION_TIME_LINE=$(grep "^Creation time:" "$LOG" | tail -1)
    EXPORT_TIME_LINE=$(grep "^Export time:" "$LOG" | tail -1)
    ROUNDTRIP_LINE=$(grep "^Roundtrip import time:" "$LOG" | tail -1)
    TOTAL_ROUNDTRIP_LINE=$(grep "^Total roundtrip time:" "$LOG" | tail -1)
    TOTAL_TIME_LINE=$(grep "^Total script time:" "$LOG" | tail -1)

    if [ ! -z "$VERTEX_LINE" ]; then
        echo "  $VERTEX_LINE"
        # Extract rate from the next line after "Vertices:" (format: "  (5258 vertices/sec)")
        VERTEX_RATE=$(grep -A 1 "^Vertices:" "$LOG" | tail -1 | grep -oP '\(\K[0-9]+(?= vertices/sec\))')
        if [ ! -z "$VERTEX_RATE" ]; then
            echo "    → ${VERTEX_RATE} vertices/sec"
        fi
    fi

    if [ ! -z "$EDGE_LINE" ]; then
        echo "  $EDGE_LINE"
        # Extract rate from the next line after "Edges:" (format: "  (5606 edges/sec)")
        EDGE_RATE=$(grep -A 1 "^Edges:" "$LOG" | tail -1 | grep -oP '\(\K[0-9]+(?= edges/sec\))')
        if [ ! -z "$EDGE_RATE" ]; then
            echo "    → ${EDGE_RATE} edges/sec"
        fi
    fi

    if [ ! -z "$CREATION_TIME_LINE" ]; then
        echo "  $CREATION_TIME_LINE"
    fi

    if [ ! -z "$EXPORT_TIME_LINE" ]; then
        echo ""
        echo "  $EXPORT_TIME_LINE"
        if [ ! -z "$ROUNDTRIP_LINE" ]; then
            echo "  $ROUNDTRIP_LINE"
        fi
        if [ ! -z "$TOTAL_ROUNDTRIP_LINE" ]; then
            echo "  $TOTAL_ROUNDTRIP_LINE"
        fi
    fi

    if [ ! -z "$TOTAL_TIME_LINE" ]; then
        echo ""
        echo "  $TOTAL_TIME_LINE"
    fi
    echo ""
done

echo "------------------------------------------------------------------------"
echo ""
echo "Full logs saved in: $LOG_DIR"
echo ""

# Calculate total script run time
SCRIPT_END=$(date +%s)
SCRIPT_DURATION=$((SCRIPT_END - SCRIPT_START))
SCRIPT_MINUTES=$((SCRIPT_DURATION / 60))
SCRIPT_SECONDS=$((SCRIPT_DURATION % 60))

echo "========================================================================"
echo "TOTAL SCRIPT RUN TIME: ${SCRIPT_MINUTES}m ${SCRIPT_SECONDS}s"
echo "========================================================================"
echo ""

# Move exports from working directories to log directory before cleanup
if [ ! -z "$EXPORT_FLAG" ]; then
    echo "Moving exports from working directories..."
    for method in "${!RUN_METHODS[@]}"; do
        # Check for export in working directory exports/ folder
        WORK_EXPORT="./benchmark_work/${method}/exports/benchmark_${method}_db.jsonl.tgz"
        if [ -f "$WORK_EXPORT" ]; then
            mv "$WORK_EXPORT" "$LOG_DIR/benchmark_${method}_db.jsonl.tgz"
            echo "  ✓ Moved benchmark_${method}_db.jsonl.tgz to $LOG_DIR/"
        fi
    done
    echo ""
fi

# Show exported databases if export was enabled
if [ ! -z "$EXPORT_FLAG" ]; then
    echo "EXPORTED GRAPH DATABASES:"
    echo "------------------------------------------------------------------------"
    echo ""

    EXPORT_COUNT=0
    TOTAL_EXPORT_SIZE=0

    for method in "${!RUN_METHODS[@]}"; do
        # Check for export file in log directory
        EXPORT_FILE="$LOG_DIR/benchmark_${method}_db.jsonl.tgz"
        if [ -f "$EXPORT_FILE" ]; then
            EXPORT_COUNT=$((EXPORT_COUNT + 1))
            FILE_SIZE=$(stat -f%z "$EXPORT_FILE" 2> /dev/null || stat -c%s "$EXPORT_FILE" 2> /dev/null)
            SIZE_MB=$(echo "scale=2; $FILE_SIZE / 1048576" | bc)
            TOTAL_EXPORT_SIZE=$((TOTAL_EXPORT_SIZE + FILE_SIZE))

            echo "  [$method] $EXPORT_FILE"
            echo "    Size: ${SIZE_MB} MB"
            echo ""
        fi
    done

    if [ $EXPORT_COUNT -gt 0 ]; then
        TOTAL_SIZE_MB=$(echo "scale=2; $TOTAL_EXPORT_SIZE / 1048576" | bc)
        echo "  Total: $EXPORT_COUNT export(s), ${TOTAL_SIZE_MB} MB"
        echo ""
        echo "  💡 These exports can be used to:"
        echo "     • Import with: python 05_csv_import_graph.py --import-jsonl <file>"
        echo "     • Skip CSV import step in future benchmarks"
        echo "     • Share reproducible benchmark databases"
    else
        echo "  ⚠️  No exports found (check logs for errors)"
    fi
    echo ""
    echo "------------------------------------------------------------------------"
    echo ""
fi

# Clean up temporary databases
echo "Cleaning up temporary databases..."

# Remove temporary database copies (used for parallel runs)
if [ $NUM_METHODS -gt 1 ] && [ -z "$IMPORT_JSONL" ]; then
    for i in {1..6}; do
        if [ -d "./my_test_databases/ml_${SIZE}_db_copy${i}" ]; then
            rm -rf "./my_test_databases/ml_${SIZE}_db_copy${i}"
            echo "  ✓ Removed ml_${SIZE}_db_copy${i}"
        fi
    done
fi

# Remove working directories (contains benchmark databases and logs)
if [ -d "./benchmark_work" ]; then
    rm -rf "./benchmark_work"
    echo "  ✓ Removed ./benchmark_work"
fi

echo "✓ Cleanup complete"
echo ""

# Check for any failures
FAILED=0
for method in "${!STATUSES[@]}"; do
    if [ ${STATUSES[$method]} -ne 0 ]; then
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
