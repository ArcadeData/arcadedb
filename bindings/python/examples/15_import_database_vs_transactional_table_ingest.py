#!/usr/bin/env python3
"""
Example 15: Import Database vs Transactional Table Ingest.

This script generates synthetic multi-table CSV datasets with configurable row
and column counts, then loads them into separate ArcadeDB databases using:
1) Transactional INSERTs in batches
2) Async SQL INSERTs via async executor
3) SQL IMPORT DATABASE from CSV files
4) Python `db.import_documents(...)` wrapper over the Java importer

Goal: provide a more realistic ingest-speed comparison than a single tiny table.
This benchmark includes importer-based paths because they are possible, but the
repository recommendation for Python-managed document preload remains async SQL insert.
In practice, both `IMPORT DATABASE` and `db.import_documents(...)` have shown
reliability issues on larger real workloads, including memory pressure and possible OoM
failures.

Observed benchmark result (2026-03-19, before `db.import_documents(...)` was added):
For:
- tables=10
- rows-per-table=1,000,000
- columns=20 (plus id => 21 columns/table)
- string-size=128
- batch-size=10,000
- heap-size=8g

Measured ingest times:
- Transactional INSERT: 189.921s
- Async SQL INSERT: 146.670s
- IMPORT DATABASE (`parallel=1`): 58.281s
- IMPORT DATABASE (`parallel=4`): 48.580s

In this synthetic shape, increasing SQL import parallelism from 1 to 4 did not improve
throughput materially. IMPORT DATABASE was faster than both Async SQL and Transactional
INSERT, but that does not make the importer-based paths the default recommendation for
the rest of the examples.

Known limitation:
On some ArcadeDB import code paths, `IMPORT DATABASE` with CSV documents may not apply
`commitEvery` as expected for transaction splitting. Very large single CSV tables can
therefore hit the transaction-buffer limit (for example, >2GB tx buffer) even when
`commitEvery` is configured.
"""

import argparse
import shutil
import time
from pathlib import Path
from typing import Dict, List, Tuple

import arcadedb_embedded as arcadedb

ColumnDef = Tuple[str, str]

NUMERIC_COLUMN_TYPES = {"INTEGER", "LONG"}


def result_int(row: object, key: str) -> int:
    value = row.get(key)
    return 0 if value is None else int(value)


def build_table_name(table_index: int) -> str:
    return f"Bench{table_index:02d}"


def build_column_defs(extra_columns: int) -> List[ColumnDef]:
    columns: List[ColumnDef] = [("id", "LONG")]

    int_cols = extra_columns // 2
    str_cols = extra_columns - int_cols

    for idx in range(1, int_cols + 1):
        columns.append((f"i{idx}", "LONG"))

    for idx in range(1, str_cols + 1):
        columns.append((f"s{idx}", "STRING"))

    return columns


def make_string_value(table_name: str, column_name: str, row_id: int, size: int) -> str:
    base = f"{table_name}_{column_name}_{row_id}"
    if len(base) >= size:
        return base[:size]

    padding = "x" * (size - len(base))
    return base + padding


def build_csv_for_table(
    csv_path: Path,
    table_name: str,
    rows_per_table: int,
    columns: List[ColumnDef],
    string_size: int,
) -> None:
    header = ",".join(column_name for column_name, _ in columns)
    with csv_path.open("w", encoding="utf-8") as handle:
        handle.write(header + "\n")

        for row_id in range(1, rows_per_table + 1):
            values: List[str] = []
            for column_name, column_type in columns:
                if column_name == "id":
                    values.append(str(row_id))
                elif column_type in NUMERIC_COLUMN_TYPES:
                    values.append(str((row_id * 31 + len(column_name)) % 1000000))
                else:
                    values.append(
                        make_string_value(table_name, column_name, row_id, string_size)
                    )

            handle.write(",".join(values) + "\n")


def recreate_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def print_faster_statement(
    faster_name: str,
    faster_seconds: float,
    slower_name: str,
    slower_seconds: float,
) -> None:
    if faster_seconds <= 0 or slower_seconds <= 0:
        return
    ratio = slower_seconds / faster_seconds
    pct_less_time = (1.0 - (faster_seconds / slower_seconds)) * 100.0
    print(
        (
            f"- {faster_name} is {ratio:.2f}x faster than {slower_name} "
            f"({slower_seconds:.3f}s -> {faster_seconds:.3f}s, "
            f"{pct_less_time:.1f}% less time)"
        )
    )


def sample_ids(total: int) -> List[int]:
    if total <= 0:
        return []
    return sorted({1, max(1, total // 2), total})


def collect_table_sample(
    db: object,
    table_name: str,
    row_id: int,
    columns: List[ColumnDef],
) -> dict:
    row = db.query("sql", f"SELECT FROM {table_name} WHERE id = {row_id}").one()
    sample: dict = {}

    for column_name, column_type in columns:
        value = row.get(column_name)
        if value is not None and column_type in NUMERIC_COLUMN_TYPES:
            sample[column_name] = int(value)
        else:
            sample[column_name] = value

    return sample


def collect_table_signatures(
    db_path: Path,
    heap_size: str,
    table_names: List[str],
    columns: List[ColumnDef],
    rows_per_table: int,
) -> dict:
    with arcadedb.open_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    ) as db:
        int_props = [
            name
            for name, kind in columns
            if kind in NUMERIC_COLUMN_TYPES and name != "id"
        ]
        table_signatures = {}

        for table_name in table_names:
            table_type = db.schema.get_type(table_name)
            aggregate_fields = [
                "count(*) AS count",
                "sum(id) AS sum_id",
                "min(id) AS min_id",
                "max(id) AS max_id",
            ] + [f"sum({name}) AS sum_{name}" for name in int_props]

            aggregate = db.query(
                "sql",
                f"SELECT {', '.join(aggregate_fields)} FROM {table_name}",
            ).one()

            table_signatures[table_name] = {
                "schema": {
                    column_name: str(table_type.getProperty(column_name).getType())
                    for column_name, _ in columns
                },
                "aggregate": {
                    "count": result_int(aggregate, "count"),
                    "sum_id": result_int(aggregate, "sum_id"),
                    "min_id": result_int(aggregate, "min_id"),
                    "max_id": result_int(aggregate, "max_id"),
                    **{
                        f"sum_{name}": result_int(aggregate, f"sum_{name}")
                        for name in int_props
                    },
                },
                "samples": {
                    row_id: collect_table_sample(db, table_name, row_id, columns)
                    for row_id in sample_ids(rows_per_table)
                },
            }

        return table_signatures


def describe_mapping_differences(
    prefix: str,
    baseline: dict,
    candidate: dict,
) -> List[str]:
    differences: List[str] = []

    for key in sorted(set(baseline) | set(candidate)):
        baseline_value = baseline.get(key)
        candidate_value = candidate.get(key)
        if baseline_value != candidate_value:
            differences.append(
                f"{prefix}.{key}: expected {baseline_value!r}, got {candidate_value!r}"
            )

    return differences


def describe_table_signature_differences(
    baseline_name: str,
    baseline: dict,
    candidate_name: str,
    candidate: dict,
) -> List[str]:
    differences: List[str] = []

    for table_name in sorted(set(baseline) | set(candidate)):
        baseline_table = baseline.get(table_name)
        candidate_table = candidate.get(table_name)

        if baseline_table is None or candidate_table is None:
            differences.append(
                (
                    f"{candidate_name}.{table_name}: table presence differs "
                    f"from {baseline_name}"
                )
            )
            continue

        differences.extend(
            describe_mapping_differences(
                f"{candidate_name}.{table_name}.schema",
                baseline_table["schema"],
                candidate_table["schema"],
            )
        )
        differences.extend(
            describe_mapping_differences(
                f"{candidate_name}.{table_name}.aggregate",
                baseline_table["aggregate"],
                candidate_table["aggregate"],
            )
        )

        for row_id in sorted(
            set(baseline_table["samples"]) | set(candidate_table["samples"])
        ):
            baseline_sample = baseline_table["samples"].get(row_id)
            candidate_sample = candidate_table["samples"].get(row_id)

            if baseline_sample is None or candidate_sample is None:
                differences.append(
                    (
                        f"{candidate_name}.{table_name}.samples[{row_id}]: "
                        f"sample presence differs from {baseline_name}"
                    )
                )
                continue

            differences.extend(
                describe_mapping_differences(
                    f"{candidate_name}.{table_name}.samples[{row_id}]",
                    baseline_sample,
                    candidate_sample,
                )
            )

    return differences


def validate_table_signatures_or_fail(signatures: Dict[str, dict]) -> None:
    baseline_name = next(iter(signatures))
    baseline = signatures[baseline_name]
    problems: List[str] = []

    for name, signature in signatures.items():
        if signature != baseline:
            problems.extend(
                describe_table_signature_differences(
                    baseline_name,
                    baseline,
                    name,
                    signature,
                )
            )

    if problems:
        raise RuntimeError("Logical table output differs:\n- " + "\n- ".join(problems))


def run_transactional_load(
    db_path: Path,
    table_names: List[str],
    rows_per_table: int,
    columns: List[ColumnDef],
    string_size: int,
    batch_size: int,
    heap_size: str,
) -> dict:
    recreate_dir(db_path)

    db = arcadedb.create_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    )

    start = time.perf_counter()

    total_loaded = 0

    for table_name in table_names:
        db.command("sql", f"CREATE DOCUMENT TYPE {table_name}")
        for column_name, column_type in columns:
            db.command(
                "sql",
                f"CREATE PROPERTY {table_name}.{column_name} {column_type}",
            )

        placeholders = ", ".join(f"{column_name} = ?" for column_name, _ in columns)
        insert_sql = f"INSERT INTO {table_name} SET {placeholders}"

        for begin in range(1, rows_per_table + 1, batch_size):
            end = min(begin + batch_size - 1, rows_per_table)
            with db.transaction():
                for row_id in range(begin, end + 1):
                    payload: List[object] = []
                    for column_name, column_type in columns:
                        if column_name == "id":
                            payload.append(row_id)
                        elif column_type in NUMERIC_COLUMN_TYPES:
                            payload.append((row_id * 31 + len(column_name)) % 1000000)
                        else:
                            payload.append(
                                make_string_value(
                                    table_name, column_name, row_id, string_size
                                )
                            )

                    db.command("sql", insert_sql, payload)

        loaded = (
            db.query("sql", f"SELECT count(*) AS c FROM {table_name}").one().get("c")
            or 0
        )
        total_loaded += int(loaded)

    elapsed = time.perf_counter() - start
    db.close()

    return {
        "rows": total_loaded,
        "seconds": elapsed,
    }


def run_async_sql_load(
    db_path: Path,
    table_names: List[str],
    rows_per_table: int,
    columns: List[ColumnDef],
    string_size: int,
    batch_size: int,
    async_parallel: int,
    heap_size: str,
) -> dict:
    recreate_dir(db_path)

    db = arcadedb.create_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    )

    db.set_read_your_writes(False)
    async_exec = db.async_executor()
    async_exec.set_parallel_level(max(1, async_parallel))
    async_exec.set_commit_every(batch_size)
    async_exec.set_transaction_use_wal(False)

    errors: List[Exception] = []

    def on_error(exc: Exception):
        errors.append(exc)

    async_exec.on_error(on_error)

    start = time.perf_counter()
    total_loaded = 0

    try:
        for table_name in table_names:
            db.command("sql", f"CREATE DOCUMENT TYPE {table_name}")
            for column_name, column_type in columns:
                db.command(
                    "sql",
                    f"CREATE PROPERTY {table_name}.{column_name} {column_type}",
                )

            placeholders = ", ".join(f"{column_name} = ?" for column_name, _ in columns)
            insert_sql = f"INSERT INTO {table_name} SET {placeholders}"

            for row_id in range(1, rows_per_table + 1):
                payload: List[object] = []
                for column_name, column_type in columns:
                    if column_name == "id":
                        payload.append(row_id)
                    elif column_type in NUMERIC_COLUMN_TYPES:
                        payload.append((row_id * 31 + len(column_name)) % 1000000)
                    else:
                        payload.append(
                            make_string_value(
                                table_name,
                                column_name,
                                row_id,
                                string_size,
                            )
                        )

                async_exec.command("sql", insert_sql, args=payload)

            async_exec.wait_completion()

            loaded = (
                db.query("sql", f"SELECT count(*) AS c FROM {table_name}")
                .one()
                .get("c")
                or 0
            )
            total_loaded += int(loaded)

        elapsed = time.perf_counter() - start
        if errors:
            raise RuntimeError(
                f"Async SQL ingest reported {len(errors)} errors (first: {errors[0]})"
            )

    finally:
        async_exec.wait_completion()
        async_exec.close()
        db.set_read_your_writes(True)
        async_exec.set_transaction_use_wal(True)
        db.close()

    return {
        "rows": total_loaded,
        "seconds": elapsed,
    }


def validate_parity_or_fail(
    expected_rows: int,
    txn: dict,
    async_sql: dict,
    sql_import: dict,
    python_import: dict,
):
    problems: List[str] = []

    if txn["rows"] != expected_rows:
        problems.append(
            "transactional row count mismatch "
            f"(expected={expected_rows:,}, got={txn['rows']:,})"
        )

    if async_sql["rows"] != expected_rows:
        problems.append(
            "async-sql row count mismatch "
            f"(expected={expected_rows:,}, got={async_sql['rows']:,})"
        )

    if sql_import["rows"] != expected_rows:
        problems.append(
            "sql-import row count mismatch "
            f"(expected={expected_rows:,}, got={sql_import['rows']:,})"
        )

    if python_import["rows"] != expected_rows:
        problems.append(
            "python-import row count mismatch "
            f"(expected={expected_rows:,}, got={python_import['rows']:,})"
        )

    row_counts = {
        "txn": txn["rows"],
        "async-sql": async_sql["rows"],
        "sql-import": sql_import["rows"],
        "python-import": python_import["rows"],
    }
    if len(set(row_counts.values())) != 1:
        problems.append(
            "method parity mismatch "
            + "("
            + ", ".join(f"{name}={count:,}" for name, count in row_counts.items())
            + ")"
        )

    if problems:
        raise RuntimeError("Invalid benchmark run:\n- " + "\n- ".join(problems))


def run_import_database_load(
    db_path: Path,
    table_csv_map: Dict[str, Path],
    batch_size: int,
    heap_size: str,
    parallel: int,
) -> dict:
    recreate_dir(db_path)

    db = arcadedb.create_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    )

    db.set_read_your_writes(False)
    async_exec = db.async_executor()
    async_exec.set_parallel_level(max(1, parallel))
    async_exec.set_commit_every(batch_size)
    async_exec.set_transaction_use_wal(False)

    start = time.perf_counter()

    total_loaded = 0
    total_errors = 0

    try:
        for table_name, csv_path in table_csv_map.items():
            csv_url = f"file://{csv_path.resolve().as_posix()}"
            result = db.command(
                "sql",
                (
                    "IMPORT DATABASE WITH "
                    f"documents = '{csv_url}', "
                    "documentsFileType = 'csv', "
                    f"documentType = '{table_name}', "
                    f"commitEvery = {batch_size}, "
                    f"parallel = {parallel}"
                ),
            ).one()

            db.async_executor().wait_completion()

            total_errors += int(result.get("errors") or 0)
            loaded = (
                db.query("sql", f"SELECT count(*) AS c FROM {table_name}")
                .one()
                .get("c")
                or 0
            )
            total_loaded += int(loaded)

        elapsed = time.perf_counter() - start
    finally:
        db.async_executor().wait_completion()
        db.set_read_your_writes(True)
        async_exec.set_transaction_use_wal(True)
        db.close()

    return {
        "rows": total_loaded,
        "seconds": elapsed,
        "errors": total_errors,
    }


def run_import_documents_load(
    db_path: Path,
    table_csv_map: Dict[str, Path],
    batch_size: int,
    heap_size: str,
    parallel: int,
    import_chunk_rows: int,
) -> dict:
    recreate_dir(db_path)
    staging_dir = db_path.parent / f"{db_path.name}_import_documents_chunks"
    recreate_dir(staging_dir)

    try:
        with arcadedb.create_database(
            str(db_path),
            jvm_kwargs={"heap_size": heap_size} if heap_size else None,
        ) as db:
            start = time.perf_counter()
            import_parallel = max(1, parallel)

            total_loaded = 0
            total_errors = 0

            for table_name, csv_path in table_csv_map.items():
                with csv_path.open("r", encoding="utf-8", newline="") as source:
                    header = source.readline()
                    chunk_index = 0
                    rows_in_chunk = 0
                    chunk_path: Path | None = None
                    chunk_handle = None
                    table_had_chunks = False

                    def open_chunk(
                        table_name: str = table_name,
                        header: str = header,
                    ) -> None:
                        nonlocal chunk_path, chunk_handle, chunk_index
                        chunk_path = (
                            staging_dir / f"{table_name}_chunk_{chunk_index:05d}.csv"
                        )
                        chunk_index += 1
                        chunk_handle = chunk_path.open("w", encoding="utf-8")
                        chunk_handle.write(header)

                    def import_chunk(table_name: str = table_name) -> None:
                        nonlocal chunk_path, chunk_handle, rows_in_chunk
                        nonlocal total_errors, table_had_chunks
                        if (
                            chunk_path is None
                            or chunk_handle is None
                            or rows_in_chunk == 0
                        ):
                            return

                        chunk_handle.close()
                        chunk_handle = None

                        try:
                            result = db.import_documents(
                                chunk_path,
                                document_type=table_name,
                                file_type="csv",
                                commit_every=batch_size,
                                parallel=import_parallel,
                                wal=False,
                            )
                            total_errors += int(result.get("errors") or 0)
                            table_had_chunks = True
                        finally:
                            chunk_path.unlink(missing_ok=True)

                        chunk_path = None
                        rows_in_chunk = 0

                    if not header:
                        continue

                    # Example 15 generates CSV values without commas, quotes, or
                    # embedded newlines, so line-based chunking is safe here and
                    # avoids the extra csv.reader/csv.writer rewrite cost.
                    for line in source:
                        if chunk_handle is None:
                            open_chunk()

                        chunk_handle.write(line)
                        rows_in_chunk += 1

                        if rows_in_chunk >= import_chunk_rows:
                            import_chunk()

                    import_chunk()

                if not table_had_chunks:
                    continue

                loaded = (
                    db.query("sql", f"SELECT count(*) AS c FROM {table_name}")
                    .one()
                    .get("c")
                    or 0
                )
                total_loaded += int(loaded)

            elapsed = time.perf_counter() - start
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)

    return {
        "rows": total_loaded,
        "seconds": elapsed,
        "errors": total_errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Example 15: Import Database vs Transactional Table Ingest"
    )
    parser.add_argument(
        "--rows-per-table",
        type=int,
        default=500_000,
        help="Rows to generate for each table",
    )
    parser.add_argument(
        "--tables",
        type=int,
        default=8,
        help="Number of tables to generate/import",
    )
    parser.add_argument(
        "--columns",
        type=int,
        default=40,
        help="Extra columns per table (in addition to id)",
    )
    parser.add_argument(
        "--string-size",
        type=int,
        default=64,
        help="Size in bytes for generated string column values",
    )
    parser.add_argument("--batch-size", type=int, default=10_000, help="Batch size")
    parser.add_argument(
        "--async-parallel",
        type=int,
        default=1,
        help="Parallel workers for async SQL path (default 1 for stability)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=8,
        help="Parallel workers for SQL import path",
    )
    parser.add_argument(
        "--import-chunk-rows",
        type=int,
        default=100_000,
        help="Maximum CSV rows per import_documents chunk",
    )
    parser.add_argument("--heap-size", type=str, default="4g", help="JVM heap size")
    parser.add_argument(
        "--work-dir",
        type=str,
        default="./my_test_databases/import_vs_txn_dummy",
        help="Output working directory",
    )
    args = parser.parse_args()
    if args.import_chunk_rows <= 0:
        parser.error("--import-chunk-rows must be > 0")

    work_dir = Path(args.work_dir)
    recreate_dir(work_dir)

    data_dir = work_dir / "csv"
    recreate_dir(data_dir)

    columns = build_column_defs(args.columns)
    table_names = [build_table_name(index) for index in range(1, args.tables + 1)]
    table_csv_map: Dict[str, Path] = {}

    print(
        (
            f"Generating CSVs: {len(table_names)} tables x "
            f"{args.rows_per_table:,} rows/table "
            f"with {len(columns)} columns/table"
        )
    )
    print(f"String payload size: {args.string_size} bytes per STRING column value")
    for table_name in table_names:
        csv_path = data_dir / f"{table_name}.csv"
        build_csv_for_table(
            csv_path=csv_path,
            table_name=table_name,
            rows_per_table=args.rows_per_table,
            columns=columns,
            string_size=args.string_size,
        )
        table_csv_map[table_name] = csv_path

    expected_rows = args.rows_per_table * len(table_names)

    txn_db = work_dir / "db_transactional"
    async_sql_db = work_dir / "db_async_sql"
    imp_db = work_dir / "db_import_database"
    py_imp_db = work_dir / "db_import_documents"

    print("\nRunning transactional INSERT benchmark...")
    txn = run_transactional_load(
        db_path=txn_db,
        table_names=table_names,
        rows_per_table=args.rows_per_table,
        columns=columns,
        string_size=args.string_size,
        batch_size=args.batch_size,
        heap_size=args.heap_size,
    )

    print("Running async SQL INSERT benchmark...")
    async_sql = run_async_sql_load(
        db_path=async_sql_db,
        table_names=table_names,
        rows_per_table=args.rows_per_table,
        columns=columns,
        string_size=args.string_size,
        batch_size=args.batch_size,
        async_parallel=args.async_parallel,
        heap_size=args.heap_size,
    )

    print("Running IMPORT DATABASE benchmark...")
    imp = run_import_database_load(
        db_path=imp_db,
        table_csv_map=table_csv_map,
        batch_size=args.batch_size,
        heap_size=args.heap_size,
        parallel=args.parallel,
    )

    print("Running Python import_documents benchmark...")
    py_imp = run_import_documents_load(
        db_path=py_imp_db,
        table_csv_map=table_csv_map,
        batch_size=args.batch_size,
        heap_size=args.heap_size,
        parallel=args.parallel,
        import_chunk_rows=args.import_chunk_rows,
    )

    print("\n=== Results ===")
    print(
        (
            f"Dataset shape: {len(table_names)} tables, "
            f"{args.rows_per_table:,} rows/table, "
            f"{len(columns)} columns/table"
        )
    )
    print(f"Expected rows: {expected_rows:,}")
    print(f"Transactional INSERT: {txn['rows']:,} rows in {txn['seconds']:.3f}s")
    print(
        "Async SQL INSERT:    "
        f"{async_sql['rows']:,} rows in {async_sql['seconds']:.3f}s"
    )
    print(
        f"IMPORT DATABASE:     {imp['rows']:,} rows in {imp['seconds']:.3f}s "
        f"(errors={imp['errors']})"
    )
    print(
        f"import_documents:    {py_imp['rows']:,} rows in {py_imp['seconds']:.3f}s "
        f"(errors={py_imp['errors']})"
    )

    print("\nPerformance comparison:")
    print_faster_statement(
        "Async SQL INSERT",
        async_sql["seconds"],
        "Transactional INSERT",
        txn["seconds"],
    )
    print_faster_statement(
        "IMPORT DATABASE",
        imp["seconds"],
        "Transactional INSERT",
        txn["seconds"],
    )
    print_faster_statement(
        "IMPORT DATABASE",
        imp["seconds"],
        "Async SQL INSERT",
        async_sql["seconds"],
    )
    print_faster_statement(
        "import_documents",
        py_imp["seconds"],
        "Transactional INSERT",
        txn["seconds"],
    )
    print_faster_statement(
        "import_documents",
        py_imp["seconds"],
        "Async SQL INSERT",
        async_sql["seconds"],
    )
    print_faster_statement(
        "import_documents",
        py_imp["seconds"],
        "IMPORT DATABASE",
        imp["seconds"],
    )

    validate_parity_or_fail(expected_rows, txn, async_sql, imp, py_imp)

    print("\nRunning lightweight logical parity check...")
    signatures = {
        "transactional": collect_table_signatures(
            db_path=txn_db,
            heap_size=args.heap_size,
            table_names=table_names,
            columns=columns,
            rows_per_table=args.rows_per_table,
        ),
        "async_sql": collect_table_signatures(
            db_path=async_sql_db,
            heap_size=args.heap_size,
            table_names=table_names,
            columns=columns,
            rows_per_table=args.rows_per_table,
        ),
        "import_database": collect_table_signatures(
            db_path=imp_db,
            heap_size=args.heap_size,
            table_names=table_names,
            columns=columns,
            rows_per_table=args.rows_per_table,
        ),
        "import_documents": collect_table_signatures(
            db_path=py_imp_db,
            heap_size=args.heap_size,
            table_names=table_names,
            columns=columns,
            rows_per_table=args.rows_per_table,
        ),
    }
    validate_table_signatures_or_fail(signatures)
    print("Logical parity: all four methods produced the same sampled table output")

    print(f"\nArtifacts kept in: {work_dir}")


if __name__ == "__main__":
    main()
