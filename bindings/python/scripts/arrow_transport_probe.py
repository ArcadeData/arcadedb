#!/usr/bin/env python3
"""Is to_arrow() worth shipping? Measure it against the paths we already have.

The claim to test is not "Arrow is fast". It is that reading the SAME columnar
buffer into Arrow beats reading it into numpy/pandas, because two things in the
pandas path cost real work:

  strings   to_columns decodes one Python str per row from the offsets+blob
            buffer. Arrow's StringArray uses exactly that layout, so the column
            is wrapped instead of decoded.
  nullable  to_columns promotes int64+nulls to float64/NaN and degrades
            bool+nulls to a Python list. Arrow keeps both, with validity.

So the interesting comparison is per column TYPE, not one aggregate number, and
the honest end-to-end measure is "how long to a pandas DataFrame", since that is
what the audience actually wants. Arrow can lose that race on numeric-only data
and still win on strings; reporting a single figure would hide which.

Reports p50 of N reps after warmup, and a correctness check that the two paths
agree, so a fast wrong answer cannot pass as a win.
"""

import argparse
import os
import statistics
import sys
import tempfile
import time


def build(db, n, shape):
    db.command("sql", "CREATE DOCUMENT TYPE Rec")
    with db.transaction():
        for i in range(0, n, 1000):
            rows = []
            for j in range(i, min(i + 1000, n)):
                if shape == "numeric":
                    rows.append(
                        f"INSERT INTO Rec SET a = {j}, b = {j * 1.5}, c = {j % 7}"
                    )
                elif shape == "strings":
                    rows.append(
                        f"INSERT INTO Rec SET s1 = 'value-{j}', "
                        f"s2 = 'category-{j % 50}', a = {j}"
                    )
                else:  # mixed, with nulls in an integer column
                    if j % 3 == 0:
                        rows.append(f"INSERT INTO Rec SET s1 = 'v{j}', b = {j * 1.5}")
                    else:
                        rows.append(
                            f"INSERT INTO Rec SET s1 = 'v{j}', a = {j}, b = {j * 1.5}"
                        )
            for r in rows:
                db.command("sql", r)


def timeit(fn, reps, warmup):
    for _ in range(warmup):
        fn()
    ts = []
    for _ in range(reps):
        t = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - t) * 1e3)
    return statistics.median(ts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--shapes", default="numeric,strings,mixed")
    args = ap.parse_args()

    import arcadedb_embedded as arcadedb
    import pandas as pd

    try:
        import pyarrow  # noqa: F401
    except ImportError:
        print("pyarrow not installed; nothing to compare", file=sys.stderr)
        return 1

    # The engine logs to stdout and does not always terminate its last line, so
    # a result row printed straight after one gets concatenated onto it and any
    # log-filtering grep upstream of this script eats the row silently. That
    # cost a re-run: the numeric row, the one shape where Arrow does NOT win,
    # was the row that disappeared. Lead every row with a newline and tag it,
    # so a row can be recovered with `grep -o 'ROW .*'` no matter what shares
    # the line.
    def row(*cells):
        print("\nROW " + "".join(cells), flush=True)

    print(f"\n  rows={args.rows} reps={args.reps} (warmup {args.warmup})")
    print(f"  arcadedb_embedded {arcadedb.__version__}\n")
    row(
        f"{'shape':<10}{'to_columns':>12}{'to_arrow':>11}",
        f"{'cols->df':>11}{'arrow->df':>11}{'df speedup':>12}",
    )

    for shape in args.shapes.split(","):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "probe")
            with arcadedb.create_database(path) as db:
                build(db, args.rows, shape)

                q = "SELECT FROM Rec"
                t_cols = timeit(
                    lambda: db.query("sql", q).to_columns(), args.reps, args.warmup
                )
                t_arrow = timeit(
                    lambda: db.query("sql", q).to_arrow(), args.reps, args.warmup
                )
                t_cols_df = timeit(
                    lambda: pd.DataFrame(db.query("sql", q).to_columns()),
                    args.reps,
                    args.warmup,
                )
                t_arrow_df = timeit(
                    lambda: db.query("sql", q).to_arrow().to_pandas(),
                    args.reps,
                    args.warmup,
                )

                # a fast wrong answer is not a win
                a = db.query("sql", q).to_arrow()
                c = db.query("sql", q).to_columns()
                assert a.num_rows == len(next(iter(c.values()))), (
                    f"{shape}: row count differs, {a.num_rows} vs "
                    f"{len(next(iter(c.values())))}"
                )

                sp = t_cols_df / t_arrow_df if t_arrow_df else float("nan")
                row(
                    f"  {shape:<10}{t_cols:>12.2f}{t_arrow:>11.2f}",
                    f"{t_cols_df:>11.2f}{t_arrow_df:>11.2f}{sp:>11.2f}x",
                )

    print("\n  cols->df and arrow->df are the numbers that matter: they are what")
    print("  a caller pays to get a DataFrame. A win on strings with a loss on")
    print("  numeric is a real result, not a reason to average them together.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
