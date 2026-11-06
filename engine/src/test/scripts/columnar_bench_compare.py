#!/usr/bin/env python3
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
# SPDX-License-Identifier: Apache-2.0
#
"""Compares the JSON reports of the columnar benchmarks (BenchmarkReport), computes the noise floor and ranks profile samples.

Standard library only; run with python3 -I.

  compare <dirOrFileA> <dirOrFileB> [--noise noise-floor.json] [--pair OFF:ON] [--min-samples N]
      A is the baseline (off), B the candidate (on). A row with fewer than N samples on a side (default 5, the measured rounds of
      the protocol) is INVALID and makes the exit status nonzero; without --noise the verdicts use the deviation of the run alone
      and are not protocol-valid (a warning says so). The GC figures (gcCount, gcMs) stay in the JSON but are not verdict metrics:
      a handful of collections per phase gives them floors of 65-100%; the apply allocation is the GC proxy. Every sample of a (benchmark, metric, variant) found in the JSON files of a
      side is pooled. Without --pair, each variant of A is compared with the same variant of B; with --pair OFF:ON, variant OFF of
      A with variant ON of B (for the two variants of one run: compare <dir> <dir> --pair off:on, or --pair Boff:B for the ingest
      benchmark, whose reports from before the tuning switches were removed hold a Boff variant).
  noise <dir> [--out file]
      Per metric of every report in <dir> that has an on/off pair of variants (on/off, or B/Boff in the older ingest reports): max(|median_on - median_off| /
      median_off, deviation of each variant), where the deviation is the median of |x - median| / median over the samples (one
      outlier round cannot set it, unlike a range). Written to --out, default noise-floor.json next to <dir>.
  rank <collapsed.txt> <categories.tsv>
      Sums the samples of a collapsed stack file (async-profiler jfrconv -o collapsed, with --threads) by category. Each line of
      the TSV is: thread regex, frame regex, category. A stack takes the category of the first line whose thread regex matches its
      first frame (the thread) and whose frame regex matches a frame; otherwise "other".
  collect <srcDir> <destDir> <marker> [--suffix S]
      Moves the *.json files of <srcDir> newer than <marker> into <destDir>, as <name>-<S>.json when a suffix is given (the load
      ITs write fixed names, so each run is labelled); a destination that exists is refused, never overwritten. Prints each file.
"""

import argparse
import json
import os
import re
import shutil
import sys

VERDICT_RULE = ("delta = (median_off - median_on) / median_off, lower is better; threshold = max(noise floor of the metric, "
                "median relative deviation median(|x - median|) / median of either side). WIN iff delta > threshold and median_on < min_off; "
                "LOSS iff -delta > threshold and median_off < min_on; otherwise NOISE.")

ON_OFF_PAIRS = (("on", "off"), ("B", "Boff"))

# kept in the JSON, never a verdict metric (Task 1: floors of 0.65-1.0 at 0-19 collections per phase)
EXCLUDED_FIGURES = ("gcCount", "gcMs")

MIN_SAMPLES = 5

NO_NOISE_WARNING = ("WARNING: no noise floor applied (--noise): the threshold is the deviation of this run alone, "
                    "so these verdicts are not protocol-valid.")


def median(values):
    ordered = sorted(values)
    n = len(ordered)
    if n == 0:
        return 0.0
    middle = n // 2
    return float(ordered[middle]) if n % 2 == 1 else (ordered[middle - 1] + ordered[middle]) / 2.0


def deviation(values):
    """The median of |x - median| / median: the typical relative distance of a round from the median, blind to one outlier."""
    m = median(values)
    return 0.0 if m == 0 else median([abs(v - m) for v in values]) / abs(m)


def verdict(off, on, noise_floor, min_samples=MIN_SAMPLES):
    """The verdict of the measurement protocol for a lower-is-better metric, off the baseline and on the candidate; INVALID with
    fewer than min_samples samples on either side."""
    median_off = median(off)
    median_on = median(on)
    threshold = max(noise_floor or 0.0, deviation(off), deviation(on))
    delta = 0.0 if median_off == 0 else (median_off - median_on) / median_off
    result = "NOISE"
    note = ""
    if min(len(off), len(on)) < min_samples:
        result = "INVALID"
        note = "fewer than %d samples per side (%d off, %d on)" % (min_samples, len(off), len(on))
    elif median_off != 0:
        if delta > threshold and median_on < min(off):
            result = "WIN"
        elif -delta > threshold and median_off < min(on):
            result = "LOSS"
    return {"median_off": median_off, "median_on": median_on, "delta": delta, "threshold": threshold, "verdict": result,
            "note": note, "n_off": len(off), "n_on": len(on)}


def report_files(path):
    if os.path.isfile(path):
        return [path]
    return [os.path.join(path, name) for name in sorted(os.listdir(path)) if name.endswith(".json")]


def load(path):
    """{(benchmark, metric): {"unit": unit, "variants": {variant: [samples]}}} pooled over the report files of the path."""
    pooled = {}
    for file in report_files(path):
        with open(file, encoding="utf-8") as f:
            content = json.load(f)
        if not isinstance(content, dict) or "benchmark" not in content or "metrics" not in content:
            continue  # machine-*.json, noise-floor.json
        for metric, body in content["metrics"].items():
            if metric.rsplit(".", 1)[-1] in EXCLUDED_FIGURES:
                continue
            entry = pooled.setdefault((content["benchmark"], metric), {"unit": body.get("unit", ""), "variants": {}})
            for variant, samples in body.get("variants", {}).items():
                entry["variants"].setdefault(variant, []).extend(samples)
    return pooled


def floor_of(floors, benchmark, metric):
    return floors.get(benchmark, {}).get(metric, 0.0) if floors else 0.0


def compare(path_a, path_b, noise_file, pair, min_samples=MIN_SAMPLES):
    """The rows of A (off) against B (on); pair is None (each variant against itself) or (off variant, on variant)."""
    floors = None
    if noise_file:
        with open(noise_file, encoding="utf-8") as f:
            floors = json.load(f)
    a = load(path_a)
    b = load(path_b)
    rows = []
    for key in sorted(a.keys() & b.keys()):
        benchmark, metric = key
        if pair is None:
            variants = [(v, v) for v in a[key]["variants"] if v in b[key]["variants"]]
        elif pair[0] in a[key]["variants"] and pair[1] in b[key]["variants"]:
            variants = [pair]
        else:
            variants = []
        for off_variant, on_variant in variants:
            row = verdict(a[key]["variants"][off_variant], b[key]["variants"][on_variant], floor_of(floors, benchmark, metric),
                          min_samples)
            row.update({"benchmark": benchmark, "metric": metric, "unit": a[key]["unit"],
                        "variant": off_variant if off_variant == on_variant else off_variant + "->" + on_variant})
            rows.append(row)
    return rows


def print_rows(rows, noise_applied):
    header = ("benchmark", "metric", "variant", "unit", "n", "median off", "median on", "delta", "threshold", "verdict", "note")
    table: list[tuple[str, ...]] = [header]
    for r in rows:
        table.append((str(r["benchmark"]), str(r["metric"]), str(r["variant"]), str(r["unit"]), "%d/%d" % (r["n_off"], r["n_on"]),
                      "%.3f" % r["median_off"], "%.3f" % r["median_on"], "%+.2f%%" % (100 * r["delta"]),
                      "%.2f%%" % (100 * r["threshold"]), str(r["verdict"]), str(r["note"])))
    widths = [max(len(str(row[i])) for row in table) for i in range(len(header))]
    for row in table:
        print("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)).rstrip())
    print()
    print("Verdict rule: " + VERDICT_RULE)
    if not noise_applied:
        print(NO_NOISE_WARNING)


def noise(directory, out):
    """Writes the noise floor of every metric with an on/off pair of variants; returns the file written."""
    floors = {}
    for (benchmark, metric), body in sorted(load(directory).items()):
        variants = body["variants"]
        for on_name, off_name in ON_OFF_PAIRS:
            if on_name in variants and off_name in variants:
                on, off = variants[on_name], variants[off_name]
                median_off = median(off)
                gap = 0.0 if median_off == 0 else abs(median(on) - median_off) / median_off
                floors.setdefault(benchmark, {})[metric] = max(gap, deviation(on), deviation(off))
                break
    if out is None:
        out = os.path.join(os.path.dirname(os.path.abspath(directory.rstrip(os.sep))), "noise-floor.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(floors, f, indent=2, sort_keys=True)
        f.write("\n")
    return out


def collect(source, destination, marker, suffix):
    """Moves the JSON files of source newer than marker into destination, renamed <name>-<suffix>.json when a suffix is given;
    refuses (FileExistsError) a destination name that exists. Returns the destination paths."""
    if not os.path.isdir(source):
        return []
    since = os.path.getmtime(marker)
    moved = []
    for name in sorted(os.listdir(source)):
        path = os.path.join(source, name)
        if not name.endswith(".json") or not os.path.isfile(path) or os.path.getmtime(path) <= since:
            continue
        target = os.path.join(destination, name[:-len(".json")] + "-" + suffix + ".json" if suffix else name)
        if os.path.exists(target):
            raise FileExistsError("refusing to overwrite " + target)
        shutil.move(path, target)
        moved.append(target)
    return moved


def rank(collapsed, categories):
    """[(category, samples)] by samples descending, ties by name."""
    rules = []
    with open(categories, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) != 3:
                raise ValueError("categories line needs 3 tab-separated fields: " + line)
            rules.append((re.compile(fields[0]), re.compile(fields[1]), fields[2]))
    totals = {}
    with open(collapsed, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            stack, _, count = line.rpartition(" ")
            frames = stack.split(";")
            category = "other"
            for thread_regex, frame_regex, name in rules:
                if thread_regex.search(frames[0]) and any(frame_regex.search(frame) for frame in frames[1:]):
                    category = name
                    break
            totals[category] = totals.get(category, 0) + int(count)
    return sorted(totals.items(), key=lambda item: (-item[1], item[0]))


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    c = commands.add_parser("compare")
    c.add_argument("a")
    c.add_argument("b")
    c.add_argument("--noise")
    c.add_argument("--pair", help="OFF:ON variant names, e.g. off:on or Boff:B")
    c.add_argument("--min-samples", type=int, default=MIN_SAMPLES, help="samples per side below which a row is INVALID")
    n = commands.add_parser("noise")
    n.add_argument("dir")
    n.add_argument("--out")
    r = commands.add_parser("rank")
    r.add_argument("collapsed")
    r.add_argument("categories")
    m = commands.add_parser("collect")
    m.add_argument("source")
    m.add_argument("destination")
    m.add_argument("marker")
    m.add_argument("--suffix")
    args = parser.parse_args(argv)

    if args.command == "compare":
        pair = None
        if args.pair:
            off_variant, sep, on_variant = args.pair.partition(":")
            if not sep or not off_variant or not on_variant:
                parser.error("--pair takes OFF:ON")
            pair = (off_variant, on_variant)
        rows = compare(args.a, args.b, args.noise, pair, args.min_samples)
        if not rows:
            print("No metric in common between %s and %s" % (args.a, args.b))
            return 1
        print_rows(rows, bool(args.noise))
        if any(r["verdict"] == "INVALID" for r in rows):
            return 3
    elif args.command == "noise":
        print("Wrote " + noise(args.dir, args.out))
    elif args.command == "collect":
        moved = collect(args.source, args.destination, args.marker, args.suffix)
        for path in moved:
            print("report: " + path)
        return 0 if moved else 1
    else:
        ranking = rank(args.collapsed, args.categories)
        total = sum(samples for _, samples in ranking) or 1
        width = max(len(name) for name, _ in ranking) if ranking else 8
        for name, samples in ranking:
            print("%s  %10d  %6.2f%%" % (name.ljust(width), samples, 100.0 * samples / total))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
