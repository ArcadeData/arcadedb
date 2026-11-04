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
"""Unit tests of columnar_bench_compare.py. Run: python3 -I engine/src/test/scripts/test_columnar_bench_compare.py"""

import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

# -I leaves the script directory off sys.path: load the module by its path, without leaving a __pycache__ in the source tree
sys.dont_write_bytecode = True
_SPEC = importlib.util.spec_from_file_location(
    "columnar_bench_compare", os.path.join(os.path.dirname(os.path.abspath(__file__)), "columnar_bench_compare.py"))
if _SPEC is None or _SPEC.loader is None:
    raise ImportError("cannot load columnar_bench_compare.py next to " + __file__)
cbc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(cbc)


def report(benchmark, metric, variants, unit="ms"):
    return {"benchmark": benchmark, "label": "t", "lever": "NONE", "machine": {"cores": 8, "loadAverage": 1.0},
            "metrics": {metric: {"unit": unit, "variants": variants}}}


def write(directory, name, content):
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(content, f)
    return path


class VerdictTest(unittest.TestCase):

    def test_win_beyond_noise(self):
        off = [100, 101, 102, 99, 100]
        on = [80, 81, 79, 80, 82]
        row = cbc.verdict(off, on, 0.02)
        self.assertEqual("WIN", row["verdict"])
        self.assertAlmostEqual(0.2, row["delta"], places=6)
        # the median relative deviation of each side (off 1/100, on 1/80) and the floor: the larger is the threshold
        self.assertAlmostEqual(max(0.02, 1 / 100, 1 / 80), row["threshold"], places=6)

    def test_one_outlier_does_not_dominate_the_threshold(self):
        # one slow round among five: a range-based threshold ((200 - 100) / (2 * 100) = 50%) would hide a clear 10% gain
        off = [100, 100, 100, 100, 200]
        on = [90, 90, 90, 90, 90]
        row = cbc.verdict(off, on, 0.0)
        self.assertAlmostEqual(0.0, row["threshold"], places=6)
        self.assertEqual("WIN", row["verdict"])
        self.assertAlmostEqual(0.0, cbc.deviation(off), places=6)
        self.assertAlmostEqual(0.02, cbc.deviation([100, 98, 102, 100, 150]), places=6)

    def test_one_outlier_does_not_dominate_the_noise_floor(self):
        with tempfile.TemporaryDirectory() as tmp:
            nf_dir = os.path.join(tmp, "nf")
            write(nf_dir, "X-nf-1.json", report("X", "m", {"on": [100, 101, 99, 100, 300], "off": [100, 100, 102, 98, 100]}))
            with open(cbc.noise(nf_dir, None), encoding="utf-8") as f:
                floor = json.load(f)
            # medians equal, median deviations 1% and 0%: the 300 outlier does not set the floor
            self.assertAlmostEqual(0.01, floor["X"]["m"], places=6)

    def test_gc_figures_are_not_verdict_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, "runs")
            content = report("I", "insert.gcCount", {"B": [1] * 5, "Boff": [3] * 5}, unit="count")
            content["metrics"]["insert.gcMs"] = {"unit": "ms", "variants": {"B": [5] * 5, "Boff": [9] * 5}}
            content["metrics"]["insert.applyCpuMs"] = {"unit": "ms", "variants": {"B": [50] * 5, "Boff": [50] * 5}}
            write(d, "I-r-1.json", content)
            self.assertEqual({"insert.applyCpuMs"}, {r["metric"] for r in cbc.compare(d, d, None, None)})
            with open(cbc.noise(d, None), encoding="utf-8") as f:
                self.assertEqual({"I": {"insert.applyCpuMs": 0.0}}, json.load(f))


class CollectTest(unittest.TestCase):

    def test_collect_labels_every_run_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "facet-load")
            out = os.path.join(tmp, "out")
            os.makedirs(out)
            marker = write(tmp, "marker", {})
            os.utime(marker, (1, 1))
            write(src, "SingleServerFacetLoadTestIT-HTTP.json", {"run": 1})
            write(src, "SingleServerFacetLoadTestIT-gRPC.json", {"run": 1})
            old = write(src, "Stale.json", {})
            os.utime(old, (0, 0))
            moved = cbc.collect(src, out, marker, "nf-run1")
            self.assertEqual(["SingleServerFacetLoadTestIT-HTTP-nf-run1.json", "SingleServerFacetLoadTestIT-gRPC-nf-run1.json"],
                             sorted(os.path.basename(p) for p in moved))
            # the IT writes the same fixed names again on the next run: they land beside the first ones
            write(src, "SingleServerFacetLoadTestIT-HTTP.json", {"run": 2})
            moved = cbc.collect(src, out, marker, "nf-run2")
            self.assertEqual(["SingleServerFacetLoadTestIT-HTTP-nf-run2.json"], [os.path.basename(p) for p in moved])
            self.assertEqual(3, len(os.listdir(out)))
            # a name already taken is refused, never overwritten
            write(src, "SingleServerFacetLoadTestIT-HTTP.json", {"run": 3})
            with self.assertRaises(FileExistsError):
                cbc.collect(src, out, marker, "nf-run2")
            with open(os.path.join(out, "SingleServerFacetLoadTestIT-HTTP-nf-run2.json"), encoding="utf-8") as f:
                self.assertEqual({"run": 2}, json.load(f))
            # without a suffix the name is kept (the engine reports carry their own label and time stamp)
            write(src, "X-smoke-1.json", {})
            self.assertEqual(["X-smoke-1.json"], [os.path.basename(p) for p in cbc.collect(src, out, marker, None)
                                                  if os.path.basename(p).startswith("X-")])

    def test_within_noise_is_noise(self):
        off = [100, 100, 100, 100, 100]
        on = [97, 97, 97, 97, 97]
        # 3% better, all on below every off, but the floor is 5%
        self.assertEqual("NOISE", cbc.verdict(off, on, 0.05)["verdict"])
        # without a floor the same numbers win
        self.assertEqual("WIN", cbc.verdict(off, on, 0.0)["verdict"])

    def test_overlapping_ranges_is_noise(self):
        # delta 0.30 beats the half-spread 0.25, but the median on (70) is not below the fastest off (50)
        off = [50, 100, 100, 100, 100]
        on = [70, 70, 70, 70, 70]
        row = cbc.verdict(off, on, 0.0)
        self.assertGreater(row["delta"], row["threshold"])
        self.assertEqual("NOISE", row["verdict"])

    def test_regression_is_loss(self):
        off = [100, 101, 99, 100, 100]
        on = [130, 131, 129, 130, 132]
        row = cbc.verdict(off, on, 0.02)
        self.assertEqual("LOSS", row["verdict"])
        self.assertAlmostEqual(-0.3, row["delta"], places=6)

    def test_identical_samples_are_noise(self):
        self.assertEqual("NOISE", cbc.verdict([5], [5], 0.0, min_samples=1)["verdict"])
        self.assertEqual("NOISE", cbc.verdict([0, 0], [0, 0], 0.0, min_samples=1)["verdict"])

    def test_fewer_than_five_samples_is_invalid(self):
        # one sample per side has no spread: the protocol needs 5 measured rounds per side before any verdict
        row = cbc.verdict([100], [50], 0.0)
        self.assertEqual("INVALID", row["verdict"])
        self.assertIn("5", row["note"])
        row = cbc.verdict([100, 100, 100, 100], [50, 50, 50, 50, 50], 0.0)
        self.assertEqual("INVALID", row["verdict"])
        self.assertEqual("WIN", cbc.verdict([100] * 5, [50] * 5, 0.0)["verdict"])
        self.assertEqual("", cbc.verdict([100] * 5, [50] * 5, 0.0)["note"])
        # the tests may lower the minimum for small fixtures
        self.assertEqual("WIN", cbc.verdict([100], [50], 0.0, min_samples=1)["verdict"])

    def test_noise_floor_file_raises_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = write(os.path.join(tmp, "a"), "X-a-1.json", report("X", "insert.ms", {"B": [100, 100, 100, 100, 100]}))
            b = write(os.path.join(tmp, "b"), "X-b-1.json", report("X", "insert.ms", {"B": [94, 94, 94, 94, 94]}))
            rows = cbc.compare(a, b, None, None)
            self.assertEqual(["WIN"], [r["verdict"] for r in rows])

            # a noise floor computed from an on/off run of the same metric: 10% apart
            nf_dir = os.path.join(tmp, "nf")
            write(nf_dir, "X-nf-1.json", report("X", "insert.ms", {"on": [100, 100, 100], "off": [110, 110, 110]}))
            out = cbc.noise(nf_dir, None)
            self.assertEqual(os.path.join(tmp, "noise-floor.json"), out)
            with open(out, encoding="utf-8") as f:
                floor = json.load(f)
            self.assertAlmostEqual(10 / 110, floor["X"]["insert.ms"], places=6)

            rows = cbc.compare(a, b, out, None)
            self.assertEqual(["NOISE"], [r["verdict"] for r in rows])
            self.assertAlmostEqual(10 / 110, rows[0]["threshold"], places=6)

    def test_compare_same_directory_is_noise_and_pairs_variants(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, "smoke")
            write(d, "Y-smoke-1.json", report("Y", "m", {"B": [10], "Boff": [20], "A": [5]}))
            write(d, "machine-smoke-1.json", {"git": {"rev": "abc"}})
            rows = cbc.compare(d, d, None, None, min_samples=1)
            self.assertEqual(3, len(rows))
            self.assertEqual({"NOISE"}, {r["verdict"] for r in rows})
            # --pair Boff:B reads the off side from A's Boff and the on side from B's B
            paired = cbc.compare(d, d, None, ("Boff", "B"), min_samples=1)
            self.assertEqual(1, len(paired))
            self.assertEqual("WIN", paired[0]["verdict"])
            # with the protocol's minimum of 5 samples, one sample per side is no verdict
            self.assertEqual({"INVALID"}, {r["verdict"] for r in cbc.compare(d, d, None, None)})
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                cbc.print_rows(rows, True)
            self.assertIn("NOISE", buffer.getvalue())
            self.assertIn("WIN iff", buffer.getvalue())
            self.assertNotIn("WARNING", buffer.getvalue())

    def test_main_warns_without_noise_floor_and_fails_on_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, "runs")
            write(d, "Z-r-1.json", report("Z", "m", {"on": [10] * 5, "off": [10] * 5}))
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                status = cbc.main(["compare", d, d])
            self.assertEqual(0, status)
            self.assertIn("WARNING: no noise floor applied", buffer.getvalue())

            floor = write(tmp, "noise-floor.json", {"Z": {"m": 0.01}})
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                status = cbc.main(["compare", d, d, "--noise", floor])
            self.assertEqual(0, status)
            self.assertNotIn("WARNING", buffer.getvalue())

            small = os.path.join(tmp, "small")
            write(small, "Z-s-1.json", report("Z", "m", {"on": [10], "off": [12]}))
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                status = cbc.main(["compare", small, small, "--noise", floor])
            self.assertNotEqual(0, status)
            self.assertIn("INVALID", buffer.getvalue())
            with redirect_stdout(io.StringIO()):
                self.assertEqual(0, cbc.main(["compare", small, small, "--noise", floor, "--min-samples", "1"]))


class RankTest(unittest.TestCase):

    def test_rank_sums_samples_by_category(self):
        with tempfile.TemporaryDirectory() as tmp:
            collapsed = os.path.join(tmp, "cpu.collapsed")
            with open(collapsed, "w", encoding="utf-8") as f:
                f.write("[ArcadeDB-ColumnarApply-1 tid=1];Pass.run;BinarySerializer.deserialize 30\n")
                f.write("[ArcadeDB-ColumnarApply-2 tid=2];Pass.run;GlobalDictionary.ordinal 20\n")
                f.write("[ArcadeDB-ColumnarApply-1 tid=1];Pass.run;BinarySerializer.readString 10\n")
                f.write("[main tid=3];Test.run;DirtyPageTracker.markPages 15\n")
                f.write("[main tid=3];Test.run;LocalBucket.createRecord 20\n")
                f.write("[GC Thread#0 tid=9];G1.collect 5\n")
            categories = os.path.join(tmp, "categories.tsv")
            with open(categories, "w", encoding="utf-8") as f:
                f.write("# thread regex\tframe regex\tcategory\n")
                f.write("ColumnarApply\tnewImmutableRecord|BinarySerializer\tapply.decode\n")
                f.write("ColumnarApply\tGlobalDictionary\tapply.dictionary\n")
                f.write("main\tmarkPages|DirtyPageTracker\tcommit.marks\n")
                f.write("main\t.*\tcommit.other\n")
            ranking = cbc.rank(collapsed, categories)
            # first matching row wins; ties ordered by name
            self.assertEqual([("apply.decode", 40), ("apply.dictionary", 20), ("commit.other", 20), ("commit.marks", 15),
                              ("other", 5)], ranking)


if __name__ == "__main__":
    unittest.main()
