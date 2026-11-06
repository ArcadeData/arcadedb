/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.columnar;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** The benchmark report: statistics, the rotating on/off pairs and the JSON it writes. No timing is asserted. */
class BenchmarkReportTest {
  private static final String LABEL = "columnar.bench.label";
  private static final String OUT   = "columnar.bench.out";

  @TempDir
  Path dir;

  private String label;
  private String out;

  @BeforeEach
  void saveProperties() {
    label = System.getProperty(LABEL);
    out = System.getProperty(OUT);
  }

  @AfterEach
  void restoreProperties() {
    restore(LABEL, label);
    restore(OUT, out);
  }

  @Test
  void medianAndSpreadOfFiveSamples() {
    final double[] samples = { 10, 12, 11, 30, 9 };
    assertThat(BenchmarkReport.median(samples)).isEqualTo(11d);
    assertThat(BenchmarkReport.spread(samples)).isCloseTo(21d / 11d, within(1e-12));
    // the input is not reordered
    assertThat(samples).containsExactly(10, 12, 11, 30, 9);
    // an even count: the mean of the middle two
    assertThat(BenchmarkReport.median(new double[] { 4, 1, 3, 2 })).isEqualTo(2.5d);
  }

  @Test
  void writesNothingWithoutLabel() {
    System.clearProperty(LABEL);
    System.setProperty(OUT, dir.toString());
    final BenchmarkReport report = new BenchmarkReport("unit");
    report.add("m", "ms", "on", 1);
    assertThat(report.write()).isNull();
    assertThat(dir.toFile().list()).isEmpty();
  }

  @Test
  void jsonCarriesMachineStateAndEveryVariant() throws Exception {
    System.setProperty(LABEL, "smoke");
    System.setProperty(OUT, dir.resolve("reports").toString());
    final BenchmarkReport report = new BenchmarkReport("UnitBenchmark");
    for (int i = 0; i < 5; i++) {
      report.add("m", "ms", "on", 10 + i);
      report.add("m", "ms", "off", 20 + i);
    }
    report.add("bytes", "MB", "B'", 3.5);

    final Path written = report.write();
    assertThat(written).isNotNull();
    assertThat(written.getParent()).isEqualTo(dir.resolve("reports"));
    assertThat(written.getFileName().toString()).matches("UnitBenchmark-smoke-\\d+\\.json");

    final JSONObject json = new JSONObject(Files.readString(written));
    assertThat(json.getString("benchmark")).isEqualTo("UnitBenchmark");
    assertThat(json.getString("label")).isEqualTo("smoke");
    assertThat(json.has("lever")).as("no tuning lever is left to name").isFalse();

    final JSONObject machine = json.getJSONObject("machine");
    assertThat(machine.getInt("cores")).isEqualTo(Runtime.getRuntime().availableProcessors());
    assertThat(machine.has("loadAverage")).isTrue();
    for (final String key : new String[] { "os", "arch", "java.version", "maxMemory", "columnar.bench.rev", "columnar.bench.dirty",
        "startedAt" })
      assertThat(machine.has(key)).as(key).isTrue();

    final JSONObject m = json.getJSONObject("metrics").getJSONObject("m");
    assertThat(m.getString("unit")).isEqualTo("ms");
    final JSONArray on = m.getJSONObject("variants").getJSONArray("on");
    final JSONArray off = m.getJSONObject("variants").getJSONArray("off");
    assertThat(on.length()).isEqualTo(5);
    assertThat(off.length()).isEqualTo(5);
    assertThat(on.getDouble(0)).isEqualTo(10d);
    assertThat(off.getDouble(4)).isEqualTo(24d);
    final JSONObject bytes = json.getJSONObject("metrics").getJSONObject("bytes");
    assertThat(bytes.getString("unit")).isEqualTo("MB");
    assertThat(bytes.getJSONObject("variants").getJSONArray("B'").getDouble(0)).isEqualTo(3.5d);
  }

  @Test
  void pairsRotateTheOrderOfTheVariants() {
    final BenchmarkReport report = new BenchmarkReport("UnitBenchmark");
    final List<String> order = new ArrayList<>();
    report.pairs(2, 5, recorder -> {
      order.add(recorder.variant());
      recorder.nanos("m", order.size() * 1_000_000L);
    });
    // 2 warm-up rounds and 5 measured ones, each an on/off pair whose order rotates
    assertThat(order).containsExactly("on", "off", "off", "on", "on", "off", "off", "on", "on", "off", "off", "on", "on", "off");

    final JSONObject json = report.toJSON();
    final JSONObject metric = json.getJSONObject("metrics").getJSONObject("m");
    assertThat(metric.getString("unit")).isEqualTo("ms");
    // the warm-ups (calls 1 to 4) are not recorded; call n took n ms
    assertThat(metric.getJSONObject("variants").getJSONArray("on").toListOfDoubles()).containsExactly(5d, 8d, 9d, 12d, 13d);
    assertThat(metric.getJSONObject("variants").getJSONArray("off").toListOfDoubles()).containsExactly(6d, 7d, 10d, 11d, 14d);

    // the single-metric form records the same way
    final long[] calls = new long[1];
    report.pairs(0, 1, "n", () -> ++calls[0] * 1_000_000L);
    assertThat(report.samples("n", BenchmarkReport.ON)).containsExactly(1);
    assertThat(report.samples("n", BenchmarkReport.OFF)).containsExactly(2);
  }

  @Test
  void pairsRecordEveryMetricOfARoundUnderItsVariant() {
    final BenchmarkReport report = new BenchmarkReport("UnitBenchmark");
    final int[] call = new int[1];
    // one round times several queries: each one separately and their sum
    report.pairs(1, 2, recorder -> {
      call[0]++;
      recorder.nanos("q.brand", call[0] * 1_000_000L);
      recorder.nanos("q.tags", 10 * call[0] * 1_000_000L);
      recorder.nanos("q", 11 * call[0] * 1_000_000L);
    });
    assertThat(call[0]).isEqualTo(6);
    // calls 1 and 2 are the warm-up pair (on, off); then off (3), on (4), on (5), off (6)
    assertThat(report.samples("q.brand", BenchmarkReport.ON)).containsExactly(4, 5);
    assertThat(report.samples("q.brand", BenchmarkReport.OFF)).containsExactly(3, 6);
    assertThat(report.samples("q.tags", BenchmarkReport.ON)).containsExactly(40, 50);
    assertThat(report.samples("q", BenchmarkReport.OFF)).containsExactly(33, 66);
  }

  private static void restore(final String key, final String value) {
    if (value == null)
      System.clearProperty(key);
    else
      System.setProperty(key, value);
  }
}
