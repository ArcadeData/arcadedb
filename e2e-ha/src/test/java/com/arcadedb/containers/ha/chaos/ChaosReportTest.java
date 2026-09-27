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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosReportTest {
  @TempDir
  Path dir;

  private final ChaosConfig config = ChaosConfig.fromProperties(ChaosConfigTest.props("chaos.seed", "42"));

  @Test
  void stepsAndTrendsAreFlushedAsTheyAreWritten() throws Exception {
    try (final ChaosReport report = new ChaosReport(dir, config)) {
      report.step(new StepRecord(1, "pause", "LEADER [0]", 0, 1, 1200, 300, 55, 1000, 3, 1));
      report.step(new StepRecord(2, "kill", "FOLLOWER [2]", 1, 1, 900, 250, 80, 1100, 3, 2));
      report.trend(new TrendRow(2, new long[] { 1, 2, 3 }, new long[] { 4, 5, 6 }, new long[] { 7, 8, 9 }, 12.5, 400));

      final List<String> steps = Files.readAllLines(dir.resolve("steps.log"));
      assertThat(steps).hasSize(2);
      assertThat(steps.get(1)).contains("step=2").contains("fault=kill").contains("targets=FOLLOWER [2]");

      final List<String> trends = Files.readAllLines(dir.resolve("trends.csv"));
      assertThat(trends).hasSize(4);
      assertThat(trends.getFirst()).isEqualTo(TrendRow.CSV_HEADER);
      assertThat(trends.get(3)).isEqualTo("2,2,3,6,9,12.50,400");
    }
  }

  @Test
  void summaryCarriesResultCountsAndReplay() throws Exception {
    final Ledger ledger = new Ledger(1);
    ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    try (final ChaosReport report = new ChaosReport(dir, config)) {
      report.summary(new ChaosResult(ResultKind.SAFETY, "I1 (SAFETY): 1 acknowledged writes are missing", 3, List.of()),
          ledger, 2);
    }
    final JSONObject summary = new JSONObject(Files.readString(dir.resolve("summary.json")));
    assertThat(summary.getString("result")).isEqualTo("SAFETY");
    assertThat(summary.getLong("seed")).isEqualTo(42L);
    assertThat(summary.getInt("steps")).isEqualTo(3);
    assertThat(summary.getJSONObject("ledger").getLong("acked")).isEqualTo(1L);
    assertThat(summary.getLong("lateCommits")).isEqualTo(2L);
    assertThat(summary.getString("replay")).contains("-Dchaos.seed=42");
  }

  @Test
  void ledgerDiffPrefersPerKeyDetails() throws Exception {
    try (final ChaosReport report = new ChaosReport(dir, config)) {
      report.ledgerDiff(List.of(new Violation(ResultKind.SAFETY, "CONVERGENCE", "31 keys differ",
          new long[] { Ledger.key(0, 3) }, List.of("w0-3 outcome=ACKED pair=false present=[1, 2] missing=[0] withEdge=[]"))));
    }
    final String diff = Files.readString(dir.resolve("ledger-diff.txt"));
    assertThat(diff).contains("== CONVERGENCE (SAFETY): 31 keys differ")
        .contains("w0-3 outcome=ACKED pair=false present=[1, 2] missing=[0] withEdge=[]");
  }

  @Test
  void ledgerDiffListsKeysPerViolation() throws Exception {
    try (final ChaosReport report = new ChaosReport(dir, config)) {
      report.ledgerDiff(List.of(new Violation(ResultKind.SAFETY, "I1", "1 acknowledged writes are missing",
          new long[] { Ledger.key(0, 3) })));
    }
    final String diff = Files.readString(dir.resolve("ledger-diff.txt"));
    assertThat(diff).contains("== I1 (SAFETY): 1 acknowledged writes are missing").contains("w0-3");
  }
}
