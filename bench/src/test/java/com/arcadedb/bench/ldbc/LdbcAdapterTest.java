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
package com.arcadedb.bench.ldbc;

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.model.RunInfo;
import com.arcadedb.bench.model.RunResult;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class LdbcAdapterTest {
  private static JSONObject fixture() throws IOException {
    try (final InputStream in = LdbcAdapterTest.class.getResourceAsStream("/mode1-summary.json")) {
      return new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private static RunInfo info() {
    final Instant t = Instant.parse("2026-11-17T02:00:00Z");
    return new RunInfo("r-ldbc", t, t, "nightly", "ldbc",
        new JSONObject().put("commit", "abc").put("variant", "jvm"), new JSONObject(), new JSONObject());
  }

  private static CaseResult find(final RunResult r, final String name) {
    return r.results().stream().filter(c -> c.caseName().equals(name)).findFirst().orElseThrow();
  }

  @Test
  void convertsEveryAlgorithm() throws IOException {
    final RunResult r = LdbcAdapter.convert(fixture(), info(), "datagen-7_5-fb");
    assertThat(r.results()).hasSize(12);
    final CaseResult bfs = find(r, "olap-bfs");
    assertThat(bfs.workload()).isEqualTo("graph-analytics");
    assertThat(bfs.scale()).isEqualTo("datagen-7_5-fb");
    assertThat(bfs.reps()).isEqualTo(1);
    assertThat(bfs.warmup()).isZero();
    assertThat(bfs.status()).isEqualTo(CaseResult.Status.OK);
    assertThat(bfs.metrics()).hasSize(1);
    assertThat(bfs.metrics().getFirst().name()).isEqualTo("processing_s");
    assertThat(bfs.metrics().getFirst().unit()).isEqualTo("s");
    assertThat(bfs.metrics().getFirst().better()).isEqualTo(Metric.Better.LOWER);
    assertThat(bfs.metrics().getFirst().median()).isEqualTo(1.84);
    assertThat(bfs.metrics().getFirst().values()).containsExactly(1.84);
    assertThat(find(r, "oltp-sssp").metrics().getFirst().median()).isEqualTo(11.3);
  }

  @Test
  void failedAlgorithmIsFailedCase() throws IOException {
    final CaseResult lcc = find(LdbcAdapter.convert(fixture(), info(), "d"), "olap-lcc");
    assertThat(lcc.status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(lcc.error()).isNotNull();
  }

  @Test
  void sourceIsLdbcAndVariantJvm() throws IOException {
    final RunResult r = LdbcAdapter.convert(fixture(), info(), "d");
    assertThat(r.info().source()).isEqualTo("ldbc");
    assertThat(r.info().arcadedb().getString("variant")).isEqualTo("jvm");
  }

  @Test
  void emptyModeBecomesFailedRunCase() {
    final JSONObject summary = new JSONObject("{\"olap\":{\"outcome\":\"<Outcome.FAILURE>\",\"algorithms\":{}},\"oltp\":{\"outcome\":\"boom\"}}");
    final RunResult r = LdbcAdapter.convert(summary, info(), "d");
    assertThat(r.results()).extracting(CaseResult::caseName).containsExactlyInAnyOrder("olap-_run", "oltp-_run");
    assertThat(find(r, "olap-_run").status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(find(r, "olap-_run").error()).isEqualTo("<Outcome.FAILURE>");
    assertThat(find(r, "oltp-_run").error()).isEqualTo("boom");
  }
}
