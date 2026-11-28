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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.workload.mixed.MixedPhase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class MixedWorkloadTest {
  private static final List<String> METRICS = List.of("ingest_p99_ms", "search_p99_ms", "compaction_p99_ms", "ddl_p99_ms",
      "timeseries_p99_ms", "vector_churn_p99_ms", "max_commit_gap_ms", "ops_per_s", "ingest_ops", "search_ops", "compaction_ops", "ddl_ops",
      "timeseries_ops", "vector_churn_ops");

  @Test
  void listsOneCase() {
    final MixedWorkload workload = new MixedWorkload();
    assertThat(workload.name()).isEqualTo("mixed");
    assertThat(workload.cases()).isEqualTo(List.of("mixed-phase"));
    assertThat(MixedWorkload.DATABASE).isEqualTo("mixed");
  }

  @Test
  void declaresThreeRepsAndOneWarmUp() {
    final MixedWorkload workload = new MixedWorkload();
    assertThat(workload.reps()).hasValue(3);
    assertThat(workload.warmup()).hasValue(1);
  }

  @Test
  void sizesPerScale() {
    assertThat(MixedWorkload.baseDocs(Scale.XS)).isEqualTo(5_000);
    assertThat(MixedWorkload.baseDocs(Scale.S)).isEqualTo(50_000);
    assertThat(MixedWorkload.baseDocs(Scale.M)).isEqualTo(200_000);
    assertThat(MixedWorkload.baseDocs(Scale.L)).isEqualTo(1_000_000);
    assertThat(MixedWorkload.window(Scale.XS)).isEqualTo(Duration.ofSeconds(10));
    assertThat(MixedWorkload.window(Scale.S)).isEqualTo(Duration.ofSeconds(90));
    assertThat(MixedWorkload.window(Scale.M)).isEqualTo(Duration.ofSeconds(90));
    assertThat(MixedWorkload.window(Scale.L)).isEqualTo(Duration.ofSeconds(120));
    assertThat(MixedPhase.joinBound(Scale.XS)).isEqualTo(Duration.ofSeconds(60));
    assertThat(MixedPhase.joinBound(Scale.S)).isEqualTo(Duration.ofMinutes(3));
    assertThat(MixedPhase.joinBound(Scale.M)).isEqualTo(Duration.ofMinutes(5));
    assertThat(MixedPhase.joinBound(Scale.L)).isEqualTo(Duration.ofMinutes(5));
    // the worst case of one step (one warm-up or one rep) must stay under the 30 min PER-STEP limit (not the whole-case cap):
    // the window, the join bound twice (end by itself, then unwind after the interrupt) and the check wait
    for (final Scale scale : Scale.values()) {
      final Duration worst = MixedWorkload.window(scale).plus(MixedPhase.joinBound(scale).multipliedBy(2)).plus(MixedPhase.CHECK_WAIT);
      assertThat(worst).as(scale.name()).isLessThan(Duration.ofMinutes(30));
    }
  }

  @Test
  void publishesOneP99PerOperationAndTheLongestGap() throws Exception {
    final Map<String, Metric> metrics = byName(MixedWorkload.publish(result(List.of())).perRep());
    assertThat(metrics.keySet()).containsExactlyElementsOf(METRICS);
    for (final String name : METRICS.subList(0, 7)) {
      assertThat(metrics.get(name).unit()).as(name).isEqualTo("ms");
      assertThat(metrics.get(name).better()).as(name).isEqualTo(Metric.Better.LOWER);
    }
    // ingest recorded 1..100 ms: the p99 is 99 ms
    assertThat(metrics.get("ingest_p99_ms").median()).isEqualTo(99.0);
    assertThat(metrics.get("max_commit_gap_ms").median()).isEqualTo(1_500.0);
    // 600 operations over 10 s
    assertThat(metrics.get("ops_per_s").median()).isEqualTo(60.0);
    assertThat(metrics.get("ops_per_s").better()).isEqualTo(Metric.Better.HIGHER);
    // every loop recorded 100 operations; the counts are published so the 1,000 operations rule can be verified
    for (final String loop : MixedPhase.LOOPS) {
      assertThat(metrics.get(loop + "_ops").median()).as(loop).isEqualTo(100.0);
      assertThat(metrics.get(loop + "_ops").unit()).as(loop).isEqualTo("operations");
    }
  }

  @Test
  void aFailedLoopFailsTheCaseWithItsMetrics() {
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> MixedWorkload.publish(result(List.of("ingest: boom"))));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("ingest: boom");
    assertThat(byName(e.metrics()).keySet()).containsExactlyElementsOf(METRICS);
  }

  private static MixedPhase.Result result(final List<String> failures) {
    final Map<String, LatencyRecorder> latencies = new LinkedHashMap<>();
    for (final String loop : MixedPhase.LOOPS) {
      final LatencyRecorder recorder = new LatencyRecorder(100);
      for (int i = 1; i <= 100; i++)
        recorder.record(i * 1_000_000L);
      latencies.put(loop, recorder);
    }
    return new MixedPhase.Result(latencies, 1_500_000_000L, 600, 10.0, failures);
  }

  private static Map<String, Metric> byName(final List<Metric> metrics) {
    final Map<String, Metric> map = new LinkedHashMap<>();
    for (final Metric m : metrics)
      map.put(m.name(), m);
    return map;
  }
}
