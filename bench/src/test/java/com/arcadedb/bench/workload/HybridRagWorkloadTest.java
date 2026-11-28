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
import com.arcadedb.bench.workload.rag.RagDataset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class HybridRagWorkloadTest {
  private static final List<String> CASES = List.of("vector-k100-filtered", "fulltext-filtered", "hop2-filtered", "rag-pipeline");

  @Test
  void listsFourCases() {
    final HybridRagWorkload workload = new HybridRagWorkload();
    assertThat(workload.name()).isEqualTo("hybrid-rag");
    assertThat(workload.cases()).isEqualTo(CASES);
    assertThat(HybridRagWorkload.DATABASE).isEqualTo("hybrid_rag");
  }

  @Test
  void sizesPerScale() {
    assertThat(HybridRagWorkload.chunks(Scale.XS)).isEqualTo(5_000);
    assertThat(HybridRagWorkload.chunks(Scale.S)).isEqualTo(50_000);
    assertThat(HybridRagWorkload.chunks(Scale.M)).isEqualTo(200_000);
    assertThat(HybridRagWorkload.chunks(Scale.L)).isEqualTo(1_000_000);
    assertThat(HybridRagWorkload.queries(Scale.XS)).isEqualTo(100);
    // S sizing rule: a latency case runs at least 1,000 operations per repetition at S
    assertThat(HybridRagWorkload.queries(Scale.S)).isEqualTo(1_000);
    assertThat(HybridRagWorkload.queries(Scale.M)).isEqualTo(1_000);
    assertThat(HybridRagWorkload.queries(Scale.L)).isEqualTo(1_000);
  }

  @Test
  void aRestrictedRowInAnyCaseFailsIt() {
    for (final String caseName : CASES) {
      final IllegalStateException e = catchThrowableOfType(IllegalStateException.class,
          () -> HybridRagWorkload.checkVisible(caseName, 3, List.of(new HybridRagWorkload.Row(7, 3, "restricted"))));
      assertThat(e).as(caseName).isNotNull();
      assertThat(e.getMessage()).contains(caseName).contains("7");
    }
  }

  @Test
  void anotherTenantsPrivateRowFailsTheCase() {
    for (final String caseName : CASES) {
      final IllegalStateException e = catchThrowableOfType(IllegalStateException.class,
          () -> HybridRagWorkload.checkVisible(caseName, 3,
              List.of(new HybridRagWorkload.Row(1, 3, "public"), new HybridRagWorkload.Row(8, 4, "private"))));
      assertThat(e).as(caseName).isNotNull();
      assertThat(e.getMessage()).contains(caseName).contains("8");
    }
  }

  @Test
  void visibleRowsPass() {
    for (final String caseName : CASES)
      assertThatCode(() -> HybridRagWorkload.checkVisible(caseName, 3,
          List.of(new HybridRagWorkload.Row(1, 9, "public"), new HybridRagWorkload.Row(2, 3, "private")))).doesNotThrowAnyException();
  }

  @Test
  void rrfFusionPrefersIdsOnBothLists() {
    final int[] fused = HybridRagWorkload.fuse(new int[] { 1, 2, 3 }, new int[] { 3, 4 }, 10);
    assertThat(fused[0]).isEqualTo(3);
    assertThat(fused).containsExactlyInAnyOrder(1, 2, 3, 4);
    // 3: 1/63 + 1/61; then 1 (1/61, first of a list) before 2 and 4 (1/62 each); the top n are kept
    assertThat(HybridRagWorkload.fuse(new int[] { 1, 2, 3 }, new int[] { 3, 4 }, 2)).containsExactly(3, 1);
  }

  @Test
  void recallBelowTheFloorFailsWithMetrics() {
    final LatencyRecorder latencies = new LatencyRecorder(4);
    for (int i = 1; i <= 4; i++)
      latencies.record(i * 1_000_000L);
    final double recall = HybridRagWorkload.MIN_RECALL_K100 - 0.01;
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> HybridRagWorkload.publish("vector-k100-filtered", 4, latencies, recall));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("recall");
    final Metric metric = e.metrics().stream().filter(m -> m.name().equals("recall_at_100")).findFirst().orElse(null);
    assertThat(metric).isNotNull();
    assertThat(metric.unit()).isEqualTo("ratio");
    assertThat(metric.better()).isEqualTo(Metric.Better.HIGHER);
    assertThat(metric.median()).isEqualTo(recall);
  }

  @Test
  void recallAtTheFloorPasses() throws Exception {
    final LatencyRecorder latencies = new LatencyRecorder(1);
    latencies.record(1_000_000L);
    assertThat(HybridRagWorkload.publish("rag-pipeline", 1, latencies, HybridRagWorkload.MIN_RECALL_K100).perRep())
        .extracting(Metric::name).contains("recall_at_100", "latency_p99_ms");
  }

  @Test
  void theFullTextAndWalkCasesReportNoRecall() throws Exception {
    final LatencyRecorder latencies = new LatencyRecorder(1);
    latencies.record(1_000_000L);
    assertThat(HybridRagWorkload.publish("hop2-filtered", 1, latencies, Double.NaN).perRep()).extracting(Metric::name)
        .doesNotContain("recall_at_100").contains("queries_per_s", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms");
  }

  // the visibility check runs inside the timed loop: a leaked row on the third query fails the case with the metrics of the
  // two queries before it
  @Test
  void aLeakInTheTimedLoopFailsTheCaseWithTheMetricsSoFar() {
    final List<Integer> ran = new ArrayList<>();
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> HybridRagWorkload.measureLoop("hop2-filtered", 10, null, (query, tenant) -> {
          ran.add(query);
          return query == 2 ? List.of(new HybridRagWorkload.Row(1, tenant, "public"), new HybridRagWorkload.Row(42, tenant, "restricted"))
              : List.of(new HybridRagWorkload.Row(query, tenant, "private"));
        }));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("hop2-filtered").contains("42");
    assertThat(ran).containsExactly(0, 1, 2);
    assertThat(e.metrics()).extracting(Metric::name).contains("queries_per_s", "latency_p99_ms").doesNotContain("recall_at_100");
  }

  @Test
  void aLeakInAVectorCaseCarriesTheRecallSoFar() {
    final int[][] exact = { { 1 }, { 2 } };
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> HybridRagWorkload.measureLoop("vector-k100-filtered", 2, exact,
            (query, tenant) -> query == 0 ? List.of(new HybridRagWorkload.Row(1, 9, "public")) : List.of(new HybridRagWorkload.Row(2, 5, "private"))));
    assertThat(e).isNotNull();
    assertThat(e.metrics()).filteredOn(m -> m.name().equals("recall_at_100")).singleElement().extracting(Metric::median).isEqualTo(1.0);
  }

  // a client fault is not a leak: it propagates as itself and is not reported as a visibility failure
  @Test
  void aClientFaultIsNotReportedAsALeak() {
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class,
        () -> HybridRagWorkload.measureLoop("fulltext-filtered", 3, null, (query, tenant) -> {
          throw new IllegalStateException("connection reset");
        }));
    assertThat(e).isNotNull().isNotInstanceOf(HybridRagWorkload.VisibilityLeakException.class);
    assertThat(e.getMessage()).isEqualTo("connection reset");
  }

  @Test
  void visibleRowsMeasureEveryQuery() throws Exception {
    final int[][] exact = { { 1 }, { 2 } };
    assertThat(HybridRagWorkload.measureLoop("rag-pipeline", 2, exact,
        (query, tenant) -> List.of(new HybridRagWorkload.Row(query + 1, tenant, "private"))).perRep())
        .filteredOn(m -> m.name().equals("recall_at_100")).singleElement().extracting(Metric::median).isEqualTo(1.0);
  }

  // the walk is chunk -> mentioned entity -> related entity -> its mentioners: an even start must reach a hub (ids 0-9) at the
  // second hop, where the fan-out is
  @Test
  void everyEvenStartReachesAHubAtTheSecondHop() {
    for (final int chunks : new int[] { HybridRagWorkload.chunks(Scale.XS), HybridRagWorkload.chunks(Scale.S) }) {
      final RagDataset dataset = new RagDataset(42L, chunks);
      final int[] starts = HybridRagWorkload.starts(42L, dataset, chunks, 1_000);
      for (int i = 0; i < starts.length; i += 2)
        assertThat(reachesHub(dataset, starts[i])).as("start %d of %d chunks", starts[i], chunks).isTrue();
      // and the odd starts are not forced: some of them do not reach a hub
      int oddWithoutHub = 0;
      for (int i = 1; i < starts.length; i += 2)
        if (!reachesHub(dataset, starts[i]))
          oddWithoutHub++;
      assertThat(oddWithoutHub).as("%d chunks", chunks).isGreaterThan(0);
    }
  }

  private static boolean reachesHub(final RagDataset dataset, final int chunk) {
    for (final int m : dataset.mentions(chunk))
      for (final int r : dataset.related(m))
        if (r < 10)
          return true;
    return false;
  }
}
