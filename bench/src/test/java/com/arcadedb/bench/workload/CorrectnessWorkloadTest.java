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
import com.arcadedb.bench.runner.Workload;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CorrectnessWorkloadTest {
  @Test
  void casesAreTheThreeOfTheSpec() {
    final CorrectnessWorkload workload = new CorrectnessWorkload();
    assertThat(workload.name()).isEqualTo("correctness");
    assertThat(workload.cases()).containsExactly("vector-recall-after-churn", "index-vs-scan-count", "count-star-vs-iterated");
  }

  @Test
  void isRegistered() {
    assertThat(WorkloadRegistry.all()).extracting(Workload::name).contains("correctness");
  }

  @Test
  void equalCountsPass() {
    assertThatCode(() -> CorrectnessWorkload.checkCount("index", 10, 10)).doesNotThrowAnyException();
  }

  @Test
  void differentCountsFailNamingBoth() {
    assertThatThrownBy(() -> CorrectnessWorkload.checkCount("index", 10, 9)).isInstanceOf(IllegalStateException.class)
        .hasMessage("index: expected 10, got 9");
  }

  @Test
  void predicatesAreSelective() {
    final long[] values = ledger(CorrectnessWorkload.items(Scale.XS));
    assertThat(CorrectnessWorkload.predicates()).hasSizeGreaterThanOrEqualTo(4);
    for (final String predicate : CorrectnessWorkload.predicates()) {
      final long matches = CorrectnessWorkload.expectedMatches(predicate, values);
      assertThat(matches).as(predicate).isGreaterThan(0).isLessThan(values.length);
    }
  }

  @Test
  void predicatesCoverEqualityRangeInListAndOpenRange() {
    final List<String> predicates = CorrectnessWorkload.predicates();
    assertThat(predicates).anyMatch(p -> p.startsWith("v = "));
    assertThat(predicates).anyMatch(p -> p.contains(" BETWEEN "));
    assertThat(predicates).anyMatch(p -> p.contains(" IN ["));
    assertThat(predicates).anyMatch(p -> p.startsWith("v > "));
  }

  @Test
  void theMirrorEvaluatesEachShape() {
    final long[] values = { 1, 2, 3, 3, 10 };
    assertThat(CorrectnessWorkload.expectedMatches("v = 3", values)).isEqualTo(2);
    assertThat(CorrectnessWorkload.expectedMatches("v BETWEEN 2 AND 3", values)).isEqualTo(3);
    assertThat(CorrectnessWorkload.expectedMatches("v IN [1, 10, 99]", values)).isEqualTo(2);
    assertThat(CorrectnessWorkload.expectedMatches("v > 3", values)).isEqualTo(1);
    assertThatThrownBy(() -> CorrectnessWorkload.expectedMatches("v < 3", values)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theScanFieldReplacesTheIndexedOne() {
    assertThat(CorrectnessWorkload.on("v BETWEEN 1 AND 2", "v_copy")).isEqualTo("v_copy BETWEEN 1 AND 2");
  }

  @Test
  void recallUnderGateFails() {
    final List<Metric> metrics = List.of(Metrics.of("recall_at_10", "ratio", Metric.Better.HIGHER, 0.89));
    assertThatThrownBy(() -> CorrectnessWorkload.checkRecall(0.89, metrics)).isInstanceOfSatisfying(CaseFailedWithMetrics.class,
        e -> {
          assertThat(e.metrics()).isEqualTo(metrics);
          assertThat(e.getMessage()).contains("recall").contains("0.89");
        });
    assertThatCode(() -> CorrectnessWorkload.checkRecall(0.90, metrics)).doesNotThrowAnyException();
  }

  @Test
  void sizesGrowWithTheScaleAndSatisfyTheSRule() {
    for (final Scale scale : new Scale[] { Scale.XS, Scale.S, Scale.M }) {
      final Scale next = Scale.values()[scale.ordinal() + 1];
      assertThat(CorrectnessWorkload.items(next)).isGreaterThan(CorrectnessWorkload.items(scale));
      assertThat(CorrectnessWorkload.vectors(next)).isGreaterThan(CorrectnessWorkload.vectors(scale));
    }
    // the churn must touch rows at every scale
    assertThat(CorrectnessWorkload.items(Scale.XS) / CorrectnessWorkload.ITEM_CHURN_EVERY).isGreaterThan(0);
    assertThat(CorrectnessWorkload.vectors(Scale.XS) / CorrectnessWorkload.VECTOR_CHURN_EVERY).isGreaterThan(0);
    // S is raised to the 5 s per repetition rule: not under what phase 1 used for the other workloads
    assertThat(CorrectnessWorkload.items(Scale.S)).isGreaterThanOrEqualTo(3_000_000);
    assertThat(CorrectnessWorkload.vectors(Scale.S)).isGreaterThanOrEqualTo(80_000);
  }

  private static long[] ledger(final int n) {
    final SplittableRandom random = new SplittableRandom(7);
    final long[] values = new long[n];
    for (int i = 0; i < n; i++)
      values[i] = random.nextInt(CorrectnessWorkload.DOMAIN);
    return values;
  }
}
