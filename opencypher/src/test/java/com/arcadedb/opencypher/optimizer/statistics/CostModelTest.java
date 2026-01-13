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
package com.arcadedb.opencypher.optimizer.statistics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CostModel class.
 */
class CostModelTest {
  private CostModel costModel;
  private MockStatisticsProvider statistics;

  @BeforeEach
  void setUp() {
    statistics = new MockStatisticsProvider();
    costModel = new CostModel(statistics);
  }

  @Test
  void testEstimateScanCost() {
    statistics.setCardinality("Person", 10000);

    final double scanCost = costModel.estimateScanCost("Person");
    assertThat(scanCost).isEqualTo(10000.0);
  }

  @Test
  void testEstimateIndexSeekCost() {
    statistics.setCardinality("Person", 10000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("email"), true, "Person.email");
    statistics.addIndex(index);

    final double indexCost = costModel.estimateIndexSeekCost("Person", "email", 0.001);

    // Cost = INDEX_SEEK_COST + (estimated_rows * INDEX_LOOKUP_COST_PER_ROW)
    // Cost = 5.0 + (10000 * 0.001 * 0.1) = 5.0 + 1.0 = 6.0
    assertThat(indexCost).isEqualTo(6.0);
  }

  @Test
  void testEstimateFilterCost() {
    final double filterCost = costModel.estimateFilterCost(1000);
    assertThat(filterCost).isEqualTo(500.0); // 1000 * 0.5
  }

  @Test
  void testEstimateExpandCost() {
    final double expandCost = costModel.estimateExpandCost(100, 10.0);
    assertThat(expandCost).isEqualTo(2000.0); // 100 * 10 * 2.0
  }

  @Test
  void testEstimateExpandIntoCost() {
    final double expandIntoCost = costModel.estimateExpandIntoCost(100);
    assertThat(expandIntoCost).isEqualTo(100.0); // Much cheaper than ExpandAll
  }

  @Test
  void testEstimateHashJoinCost() {
    final double joinCost = costModel.estimateHashJoinCost(1000, 500);

    // Build from smaller (500), probe from larger (1000)
    // Cost = (500 * 1.0) + (1000 * 0.5) = 500 + 500 = 1000
    assertThat(joinCost).isEqualTo(1000.0);
  }

  @Test
  void testEstimateFilterCardinality() {
    final long outputCardinality = costModel.estimateFilterCardinality(1000, 0.1);
    assertThat(outputCardinality).isEqualTo(100);
  }

  @Test
  void testEstimateEqualitySelectivityWithUniqueIndex() {
    statistics.setCardinality("Person", 10000);
    final IndexStatistics uniqueIndex = new IndexStatistics("Person",
        Arrays.asList("id"), true, "Person.id");
    statistics.addIndex(uniqueIndex);

    final double selectivity = costModel.estimateEqualitySelectivity("Person", "id", true);

    // Unique index: 1/N = 1/10000 = 0.0001
    assertThat(selectivity).isEqualTo(0.0001);
  }

  @Test
  void testEstimateEqualitySelectivityWithNonUniqueIndex() {
    statistics.setCardinality("Person", 10000);
    final IndexStatistics nonUniqueIndex = new IndexStatistics("Person",
        Arrays.asList("name"), false, "Person.name");
    statistics.addIndex(nonUniqueIndex);

    final double selectivity = costModel.estimateEqualitySelectivity("Person", "name", true);

    // Non-unique index: default equality selectivity
    assertThat(selectivity).isEqualTo(CostModel.SELECTIVITY_EQUALITY);
  }

  @Test
  void testEstimateEqualitySelectivityWithoutIndex() {
    final double selectivity = costModel.estimateEqualitySelectivity("Person", "age", false);
    assertThat(selectivity).isEqualTo(CostModel.SELECTIVITY_EQUALITY);
  }

  @Test
  void testEstimateRangeSelectivity() {
    final double selectivity = costModel.estimateRangeSelectivity();
    assertThat(selectivity).isEqualTo(CostModel.SELECTIVITY_RANGE);
  }

  @Test
  void testCombineSelectivitiesAnd() {
    final double combined = costModel.combineSelectivitiesAnd(0.1, 0.5, 0.2);
    assertThat(combined).isCloseTo(0.01, org.assertj.core.data.Offset.offset(0.0001)); // 0.1 * 0.5 * 0.2
  }

  @Test
  void testCombineSelectivitiesOr() {
    final double combined = costModel.combineSelectivitiesOr(0.1, 0.2);

    // 1 - (1 - 0.1) * (1 - 0.2) = 1 - (0.9 * 0.8) = 1 - 0.72 = 0.28
    assertThat(combined).isCloseTo(0.28, org.assertj.core.data.Offset.offset(0.0001));
  }

  @Test
  void testShouldUseIndexWhenSelective() {
    statistics.setCardinality("Person", 100000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("email"), false, "Person.email");
    statistics.addIndex(index);

    // Selectivity = 0.05 (5%), below threshold of 10%
    // Scan cost = 100000, Index cost = 5 + (5000 * 0.1) = 505
    final boolean shouldUse = costModel.shouldUseIndex("Person", "email", 0.05);
    assertThat(shouldUse).isTrue();
  }

  @Test
  void testShouldNotUseIndexWhenNotSelective() {
    statistics.setCardinality("Person", 100000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("country"), false, "Person.country");
    statistics.addIndex(index);

    // Selectivity = 0.5 (50%), above threshold of 10%
    final boolean shouldUse = costModel.shouldUseIndex("Person", "country", 0.5);
    assertThat(shouldUse).isFalse();
  }

  @Test
  void testEstimateAverageDegree() {
    final double avgDegree = costModel.estimateAverageDegree("KNOWS");
    assertThat(avgDegree).isEqualTo(CostModel.DEFAULT_AVG_DEGREE);
  }

  /**
   * Mock statistics provider for testing.
   */
  private static class MockStatisticsProvider extends StatisticsProvider {
    private final Map<String, Long> cardinalities = new HashMap<>();
    private final Map<String, List<IndexStatistics>> indexes = new HashMap<>();

    public MockStatisticsProvider() {
      super(null); // No real database needed for tests
    }

    public void setCardinality(final String typeName, final long cardinality) {
      cardinalities.put(typeName, cardinality);
    }

    public void addIndex(final IndexStatistics index) {
      indexes.computeIfAbsent(index.getTypeName(), k -> new java.util.ArrayList<>())
             .add(index);
    }

    @Override
    public long getCardinality(final String typeName) {
      return cardinalities.getOrDefault(typeName, 0L);
    }

    @Override
    public IndexStatistics findIndexForProperty(final String typeName, final String propertyName) {
      final List<IndexStatistics> typeIndexes = indexes.get(typeName);
      if (typeIndexes == null) {
        return null;
      }
      return typeIndexes.stream()
          .filter(idx -> idx.canBeUsedForProperty(propertyName))
          .findFirst()
          .orElse(null);
    }
  }
}
