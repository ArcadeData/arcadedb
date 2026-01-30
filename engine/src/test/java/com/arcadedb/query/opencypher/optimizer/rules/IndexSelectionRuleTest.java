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
package com.arcadedb.query.opencypher.optimizer.rules;

import com.arcadedb.query.opencypher.executor.operators.NodeByLabelScan;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexSeek;
import com.arcadedb.query.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.query.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalNode;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.query.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.query.opencypher.optimizer.statistics.IndexStatistics;
import com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for IndexSelectionRule class.
 */
class IndexSelectionRuleTest {
  private IndexSelectionRule rule;
  private MockStatisticsProvider statistics;
  private CostModel costModel;

  @BeforeEach
  void setUp() {
    statistics = new MockStatisticsProvider();
    costModel = new CostModel(statistics);
    rule = new IndexSelectionRule(statistics, costModel);
  }

  @Test
  void getRuleName() {
    assertThat(rule.getRuleName()).isEqualTo("IndexSelection");
  }

  @Test
  void getPriority() {
    assertThat(rule.getPriority()).isEqualTo(10); // Highest priority
  }

  @Test
  void isApplicableWithNodes() {
    final LogicalNode node = new LogicalNode("p", Arrays.asList("Person"),
        Collections.singletonMap("id", 123));
    final LogicalPlan plan = LogicalPlan.forTesting(Collections.singletonMap("p", node));

    assertThat(rule.isApplicable(plan)).isTrue();
  }

  @Test
  void isNotApplicableWithoutNodes() {
    final LogicalPlan emptyPlan = LogicalPlan.forTesting(Collections.emptyMap());

    assertThat(rule.isApplicable(emptyPlan)).isFalse();
  }

  @Test
  void createAnchorOperatorWithIndexSeek() {
    // Setup: Anchor with index
    statistics.setCardinality("Person", 10000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("id"), true, "Person.id");
    statistics.addIndex(index);

    final LogicalNode node = new LogicalNode("p", Arrays.asList("Person"),
        Collections.singletonMap("id", 123));
    // Use new constructor that includes propertyValue
    final AnchorSelection anchor = new AnchorSelection("p", node, true, index, "id", 123, 5.1, 1);

    // When: Create operator
    final PhysicalOperator operator = rule.createAnchorOperator(anchor);

    // Then: Should create NodeIndexSeek
    assertThat(operator).isInstanceOf(NodeIndexSeek.class);
    final NodeIndexSeek indexSeek = (NodeIndexSeek) operator;
    assertThat(indexSeek.getVariable()).isEqualTo("p");
    assertThat(indexSeek.getLabel()).isEqualTo("Person");
    assertThat(indexSeek.getPropertyName()).isEqualTo("id");
    assertThat(indexSeek.getPropertyValue()).isEqualTo(123);
  }

  @Test
  void createAnchorOperatorWithLabelScan() {
    // Setup: Anchor without index
    statistics.setCardinality("Person", 10000);

    final LogicalNode node = new LogicalNode("p", Arrays.asList("Person"),
        Collections.singletonMap("name", "Alice"));
    final AnchorSelection anchor = new AnchorSelection("p", node, false, null, "name", 1000.0, 1000);

    // When: Create operator
    final PhysicalOperator operator = rule.createAnchorOperator(anchor);

    // Then: Should create NodeByLabelScan
    assertThat(operator).isInstanceOf(NodeByLabelScan.class);
    final NodeByLabelScan scan = (NodeByLabelScan) operator;
    assertThat(scan.getVariable()).isEqualTo("p");
    assertThat(scan.getLabel()).isEqualTo("Person");
  }

  @Test
  void shouldUseIndexWhenSelective() {
    statistics.setCardinality("Person", 100000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("email"), true, "Person.email");
    statistics.addIndex(index);

    // Selectivity = 0.05 (5%), below threshold of 10%
    // Scan cost = 100000, Index cost = 5 + (5000 * 0.1) = 505
    final boolean shouldUse = rule.shouldUseIndex("Person", 100000, 0.05);

    assertThat(shouldUse).isTrue();
  }

  @Test
  void shouldNotUseIndexWhenNotSelective() {
    statistics.setCardinality("Person", 100000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("country"), false, "Person.country");
    statistics.addIndex(index);

    // Selectivity = 0.5 (50%), above threshold of 10%
    final boolean shouldUse = rule.shouldUseIndex("Person", 100000, 0.5);

    assertThat(shouldUse).isFalse();
  }

  @Test
  void shouldUseIndexAtThreshold() {
    statistics.setCardinality("Person", 100000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("category"), false, "Person.category");
    statistics.addIndex(index);

    // Selectivity = 0.09 (9%), just below threshold of 10%
    final boolean shouldUse = rule.shouldUseIndex("Person", 100000, 0.09);

    assertThat(shouldUse).isTrue();
  }

  @Test
  void shouldNotUseIndexJustAboveThreshold() {
    statistics.setCardinality("Person", 100000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("status"), false, "Person.status");
    statistics.addIndex(index);

    // Selectivity = 0.11 (11%), just above threshold of 10%
    final boolean shouldUse = rule.shouldUseIndex("Person", 100000, 0.11);

    assertThat(shouldUse).isFalse();
  }

  @Test
  void shouldUseIndexForUniqueConstraint() {
    statistics.setCardinality("Person", 1000000);
    final IndexStatistics uniqueIndex = new IndexStatistics("Person",
        Arrays.asList("ssn"), true, "Person.ssn");
    statistics.addIndex(uniqueIndex);

    // Selectivity = 0.0001 (0.01%), very selective
    final boolean shouldUse = rule.shouldUseIndex("Person", 1000000, 0.0001);

    assertThat(shouldUse).isTrue();
  }

  /**
   * Mock statistics provider for testing.
   */
  private static class MockStatisticsProvider extends StatisticsProvider {
    private final Map<String, Long> cardinalities = new HashMap<>();
    private final Map<String, List<IndexStatistics>> indexes = new HashMap<>();

    public MockStatisticsProvider() {
      super(null);
    }

    public void setCardinality(final String typeName, final long cardinality) {
      cardinalities.put(typeName, cardinality);
    }

    public void addIndex(final IndexStatistics index) {
      indexes.computeIfAbsent(index.getTypeName(), k -> new ArrayList<>())
             .add(index);
    }

    @Override
    public long getCardinality(final String typeName) {
      return cardinalities.getOrDefault(typeName, 0L);
    }
  }
}
