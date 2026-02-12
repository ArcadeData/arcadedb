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
package com.arcadedb.query.opencypher.optimizer;

import com.arcadedb.query.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalNode;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.query.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.query.opencypher.optimizer.statistics.IndexStatistics;
import com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider;
import com.arcadedb.query.opencypher.optimizer.statistics.TypeStatistics;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for AnchorSelector class.
 */
class AnchorSelectorTest {
  private AnchorSelector anchorSelector;
  private MockStatisticsProvider statistics;
  private CostModel costModel;

  @BeforeEach
  void setUp() {
    statistics = new MockStatisticsProvider();
    costModel = new CostModel(statistics);
    anchorSelector = new AnchorSelector(statistics, costModel);
  }

  @Test
  void selectAnchorWithIndexedProperty() {
    // Setup: Person with indexed 'id' property
    statistics.setCardinality("Person", 10000);
    final IndexStatistics idIndex = new IndexStatistics("Person",
        Arrays.asList("id"), true, "Person.id");
    statistics.addIndex(idIndex);

    // Create logical plan with node having indexed property
    final LogicalPlan plan = createPlanWithNode("p", "Person",
        CollectionUtils.singletonMap("id", 123));

    // When: Select anchor
    final AnchorSelection anchor = anchorSelector.selectAnchor(plan);

    // Then: Should use index (lowest cost)
    assertThat(anchor.getVariable()).isEqualTo("p");
    assertThat(anchor.useIndex()).isTrue();
    assertThat(anchor.getPropertyName()).isEqualTo("id");
    assertThat(anchor.getIndex()).isNotNull();
    assertThat(anchor.getIndex().getIndexName()).isEqualTo("Person.id");
    assertThat(anchor.getEstimatedCost()).isLessThan(100.0); // Much cheaper than scan
  }

  @Test
  void selectAnchorWithoutIndex() {
    // Setup: Person without index
    statistics.setCardinality("Person", 10000);

    // Create logical plan with node having property but no index
    final LogicalPlan plan = createPlanWithNode("p", "Person",
        CollectionUtils.singletonMap("name", "Alice"));

    // When: Select anchor
    final AnchorSelection anchor = anchorSelector.selectAnchor(plan);

    // Then: Should use filtered scan
    assertThat(anchor.getVariable()).isEqualTo("p");
    assertThat(anchor.useIndex()).isFalse();
    assertThat(anchor.getPropertyName()).isEqualTo("name");
    assertThat(anchor.getIndex()).isNull();
    assertThat(anchor.getEstimatedCost()).isGreaterThan(100.0); // Scan cost
  }

  @Test
  void selectAnchorWithNoPredicates() {
    // Setup: Person with no predicates
    statistics.setCardinality("Person", 10000);

    // Create logical plan with node having no properties
    final LogicalPlan plan = createPlanWithNode("p", "Person", Collections.emptyMap());

    // When: Select anchor
    final AnchorSelection anchor = anchorSelector.selectAnchor(plan);

    // Then: Should use full scan
    assertThat(anchor.getVariable()).isEqualTo("p");
    assertThat(anchor.useIndex()).isFalse();
    assertThat(anchor.getPropertyName()).isNull();
    assertThat(anchor.getIndex()).isNull();
    assertThat(anchor.getEstimatedCardinality()).isEqualTo(10000); // All rows
  }

  @Test
  void selectAnchorPrefersIndexedNode() {
    // Setup: Two nodes, one with index, one without
    statistics.setCardinality("Person", 10000);
    statistics.setCardinality("Company", 1000);

    final IndexStatistics personIndex = new IndexStatistics("Person",
        Arrays.asList("id"), true, "Person.id");
    statistics.addIndex(personIndex);

    // Create plan with two nodes
    final LogicalPlan plan = createPlanWithTwoNodes(
        "p", "Person", CollectionUtils.singletonMap("id", 123),
        "c", "Company", CollectionUtils.singletonMap("name", "Acme")
    );

    // When: Select anchor
    final AnchorSelection anchor = anchorSelector.selectAnchor(plan);

    // Then: Should prefer Person (has index) even though Company is smaller
    assertThat(anchor.getVariable()).isEqualTo("p");
    assertThat(anchor.useIndex()).isTrue();
  }

  @Test
  void selectAnchorPrefersSmallerType() {
    // Setup: Two nodes, no indexes
    statistics.setCardinality("Person", 10000);
    statistics.setCardinality("Company", 1000);

    // Create plan with two nodes, both with properties but no indexes
    final LogicalPlan plan = createPlanWithTwoNodes(
        "p", "Person", CollectionUtils.singletonMap("age", 30),
        "c", "Company", CollectionUtils.singletonMap("name", "Acme")
    );

    // When: Select anchor
    final AnchorSelection anchor = anchorSelector.selectAnchor(plan);

    // Then: Should prefer Company (smaller cardinality)
    assertThat(anchor.getVariable()).isEqualTo("c");
    assertThat(anchor.useIndex()).isFalse();
    assertThat(anchor.getEstimatedCardinality()).isLessThan(2000); // ~1000 * 0.1 selectivity
  }

  @Test
  void selectAnchorPrefersUniqueIndex() {
    // Setup: Two nodes, both with indexes, one unique
    statistics.setCardinality("Person", 10000);
    statistics.setCardinality("Company", 10000);

    final IndexStatistics uniqueIndex = new IndexStatistics("Person",
        Arrays.asList("id"), true, "Person.id");
    final IndexStatistics nonUniqueIndex = new IndexStatistics("Company",
        Arrays.asList("country"), false, "Company.country");
    statistics.addIndex(uniqueIndex);
    statistics.addIndex(nonUniqueIndex);

    // Create plan with two nodes
    final LogicalPlan plan = createPlanWithTwoNodes(
        "p", "Person", CollectionUtils.singletonMap("id", 123),
        "c", "Company", CollectionUtils.singletonMap("country", "USA")
    );

    // When: Select anchor
    final AnchorSelection anchor = anchorSelector.selectAnchor(plan);

    // Then: Should prefer Person (unique index = lower selectivity)
    assertThat(anchor.getVariable()).isEqualTo("p");
    assertThat(anchor.useIndex()).isTrue();
    assertThat(anchor.getIndex().isUnique()).isTrue();
    assertThat(anchor.getEstimatedCardinality()).isLessThanOrEqualTo(10L); // Very selective (10000 * 0.001 = 10)
  }

  @Test
  void selectAnchorWithCompositeIndex() {
    // Setup: Node with composite index
    statistics.setCardinality("Person", 10000);
    final IndexStatistics compositeIndex = new IndexStatistics("Person",
        Arrays.asList("firstName", "lastName"), false, "Person.name");
    statistics.addIndex(compositeIndex);

    // Create plan with node matching first column of composite index
    final LogicalPlan plan = createPlanWithNode("p", "Person",
        CollectionUtils.singletonMap("firstName", "John"));

    // When: Select anchor
    final AnchorSelection anchor = anchorSelector.selectAnchor(plan);

    // Then: Should use composite index (matches first column)
    assertThat(anchor.getVariable()).isEqualTo("p");
    assertThat(anchor.useIndex()).isTrue();
    assertThat(anchor.getIndex().getPropertyNames()).contains("firstName");
  }

  @Test
  void selectAnchorRejectsEmptyPlan() {
    // Create empty logical plan (no nodes)
    final LogicalPlan emptyPlan = TestLogicalPlanBuilder.create().build();

    // When/Then: Should throw exception
    assertThatThrownBy(() -> anchorSelector.selectAnchor(emptyPlan))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot select anchor from empty logical plan");
  }

  @Test
  void shouldUseIndexWhenIndexAvailable() {
    // Setup: Node with indexed property
    statistics.setCardinality("Person", 10000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("email"), true, "Person.email");
    statistics.addIndex(index);

    final LogicalNode node = new LogicalNode("p", Arrays.asList("Person"),
        CollectionUtils.singletonMap("email", "alice@example.com"));

    // When: Check if should use index
    final boolean shouldUse = anchorSelector.shouldUseIndex(node, "Person");

    // Then: Should use index
    assertThat(shouldUse).isTrue();
  }

  @Test
  void shouldNotUseIndexWhenNoIndex() {
    // Setup: Node without index
    statistics.setCardinality("Person", 10000);

    final LogicalNode node = new LogicalNode("p", Arrays.asList("Person"),
        CollectionUtils.singletonMap("name", "Alice"));

    // When: Check if should use index
    final boolean shouldUse = anchorSelector.shouldUseIndex(node, "Person");

    // Then: Should not use index
    assertThat(shouldUse).isFalse();
  }

  @Test
  void shouldNotUseIndexWhenNoProperties() {
    // Setup: Node with no properties
    statistics.setCardinality("Person", 10000);
    final IndexStatistics index = new IndexStatistics("Person",
        Arrays.asList("id"), true, "Person.id");
    statistics.addIndex(index);

    final LogicalNode node = new LogicalNode("p", Arrays.asList("Person"),
        Collections.emptyMap());

    // When: Check if should use index
    final boolean shouldUse = anchorSelector.shouldUseIndex(node, "Person");

    // Then: Should not use index (no predicates to match)
    assertThat(shouldUse).isFalse();
  }

  // Helper methods

  private LogicalPlan createPlanWithNode(final String variable, final String label,
                                          final Map<String, Object> properties) {
    return TestLogicalPlanBuilder.create()
        .withNode(variable, label, properties)
        .build();
  }

  private LogicalPlan createPlanWithTwoNodes(final String var1, final String label1,
                                              final Map<String, Object> props1,
                                              final String var2, final String label2,
                                              final Map<String, Object> props2) {
    return TestLogicalPlanBuilder.create()
        .withNode(var1, label1, props1)
        .withNode(var2, label2, props2)
        .build();
  }

  /**
   * Test builder for creating LogicalPlan instances for testing.
   * Uses reflection to bypass the private constructor and populate nodes.
   */
  private static class TestLogicalPlanBuilder {
    private final Map<String, LogicalNode> nodes = new HashMap<>();

    public static TestLogicalPlanBuilder create() {
      return new TestLogicalPlanBuilder();
    }

    public TestLogicalPlanBuilder withNode(final String variable, final String label,
                                            final Map<String, Object> properties) {
      final LogicalNode node = new LogicalNode(variable, Arrays.asList(label), properties);
      nodes.put(variable, node);
      return this;
    }

    public LogicalPlan build() {
      // Use public factory method for testing
      return LogicalPlan.forTesting(this.nodes);
    }
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
      indexes.computeIfAbsent(index.getTypeName(), k -> new ArrayList<>())
             .add(index);
    }

    @Override
    public long getCardinality(final String typeName) {
      return cardinalities.getOrDefault(typeName, 0L);
    }

    @Override
    public TypeStatistics getTypeStatistics(final String typeName) {
      final long cardinality = cardinalities.getOrDefault(typeName, 0L);
      if (cardinality == 0) {
        return null;
      }
      return new TypeStatistics(typeName, cardinality, true);
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

    @Override
    public List<IndexStatistics> getIndexesForType(final String typeName) {
      return indexes.getOrDefault(typeName, Collections.emptyList());
    }
  }
}
