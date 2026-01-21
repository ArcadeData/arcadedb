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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.executor.operators.NodeByLabelScan;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexSeek;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalNode;
import com.arcadedb.query.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.opencypher.optimizer.rules.*;
import com.arcadedb.query.opencypher.optimizer.statistics.CostModel;
import com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for CypherOptimizer.
 * Tests the full optimization flow with real database and statistics.
 */
public class CypherOptimizerIntegrationTest {
  private Database database;

  @BeforeEach
  public void setUp() {
    database = new DatabaseFactory("./target/databases/CypherOptimizerTest").create();

    database.transaction(() -> {
      // Create schema for Person type
      if (!database.getSchema().existsType("Person")) {
        database.getSchema().createVertexType("Person");
        database.getSchema().getType("Person").createProperty("id", Integer.class);
        database.getSchema().getType("Person").createProperty("email", String.class);
        database.getSchema().getType("Person").createProperty("name", String.class);
        database.getSchema().getType("Person").createProperty("age", Integer.class);

        // Create indexes
        database.getSchema().getType("Person")
            .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
        database.getSchema().getType("Person")
            .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "email");
      }

      // Create schema for Company type
      if (!database.getSchema().existsType("Company")) {
        database.getSchema().createVertexType("Company");
        database.getSchema().getType("Company").createProperty("id", Integer.class);
        database.getSchema().getType("Company").createProperty("name", String.class);
      }

      // Create test data - 1000 persons
      for (int i = 0; i < 1000; i++) {
        final MutableVertex person = database.newVertex("Person");
        person.set("id", i);
        person.set("email", "user" + i + "@example.com");
        person.set("name", "Person" + i);
        person.set("age", 20 + (i % 50));
        person.save();
      }

      // Create test data - 100 companies
      for (int i = 0; i < 100; i++) {
        final MutableVertex company = database.newVertex("Company");
        company.set("id", i);
        company.set("name", "Company" + i);
        company.save();
      }
    });
  }

  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  public void testStatisticsCollection() {
    // Given: StatisticsProvider
    final StatisticsProvider stats = new StatisticsProvider((DatabaseInternal) database);

    // When: Collect statistics
    stats.collectStatistics(Arrays.asList("Person", "Company"));

    // Then: Statistics should be correct
    assertThat(stats.getCardinality("Person")).isEqualTo(1000);
    assertThat(stats.getCardinality("Company")).isEqualTo(100);
  }

  @Test
  public void testAnchorSelectorWithIndexedProperty() {
    // Given: Plan with indexed property
    final LogicalNode node = new LogicalNode("p", Arrays.asList("Person"),
        Collections.singletonMap("id", 123));
    final LogicalPlan plan = LogicalPlan.forTesting(Collections.singletonMap("p", node));

    final StatisticsProvider stats = new StatisticsProvider((DatabaseInternal) database);
    stats.collectStatistics(Collections.singletonList("Person"));

    final CostModel costModel = new CostModel(stats);
    final AnchorSelector selector = new AnchorSelector(stats, costModel);

    // When: Select anchor
    final var anchor = selector.selectAnchor(plan);

    // Then: Should use index
    assertThat(anchor.useIndex()).isTrue();
    assertThat(anchor.getPropertyName()).isEqualTo("id");
    assertThat(anchor.getEstimatedCost()).isLessThan(100.0);
  }

  @Test
  public void testAnchorSelectorWithoutIndex() {
    // Given: Plan without index
    final LogicalNode node = new LogicalNode("p", Arrays.asList("Person"),
        Collections.singletonMap("name", "Alice"));
    final LogicalPlan plan = LogicalPlan.forTesting(Collections.singletonMap("p", node));

    final StatisticsProvider stats = new StatisticsProvider((DatabaseInternal) database);
    stats.collectStatistics(Collections.singletonList("Person"));

    final CostModel costModel = new CostModel(stats);
    final AnchorSelector selector = new AnchorSelector(stats, costModel);

    // When: Select anchor
    final var anchor = selector.selectAnchor(plan);

    // Then: Should not use index
    assertThat(anchor.useIndex()).isFalse();
    assertThat(anchor.getEstimatedCost()).isGreaterThanOrEqualTo(100.0); // Filtered scan cost
  }

  @Test
  public void testAnchorSelectorPrefersSmallerType() {
    // Given: Plan with two types
    final LogicalNode person = new LogicalNode("p", Arrays.asList("Person"),
        Collections.emptyMap());
    final LogicalNode company = new LogicalNode("c", Arrays.asList("Company"),
        Collections.emptyMap());

    final Map<String, LogicalNode> nodes = new HashMap<>();
    nodes.put("p", person);
    nodes.put("c", company);
    final LogicalPlan plan = LogicalPlan.forTesting(nodes);

    final StatisticsProvider stats = new StatisticsProvider((DatabaseInternal) database);
    stats.collectStatistics(Arrays.asList("Person", "Company"));

    final CostModel costModel = new CostModel(stats);
    final AnchorSelector selector = new AnchorSelector(stats, costModel);

    // When: Select anchor
    final var anchor = selector.selectAnchor(plan);

    // Then: Should prefer Company (smaller)
    assertThat(anchor.getVariable()).isEqualTo("c");
    assertThat(anchor.getEstimatedCardinality()).isEqualTo(100);
  }

  @Test
  public void testCostModelWithRealStatistics() {
    // Given: CostModel with real statistics
    final StatisticsProvider stats = new StatisticsProvider((DatabaseInternal) database);
    stats.collectStatistics(Arrays.asList("Person", "Company"));

    final CostModel costModel = new CostModel(stats);

    // When: Estimate costs
    final double scanCost = costModel.estimateScanCost("Person");
    final double indexCost = costModel.estimateIndexSeekCost("Person", "id", 0.001);

    // Then: Index should be cheaper than scan
    assertThat(indexCost).isLessThan(scanCost);
    assertThat(scanCost).isEqualTo(1000.0); // 1000 persons * 1.0 cost per row
  }

  @Test
  public void testIndexStatisticsCollection() {
    // Given: StatisticsProvider
    final StatisticsProvider stats = new StatisticsProvider((DatabaseInternal) database);
    stats.collectStatistics(Collections.singletonList("Person"));

    // When: Query index statistics
    final var indexes = stats.getIndexesForType("Person");

    // Then: Should find indexes
    assertThat(indexes).isNotEmpty();
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("id"));
    assertThat(indexes).anyMatch(idx -> idx.getPropertyNames().contains("email"));
  }

  @Test
  public void testOptimizerRulesOrdering() {
    // Given: StatisticsProvider
    final StatisticsProvider stats = new StatisticsProvider((DatabaseInternal) database);
    final CostModel costModel = new CostModel(stats);

    // When: Create rules
    final var rules = new ArrayList<OptimizationRule>();
    rules.add(new IndexSelectionRule(stats, costModel));
    rules.add(new FilterPushdownRule());
    rules.add(new ExpandIntoRule());
    rules.add(new JoinOrderRule(stats, costModel));

    // Then: Rules should be in priority order
    assertThat(rules.get(0).getPriority()).isEqualTo(10); // IndexSelection
    assertThat(rules.get(1).getPriority()).isEqualTo(20); // FilterPushdown
    assertThat(rules.get(2).getPriority()).isEqualTo(30); // ExpandInto
    assertThat(rules.get(3).getPriority()).isEqualTo(40); // JoinOrder
  }
}
