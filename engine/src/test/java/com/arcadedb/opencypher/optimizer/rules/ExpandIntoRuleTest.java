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
package com.arcadedb.opencypher.optimizer.rules;

import com.arcadedb.opencypher.ast.Direction;
import com.arcadedb.opencypher.optimizer.plan.LogicalNode;
import com.arcadedb.opencypher.optimizer.plan.LogicalPlan;
import com.arcadedb.opencypher.optimizer.plan.LogicalRelationship;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ExpandIntoRule class (KEY OPTIMIZATION for 5-10x speedup).
 */
class ExpandIntoRuleTest {
  private ExpandIntoRule rule;

  @BeforeEach
  void setUp() {
    rule = new ExpandIntoRule();
  }

  @Test
  void testGetRuleName() {
    assertThat(rule.getRuleName()).isEqualTo("ExpandInto");
  }

  @Test
  void testGetPriority() {
    assertThat(rule.getPriority()).isEqualTo(30); // Medium-high priority
  }

  @Test
  void testIsApplicableWithBoundedRelationship() {
    // Create plan with two bounded nodes and relationship between them
    final LogicalNode nodeA = new LogicalNode("a", Arrays.asList("Person"),
        Collections.singletonMap("id", 1));
    final LogicalNode nodeB = new LogicalNode("b", Arrays.asList("Person"),
        Collections.singletonMap("id", 2));

    final Map<String, LogicalNode> nodes = new HashMap<>();
    nodes.put("a", nodeA);
    nodes.put("b", nodeB);

    final LogicalPlan plan = LogicalPlan.forTesting(nodes);
    // Note: LogicalPlan doesn't have public API to add relationships for testing
    // This tests the node-level logic only

    // ExpandIntoRule should be applicable if there are multiple relationships
    // For now, test that it doesn't crash with nodes only
    assertThat(rule.isApplicable(plan)).isFalse(); // No relationships yet
  }

  @Test
  void testDetermineBoundVariablesWithAnchor() {
    // Create plan with one node having properties (bound) and one without (unbound)
    final LogicalNode boundNode = new LogicalNode("a", Arrays.asList("Person"),
        Collections.singletonMap("id", 1));
    final LogicalNode unboundNode = new LogicalNode("b", Arrays.asList("Person"),
        Collections.emptyMap());

    final Map<String, LogicalNode> nodes = new HashMap<>();
    nodes.put("a", boundNode);
    nodes.put("b", unboundNode);

    final LogicalPlan plan = LogicalPlan.forTesting(nodes);

    // When: Determine bound variables
    final Set<String> boundVars = rule.determineBoundVariables(plan, "a");

    // Then: Anchor and nodes with properties are bound
    assertThat(boundVars).contains("a"); // Anchor
    assertThat(boundVars).contains("a"); // Has properties
    assertThat(boundVars).doesNotContain("b"); // No properties
  }

  @Test
  void testDetermineBoundVariablesWithMultipleBoundNodes() {
    // Create plan with multiple nodes having properties
    final LogicalNode nodeA = new LogicalNode("a", Arrays.asList("Person"),
        Collections.singletonMap("id", 1));
    final LogicalNode nodeB = new LogicalNode("b", Arrays.asList("Person"),
        Collections.singletonMap("id", 2));
    final LogicalNode nodeC = new LogicalNode("c", Arrays.asList("Company"),
        Collections.emptyMap());

    final Map<String, LogicalNode> nodes = new HashMap<>();
    nodes.put("a", nodeA);
    nodes.put("b", nodeB);
    nodes.put("c", nodeC);

    final LogicalPlan plan = LogicalPlan.forTesting(nodes);

    // When: Determine bound variables
    final Set<String> boundVars = rule.determineBoundVariables(plan, "a");

    // Then: All nodes with properties are bound
    assertThat(boundVars).contains("a"); // Anchor + properties
    assertThat(boundVars).contains("b"); // Has properties
    assertThat(boundVars).doesNotContain("c"); // No properties
  }

  @Test
  void testShouldUseExpandIntoWhenBothEndpointsBound() {
    // Setup: Both source and target are bound
    final LogicalRelationship rel = new LogicalRelationship(
        "r", "a", "b", Arrays.asList("KNOWS"), Direction.OUT,
        Collections.emptyMap(), null, null
    );

    final Set<String> boundVars = new HashSet<>(Arrays.asList("a", "b"));

    // When: Check if should use ExpandInto
    final boolean shouldUse = rule.shouldUseExpandInto(rel, boundVars);

    // Then: Should use ExpandInto
    assertThat(shouldUse).isTrue();
  }

  @Test
  void testShouldNotUseExpandIntoWhenOnlySourceBound() {
    // Setup: Only source is bound
    final LogicalRelationship rel = new LogicalRelationship(
        "r", "a", "b", Arrays.asList("KNOWS"), Direction.OUT,
        Collections.emptyMap(), null, null
    );

    final Set<String> boundVars = new HashSet<>(Collections.singletonList("a"));

    // When: Check if should use ExpandInto
    final boolean shouldUse = rule.shouldUseExpandInto(rel, boundVars);

    // Then: Should not use ExpandInto (use ExpandAll instead)
    assertThat(shouldUse).isFalse();
  }

  @Test
  void testShouldNotUseExpandIntoWhenOnlyTargetBound() {
    // Setup: Only target is bound
    final LogicalRelationship rel = new LogicalRelationship(
        "r", "a", "b", Arrays.asList("KNOWS"), Direction.OUT,
        Collections.emptyMap(), null, null
    );

    final Set<String> boundVars = new HashSet<>(Collections.singletonList("b"));

    // When: Check if should use ExpandInto
    final boolean shouldUse = rule.shouldUseExpandInto(rel, boundVars);

    // Then: Should not use ExpandInto (use ExpandAll instead)
    assertThat(shouldUse).isFalse();
  }

  @Test
  void testShouldNotUseExpandIntoWhenNeitherBound() {
    // Setup: Neither endpoint is bound
    final LogicalRelationship rel = new LogicalRelationship(
        "r", "a", "b", Arrays.asList("KNOWS"), Direction.OUT,
        Collections.emptyMap(), null, null
    );

    final Set<String> boundVars = new HashSet<>();

    // When: Check if should use ExpandInto
    final boolean shouldUse = rule.shouldUseExpandInto(rel, boundVars);

    // Then: Should not use ExpandInto
    assertThat(shouldUse).isFalse();
  }

  @Test
  void testDetermineBoundVariablesEmptyPlan() {
    // Create empty plan
    final LogicalPlan plan = LogicalPlan.forTesting(Collections.emptyMap());

    // When: Determine bound variables
    final Set<String> boundVars = rule.determineBoundVariables(plan, "anchor");

    // Then: Only anchor is bound
    assertThat(boundVars).containsExactly("anchor");
  }

  @Test
  void testDetermineBoundVariablesWithNullProperties() {
    // Create node with null properties
    final LogicalNode node = new LogicalNode("a", Arrays.asList("Person"), null);
    final LogicalPlan plan = LogicalPlan.forTesting(Collections.singletonMap("a", node));

    // When: Determine bound variables
    final Set<String> boundVars = rule.determineBoundVariables(plan, "a");

    // Then: Only anchor is bound (null properties don't count)
    assertThat(boundVars).containsExactly("a"); // Anchor only
  }
}
