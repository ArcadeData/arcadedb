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
package com.arcadedb.query.opencypher.procedures;

import com.arcadedb.function.procedure.Procedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for CypherProcedureRegistry.
 * <p>
 * This test class modifies global registry state and must run with exclusive access
 * to the CypherProcedureRegistry to avoid interfering with other tests.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("CypherProcedureRegistry")
class CypherProcedureRegistryTest {

  private int initialProcedureCount;
  private static Map<String, CypherProcedure> savedProcedures;

  @BeforeEach
  void setUp() {
    // Store initial count but don't reset - we want to preserve the shared global state
    // Only unregister test procedures we add
    initialProcedureCount = CypherProcedureRegistry.size();

    // Save all current procedures before any test that might modify them
    if (savedProcedures == null) {
      savedProcedures = new HashMap<>();
      for (final String name : CypherProcedureRegistry.getProcedureNames()) {
        savedProcedures.put(name, CypherProcedureRegistry.get(name));
      }
    }
  }

  @AfterEach
  void tearDown() {
    // Clean up only test procedures we added, not all procedures
    // This preserves the global state for other tests
    CypherProcedureRegistry.unregister("test.procedure");
    CypherProcedureRegistry.unregister("test.duplicate");
    CypherProcedureRegistry.unregister("test.replace");
    CypherProcedureRegistry.unregister("test.get");
    CypherProcedureRegistry.unregister("test.has");
    CypherProcedureRegistry.unregister("test.size1");
    CypherProcedureRegistry.unregister("test.size2");
    CypherProcedureRegistry.unregister("test.unregister");
    CypherProcedureRegistry.unregister("test.apocunregister");
    CypherProcedureRegistry.unregister("test.reset");
    CypherProcedureRegistry.unregister("test.custom");
    CypherProcedureRegistry.unregister("test.apoc");
  }

  /**
   * Restores the original procedure instances to maintain instance equality across test classes.
   */
  private void restoreSavedProcedures() {
    CypherProcedureRegistry.clear();
    for (final Map.Entry<String, CypherProcedure> entry : savedProcedures.entrySet()) {
      CypherProcedureRegistry.registerOrReplace(entry.getValue());
    }
  }

  @Test
  void staticInitialization() {
    // Verify that built-in procedures are registered on initialization
    assertThat(CypherProcedureRegistry.size()).isGreaterThan(0);

    // Verify specific built-in procedures are registered
    assertThat(CypherProcedureRegistry.hasProcedure("merge.relationship")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("merge.node")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.dijkstra")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.astar")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.allsimplepaths")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.bellmanford")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.pagerank")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.betweenness")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.wcc")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.louvain")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.labelpropagation")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.closeness")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.degree")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.triangleCount")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.kcore")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.scc")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.mst")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.jaccard")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.randomWalk")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.expand")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.expandconfig")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.subgraphnodes")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.subgraphall")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.spanningtree")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.graph")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.schema")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.stats")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.nodetypeproperties")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.reltypeproperties")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.hits")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.harmonic")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.eigenvector")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.articulationPoints")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.bridges")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.topologicalSort")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.apsp")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.adamicAdar")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.katz")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.voteRank")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.maxFlow")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.kShortestPaths")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.simRank")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.clique")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.graphSummary")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.modularityScore")).isTrue();
  }

  @Test
  void registerNewProcedure() {
    // Create and register a test procedure
    final TestProcedure procedure = new TestProcedure("test.procedure");
    CypherProcedureRegistry.register(procedure);

    // Verify procedure is registered
    assertThat(CypherProcedureRegistry.hasProcedure("test.procedure")).isTrue();
    assertThat(CypherProcedureRegistry.get("test.procedure")).isEqualTo(procedure);
    assertThat(CypherProcedureRegistry.size()).isEqualTo(initialProcedureCount + 1);
  }

  @Test
  void registerDuplicateProcedure() {
    // Register a procedure
    final TestProcedure procedure1 = new TestProcedure("test.duplicate");
    CypherProcedureRegistry.register(procedure1);

    final int sizeAfterFirst = CypherProcedureRegistry.size();

    // Try to register another procedure with the same name
    final TestProcedure procedure2 = new TestProcedure("test.duplicate");
    CypherProcedureRegistry.register(procedure2);

    // Should not replace the existing procedure
    assertThat(CypherProcedureRegistry.get("test.duplicate")).isEqualTo(procedure1);
    assertThat(CypherProcedureRegistry.size()).isEqualTo(sizeAfterFirst);
  }

  @Test
  void registerOrReplace() {
    // Register a procedure
    final TestProcedure procedure1 = new TestProcedure("test.replace");
    CypherProcedureRegistry.registerOrReplace(procedure1);

    assertThat(CypherProcedureRegistry.get("test.replace")).isEqualTo(procedure1);
    final int sizeAfterFirst = CypherProcedureRegistry.size();

    // Replace with another procedure with the same name
    final TestProcedure procedure2 = new TestProcedure("test.replace");
    CypherProcedureRegistry.registerOrReplace(procedure2);

    // Should replace the existing procedure
    assertThat(CypherProcedureRegistry.get("test.replace")).isEqualTo(procedure2);
    assertThat(CypherProcedureRegistry.size()).isEqualTo(sizeAfterFirst);
  }

  @Test
  void getExistingProcedure() {
    final TestProcedure procedure = new TestProcedure("test.get");
    CypherProcedureRegistry.register(procedure);

    final CypherProcedure retrieved = CypherProcedureRegistry.get("test.get");

    assertThat(retrieved).isNotNull();
    assertThat(retrieved).isEqualTo(procedure);
  }

  @Test
  void getNonExistingProcedure() {
    final CypherProcedure retrieved = CypherProcedureRegistry.get("test.nonexistent");

    assertThat(retrieved).isNull();
  }

  @Test
  void getWithApocPrefix() {
    // Test APOC compatibility - apoc. prefix should be stripped
    final CypherProcedure mergeRelationship = CypherProcedureRegistry.get("merge.relationship");
    final CypherProcedure apocMergeRelationship = CypherProcedureRegistry.get("apoc.merge.relationship");

    assertThat(apocMergeRelationship).isNotNull();
    assertThat(apocMergeRelationship).isEqualTo(mergeRelationship);
  }

  @Test
  void getCaseInsensitive() {
    // Test case insensitivity
    final CypherProcedure lowercase = CypherProcedureRegistry.get("merge.relationship");
    final CypherProcedure uppercase = CypherProcedureRegistry.get("MERGE.RELATIONSHIP");
    final CypherProcedure mixedcase = CypherProcedureRegistry.get("Merge.Relationship");

    assertThat(uppercase).isNotNull();
    assertThat(mixedcase).isNotNull();
    assertThat(uppercase).isEqualTo(lowercase);
    assertThat(mixedcase).isEqualTo(lowercase);
  }

  @Test
  void hasProcedure() {
    final TestProcedure procedure = new TestProcedure("test.has");
    CypherProcedureRegistry.register(procedure);

    assertThat(CypherProcedureRegistry.hasProcedure("test.has")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("test.notfound")).isFalse();
  }

  @Test
  void hasProcedureWithApocPrefix() {
    // Test APOC compatibility in hasProcedure
    assertThat(CypherProcedureRegistry.hasProcedure("merge.relationship")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.merge.relationship")).isTrue();
  }

  @Test
  void hasProcedureCaseInsensitive() {
    assertThat(CypherProcedureRegistry.hasProcedure("merge.relationship")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("MERGE.RELATIONSHIP")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("Merge.Relationship")).isTrue();
  }

  @Test
  void getProcedureNames() {
    final Set<String> names = CypherProcedureRegistry.getProcedureNames();

    assertThat(names).isNotNull();
    assertThat(names).isNotEmpty();
    assertThat(names).contains("merge.relationship", "merge.node", "algo.dijkstra");
  }

  @Test
  void getProcedureNamesIsUnmodifiable() {
    final Set<String> names = CypherProcedureRegistry.getProcedureNames();

    // Verify the set is unmodifiable by attempting to modify it
    assertThat(names).isNotEmpty();
    assertThatThrownBy(() -> names.add("test.modify"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> names.remove("merge.relationship"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> names.clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getAllProcedures() {
    final var procedures = CypherProcedureRegistry.getAllProcedures();

    assertThat(procedures).isNotNull();
    assertThat(procedures).isNotEmpty();
    assertThat(procedures.size()).isEqualTo(CypherProcedureRegistry.size());
  }

  @Test
  void getAllProceduresIsUnmodifiable() {
    final var procedures = CypherProcedureRegistry.getAllProcedures();

    // Verify the collection is unmodifiable by attempting to modify it
    assertThat(procedures).isNotEmpty();
    final TestProcedure testProc = new TestProcedure("test.modify");
    assertThatThrownBy(() -> procedures.add(testProc))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> procedures.clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void size() {
    final int initialSize = CypherProcedureRegistry.size();

    final TestProcedure procedure1 = new TestProcedure("test.size1");
    CypherProcedureRegistry.register(procedure1);
    assertThat(CypherProcedureRegistry.size()).isEqualTo(initialSize + 1);

    final TestProcedure procedure2 = new TestProcedure("test.size2");
    CypherProcedureRegistry.register(procedure2);
    assertThat(CypherProcedureRegistry.size()).isEqualTo(initialSize + 2);
  }

  @Test
  void unregisterExistingProcedure() {
    final TestProcedure procedure = new TestProcedure("test.unregister");
    CypherProcedureRegistry.register(procedure);

    assertThat(CypherProcedureRegistry.hasProcedure("test.unregister")).isTrue();

    final CypherProcedure unregistered = CypherProcedureRegistry.unregister("test.unregister");

    assertThat(unregistered).isEqualTo(procedure);
    assertThat(CypherProcedureRegistry.hasProcedure("test.unregister")).isFalse();
  }

  @Test
  void unregisterNonExistingProcedure() {
    final CypherProcedure unregistered = CypherProcedureRegistry.unregister("test.notfound");

    assertThat(unregistered).isNull();
  }

  @Test
  void unregisterWithApocPrefix() {
    final TestProcedure procedure = new TestProcedure("test.apocunregister");
    CypherProcedureRegistry.register(procedure);

    // Unregister using apoc. prefix
    final CypherProcedure unregistered = CypherProcedureRegistry.unregister("apoc.test.apocunregister");

    assertThat(unregistered).isEqualTo(procedure);
    assertThat(CypherProcedureRegistry.hasProcedure("test.apocunregister")).isFalse();
  }

  @Test
  void clear() {
    // Save the current state
    final int sizeBeforeClear = CypherProcedureRegistry.size();

    // Clear the registry
    CypherProcedureRegistry.clear();

    assertThat(CypherProcedureRegistry.size()).isZero();
    assertThat(CypherProcedureRegistry.getProcedureNames()).isEmpty();
    assertThat(CypherProcedureRegistry.getAllProcedures()).isEmpty();

    // Immediately restore the original instances to not affect other tests
    restoreSavedProcedures();
    assertThat(CypherProcedureRegistry.size()).isEqualTo(sizeBeforeClear);
  }

  @Test
  void reset() {
    // Add a custom procedure
    final TestProcedure procedure = new TestProcedure("test.reset");
    CypherProcedureRegistry.register(procedure);

    assertThat(CypherProcedureRegistry.hasProcedure("test.reset")).isTrue();

    // Reset registry - this will create new instances but should have same count
    CypherProcedureRegistry.reset();

    // Custom procedure should be removed
    assertThat(CypherProcedureRegistry.hasProcedure("test.reset")).isFalse();

    // Built-in procedures should be restored (may be new instances, but same names)
    assertThat(CypherProcedureRegistry.size()).isEqualTo(initialProcedureCount);
    assertThat(CypherProcedureRegistry.hasProcedure("merge.relationship")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.dijkstra")).isTrue();

    // Immediately restore the original instances to maintain instance equality for other tests
    restoreSavedProcedures();
  }

  @Test
  void apocPrefixWithCustomProcedure() {
    // Test that APOC prefix works with custom procedures too
    final TestProcedure procedure = new TestProcedure("test.custom");
    CypherProcedureRegistry.register(procedure);

    final CypherProcedure withoutPrefix = CypherProcedureRegistry.get("test.custom");
    final CypherProcedure withPrefix = CypherProcedureRegistry.get("apoc.test.custom");

    assertThat(withPrefix).isNotNull();
    assertThat(withPrefix).isEqualTo(withoutPrefix);
  }

  @Test
  void multipleApocPrefixes() {
    // Test that APOC prefix is only stripped once
    final TestProcedure procedure = new TestProcedure("test.apoc");
    CypherProcedureRegistry.register(procedure);

    // "apoc.apoc.test.apoc" should become "apoc.test.apoc" (only first apoc. stripped)
    final CypherProcedure retrieved = CypherProcedureRegistry.get("apoc.apoc.test.apoc");

    assertThat(retrieved).isNull();
  }

  @Test
  void cypherProcedureInterface() {
    // Test that CypherProcedure extends Procedure
    final TestProcedure procedure = new TestProcedure("test.interface");

    // Verify it's a CypherProcedure
    assertThat(procedure).isInstanceOf(CypherProcedure.class);

    // Verify it's also a Procedure (CypherProcedure extends Procedure)
    assertThat(procedure).isInstanceOf(Procedure.class);

    // Verify all Procedure methods work
    assertThat(procedure.getName()).isEqualTo("test.interface");
    assertThat(procedure.getMinArgs()).isEqualTo(0);
    assertThat(procedure.getMaxArgs()).isEqualTo(5);
    assertThat(procedure.getDescription()).isNotEmpty();
    assertThat(procedure.getYieldFields()).isNotEmpty();
    assertThat(procedure.execute(new Object[0], null, null)).isNotNull();
  }

  /**
   * Simple test implementation of CypherProcedure for testing purposes.
   */
  private static class TestProcedure implements CypherProcedure {
    private final String name;

    TestProcedure(final String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public int getMinArgs() {
      return 0;
    }

    @Override
    public int getMaxArgs() {
      return 5;
    }

    @Override
    public String getDescription() {
      return "Test procedure for unit testing";
    }

    @Override
    public List<String> getYieldFields() {
      return List.of("result");
    }

    @Override
    public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
      return Stream.empty();
    }
  }
}
