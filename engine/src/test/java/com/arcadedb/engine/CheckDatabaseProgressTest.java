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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CHECK DATABASE progress reporting: the checker emits step-by-step progress with percentages through
 * {@link com.arcadedb.utility.ProgressCallback}, and the SQL statement publishes/retires the operation in
 * {@link OperationProgressRegistry}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseProgressTest extends TestHelper {

  private record Emission(String stepName, int stepIndex, int totalSteps, long done, long total) {
  }

  private void createSampleGraph() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person", 1);
      database.getSchema().createVertexType("Company", 1);
      database.getSchema().createEdgeType("WorksAt", 1);
      database.getSchema().createDocumentType("Note", 1);
    });

    database.transaction(() -> {
      final MutableVertex company = database.newVertex("Company").set("name", "Arcade").save();
      for (int i = 0; i < 500; i++) {
        final MutableVertex person = database.newVertex("Person").set("i", i).save();
        person.newEdge("WorksAt", company);
      }
      for (int i = 0; i < 100; i++) {
        final MutableDocument note = database.newDocument("Note");
        note.set("i", i).save();
      }
    });
  }

  @Test
  void checkerEmitsOrderedStepsWithPercentages() {
    createSampleGraph();

    final List<Emission> emissions = new ArrayList<>();
    final DatabaseChecker checker = new DatabaseChecker(database).setVerboseLevel(0)
        .setProgressCallback((stepName, stepIndex, totalSteps, done, total) ->
            emissions.add(new Emission(stepName, stepIndex, totalSteps, done, total)));

    checker.check();

    assertThat(emissions).isNotEmpty();

    final int totalSteps = emissions.getFirst().totalSteps;
    // 1 edge type + 2 vertex types + 1 document type + buckets + external + indexes = 7 (no fix, no compress).
    assertThat(totalSteps).isEqualTo(7);

    int lastStep = 0;
    for (final Emission e : emissions) {
      // STEP INDEX IS MONOTONIC, 1-BASED, BOUNDED; TOTAL STEPS IS STABLE ACROSS THE WHOLE RUN.
      assertThat(e.stepIndex).isBetween(1, totalSteps);
      assertThat(e.stepIndex).isGreaterThanOrEqualTo(lastStep);
      assertThat(e.totalSteps).isEqualTo(totalSteps);
      assertThat(e.stepName).isNotBlank();
      if (e.total > 0)
        assertThat(e.done).isLessThanOrEqualTo(e.total);
      lastStep = e.stepIndex;
    }
    // THE RUN WALKS EVERY STEP AND FINISHES THE LAST ONE.
    assertThat(lastStep).isEqualTo(totalSteps);

    // THE PER-TYPE STEPS ARE NAMED AFTER THE TYPES AND REACH 100%.
    for (final String expectedStep : new String[] { "Checking edges 'WorksAt'", "Checking vertices 'Person'",
        "Checking vertices 'Company'", "Checking documents 'Note'" }) {
      final List<Emission> step = emissions.stream().filter(e -> e.stepName.equals(expectedStep)).toList();
      assertThat(step).as("emissions for step '" + expectedStep + "'").isNotEmpty();
      final Emission last = step.getLast();
      assertThat(last.total).isGreaterThan(0);
      assertThat(last.done).isEqualTo(last.total);
    }
  }

  @Test
  void fixModeAddsRebuildIndexesStep() {
    createSampleGraph();

    final List<Emission> emissions = new ArrayList<>();
    new DatabaseChecker(database).setVerboseLevel(0).setFix(true)
        .setProgressCallback((stepName, stepIndex, totalSteps, done, total) ->
            emissions.add(new Emission(stepName, stepIndex, totalSteps, done, total)))
        .check();

    // FIX ADDS THE "Rebuilding indexes" STEP: 7 + 1.
    assertThat(emissions.getFirst().totalSteps).isEqualTo(8);
    assertThat(emissions.stream().anyMatch(e -> e.stepName.startsWith("Rebuilding indexes"))).isTrue();
  }

  @Test
  void sqlStatementRegistersAndRetiresTheOperation() {
    createSampleGraph();

    final int before = OperationProgressRegistry.instance().getOperations(database.getName()).size();

    try (final ResultSet result = database.command("sql", "CHECK DATABASE")) {
      assertThat(result.hasNext()).isTrue();
    }

    // THE OPERATION MUST BE RETIRED FROM THE REGISTRY WHEN THE STATEMENT COMPLETES.
    // The failure path (retire despite a mid-check exception) is covered by CheckDatabaseStatementProgressTest.
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).hasSize(before);
  }
}
