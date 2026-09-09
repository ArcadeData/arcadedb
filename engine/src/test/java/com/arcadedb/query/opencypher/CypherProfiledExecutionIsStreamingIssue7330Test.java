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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code $profileExecution} flag - injected by the HTTP handler for Studio's {@code profileExecution: "detailed"},
 * and by {@code ServerDatabase} for every statement while the server profiler is recording - asks for a statement to
 * be TIMED, not for it to be run differently.
 * <p>
 * It used to reroute every OpenCypher statement onto {@code CypherExecutionPlan.profile()}, which drains the whole
 * plan into heap before returning. The number the profiler then reported was the cost of a materialising execution
 * the statement never performs otherwise, and turning the diagnostic on made every Cypher read on the server
 * materialise in heap for the length of the recording window (issue #7330).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherProfiledExecutionIsStreamingIssue7330Test {
  private static final int    ROWS         = 500;
  private static final String DATABASE_DIR = "./target/databases/testcypher-profiled-streaming-7330";

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory(DATABASE_DIR).create();
    database.getSchema().createVertexType("Item");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("Item").set("idx", i).save();
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * The point of the issue: with the flag on, the statement must still stream. Pulling a single row out of
   * {@link #ROWS} and then reading the plan must report one row returned, because that is all the caller asked for -
   * the eager reroute reported all {@link #ROWS} of them, having produced every one before {@code query()} returned.
   */
  @Test
  void profiledExecutionStreamsInsteadOfDrainingThePlan() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (i:Item) RETURN i.idx AS idx",
        Map.of("$profileExecution", true))) {

      assertThat(rs.hasNext()).isTrue();
      rs.next();

      final Optional<ExecutionPlan> plan = rs.getExecutionPlan();
      assertThat(plan).isPresent();
      assertThat(plan.get().prettyPrint(0, 2)).contains("Rows Returned: 1");
    }
  }

  /**
   * Timing the statement must not change the answer it gives.
   */
  @Test
  void profiledExecutionReturnsTheSameRowsAsAnUnprofiledOne() {
    final long profiled = countRows(Map.of("$profileExecution", true));
    final long plain = countRows(Map.of());

    assertThat(profiled).isEqualTo(ROWS);
    assertThat(plain).isEqualTo(ROWS);
  }

  /**
   * And the diagnostic itself must still work: the plan is there, and it is built from the live step chain, so the
   * elapsed it reports grows with what the caller actually consumed rather than being frozen before the first row.
   */
  @Test
  void profiledExecutionStillCarriesAnExecutionPlanWithTheStepsThatRan() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (i:Item) RETURN i.idx AS idx",
        Map.of("$profileExecution", true))) {

      long consumed = 0;
      while (rs.hasNext()) {
        rs.next();
        ++consumed;
      }
      assertThat(consumed).isEqualTo(ROWS);

      final Optional<ExecutionPlan> plan = rs.getExecutionPlan();
      assertThat(plan).isPresent();

      final String text = plan.get().prettyPrint(0, 2);
      assertThat(text).contains("OpenCypher Query Profile");
      assertThat(text).contains("Rows Returned: " + ROWS);
      assertThat(plan.get().getSteps()).isNotEmpty();
    }
  }

  /**
   * The explicit {@code PROFILE <statement>} keyword is a different request - the user asking, in the statement
   * itself, for the whole execution to be run and summarised - and keeps the eager path. Neo4j's PROFILE executes
   * fully too, so this is also the parity behaviour.
   */
  @Test
  void theExplicitProfileKeywordStillExecutesEagerly() {
    try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (i:Item) RETURN i.idx AS idx")) {
      final Optional<ExecutionPlan> plan = rs.getExecutionPlan();
      assertThat(plan).isPresent();
      // Nothing has been pulled by this caller yet, and the profile already knows every row: that is the drain.
      assertThat(plan.get().prettyPrint(0, 2)).contains("Rows Returned: " + ROWS);
    }
  }

  private long countRows(final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (i:Item) RETURN i.idx AS idx", parameters)) {
      long rows = 0;
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
      return rows;
    }
  }
}
