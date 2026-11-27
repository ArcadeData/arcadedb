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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.query.opencypher.executor.steps.EagerStep;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapBudgetTestSupport;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rows a PROFILE answers, and the rows a {@code CALL { } IN TRANSACTIONS} batch holds until its transaction has committed,
 * are in-heap buffers like the ones of a sort or a join: they are counted against the element cap of one operation
 * ({@code arcadedb.queryMaxHeapElementsAllowedPerOp}) and their estimated bytes against the heap budget every query of the JVM
 * shares ({@code arcadedb.queryMaxHeapRAM}), and given back when the result set is closed.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherRowBufferHeapLimitTest extends TestHelper {
  private static final int    ROWS           = 30;
  private static final long   LIMIT          = 10;
  private static final int    BIG_ROWS       = 3_000;
  private static final String FILLER         = "x".repeat(400);
  private static final long   BUDGET_MB      = 1L;
  private static final String LIMIT_SETTING  = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey();
  private static final String PROFILE_SCAN   = "PROFILE MATCH (n:N) RETURN n.id AS id";
  private static final String PROFILE_BIG    = "PROFILE MATCH (p:P) RETURN p.name AS name";
  private static final String SCAN_IN_BATCH  = "MATCH (n:N) CALL { WITH n RETURN n.id AS v } IN TRANSACTIONS OF %d ROWS RETURN v";
  private static final String UNIT_IN_BATCH  =
      "MATCH (n:N) CALL { WITH n CREATE (:W {k: n.id}) } IN TRANSACTIONS OF %d ROWS RETURN count(*) AS c";
  private static final String WRITE_IN_BATCH =
      "MATCH (p:P) CALL { WITH p CREATE (:W {k: p.id}) RETURN p.name AS nm } IN TRANSACTIONS OF %d ROWS RETURN nm";
  // THE ROWS A BATCH COLLECTS FIT THE BUDGET, BUT THEY AND THE ROWS RESERVED TO SERVE THEM DO NOT. MEASURED: 450 AND 700 ROWS ARE
  // REFUSED WHEN THE ANSWERS ARE RESERVED, 1000 ROWS ALREADY WHILE THEY ARE COLLECTED
  private static final int    WRITE_BATCH    = 600;
  private static final String BIG_IN_BATCH   =
      "MATCH (p:P) CALL { WITH p RETURN p.name AS nm } IN TRANSACTIONS OF %d ROWS RETURN nm";

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("N");
    database.getSchema().createVertexType("P");
    database.getSchema().createVertexType("W");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("N").set("id", i).save();
      for (int i = 0; i < BIG_ROWS; i++)
        database.newVertex("P").set("id", i, "name", "name-%06d-%s".formatted(i, FILLER)).save();
    });
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, LIMIT);
  }

  @Test
  void aProfiledScanWithMoreRowsThanTheElementLimitIsRefused() {
    assertThatThrownBy(() -> drain(PROFILE_SCAN)).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(LIMIT_SETTING).hasMessageContaining("(" + LIMIT + ")");
  }

  @Test
  void aProfiledUnionWithMoreRowsThanTheElementLimitIsRefused() {
    assertThatThrownBy(() -> drain(PROFILE_SCAN + " UNION ALL MATCH (n:N) RETURN n.id AS id"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining(LIMIT_SETTING);
  }

  @Test
  void aProfiledStatementWithinTheLimitAnswersEveryRowAndItsPlan() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10_000L);
    try (final ResultSet rs = database.query("opencypher", PROFILE_SCAN)) {
      assertThat(rs.getExecutionPlan()).isPresent();
      long rows = 0;
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
      assertThat(rows).isEqualTo(ROWS);
    }
  }

  @Test
  void theRowsOfAProfiledStatementAreHeldAgainstTheHeapBudgetUntilItsResultIsClosed() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);

    final ResultSet rs = database.query("opencypher", PROFILE_BIG);
    assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThan(0L);
    rs.close();
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  @Test
  void aProfiledStatementThatOutgrowsTheHeapBudgetIsRefusedAndGivesItsHeapBack() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(BUDGET_MB);

    assertThatThrownBy(() -> drain(PROFILE_BIG)).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  @Test
  void aBatchOfOuterRowsLargerThanTheElementLimitIsRefused() {
    assertThatThrownBy(() -> drain(SCAN_IN_BATCH.formatted(1_000))).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(LIMIT_SETTING).hasMessageContaining("(" + LIMIT + ")");
  }

  @Test
  void aBatchRefusedByTheLimitsFailsTheStatementWhateverTheErrorModeSays() {
    for (final String mode : new String[] { "FAIL", "CONTINUE", "BREAK" }) {
      // The batch itself is too large, and the rows its body answers are too many
      assertThatThrownBy(() -> drain(SCAN_IN_BATCH.formatted(1_000).replace("RETURN v", "ON ERROR " + mode + " RETURN v")))
          .as(mode).isInstanceOf(CommandExecutionException.class).hasMessageContaining(LIMIT_SETTING);
      assertThatThrownBy(() -> drain(
          "UNWIND [1, 2] AS i CALL { MATCH (n:N) RETURN n.id AS v } IN TRANSACTIONS OF 1 ROWS ON ERROR " + mode + " RETURN i, v"))
          .as(mode).isInstanceOf(CommandExecutionException.class).hasMessageContaining(LIMIT_SETTING);
    }
  }

  @Test
  void theInnerRowsOfOneOuterRowAreBoundedByTheElementLimit() {
    assertThatThrownBy(
        () -> drain("UNWIND [1, 2] AS i CALL { MATCH (n:N) RETURN n.id AS v } IN TRANSACTIONS OF 1 ROWS RETURN i, v"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining(LIMIT_SETTING);
  }

  @Test
  void batchesWithinTheElementLimitRunUntilTheLastRow() {
    assertThat(drain(SCAN_IN_BATCH.formatted(5))).isEqualTo(ROWS);
    assertThat(drain(SCAN_IN_BATCH.formatted(1))).isEqualTo(ROWS);
  }

  @Test
  void theOuterRowsOfABatchCountWithTheRowsItsBodiesAnswered() {
    // Five outer rows and five answers are ten rows held: within the limit. Six and six are twelve
    assertThat(drain(SCAN_IN_BATCH.formatted(5))).isEqualTo(ROWS);
    assertThatThrownBy(() -> drain(SCAN_IN_BATCH.formatted(6))).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(LIMIT_SETTING);
  }

  @Test
  void theRowKeptForAnOuterRowOfABodyWithoutAnswersCountsToo() {
    // Six outer rows and the six rows kept for them are twelve rows held: refused before the batch commits. Five and five are ten
    assertThatThrownBy(() -> drainCommand(UNIT_IN_BATCH.formatted(6))).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(LIMIT_SETTING);
    assertThat(countOf("W")).isZero();

    assertThat(drainCommand(UNIT_IN_BATCH.formatted(5))).isEqualTo(1);
    assertThat(countOf("W")).isEqualTo(ROWS);
  }

  @Test
  void aBatchWhoseAnswersOutgrowTheHeapBudgetIsRefusedBeforeItCommits() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(BUDGET_MB);

    // What the batch collects fits the budget, what it then holds to serve does not: the writes of the batch are not committed
    assertThatThrownBy(() -> drainCommand(WRITE_IN_BATCH.formatted(WRITE_BATCH))).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining("CALL subquery batch");
    assertThat(countOf("W")).isZero();
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  @Test
  void aBatchThatOutgrowsTheHeapBudgetIsRefusedAndGivesItsHeapBack() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(BUDGET_MB);

    assertThatThrownBy(() -> drain(BIG_IN_BATCH.formatted(100_000))).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  @Test
  void smallBatchesRunWithinTheHeapBudgetAndGiveItBackWhenTheResultIsClosed() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(BUDGET_MB);

    assertThat(drain(BIG_IN_BATCH.formatted(100))).isEqualTo(BIG_ROWS);
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  @Test
  void theEagerBarrierPullsItsInputInBoundedBatches() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final BatchHintProbe probe = new BatchHintProbe(context);
    final EagerStep eager = new EagerStep(context);
    eager.setPrevious(probe);

    try (final ResultSet rs = eager.syncPull(context, 50)) {
      assertThat(rs.hasNext()).isTrue();
    }
    assertThat(probe.hints).containsExactly(AbstractExecutionStep.DEFAULT_FETCH_RECORDS_PER_PULL);
  }

  private long countOf(final String type) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:" + type + ") RETURN count(v) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long drainCommand(final String query) {
    long rows = 0;
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private long drain(final String query) {
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  /** An input that records the batch size each pull asks for and answers one row. */
  private static final class BatchHintProbe extends AbstractExecutionStep {
    private final List<Integer> hints = new ArrayList<>();

    BatchHintProbe(final CommandContext context) {
      super(context);
    }

    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) {
      hints.add(nRecords);
      final InternalResultSet rows = new InternalResultSet();
      rows.add(new ResultInternal());
      return rows;
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return "PROBE";
    }
  }
}
