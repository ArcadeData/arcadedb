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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10150: a {@code CALL path.expandConfig(...) ... LIMIT n} kept its frontier charged to the shared query-heap budget after
 * the ResultSet was closed, because the release hangs on the procedure's {@link Stream#close()} and nothing in the engine closed the
 * stream: {@code CallStep} turned it into an iterator and let the stream object go. The guarantee is one level up from the
 * procedure: whatever stream a procedure answers, closing the ResultSet of the query closes it, so a procedure that registers an
 * {@code onClose} cannot leak by being called from Cypher.
 * <p>
 * The graph is a chain of 40 diamonds, {@code j0 -> (top|bottom) -> j1 -> ...}: 2^40 trails from {@code j0}, so a walk read to a
 * {@code LIMIT} is far from its end and holds a frontier of thousands of branches.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10150ProcedureStreamClosedByCallTest extends TestHelper {
  private static final int DIAMONDS = 40;

  private static final String WALK = "path.expandConfig(s, {relationshipFilter: 'E>'}) YIELD path";

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("J");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      MutableVertex junction = database.newVertex("J").set("name", "j0").save();
      for (int i = 0; i < DIAMONDS; i++) {
        final MutableVertex top = database.newVertex("J").set("name", "t" + i).save();
        final MutableVertex bottom = database.newVertex("J").set("name", "b" + i).save();
        final MutableVertex next = database.newVertex("J").set("name", "j" + (i + 1)).save();
        junction.newEdge("E", top).save();
        junction.newEdge("E", bottom).save();
        top.newEdge("E", next).save();
        bottom.newEdge("E", next).save();
        junction = next;
      }
    });
    // The budget is on: the frontier of a walk is charged to it. TestHelper puts the setting back after the test
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L << 30);
  }

  @Test
  @Timeout(120)
  void theIssueReproduction() {
    final long before = QueryHeapBudget.getReservedBytes();
    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH (s:J {name: 'j0'}) CALL " + WALK + " RETURN path LIMIT 20000")) {
      assertThat(drain(resultSet)).isEqualTo(20000);
      assertThat(QueryHeapBudget.getReservedBytes()).as("the walk holds its frontier while the rows are read").isGreaterThan(before);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).as("closing the ResultSet gave the frontier back").isLessThanOrEqualTo(before);
  }

  @Test
  @Timeout(120)
  void aStandaloneCallGivesItBackToo() {
    final Vertex start = database.query("sql", "SELECT FROM J WHERE name = 'j0'").next().getVertex().get();
    final long before = QueryHeapBudget.getReservedBytes();
    try (final ResultSet resultSet = database.query("opencypher", "CALL path.expandConfig($s, {relationshipFilter: 'E>'}) YIELD path RETURN path LIMIT 20000",
        Map.of("s", start))) {
      assertThat(drain(resultSet)).isEqualTo(20000);
      assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThan(before);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(before);
  }

  @Test
  @Timeout(120)
  void aConsumerThatStopsBeforeAnyLimitGivesItBack() {
    final long before = QueryHeapBudget.getReservedBytes();
    try (final ResultSet resultSet = database.query("opencypher", "MATCH (s:J {name: 'j0'}) CALL " + WALK + " RETURN path")) {
      for (int i = 0; i < 20000; i++)
        resultSet.next();
      assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThan(before);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(before);
  }

  @Test
  @Timeout(120)
  void everyWalkOfAChainedCallGivesItsFrontierBack() {
    final long before = QueryHeapBudget.getReservedBytes();
    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH (s:J) WHERE s.name IN ['j0', 'j10', 'j20'] CALL " + WALK + " RETURN path LIMIT 20000")) {
      assertThat(drain(resultSet)).isEqualTo(20000);
      assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThan(before);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(before);
  }

  /** Whatever stream a procedure answers is closed with the ResultSet: the guarantee does not depend on the procedure. */
  @Test
  void theStreamOfAnyProcedureIsClosedWithTheResultSet() {
    final AtomicInteger closed = new AtomicInteger();
    final List<CypherProcedure> procedures = registerProcedures(closed);
    try {
      try (final ResultSet resultSet = database.query("opencypher", "CALL issue10150.endless() YIELD n RETURN n LIMIT 3")) {
        assertThat(drain(resultSet)).isEqualTo(3);
        assertThat(closed.get()).as("the stream is still open while the ResultSet is").isZero();
      }
      assertThat(closed.get()).isEqualTo(1);

      // The same when the stream is read to the end: closed once, by whoever gets there first
      closed.set(0);
      try (final ResultSet resultSet = database.query("opencypher", "CALL issue10150.finite() YIELD n RETURN n")) {
        assertThat(drain(resultSet)).isEqualTo(5);
      }
      assertThat(closed.get()).isEqualTo(1);

      // And one per invocation of a chained call: every input row runs the procedure and owns its stream
      closed.set(0);
      try (final ResultSet resultSet = database.query("opencypher",
          "UNWIND [1, 2, 3] AS i CALL issue10150.endless() YIELD n RETURN i, n LIMIT 4")) {
        assertThat(drain(resultSet)).isEqualTo(4);
      }
      assertThat(closed.get()).isEqualTo(3);
    } finally {
      unregister(procedures);
    }
  }

  @Test
  void aFailureWhileReadingTheStreamClosesItToo() {
    final AtomicInteger closed = new AtomicInteger();
    final List<CypherProcedure> procedures = registerProcedures(closed);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet resultSet = database.query("opencypher", "CALL issue10150.failing() YIELD n RETURN n")) {
          drain(resultSet);
        }
      }).hasStackTraceContaining("boom");
      assertThat(closed.get()).isEqualTo(1);
    } finally {
      unregister(procedures);
    }
  }

  private static int drain(final ResultSet resultSet) {
    int rows = 0;
    while (resultSet.hasNext()) {
      resultSet.next();
      ++rows;
    }
    return rows;
  }

  private static List<CypherProcedure> registerProcedures(final AtomicInteger closed) {
    final List<CypherProcedure> procedures = List.of(
        new ClosingProcedure("issue10150.endless", closed, () -> Stream.iterate(0, n -> n + 1).map(Issue10150ProcedureStreamClosedByCallTest::row)),
        new ClosingProcedure("issue10150.finite", closed, () -> IntStream.range(0, 5).boxed().map(Issue10150ProcedureStreamClosedByCallTest::row)),
        new ClosingProcedure("issue10150.failing", closed, () -> Stream.iterate(0, n -> n + 1).map(n -> {
          if (n == 2)
            throw new IllegalStateException("boom");
          return row(n);
        })));
    for (final CypherProcedure procedure : procedures)
      CypherProcedureRegistry.registerOrReplace(procedure);
    return procedures;
  }

  private static void unregister(final List<CypherProcedure> procedures) {
    for (final CypherProcedure procedure : procedures)
      CypherProcedureRegistry.unregister(procedure.getName());
  }

  private static Result row(final int n) {
    final ResultInternal row = new ResultInternal();
    row.setProperty("n", n);
    return row;
  }

  /** A procedure whose stream registers an {@code onClose} that counts, as {@code AbstractPathProcedure.pathRows} does to give its heap back. */
  private static final class ClosingProcedure implements CypherProcedure {
    private final String               name;
    private final AtomicInteger        closed;
    private final Supplier<Stream<Result>> rows;

    ClosingProcedure(final String name, final AtomicInteger closed, final Supplier<Stream<Result>> rows) {
      this.name = name;
      this.closed = closed;
      this.rows = rows;
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
      return 0;
    }

    @Override
    public String getDescription() {
      return "Test procedure whose stream counts its close()";
    }

    @Override
    public List<String> getYieldFields() {
      return List.of("n");
    }

    @Override
    public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
      return rows.get().onClose(closed::incrementAndGet);
    }
  }
}
