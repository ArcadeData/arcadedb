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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10049: with APOC's default uniqueness a walk returns every trail, and the trails of a graph with cycles or several
 * routes to a node are exponential in the depth. The walk is therefore a plain iterator: a {@code limit} ends it, the command
 * timeout ends it, and the branches it holds are charged to the query's heap, so a walk that cannot finish fails the query
 * instead of the server.
 * <p>
 * The graph is a chain of 40 diamonds, {@code j0 -> (top|bottom) -> j1 -> (top|bottom) -> j2 ...}: 2^40 trails from
 * {@code j0} to {@code j40}, which nothing enumerates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10049PathExpansionLimitsTest {
  private static final int DIAMONDS = 40;

  private static final String FROM_START = "MATCH (s:J {name: 'j0'}) ";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue10049-path-expansion-limits");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("J");
    database.getSchema().createVertexType("Z");
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
      // A node no trail reaches, for a walk that is asked for paths ending there
      database.newVertex("Z").set("name", "unreachable").save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
      database.drop();
    }
  }

  @Test
  @Timeout(120)
  void aLimitEndsAWalkWhoseAnswerNothingCouldEnumerate() {
    assertThat(count("{relationshipFilter: 'E>', limit: 5}")).isEqualTo(5);
    assertThat(count("{relationshipFilter: 'E>', limit: 5, bfs: false}")).isEqualTo(5);
    assertThat(count("{relationshipFilter: 'E>', limit: 5, uniqueness: 'NODE_PATH', bfs: false}")).isEqualTo(5);
  }

  @Test
  @Timeout(120)
  void theNodeGlobalWalkOfTheSameGraphIsNotExponential() {
    // Each of the 121 nodes once
    assertThat(count("{relationshipFilter: 'E>', uniqueness: 'NODE_GLOBAL'}")).isEqualTo(3 * DIAMONDS + 1);
  }

  @Test
  @Timeout(120)
  void theCommandTimeoutEndsAWalkThatFindsNoPath() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 500L);
    // Asked for the paths that end at a node no trail reaches, the depth-first walk meets 2^40 trails and returns none: it
    // spends its whole time inside one call, so only a check inside the walk can stop it
    assertThatThrownBy(() -> count("{relationshipFilter: 'E>', bfs: false, endNodes: [z]}", ", (z:Z {name: 'unreachable'})"))
        .satisfies(e -> assertThat(causeOfType(e)).as("got: %s", e).isNotNull());
  }

  @Test
  @Timeout(120)
  void aWalkTooWideForTheHeapFailsTheQueryNotTheServer() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);
    // The frontier of the trail walk doubles with every diamond
    assertThatThrownBy(() -> count("{relationshipFilter: 'E>'}")).hasStackTraceContaining("in-heap path expansion")
        .hasStackTraceContaining("arcadedb.queryMaxHeapElementsAllowedPerOp");
    // The depth-first walk holds one branch per level of the path it is on, and the node-global one two nodes per level
    assertThat(count("{relationshipFilter: 'E>', uniqueness: 'NODE_GLOBAL'}")).isEqualTo(3 * DIAMONDS + 1);
    assertThat(count("{relationshipFilter: 'E>', limit: 10, bfs: false}")).isEqualTo(10);
  }

  @Test
  @Timeout(120)
  void closingTheStreamGivesBackTheHeapOfAWalkThatWasNotReadToTheEnd() {
    final long previous = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L << 30);
    try {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final Vertex start = database.query("sql", "SELECT FROM J WHERE name = 'j0'").next().getVertex().get();

      try (final Stream<Result> rows = new PathExpandConfig().execute(new Object[] { start, Map.of("relationshipFilter", "E>") }, null,
          context)) {
        final Iterator<Result> iterator = rows.iterator();
        // The frontier of the trail walk doubles with every diamond: a few hundred paths in, it holds a few hundred branches
        for (int i = 0; i < 1000; i++)
          iterator.next();
        assertThat(context.getQueryHeapTracker().getUsedBytes()).as("the walk holds its frontier").isGreaterThan(0L);
      }
      assertThat(context.getQueryHeapTracker().getUsedBytes()).as("closing the stream gave it back").isZero();
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previous);
    }
  }

  private long count(final String config) {
    return count(config, "");
  }

  private long count(final String config, final String extraMatch) {
    try (final ResultSet rs = database.query("opencypher",
        FROM_START + extraMatch + " CALL path.expandConfig(s, " + config + ") YIELD path RETURN count(path) AS n")) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private static Throwable causeOfType(final Throwable throwable) {
    for (Throwable cause = throwable; cause != null; cause = cause.getCause())
      if (cause instanceof TimeoutException)
        return cause;
    return null;
  }
}
