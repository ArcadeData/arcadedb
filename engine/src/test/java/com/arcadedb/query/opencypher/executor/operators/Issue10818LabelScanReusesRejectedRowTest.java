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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10818: the openCypher label scan built a {@link ResultInternal} and its {@code LinkedHashMap} for every vertex it
 * read, before the pushed WHERE predicate looked at it, and threw both away when the predicate rejected the vertex. The
 * scan (sequential, and each unit of a parallel one) now rebinds the row the predicate rejected last to the next vertex,
 * so only a match costs a row; the index nested-loop join does the same on its sought side. A row the evaluation wrote
 * anything to is never reused, and a row handed out is never touched again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10818LabelScanReusesRejectedRowTest extends TestHelper {
  private static final int ROWS   = 20_000;
  private static final int GROUPS = 100;
  private static final int PEOPLE = 3;

  @Override
  protected void beginTest() {
    // Small units, so the fixture's pages are cut in many of them and each unit's mapper reuses its own row
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createVertexType("D", 4);
    final VertexType city = database.getSchema().createVertexType("City", 1);
    city.createProperty("code", String.class);
    city.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "code");
    database.getSchema().createVertexType("Person", 1);

    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("D").set("id", i, "grp", i % GROUPS).save();
      // 20 cities per code, one of which is in IT: the join seeks 20 and its node predicate rejects 19
      for (int i = 0; i < 2_000; i++)
        database.newVertex("City").set("id", i, "code", "C" + (i % 100), "country", i % 20 == 0 ? "IT" : "FR").save();
      // few people against many cities: seeking the cities is cheaper than reading them all
      for (int i = 0; i < PEOPLE; i++)
        database.newVertex("Person").set("id", i, "cityCode", "C" + i).save();
    });
  }

  /** A row with one binding and nothing else is rebound in place; any other state refuses the rebind. */
  @Test
  void onlyARowWithTheOneBindingIsRebound() {
    final ResultInternal row = new ResultInternal();
    row.setProperty("n", 1);
    assertThat(row.rebindSingleProperty("n", 2)).isTrue();
    assertThat((Object) row.getProperty("n")).isEqualTo(2);
    assertThat(row.getPropertyNames()).containsExactly("n");

    assertThat(row.rebindSingleProperty("m", 3)).as("another name").isFalse();

    final ResultInternal twoBindings = new ResultInternal();
    twoBindings.setProperty("n", 1);
    twoBindings.setProperty("x", 1);
    assertThat(twoBindings.rebindSingleProperty("n", 2)).isFalse();
    assertThat((Object) twoBindings.getProperty("n")).isEqualTo(1);

    final ResultInternal temporary = new ResultInternal();
    temporary.setProperty("n", 1);
    temporary.setTemporaryProperty("t", 1);
    assertThat(temporary.rebindSingleProperty("n", 2)).isFalse();

    final ResultInternal withMetadata = new ResultInternal();
    withMetadata.setProperty("n", 1);
    withMetadata.setMetadata("m", 1);
    assertThat(withMetadata.rebindSingleProperty("n", 2)).isFalse();

    final ResultInternal removed = new ResultInternal();
    removed.setProperty("n", 1);
    removed.removeProperty("x");
    assertThat(removed.rebindSingleProperty("n", 2)).isFalse();

    final ResultInternal subclass = new ResultInternal() {
    };
    subclass.setProperty("n", 1);
    assertThat(subclass.rebindSingleProperty("n", 2)).isFalse();

    assertThat(new ResultInternal(database).setProperty("n", 1).rebindSingleProperty("n", 2)).as("a row of a database").isFalse();
  }

  /** The row a predicate rejected carries the next vertex; a row that passed is handed out and never rebound. */
  @Test
  void theRejectedRowIsReusedAndAMatchIsNot() {
    final List<Vertex> vertices = vertices(10);
    final CommandContext context = new BasicCommandContext().setDatabase(database);
    final NodeByLabelScan.VertexRowFilter rows = new NodeByLabelScan.VertexRowFilter("n", grpBelow(5));

    // ids 0..9 have grp 0..9: the first five pass, the last five are rejected
    final List<Result> passed = new ArrayList<>();
    for (final Vertex vertex : vertices.subList(0, 5)) {
      final Result row = rows.map(vertex, context);
      assertThat(row).isNotNull();
      assertThat(passed).as("a fresh row per match").noneMatch(r -> r == row);
      passed.add(row);
    }

    final List<Vertex> rejected = new ArrayList<>();
    for (final Vertex vertex : vertices.subList(5, 10)) {
      assertThat(rows.map(vertex, context)).isNull();
      rejected.add(vertex);
    }

    // a match reuses the row the last rejection left, now bound to the vertex that matched
    final Vertex matching = vertices.get(1);
    final Result reused = rows.map(matching, context);
    assertThat((Object) reused.getProperty("n")).isEqualTo(matching);
    assertThat(passed).noneMatch(r -> r == reused);

    // and the rows handed out before still carry their own vertices
    for (int i = 0; i < passed.size(); i++)
      assertThat(((Vertex) passed.get(i).getProperty("n")).getIdentity()).isEqualTo(vertices.get(i).getIdentity());
  }

  /** A predicate that writes to the row it is given leaves a row that is not reused for the next vertex. */
  @Test
  void aRowThePredicateWroteToIsNotReused() {
    final List<Vertex> vertices = vertices(3);
    final CommandContext context = new BasicCommandContext().setDatabase(database);
    final List<Result> seen = new ArrayList<>();
    final BooleanExpression writing = new BooleanExpression() {
      @Override
      public boolean evaluate(final Result result, final CommandContext ctx) {
        seen.add(result);
        ((ResultInternal) result).setProperty("extra", 1);
        return false;
      }

      @Override
      public String getText() {
        return "writes";
      }
    };
    final NodeByLabelScan.VertexRowFilter rows = new NodeByLabelScan.VertexRowFilter("n", writing);
    for (final Vertex vertex : vertices)
      assertThat(rows.map(vertex, context)).isNull();

    assertThat(seen).hasSize(3);
    assertThat(new HashSet<>(seen)).as("a new row per vertex").hasSize(3);
    for (int i = 0; i < seen.size(); i++)
      assertThat(((Vertex) seen.get(i).getProperty("n")).getIdentity()).isEqualTo(vertices.get(i).getIdentity());
  }

  /** A rejected vertex costs no row: measured on this JVM against building one per vertex, as the scan used to. */
  @Test
  void aRejectedVertexAllocatesNoRow() {
    final List<Vertex> vertices = vertices(1_000);
    final CommandContext context = new BasicCommandContext().setDatabase(database);
    final BooleanExpression rejectAll = new BooleanExpression() {
      @Override
      public boolean evaluate(final Result result, final CommandContext ctx) {
        return result.getProperty("n") == null;
      }

      @Override
      public String getText() {
        return "false";
      }
    };
    final NodeByLabelScan.VertexRowFilter rows = new NodeByLabelScan.VertexRowFilter("n", rejectAll);
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();
    // the rows built are kept, so the JIT cannot drop the allocations it is asked to measure
    final Result[] kept = new Result[64];
    int matched = 0;

    for (int round = 0; round < 5; round++) {
      final long beforeOld = threads.getThreadAllocatedBytes(threadId);
      for (int r = 0; r < 20; r++)
        for (int i = 0; i < vertices.size(); i++) {
          final ResultInternal row = new ResultInternal();
          row.setProperty("n", vertices.get(i));
          if (rejectAll.evaluate(row, context))
            ++matched;
          kept[i % kept.length] = row;
        }
      final long old = threads.getThreadAllocatedBytes(threadId) - beforeOld;

      final long beforeNew = threads.getThreadAllocatedBytes(threadId);
      for (int r = 0; r < 20; r++)
        for (final Vertex vertex : vertices)
          if (rows.map(vertex, context) != null)
            ++matched;
      final long now = threads.getThreadAllocatedBytes(threadId) - beforeNew;

      if (round == 4)
        assertThat(now).as("bytes of %d rejected vertices, against %d with a row per vertex", 20 * vertices.size(), old)
            .isLessThan(old / 10);
    }
    assertThat(matched).isZero();
    assertThat(kept[0]).isNotNull();
  }

  /** The sequential and the parallel label scan answer every matching vertex once, each row binding its own. */
  @Test
  void sequentialAndParallelScansAnswerEveryMatchOnce() {
    final String query = "MATCH (n:D) WHERE n.grp = $g RETURN n";
    for (final boolean parallel : new boolean[] { false, true }) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
      try {
        assertThat(profile(query, Map.of("g", 7))).contains("NodeByLabelScan").contains("[filter:");
        if (parallel)
          assertThat(profile(query, Map.of("g", 7))).contains("[parallel]");
        for (final int g : new int[] { 0, 7, 99 }) {
          final List<Integer> ids = new ArrayList<>();
          try (final ResultSet rs = database.query("opencypher", query, Map.of("g", g))) {
            while (rs.hasNext())
              ids.add(((Document) rs.next().getProperty("n")).getInteger("id"));
          }
          assertThat(ids).as("parallel=%s, grp=%d", parallel, g).hasSize(ROWS / GROUPS).doesNotHaveDuplicates()
              .allSatisfy(id -> assertThat(id % GROUPS).isEqualTo(g));
        }
        try (final ResultSet rs = database.query("opencypher", "MATCH (n:D) WHERE n.grp = $g RETURN count(n) AS c", Map.of("g", 3))) {
          assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(ROWS / GROUPS);
        }
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
      }
    }
  }

  /** The join's sought side, whose node predicate rejects most of what the index answers, joins each person to its city. */
  @Test
  void indexNestedLoopJoinRebindsOnlyRejectedRows() {
    final String query = "MATCH (p:Person), (c:City) WHERE c.code = p.cityCode AND c.country = 'IT' RETURN p.id AS p, c.id AS c";
    assertThat(profile(query, Map.of())).contains("IndexNestedLoopJoin(c:City)");

    final Set<String> pairs = new HashSet<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(pairs.add(row.getProperty("p") + "->" + row.getProperty("c"))).isTrue();
      }
    }
    // person i lives in code Ci, whose only IT city is the one with id % 20 == 0 among the ids that are i modulo 100
    final Set<String> expected = new HashSet<>();
    for (int i = 0; i < 2_000; i++)
      if (i % 20 == 0 && i % 100 < PEOPLE)
        expected.add((i % 100) + "->" + i);
    assertThat(expected).isNotEmpty();
    assertThat(pairs).isEqualTo(expected);
  }

  private List<Vertex> vertices(final int count) {
    final List<Vertex> vertices = new ArrayList<>(count);
    try (final ResultSet rs = database.query("sql", "SELECT FROM D WHERE id < ? ORDER BY id", count)) {
      while (rs.hasNext())
        vertices.add(rs.next().getVertex().get());
    }
    assertThat(vertices).hasSize(count);
    return vertices;
  }

  private static BooleanExpression grpBelow(final int bound) {
    return new BooleanExpression() {
      @Override
      public boolean evaluate(final Result result, final CommandContext context) {
        return ((Vertex) result.getProperty("n")).getInteger("grp") < bound;
      }

      @Override
      public String getText() {
        return "n.grp < " + bound;
      }
    };
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
