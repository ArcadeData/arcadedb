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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.ResultRowCapScope;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A statement with a LIMIT, an ORDER BY or a DISTINCT is planned once per statement text and its plan is copied for each execution, as
 * a statement with only a WHERE and a projection is. The steps that sort, cut and deduplicate used to refuse the copy, so every
 * execution of such a statement planned it again. The expand() step accepts the copy too, which serves the plans copied outside the
 * statement cache (a LET subquery) and the per-execution state of every one of these steps is checked on copies of a plan that ran.
 */
class PlanCacheLimitOrderDistinctTest extends TestHelper {
  private static final int RECORDS = 200;
  private static final int NODES   = 12;
  // binds every parameter any statement below reads, so that a statement can be run once just to plan it
  private static final Map<String, Object> ANY_PARAMETERS = Map.of("g", 4, "n", 2, "id", 3, "dir", "ASC");
  // the parameters of two executions that must not see each other
  private static final Map<String, Object> FIRST          = Map.of("g", 1, "id", 7, "n", 5, "dir", "ASC");
  private static final Map<String, Object> SECOND         = Map.of("g", 2, "id", 3, "n", 5, "dir", "DESC");

  /** One statement, the column it is read through, and whether the statement cache keeps its plan. */
  private record Shape(String sql, String column, boolean cached) {
  }

  private static final List<Shape> SHAPES = List.of(
      new Shape("SELECT FROM Item WHERE grp = :g LIMIT 7", "seq", true),
      new Shape("SELECT FROM Item WHERE grp = :g SKIP 0 LIMIT 7", "seq", true),
      new Shape("SELECT FROM Item WHERE grp = :g ORDER BY seq DESC LIMIT 7", "seq", true),
      new Shape("SELECT FROM Item WHERE grp = :g ORDER BY seq", "seq", true),
      new Shape("SELECT FROM Item WHERE grp = :g ORDER BY seq :dir LIMIT 7", "seq", true),
      new Shape("SELECT DISTINCT tag FROM Item WHERE grp = :g ORDER BY tag", "tag", true),
      new Shape("SELECT DISTINCT tag FROM Item WHERE grp = :g LIMIT 10", "tag", true),
      new Shape("SELECT DISTINCT * FROM Item WHERE grp = :g LIMIT 1000", "seq", true),
      // a statement that calls expand() is not kept by the statement cache (see anExpandStatementIsNotKeptByTheStatementCache...)
      new Shape("SELECT expand(out('Link')) FROM Node WHERE id = :id", "id", false),
      new Shape("SELECT expand(vals) AS v FROM Item WHERE grp = :g", "v", false),
      new Shape("SELECT expand(vals) FROM Item WHERE grp = :g", "value", false));

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Item");
    database.getSchema().getType("Item").createProperty("grp", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Item", "grp");
    database.getSchema().getType("Item").createProperty("seq", Type.INTEGER);
    database.getSchema().createVertexType("Node");
    database.getSchema().getType("Node").createProperty("id", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Node", "id");
    database.getSchema().createEdgeType("Link");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Item").set("grp", i % 10).set("seq", i).set("tag", "t" + (i % 3)).set("vals", List.of(i, i * 1000)).save();

      final MutableVertex[] nodes = new MutableVertex[NODES];
      for (int i = 0; i < NODES; i++)
        nodes[i] = database.newVertex("Node").set("id", i).save();
      for (int i = 0; i < NODES; i++)
        for (final int target : linkedFrom(i))
          nodes[i].newEdge("Link", nodes[target]);
    });
  }

  @Test
  void aLimitedStatementIsPlannedOnce() {
    assertCachedAfterRun("SELECT FROM Item WHERE grp = :g LIMIT 3");
  }

  @Test
  void anOrderedStatementIsPlannedOnce() {
    assertCachedAfterRun("SELECT FROM Item WHERE grp = :g ORDER BY seq DESC LIMIT 4");
    assertCachedAfterRun("SELECT FROM Item WHERE grp = :g ORDER BY seq");
  }

  @Test
  void aDistinctStatementIsPlannedOnce() {
    // without an ORDER BY or a LIMIT the DISTINCT may be planned as a GROUP BY, a choice that depends on the size of the type
    assertCachedAfterRun("SELECT DISTINCT tag FROM Item WHERE grp = :g ORDER BY tag");
  }

  @Test
  void aSkipOfZeroIsPlannedOnceAndAnOffsetIsNot() {
    assertCachedAfterRun("SELECT FROM Item WHERE grp = :g SKIP 0 LIMIT 3");
    assertNotCachedAfterRun("SELECT FROM Item WHERE grp = :g SKIP 2 LIMIT 3");
  }

  @Test
  void aLimitReadFromAParameterIsPlannedForEveryExecution() {
    final String sql = "SELECT FROM Item WHERE grp = :g ORDER BY seq LIMIT :n";
    assertNotCachedAfterRun(sql);
    for (final int n : new int[] { 1, 5, 2, 7 })
      assertThat(seqs(sql, Map.of("g", 4, "n", n))).as("LIMIT %d", n).hasSize(n);
  }

  @Test
  void aCachedPlanAnswersEachExecutionWithItsOwnParameters() {
    final String top = "SELECT FROM Item WHERE grp = :g ORDER BY seq DESC LIMIT 4";
    final String asc = "SELECT FROM Item WHERE grp = :g ORDER BY seq";
    final String limited = "SELECT FROM Item WHERE grp = :g LIMIT 3";
    final String distinct = "SELECT DISTINCT tag FROM Item WHERE grp = :g ORDER BY tag";
    // planned by the first execution, copied by the others
    for (int round = 0; round < 3; round++)
      for (final int g : new int[] { 0, 7, 3, 7 }) {
        final List<Integer> own = ownSeqs(g);

        assertThat(seqs(top, Map.of("g", g))).as("%s g=%d", top, g).containsExactlyElementsOf(own.reversed().subList(0, 4));
        assertThat(seqs(asc, Map.of("g", g))).as("%s g=%d", asc, g).containsExactlyElementsOf(own);
        assertThat(seqs(limited, Map.of("g", g))).as("%s g=%d", limited, g).hasSize(3).isSubsetOf(own);

        final List<Object> tags = column(distinct, Map.of("g", g), "tag");
        assertThat(tags).doesNotHaveDuplicates().hasSize(own.stream().map(i -> i % 3).distinct().toList().size());
      }
  }

  @Test
  void anExpandStatementIsNotKeptByTheStatementCacheButItsPlanIsCopiedByALetSubquery() {
    // SelectStatement.executionPlanCanBeCached() refuses a statement whose projection calls expand(), which is not a registered
    // deterministic function, as it did before ExpandStep accepted the copy: what the step changes is every plan that is copied
    // outside the statement cache, the template a LET subquery runs for each row of its parent
    final String sql = "SELECT id, $linked AS linked, $vals AS vals FROM Node"
        + " LET $linked = (SELECT expand(out('Link')) FROM Node WHERE id = $parent.$current.id),"
        + " $vals = (SELECT expand(vals) AS v FROM Item WHERE grp = $parent.$current.id)";
    int rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final int id = row.getProperty("id");
        final List<Result> linked = row.getProperty("linked");
        final List<Result> vals = row.getProperty("vals");

        // the nodes 0, 4 and 8 have no edge and the nodes 10 and 11 have no item: an expansion of nothing between two that expand something
        assertThat(linked.stream().map(x -> x.<Integer>getProperty("id")).toList()).as("linked of id %d", id)
            .containsExactlyInAnyOrderElementsOf(linkedFrom(id));
        final List<Integer> expected = new ArrayList<>();
        if (id < 10)
          for (final int seq : ownSeqs(id)) {
            expected.add(seq);
            expected.add(seq * 1000);
          }
        // the alias of the expansion names the column of every row of every copy
        assertThat(vals).as("vals of id %d", id).allSatisfy(x -> assertThat(x.getPropertyNames()).containsExactly("v"));
        assertThat(vals.stream().map(x -> x.<Integer>getProperty("v")).toList()).as("vals of id %d", id)
            .containsExactlyInAnyOrderElementsOf(expected);
        rows++;
      }
      assertThat(rows).isEqualTo(NODES);

      final List<LetQueryStep> letSteps = rs.getExecutionPlan().orElseThrow().getSteps().stream().filter(LetQueryStep.class::isInstance)
          .map(LetQueryStep.class::cast).toList();
      assertThat(letSteps).as("the LET steps of the plan").hasSize(2);
      for (final LetQueryStep letStep : letSteps)
        assertThat(templateOf(letStep)).as("the plan LetQueryStep copies for the rows after the first").isNotNull();
    }
  }

  @Test
  void aDistinctWithoutAnOrderByDoesNotShareItsSeenSetBetweenExecutions() {
    // the DISTINCT of a type scan with no LIMIT may become a GROUP BY: with a LIMIT it stays the DistinctExecutionStep, with no sort before it
    final String byValue = "SELECT DISTINCT tag FROM Item WHERE grp = :g LIMIT 10";
    final String byIdentity = "SELECT DISTINCT * FROM Item WHERE grp = :g LIMIT 1000";
    for (final String sql : new String[] { byValue, byIdentity }) {
      assertThat(stepsOf(sql, Map.of("g", 4))).as(sql).contains("DistinctExecutionStep").doesNotContain("OrderByStep");
      assertCachedAfterRun(sql);
    }

    // every group holds the three tags: the second execution sees values the first has already met
    for (int round = 0; round < 3; round++)
      for (final int g : new int[] { 0, 3, 0, 6, 3 }) {
        assertThat(column(byValue, Map.of("g", g), "tag")).as("%s g=%d", byValue, g).containsExactlyInAnyOrder("t0", "t1", "t2");
        assertThat(column(byIdentity, Map.of("g", g), "seq")).as("%s g=%d", byIdentity, g).containsExactlyInAnyOrderElementsOf(ownSeqs(g));
      }
  }

  @Test
  void executionsOfACachedPlanThatAreOpenAtTheSameTimeKeepTheirOwnState() {
    for (final Shape shape : SHAPES) {
      if (!shape.cached)
        continue;
      // alone, the first execution plans the statement and the next ones copy the plan the cache keeps
      assertCachedAfterRun(shape.sql);
      final List<Object> expectedFirst = column(shape.sql, FIRST, shape.column);
      final List<Object> expectedSecond = column(shape.sql, SECOND, shape.column);
      assertThat(expectedFirst).as("%s alone with the first parameters", shape.sql).isNotEmpty();
      assertThat(expectedSecond).as("%s alone with the second parameters", shape.sql).isNotEmpty();

      final List<Object> first = new ArrayList<>();
      final List<Object> second = new ArrayList<>();
      try (final ResultSet rsFirst = database.query("sql", shape.sql, FIRST); final ResultSet rsSecond = database.query("sql", shape.sql, SECOND)) {
        // one row of each in turn: a counter, a buffer or a seen-set shared by the two copies mixes them up
        while (rsFirst.hasNext() || rsSecond.hasNext()) {
          if (rsFirst.hasNext())
            first.add(rsFirst.next().getProperty(shape.column));
          if (rsSecond.hasNext())
            second.add(rsSecond.next().getProperty(shape.column));
        }
      }
      assertThat(first).as("%s, first of two open executions", shape.sql).containsExactlyElementsOf(expectedFirst);
      assertThat(second).as("%s, second of two open executions", shape.sql).containsExactlyElementsOf(expectedSecond);
    }
  }

  @Test
  void aCopyOfAPlanThatAlreadyRanStartsFromScratch() {
    for (final Shape shape : SHAPES) {
      final List<Object> expectedFirst = column(shape.sql, FIRST, shape.column);
      final List<Object> expectedSecond = column(shape.sql, SECOND, shape.column);
      assertThat(expectedFirst).as("%s alone with the first parameters", shape.sql).isNotEmpty();
      assertThat(expectedSecond).as("%s alone with the second parameters", shape.sql).isNotEmpty();

      final InternalExecutionPlan plan = planOf(shape.sql, FIRST);
      assertThat(plan.canBeCached()).as("every step of the plan of %s accepts the copy", shape.sql).isTrue();
      assertThat(run(plan, shape.column)).as("%s, the plan itself", shape.sql).containsExactlyElementsOf(expectedFirst);

      // the plan that served an execution is the template of the next ones, as the plan of a LET subquery is (LetQueryStep)
      final InternalExecutionPlan copy = plan.copy(contextOf(SECOND));
      assertThat(run(copy, shape.column)).as("%s, a copy of the plan that ran", shape.sql).containsExactlyElementsOf(expectedSecond);
      assertThat(run(copy.copy(contextOf(FIRST)), shape.column)).as("%s, a copy of a copy that ran", shape.sql)
          .containsExactlyElementsOf(expectedFirst);
      assertThat(run(plan.copy(contextOf(FIRST)), shape.column)).as("%s, a second copy of the template", shape.sql)
          .containsExactlyElementsOf(expectedFirst);
    }
  }

  @Test
  void aSortDirectionReadFromAParameterIsReadByEachExecution() {
    final String sql = "SELECT FROM Item WHERE grp = :g ORDER BY seq :dir LIMIT 4";
    final List<Integer> own = ownSeqs(5);
    for (int round = 0; round < 3; round++) {
      assertThat(seqs(sql, Map.of("g", 5, "dir", "ASC"))).containsExactlyElementsOf(own.subList(0, 4));
      assertThat(seqs(sql, Map.of("g", 5, "dir", "DESC"))).containsExactlyElementsOf(own.reversed().subList(0, 4));
    }
  }

  @Test
  void aSubqueryWhoseLimitIsAParameterIsPlannedForEveryExecution() {
    final String sql = "SELECT seq FROM (SELECT FROM Item WHERE grp = :g ORDER BY seq LIMIT :n)";
    final List<Integer> own = ownSeqs(4);
    for (final int n : new int[] { 1, 5, 2, 7, 5, 1 })
      assertThat(seqs(sql, Map.of("g", 4, "n", n))).as("LIMIT %d", n).containsExactlyElementsOf(own.subList(0, n));
  }

  @Test
  void aLetSubqueryWhoseLimitDependsOnTheRowIsPlannedForEveryRow() {
    // the limit of the subquery is the id of the row: the sort the plan of the first row sized for its id must not serve the next rows
    final String sql = "SELECT id, (SELECT FROM Item WHERE grp = 1 ORDER BY seq LIMIT $parent.$current.id) AS xs FROM Node WHERE id IN [3, 1, 2, 5, 0]";
    final List<Integer> own = ownSeqs(1);
    int rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final int id = row.getProperty("id");
        final List<Result> xs = row.getProperty("xs");
        assertThat(xs.stream().map(x -> x.<Integer>getProperty("seq")).toList()).as("id %d", id).containsExactlyElementsOf(own.subList(0, id));
        rows++;
      }
    }
    assertThat(rows).isEqualTo(5);
  }

  @Test
  void aLetSubqueryWhoseSkipDependsOnTheRowIsPlannedForEveryRow() {
    // the subquery reads $parent in its SKIP only: it is correlated, so it runs for each row rather than once before the first one
    final String sql = "SELECT id, (SELECT FROM Item WHERE grp = 1 ORDER BY seq SKIP $parent.$current.id LIMIT 2) AS xs FROM Node WHERE id IN [3, 1, 0]";
    final List<Integer> own = ownSeqs(1);
    int rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final int id = row.getProperty("id");
        final List<Result> xs = row.getProperty("xs");
        assertThat(xs.stream().map(x -> x.<Integer>getProperty("seq")).toList()).as("id %d", id)
            .containsExactlyElementsOf(own.subList(id, id + 2));
        rows++;
      }
    }
    assertThat(rows).isEqualTo(3);
  }

  @Test
  void aLetSubqueryWithALimitAnOrderAndADistinctAnswersEveryRowInFull() {
    // the plan of the first row is built and run, the next rows run copies of it
    final String sql = "SELECT id, (SELECT FROM Item WHERE grp = 1 ORDER BY seq DESC LIMIT 3) AS top,"
        + " (SELECT DISTINCT tag FROM Item WHERE grp = 1 LIMIT 10) AS tags,"
        + " (SELECT expand(out('Link')) FROM Node WHERE id = $parent.$current.id) AS linked FROM Node";
    final List<Integer> own = ownSeqs(1);
    int rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final int id = row.getProperty("id");
        final List<Result> top = row.getProperty("top");
        final List<Result> tags = row.getProperty("tags");
        final List<Result> linked = row.getProperty("linked");
        assertThat(top.stream().map(x -> x.<Integer>getProperty("seq")).toList()).as("top of id %d", id)
            .containsExactlyElementsOf(own.reversed().subList(0, 3));
        assertThat(tags.stream().map(x -> x.<String>getProperty("tag")).toList()).as("tags of id %d", id).containsExactlyInAnyOrder("t0", "t1", "t2");
        assertThat(linked.stream().map(x -> x.<Integer>getProperty("id")).toList()).as("linked of id %d", id)
            .containsExactlyInAnyOrderElementsOf(linkedFrom(id));
        rows++;
      }
    }
    assertThat(rows).isEqualTo(NODES);
  }

  @Test
  void manyLiteralLimitsCannotGrowThePlanCachePastItsSize() {
    final int size = 4;
    final ExecutionPlanCache cache = ((DatabaseInternal) database).getExecutionPlanCache();
    cache.resize(size);
    final int distinctLimits = 12;
    for (int limit = 1; limit <= distinctLimits; limit++)
      run("SELECT FROM Item WHERE grp = :g LIMIT " + limit);

    // the cache is an LRU of at most `size` plans: only the most recent statements are still there
    final Set<Integer> held = IntStream.rangeClosed(1, distinctLimits).boxed()
        .filter(limit -> cache.contains("SELECT FROM Item WHERE grp = :g LIMIT " + limit)).collect(Collectors.toSet());
    assertThat(held).containsExactlyInAnyOrder(9, 10, 11, 12);
  }

  @Test
  void anOrderedPlanTakenFromTheCacheStillSortsOnlyTheRowsOfTheCallersCap() {
    final String sql = "SELECT FROM Item WHERE grp = :g ORDER BY seq";
    // planned without a cap, then copied for an execution whose caller reads 3 rows: the copy keeps the sort bound (#12016)
    assertCachedAfterRun(sql);
    final int cap = 3;
    final List<Integer> rows = new ArrayList<>();
    final String plan;
    try (final ResultRowCapScope ignored = ResultRowCapScope.open(cap);
        final ResultSet rs = database.query("sql", sql, Map.of("g", 5))) {
      while (rs.hasNext())
        rows.add(rs.next().getProperty("seq"));
      plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
    assertThat(plan).contains("buffer size: " + cap);
    assertThat(rows).containsExactlyElementsOf(ownSeqs(5).subList(0, cap));
  }

  private static List<Integer> linkedFrom(final int id) {
    final List<Integer> targets = new ArrayList<>();
    for (int k = 1; k <= id % 4; k++)
      targets.add((id + k) % NODES);
    return targets;
  }

  private static List<Integer> ownSeqs(final int g) {
    return IntStream.range(0, RECORDS).filter(i -> i % 10 == g).boxed().toList();
  }

  private void assertCachedAfterRun(final String sql) {
    run(sql);
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(sql)).as("plan cache holds: %s", sql).isTrue();
  }

  private void assertNotCachedAfterRun(final String sql) {
    run(sql);
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(sql)).as("plan cache holds: %s", sql).isFalse();
  }

  private void run(final String sql) {
    rows(sql, ANY_PARAMETERS);
  }

  private List<Integer> seqs(final String sql, final Map<String, Object> params) {
    return column(sql, params, "seq").stream().map(Integer.class::cast).toList();
  }

  private List<Object> column(final String sql, final Map<String, Object> params, final String name) {
    return rows(sql, params).stream().map(row -> row.getProperty(name)).toList();
  }

  private List<Result> rows(final String sql, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  /** The simple class names of the steps of the plan the statement runs with, outermost first. */
  private List<String> stepsOf(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<String> steps = rs.getExecutionPlan().orElseThrow().getSteps().stream().map(step -> step.getClass().getSimpleName()).toList();
      while (rs.hasNext())
        rs.next();
      return steps;
    }
  }

  private InternalExecutionPlan planOf(final String sql, final Map<String, Object> params) {
    final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
    return statement.createExecutionPlanNoCache(contextOf(params));
  }

  private BasicCommandContext contextOf(final Map<String, Object> params) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(params);
    return context;
  }

  private static List<Object> run(final InternalExecutionPlan plan, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = new LocalResultSet(plan)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }

  /** The plan a LET step keeps to copy for the rows after the first, null while it has none. */
  private static Object templateOf(final LetQueryStep letStep) {
    try {
      final Field field = LetQueryStep.class.getDeclaredField("cachedSubPlanTemplate");
      field.setAccessible(true);
      return field.get(letStep);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
