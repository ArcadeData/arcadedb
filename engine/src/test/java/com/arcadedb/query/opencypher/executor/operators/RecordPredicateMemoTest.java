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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A filter on the far end of a multi-hop MATCH sees the same vertex on many rows: {@link RecordPredicateMemo} lets it
 * evaluate a predicate once per record. These tests pin which predicates qualify, when the memo answers and gives up,
 * and that the rows a query returns do not change with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RecordPredicateMemoTest extends TestHelper {
  private static final int NODES = 40;

  private Vertex[] nodes;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");
    nodes = new Vertex[NODES];
    database.transaction(() -> {
      final MutableVertex[] created = new MutableVertex[NODES];
      for (int i = 0; i < NODES; i++) {
        created[i] = database.newVertex("N").set("id", i, "age", 30 + i % 20);
        // Every fourth vertex has no score: a comparison on it is null, neither true nor false
        if (i % 4 != 0)
          created[i].set("score", i);
        created[i].save();
      }
      for (int i = 0; i < NODES; i++)
        for (int j = i + 1; j < NODES; j++)
          created[i].newEdge("E", created[j]);
      for (int i = 0; i < NODES; i++)
        nodes[i] = created[i].getIdentity().asVertex();
    });
  }

  @Test
  void onlyAPredicateOneRecordDecidesQualifies() {
    final PropertyAccessExpression xAge = new PropertyAccessExpression("x", "age");
    final PropertyAccessExpression xId = new PropertyAccessExpression("x", "id");
    final PropertyAccessExpression yAge = new PropertyAccessExpression("y", "age");
    final LiteralExpression fortyOne = new LiteralExpression(41, "41");

    final ComparisonExpression older = new ComparisonExpression(xAge, ComparisonExpression.Operator.GREATER_THAN, fortyOne);
    final ComparisonExpression mirrored = new ComparisonExpression(fortyOne, ComparisonExpression.Operator.LESS_THAN, xAge);
    final ComparisonExpression byParameter = new ComparisonExpression(xAge, ComparisonExpression.Operator.GREATER_THAN,
        new ParameterExpression("min", "$min"));
    final ComparisonExpression low = new ComparisonExpression(xId, ComparisonExpression.Operator.LESS_THAN, fortyOne);
    final ComparisonExpression other = new ComparisonExpression(yAge, ComparisonExpression.Operator.GREATER_THAN, fortyOne);

    assertThat(RecordPredicateMemo.variableOf(older)).isEqualTo("x");
    assertThat(RecordPredicateMemo.variableOf(mirrored)).isEqualTo("x");
    assertThat(RecordPredicateMemo.variableOf(byParameter)).isEqualTo("x");
    assertThat(RecordPredicateMemo.variableOf(new LogicalExpression(LogicalExpression.Operator.AND, older, low))).isEqualTo("x");
    assertThat(RecordPredicateMemo.variableOf(new LogicalExpression(LogicalExpression.Operator.OR, older, low))).isEqualTo("x");
    assertThat(RecordPredicateMemo.variableOf(new LogicalExpression(LogicalExpression.Operator.NOT, older))).isEqualTo("x");

    // Two records, a value a row changes, a function that answers differently on each call, an unknown shape
    assertThat(RecordPredicateMemo.variableOf(new LogicalExpression(LogicalExpression.Operator.AND, older, other))).isNull();
    assertThat(RecordPredicateMemo.variableOf(
        new ComparisonExpression(xAge, ComparisonExpression.Operator.GREATER_THAN, yAge))).isNull();
    assertThat(RecordPredicateMemo.variableOf(
        new ComparisonExpression(xAge, ComparisonExpression.Operator.GREATER_THAN, new VariableExpression("limit")))).isNull();
    assertThat(RecordPredicateMemo.variableOf(new ComparisonExpression(xAge, ComparisonExpression.Operator.GREATER_THAN,
        new FunctionCallExpression("rand", List.of(), false)))).isNull();
    assertThat(RecordPredicateMemo.variableOf(new CountingPredicate())).isNull();
    assertThat(RecordPredicateMemo.variableOf(null)).isNull();
  }

  @Test
  void aRecordIsEvaluatedOnceAfterTheWarmUp() {
    final CountingPredicate predicate = new CountingPredicate();
    final RecordPredicateMemo memo = RecordPredicateMemo.of(predicate, "x", context(true));

    memo.startBatch(database);
    for (int i = 0; i < RecordPredicateMemo.WARM_UP_ROWS; i++)
      assertThat(memo.evaluate(row(nodes[i % 4]), null)).isEqualTo(answerOf(i % 4));
    assertThat(predicate.evaluations).isEqualTo(RecordPredicateMemo.WARM_UP_ROWS);
    assertThat(memo.isAnswering()).isFalse();

    memo.startBatch(database);
    assertThat(memo.isAnswering()).isTrue();
    predicate.evaluations = 0;
    for (int i = 0; i < 1_000; i++)
      assertThat(memo.evaluate(row(nodes[i % 4]), null)).isEqualTo(answerOf(i % 4));
    // true, false and null are all remembered
    assertThat(predicate.evaluations).isEqualTo(4);
    assertThat(memo.size()).isEqualTo(4);

    // A row that binds the variable to something that is no stored record is evaluated every time
    final ResultInternal unbound = new ResultInternal();
    unbound.setProperty("x", Map.of("id", 1));
    predicate.evaluations = 0;
    memo.evaluate(unbound, null);
    memo.evaluate(unbound, null);
    assertThat(predicate.evaluations).isEqualTo(2);
  }

  @Test
  void aStatementThatWritesHasNoMemoAndAnOpenTransactionDropsIt() {
    assertThat(RecordPredicateMemo.of(new CountingPredicate(), "x", context(false))).isNull();
    assertThat(RecordPredicateMemo.of(new CountingPredicate(), null, context(true))).isNull();

    final CountingPredicate predicate = new CountingPredicate();
    final RecordPredicateMemo memo = warmedUp(predicate);
    memo.evaluate(row(nodes[1]), null);
    predicate.evaluations = 0;

    // A transaction that changed nothing reads the committed state: the memo keeps answering
    database.begin();
    try {
      memo.startBatch(database);
      assertThat(memo.isAnswering()).isTrue();
    } finally {
      database.rollback();
    }

    database.begin();
    try {
      nodes[5].modify().set("age", 99).save();
      memo.startBatch(database);
      assertThat(memo.isAnswering()).isFalse();
      memo.evaluate(row(nodes[1]), null);
      memo.evaluate(row(nodes[1]), null);
      assertThat(predicate.evaluations).isEqualTo(2);
    } finally {
      database.rollback();
    }
    memo.startBatch(database);
    assertThat(memo.isAnswering()).isFalse();
    assertThat(memo.size()).isZero();
  }

  @Test
  void rowsThatNeverRepeatARecordDropIt() {
    final CountingPredicate predicate = new CountingPredicate();
    final RecordPredicateMemo memo = warmedUp(predicate);
    // 40 records on 4096 rows would be nearly all hits: bind each row to a record of its own instead
    database.transaction(() -> {
      for (int i = 0; i < RecordPredicateMemo.REVIEW_EVERY; i++)
        memo.evaluate(row(database.newVertex("N").set("id", 1_000 + i).save()), null);
    });
    assertThat(memo.isAnswering()).isFalse();
    assertThat(memo.size()).isZero();
  }

  /** The answers of filtered 3-hop MATCHes, far past the warm-up, are the ones counted by hand. */
  @Test
  void aFilteredThreeHopMatchAnswersWhatTheGraphHolds() {
    final String pattern = "MATCH (p:N)-[:E]-(:N)-[:E]-(:N)-[:E]-(x:N) WHERE p.id = 0 AND ";
    // Every vertex is the far end of some 3-hop path from 0 in a complete graph
    assertThat(count(pattern + "x.age > 41 RETURN count(DISTINCT x) AS n")).isEqualTo(distinct(i -> 30 + i % 20 > 41));
    assertThat(count(pattern + "NOT x.age > 41 RETURN count(DISTINCT x) AS n")).isEqualTo(distinct(i -> 30 + i % 20 <= 41));
    assertThat(count(pattern + "x.age > $min AND x.id < 20 RETURN count(DISTINCT x) AS n")).isEqualTo(
        distinct(i -> 30 + i % 20 > 41 && i < 20));
    assertThat(count(pattern + "(x.age > 45 OR x.id = 3) RETURN count(DISTINCT x) AS n")).isEqualTo(
        distinct(i -> 30 + i % 20 > 45 || i == 3));
    // A missing property compares to null: neither the predicate nor its negation keeps the row
    assertThat(count(pattern + "x.score >= 0 RETURN count(DISTINCT x) AS n")).isEqualTo(distinct(i -> i % 4 != 0));
    assertThat(count(pattern + "NOT x.score >= 0 RETURN count(DISTINCT x) AS n")).isZero();

    // The rows, not only the vertices: each far end is reached by the same number of paths, kept or dropped as a whole
    final long paths = count("MATCH (p:N)-[:E]-(:N)-[:E]-(:N)-[:E]-(x:N) WHERE p.id = 0 RETURN count(x) AS n");
    final long kept = count(pattern + "x.age > 41 RETURN count(x) AS n");
    final long dropped = count(pattern + "NOT x.age > 41 RETURN count(x) AS n");
    assertThat(kept + dropped).isEqualTo(paths);
    assertThat(kept).isPositive().isLessThan(paths);
  }

  /** Inside a transaction every row is evaluated on the image the transaction holds. */
  @Test
  void aFilterInsideATransactionReadsTheTransactionsOwnWrites() {
    final String query = "MATCH (p:N)-[:E]-(:N)-[:E]-(:N)-[:E]-(x:N) WHERE p.id = 0 AND x.age > 1000 RETURN count(DISTINCT x) AS n";
    assertThat(count(query)).isZero();
    database.begin();
    try {
      nodes[7].modify().set("age", 2_000).save();
      assertThat(count(query)).isEqualTo(1);
    } finally {
      database.rollback();
    }
    assertThat(count(query)).isZero();
  }

  private long distinct(final IntPredicate kept) {
    long n = 0;
    for (int i = 0; i < NODES; i++)
      if (kept.test(i))
        n++;
    return n;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of("min", 41))) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private RecordPredicateMemo warmedUp(final CountingPredicate predicate) {
    final RecordPredicateMemo memo = RecordPredicateMemo.of(predicate, "x", context(true));
    for (int i = 0; i < RecordPredicateMemo.WARM_UP_ROWS; i++)
      memo.evaluate(row(nodes[0]), null);
    memo.startBatch(database);
    assertThat(memo.isAnswering()).isTrue();
    return memo;
  }

  private CommandContext context(final boolean readOnly) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setVariable(MatchNodeStep.READ_ONLY_STATEMENT_KEY, readOnly);
    return context;
  }

  private static Result row(final Vertex vertex) {
    final ResultInternal row = new ResultInternal();
    row.setProperty("x", vertex);
    return row;
  }

  /** What {@link CountingPredicate} answers for the vertex with this id: true, false, null and true again. */
  private static Object answerOf(final int id) {
    return id == 2 ? null : id != 1;
  }

  /** Answers by the id of the vertex bound to x, and counts how often it is asked. */
  private static final class CountingPredicate implements BooleanExpression {
    private int evaluations;

    @Override
    public boolean evaluate(final Result result, final CommandContext context) {
      return Boolean.TRUE.equals(evaluateTernary(result, context));
    }

    @Override
    public Object evaluateTernary(final Result result, final CommandContext context) {
      evaluations++;
      final Object bound = result.getProperty("x");
      return bound instanceof Vertex vertex ? answerOf(vertex.getInteger("id") % 4) : Boolean.FALSE;
    }

    @Override
    public String getText() {
      return "counting";
    }
  }
}
