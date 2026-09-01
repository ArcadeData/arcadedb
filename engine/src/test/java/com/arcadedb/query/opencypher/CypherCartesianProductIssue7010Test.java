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
import com.arcadedb.query.opencypher.executor.operators.CartesianProduct;
import com.arcadedb.query.opencypher.executor.operators.PhysicalOperator;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7010: two defects in the openCypher {@code CartesianProduct} physical operator.
 * <ol>
 *   <li>its {@code ResultSet.close()} was an empty method, so it never reached either child and an
 *       index-backed child kept its cursor open for as long as the plan was retained (the chaining
 *       contract established by #5635);</li>
 *   <li>the right input was drained into a list in full before the first row was emitted, so a
 *       {@code LIMIT 1} probe over a big type paid for the whole type in heap and in scan work.</li>
 * </ol>
 * The operator is exercised directly here: the row counts and the close calls of a Cartesian product
 * are not observable from the outside of a query, and the end-to-end tests at the bottom only pin the
 * result semantics that the streaming rewrite must not change.
 */
class CypherCartesianProductIssue7010Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/CypherCartesianProductIssue7010Test").create();
    database.getSchema().createVertexType("Item");
    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        database.newVertex("Item").set("id", i).save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 1. close() must reach both children
  // ---------------------------------------------------------------------------------------------

  @Test
  void closeReachesBothChildren() {
    final RecordingOperator left = new RecordingOperator("a", 3);
    final RecordingOperator right = new RecordingOperator("b", 3);
    final ResultSet rs = product(left, right).execute(context(), -1);

    assertThat(rs.hasNext()).isTrue();
    rs.next();
    rs.close();

    assertThat(left.closeCount).as("left child result set closed exactly once").isEqualTo(1);
    assertThat(right.closeCount).as("right child result set closed exactly once").isEqualTo(1);
  }

  @Test
  void closeIsIdempotentAndSafeBeforeTheFirstRow() {
    final RecordingOperator left = new RecordingOperator("a", 3);
    final RecordingOperator right = new RecordingOperator("b", 3);
    final ResultSet rs = product(left, right).execute(context(), -1);

    // Closing without ever pulling must not blow up on the not-yet-created child result sets.
    rs.close();
    rs.close();

    assertThat(left.closeCount).isZero();
    assertThat(right.closeCount).isZero();

    final ResultSet pulled = product(left, right).execute(context(), -1);
    pulled.next();
    pulled.close();
    pulled.close();

    assertThat(left.closeCount).as("a second close() must not re-close the children").isEqualTo(1);
    assertThat(right.closeCount).as("a second close() must not re-close the children").isEqualTo(1);
  }

  @Test
  void aClosedResultSetDoesNotReopenItsChildren() {
    // close() before the first pull leaves initialized == false, so a later hasNext() used to run
    // ensureInitialized() and execute both children again - two cursors nothing would ever close,
    // the very leak this issue is about.
    final RecordingOperator left = new RecordingOperator("a", 3);
    final RecordingOperator right = new RecordingOperator("b", 3);
    final ResultSet rs = product(left, right).execute(context(), -1);

    rs.close();

    assertThat(rs.hasNext()).as("a closed result set is exhausted").isFalse();
    assertThat(left.executeCount).as("the left child must not be executed by a closed result set").isZero();
    assertThat(right.executeCount).as("the right child must not be executed by a closed result set").isZero();

    // Same guarantee once the rows have been consumed and close() has already run.
    final ResultSet consumed = product(left, right).execute(context(), -1);
    drainPairs(consumed);
    consumed.close();
    assertThat(consumed.hasNext()).isFalse();
    assertThat(left.executeCount).as("no re-execution after a drained-then-closed result set").isEqualTo(1);
    assertThat(right.executeCount).as("no re-execution after a drained-then-closed result set").isEqualTo(1);
  }

  // ---------------------------------------------------------------------------------------------
  // 2. the right input must not be materialized before the first row
  // ---------------------------------------------------------------------------------------------

  @Test
  void firstRowDoesNotDrainTheRightInput() {
    final RecordingOperator left = new RecordingOperator("a", 500);
    final RecordingOperator right = new RecordingOperator("b", 500);
    final ResultSet rs = product(left, right).execute(context(), -1);

    assertThat(rs.hasNext()).isTrue();
    final Result first = rs.next();

    assertThat(first.<Integer>getProperty("a")).isEqualTo(0);
    assertThat(first.<Integer>getProperty("b")).isEqualTo(0);
    assertThat(right.rowsProduced).as("only the right rows the consumer actually needed were pulled").isEqualTo(1);
    rs.close();
  }

  @Test
  void abandonedScanPullsOnlyTheRowsItConsumed() {
    // This is the shape of "MATCH (a:Big), (b:Big) RETURN a, b LIMIT 3": the consumer stops after 3
    // rows, so exactly 3 right rows may be touched, not the whole right input.
    final RecordingOperator left = new RecordingOperator("a", 500);
    final RecordingOperator right = new RecordingOperator("b", 500);
    final ResultSet rs = product(left, right).execute(context(), -1);

    for (int i = 0; i < 3; i++)
      rs.next();
    rs.close();

    assertThat(right.rowsProduced).isEqualTo(3);
    assertThat(left.rowsProduced).as("a single left row covers the first 3 pairs").isEqualTo(1);
  }

  @Test
  void everyPairIsStillProducedInOrder() {
    final RecordingOperator left = new RecordingOperator("a", 3);
    final RecordingOperator right = new RecordingOperator("b", 2);
    final ResultSet rs = product(left, right).execute(context(), -1);

    final List<String> pairs = drainPairs(rs);
    rs.close();

    // The right input is re-iterated for every left row even though it is pulled lazily.
    assertThat(pairs).containsExactly("0/0", "0/1", "1/0", "1/1", "2/0", "2/1");
    assertThat(right.rowsProduced).as("the right input is pulled once and replayed from the buffer").isEqualTo(2);
  }

  @Test
  void pairFilterStillSkipsRejectedPairs() {
    final RecordingOperator left = new RecordingOperator("a", 3);
    final RecordingOperator right = new RecordingOperator("b", 3);
    final BiPredicate<Result, Result> sameId =
        (l, r) -> l.<Integer>getProperty("a").equals(r.<Integer>getProperty("b"));
    final ResultSet rs = new CartesianProduct(left, right, 1.0, 9L, sameId).execute(context(), -1);

    final List<String> pairs = drainPairs(rs);
    rs.close();

    assertThat(pairs).containsExactly("0/0", "1/1", "2/2");
  }

  @Test
  void anEmptyLeftInputProducesNoRowsAndClosesBothChildren() {
    final RecordingOperator left = new RecordingOperator("a", 0);
    final RecordingOperator right = new RecordingOperator("b", 3);
    final ResultSet rs = product(left, right).execute(context(), -1);

    assertThat(rs.hasNext()).isFalse();
    assertThat(right.rowsProduced).as("an empty left input must not pull the right input at all").isZero();
    rs.close();

    assertThat(left.closeCount).isEqualTo(1);
    assertThat(right.closeCount).isEqualTo(1);
  }

  @Test
  void anEmptyRightInputProducesNoRows() {
    final RecordingOperator left = new RecordingOperator("a", 3);
    final RecordingOperator right = new RecordingOperator("b", 0);
    final ResultSet rs = product(left, right).execute(context(), -1);

    assertThat(rs.hasNext()).isFalse();
    rs.close();

    assertThat(left.closeCount).isEqualTo(1);
    assertThat(right.closeCount).isEqualTo(1);
  }

  // ---------------------------------------------------------------------------------------------
  // 3. end-to-end semantics the rewrite must not change
  // ---------------------------------------------------------------------------------------------

  @Test
  void queryLevelCartesianProductKeepsEveryPair() {
    final List<String> pairs = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Item) MATCH (b:Item) RETURN a.id AS x, b.id AS y ORDER BY x, y")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        pairs.add(row.<Object>getProperty("x") + "/" + row.<Object>getProperty("y"));
      }
    }

    assertThat(pairs).hasSize(16);
    assertThat(pairs).startsWith("0/0", "0/1", "0/2", "0/3", "1/0");
    assertThat(pairs).endsWith("3/3");
  }

  @Test
  void queryLevelLimitReturnsTheSameRowsAsTheUnlimitedQuery() {
    final List<String> unlimited = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Item) MATCH (b:Item) RETURN a.id AS x, b.id AS y")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        unlimited.add(row.<Object>getProperty("x") + "/" + row.<Object>getProperty("y"));
      }
    }

    final List<String> limited = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Item) MATCH (b:Item) RETURN a.id AS x, b.id AS y LIMIT 3")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        limited.add(row.<Object>getProperty("x") + "/" + row.<Object>getProperty("y"));
      }
    }

    assertThat(limited).hasSize(3);
    assertThat(limited).isEqualTo(unlimited.subList(0, 3));
  }

  // ---------------------------------------------------------------------------------------------

  private static CartesianProduct product(final PhysicalOperator left, final PhysicalOperator right) {
    return new CartesianProduct(left, right, 1.0, 1L);
  }

  private BasicCommandContext context() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private static List<String> drainPairs(final ResultSet rs) {
    final List<String> pairs = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      pairs.add(row.<Object>getProperty("a") + "/" + row.<Object>getProperty("b"));
    }
    return pairs;
  }

  /**
   * A leaf operator producing {@code rowCount} single-property rows lazily, recording how many rows
   * were actually pulled out of it and how many times its result set was closed.
   */
  private static final class RecordingOperator implements PhysicalOperator {
    private final String propertyName;
    private final int    rowCount;
    int rowsProduced;
    int closeCount;
    int executeCount;

    RecordingOperator(final String propertyName, final int rowCount) {
      this.propertyName = propertyName;
      this.rowCount = rowCount;
    }

    @Override
    public ResultSet execute(final CommandContext context, final int nRecords) {
      ++executeCount;
      return new ResultSet() {
        private int emitted = 0;

        @Override
        public boolean hasNext() {
          return emitted < rowCount;
        }

        @Override
        public Result next() {
          if (!hasNext())
            throw new NoSuchElementException();
          final ResultInternal row = new ResultInternal();
          row.setProperty(propertyName, emitted++);
          ++rowsProduced;
          return row;
        }

        @Override
        public void close() {
          ++closeCount;
        }
      };
    }

    @Override
    public double getEstimatedCost() {
      return rowCount;
    }

    @Override
    public long getEstimatedCardinality() {
      return rowCount;
    }

    @Override
    public String getOperatorType() {
      return "Recording";
    }

    @Override
    public String explain(final int depth) {
      return "  ".repeat(depth) + "+ Recording[" + propertyName + "]\n";
    }

    @Override
    public PhysicalOperator getChild() {
      return null;
    }

    @Override
    public void setChild(final PhysicalOperator child) {
      throw new UnsupportedOperationException();
    }
  }
}
