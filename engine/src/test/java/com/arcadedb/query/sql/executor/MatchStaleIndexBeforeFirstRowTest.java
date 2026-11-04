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
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.MatchStatement;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11219: a SQL {@code MATCH} that had returned no row yet failed to the caller when an index its plan read was dropped between the
 * planning and the first batch: the {@code IndexException} of {@code FetchFromIndexStep} reached a read-only query, where a
 * {@code SELECT} plans itself again (issues #9331, #9615). It is planned again now, the same way.
 * <p>
 * The drop is forced into that window, not hoped for: the prefetch of alias {@code a}, which runs in the first batch, calls a
 * function that drops the index the already planned root fetch of alias {@code b} reads next.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MatchStaleIndexBeforeFirstRowTest extends TestHelper {
  private static final String FUNCTION = "matchStaleIndex_dropIndex";
  private static final int    B_ROWS   = 200;

  @Test
  void aMatchWhoseIndexIsDroppedBeforeTheFirstRowIsPlannedAgain() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE PROPERTY B.id INTEGER");
    database.command("sql", "CREATE INDEX ON B (id) NOTUNIQUE");
    database.transaction(() -> {
      database.newVertex("A").set("name", "a").save();
      for (int i = 0; i < B_ROWS; i++)
        database.newVertex("B").set("id", i).save();
    });

    final AtomicInteger drops = new AtomicInteger();
    final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract(FUNCTION) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        if (database.getSchema().existsIndex("B[id]")) {
          database.getSchema().dropIndex("B[id]");
          drops.incrementAndGet();
        }
        return true;
      }

      @Override
      public String getSyntax() {
        return FUNCTION + "()";
      }
    });
    try {
      final String match = "MATCH {type: A, as: a, where: (name = 'a' AND " + FUNCTION + "() = true)},"
          + " {type: B, as: b, where: (id >= 0)} RETURN a.name as a, b.id as b";

      // THE CONTROL: THE ROOT FETCH OF b READS THE INDEX, SO DROPPING IT IS WHAT THE STATEMENT HAS TO SURVIVE
      assertThat(database.query("sql", "EXPLAIN " + match).getExecutionPlan().get().prettyPrint(0, 2))
          .contains("FETCH FROM INDEX B[id]");

      final Set<Integer> ids = new HashSet<>();
      try (final ResultSet rs = database.query("sql", match)) {
        while (rs.hasNext())
          ids.add(rs.next().<Integer>getProperty("b"));
      }

      assertThat(drops.get()).as("the index went away inside the first batch").isEqualTo(1);
      assertThat(ids).hasSize(B_ROWS);
    } finally {
      sql.getFunctionFactory().unregister(FUNCTION);
    }
  }

  /**
   * The last attempt of a statement whose index kept going away plans its root fetches without the key indexes of their types:
   * an equality and a range on the indexed property are then answered by a scan, with the same rows.
   */
  @Test
  void theLastAttemptPlansTheRootFetchWithoutKeyIndexesAndAnswersTheSameRows() {
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE PROPERTY B.id INTEGER");
    database.command("sql", "CREATE INDEX ON B (id) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < B_ROWS; i++)
        database.newVertex("B").set("id", i).save();
    });

    for (final String match : new String[] { "MATCH {type: B, as: b, where: (id = 42)} RETURN b.id as b",
        "MATCH {type: B, as: b, where: (id >= 10 AND id < 150)} RETURN b.id as b" }) {
      final MatchStatement statement = (MatchStatement) ((DatabaseInternal) database).getStatementCache().get(match);

      final BasicCommandContext withIndex = new BasicCommandContext();
      withIndex.setDatabase(database);
      final InternalExecutionPlan indexed = new MatchExecutionPlanner(statement).createExecutionPlan(withIndex);
      assertThat(indexed.prettyPrint(0, 2)).as(match).contains("FETCH FROM INDEX");

      final BasicCommandContext withoutIndex = new BasicCommandContext();
      withoutIndex.setDatabase(database);
      final InternalExecutionPlan scan = new MatchExecutionPlanner(statement).withoutKeyIndexes().createExecutionPlan(withoutIndex);
      assertThat(scan.prettyPrint(0, 2)).as(match).doesNotContain("FETCH FROM INDEX");

      assertThat(ids(new LocalResultSet(scan))).as(match).isNotEmpty().isEqualTo(ids(new LocalResultSet(indexed)));
    }
  }

  private static Set<Integer> ids(final ResultSet rs) {
    final Set<Integer> ids = new HashSet<>();
    try (rs) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("b"));
    }
    return ids;
  }
}
