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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@code SET}, a {@code REMOVE} or a {@code MERGE} with actions holds its rows until every row has written, so the clauses
 * after it read the writes of every row (issue #12656), and the rows it holds count against the per-operation element limit.
 * One that ends the statement has no clause after it to read them, so it hands them out as it writes them, and a bulk write
 * ({@code UNWIND $rows AS row MATCH (n {id: row.id}) SET n += row}, the shape of a batched loader) stays out of the limit
 * however many rows it writes ({@code CypherStepBuilder.withLastWriteStreaming}).
 * <p>
 * The limit is 10 and every statement writes 30 rows: the statement that ends in the write must run, and the same write
 * followed by a clause that reads its rows must be refused, with the statement rolled back.
 */
class CypherLastWriteStreamsRowsTest extends TestHelper {
  private static final int  ROWS  = 30;
  private static final long LIMIT = 10;

  private boolean limitWasExplicit;
  private Object  previousLimit;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("N");
      for (int i = 0; i < ROWS; i++)
        database.newVertex("N").set("id", i).set("grp", i).save();
    });
    final ContextConfiguration configuration = database.getConfiguration();
    limitWasExplicit = configuration.hasValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    previousLimit = configuration.getValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    configuration.setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, LIMIT);
  }

  /** Puts the per-database setting back as it was: the value when it was set explicitly, no entry at all when it was not. */
  @Override
  protected void endTest() {
    final ContextConfiguration configuration = database.getConfiguration();
    if (limitWasExplicit)
      configuration.setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, previousLimit);
    else
      configuration.setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, null);
  }

  @Test
  void aBulkSetThatEndsTheStatementIsNotHeldUnderTheLimit() {
    run("UNWIND range(0, 29) AS i MATCH (n:N {id: i}) SET n.touched = i");

    assertThat(count("MATCH (n:N) WHERE n.touched = n.id RETURN n.id")).isEqualTo(ROWS);
  }

  @Test
  void aSetFollowedByAClauseThatReadsItsRowsIsHeldUnderTheLimit() {
    assertRefused("UNWIND range(0, 29) AS i MATCH (n:N {id: i}) SET n.touched = i RETURN n.id AS id");
    assertRefused("UNWIND range(0, 29) AS i MATCH (n:N {id: i}) SET n.touched = i WITH n RETURN count(n) AS c");

    assertThat(count("MATCH (n:N) WHERE n.touched IS NOT NULL RETURN n.id")).as("the refused statements wrote nothing").isZero();
  }

  @Test
  void aBulkMapSetThatEndsTheStatementIsNotHeldUnderTheLimit() {
    run("UNWIND range(0, 29) AS i MATCH (n:N {id: i}) SET n += {touched: i, other: 'x'}");

    assertThat(count("MATCH (n:N) WHERE n.touched = n.id AND n.other = 'x' RETURN n.id")).isEqualTo(ROWS);
  }

  @Test
  void aBulkRemoveThatEndsTheStatementIsNotHeldUnderTheLimit() {
    run("UNWIND range(0, 29) AS i MATCH (n:N {id: i}) REMOVE n.grp");

    assertThat(count("MATCH (n:N) WHERE n.grp IS NULL RETURN n.id")).isEqualTo(ROWS);
  }

  @Test
  void aRemoveFollowedByAClauseThatReadsItsRowsIsHeldUnderTheLimit() {
    assertRefused("UNWIND range(0, 29) AS i MATCH (n:N {id: i}) REMOVE n.grp RETURN n.id AS id");

    assertThat(count("MATCH (n:N) WHERE n.grp IS NOT NULL RETURN n.id")).as("the refused statement removed nothing").isEqualTo(ROWS);
  }

  @Test
  void aBulkMergeWithActionsThatEndsTheStatementIsNotHeldUnderTheLimit() {
    run("UNWIND range(0, 29) AS i MERGE (n:N {id: i}) ON MATCH SET n.touched = i");

    assertThat(count("MATCH (n:N) WHERE n.touched = n.id RETURN n.id")).isEqualTo(ROWS);
  }

  @Test
  void aMergeWithActionsFollowedByAClauseThatReadsItsRowsIsHeldUnderTheLimit() {
    assertRefused("UNWIND range(0, 29) AS i MERGE (n:N {id: i}) ON MATCH SET n.touched = i RETURN n.id AS id");

    assertThat(count("MATCH (n:N) WHERE n.touched IS NOT NULL RETURN n.id")).as("the refused statement wrote nothing").isZero();
  }

  private void run(final String statement) {
    database.transaction(() -> database.command("opencypher", statement).close());
  }

  private void assertRefused(final String statement) {
    assertThatThrownBy(() -> database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement)) {
        while (rs.hasNext())
          rs.next();
      }
    })).as(statement)
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  private long count(final String query) {
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }
}
