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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12256: {@code DELETE VERTEX FROM (SELECT expand(out()) FROM V WHERE k = 0)} failed with a 404
 * telling the operator to run CHECK DATABASE FIX when vertex k=0 had two parallel edges to one neighbour: the subquery names
 * that neighbour twice, the first row deleted it and the second row could not. The statement deletes the set of records it
 * names, so a record an earlier row of the statement deleted is skipped.
 */
class DeleteSubqueryDuplicateRowsIssue12256Test extends TestHelper {

  private void createGraph() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
    database.transaction(() -> {
      final MutableVertex v0 = database.newVertex("V").set("k", 0).save();
      final MutableVertex v1 = database.newVertex("V").set("k", 1).save();
      final MutableVertex v2 = database.newVertex("V").set("k", 2).save();
      v0.newEdge("F", v1);
      v0.newEdge("F", v1);
      v0.newEdge("F", v2);
    });
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Long>getProperty("n");
    }
  }

  private long deleted(final String sql) {
    final long[] count = { -1 };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql)) {
        count[0] = rs.next().<Long>getProperty("count");
      }
    });
    return count[0];
  }

  private void assertCheckDatabaseClean() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result check = rs.next();
      assertThat(check.<Long>getProperty("totalWarnings")).isZero();
      assertThat(check.<Long>getProperty("totalErrors")).isZero();
    }
  }

  @Test
  void deleteVertexOfNeighboursReachedTwice() {
    createGraph();
    assertThat(deleted("DELETE VERTEX FROM (SELECT expand(out()) FROM V WHERE k = 0)")).isEqualTo(2L);
    assertThat(count("SELECT count(*) AS n FROM V")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS n FROM F")).isZero();
    assertCheckDatabaseClean();
  }

  @Test
  void deleteFromNeighboursReachedTwice() {
    createGraph();
    assertThat(deleted("DELETE FROM (SELECT expand(out()) FROM V WHERE k = 0)")).isEqualTo(2L);
    assertThat(count("SELECT count(*) AS n FROM V")).isEqualTo(1L);
    assertCheckDatabaseClean();
  }

  @Test
  void deleteVertexOfSourceReachedTwiceFromTheIncomingSide() {
    createGraph();
    assertThat(deleted("DELETE VERTEX FROM (SELECT expand(in()) FROM V WHERE k = 1)")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS n FROM V")).isEqualTo(2L);
    assertCheckDatabaseClean();
  }

  @Test
  void returnBeforeAnswersEachDeletedRecordOnce() {
    createGraph();
    final long[] rows = { 0 };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE VERTEX FROM (SELECT expand(out()) FROM V WHERE k = 0) RETURN BEFORE")) {
        while (rs.hasNext()) {
          rs.next();
          rows[0]++;
        }
      }
    });
    assertThat(rows[0]).isEqualTo(2L);
  }

  /**
   * The step is pulled once per batch of {@link AbstractExecutionStep#DEFAULT_FETCH_RECORDS_PER_PULL} rows: a neighbour named by
   * rows in different batches is still deleted once, so the set of deleted records lives as long as the statement, not the pull.
   * The ORDER BY reads every row before the first delete: without it, the delete of the neighbour takes the remaining parallel
   * edges out of the expansion still being read, and the rows of the later batches never come.
   */
  @Test
  void duplicatesStraddlingAPullBoundaryAreDeletedOnce() {
    createGraph();
    final int parallel = AbstractExecutionStep.DEFAULT_FETCH_RECORDS_PER_PULL * 2 + 50;
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE k = 0")) {
        final MutableVertex v0 = rs.next().getVertex().orElseThrow().modify();
        try (final ResultSet targets = database.query("sql", "SELECT FROM V WHERE k = 1")) {
          final MutableVertex v1 = targets.next().getVertex().orElseThrow().modify();
          for (int i = 0; i < parallel; i++)
            v0.newEdge("F", v1);
        }
      }
    });
    final String source = "(SELECT FROM (SELECT expand(out()) FROM V WHERE k = 0) ORDER BY k)";
    assertThat(count("SELECT count(*) AS n FROM " + source)).isGreaterThan(AbstractExecutionStep.DEFAULT_FETCH_RECORDS_PER_PULL * 2L);
    assertThat(deleted("DELETE VERTEX FROM " + source)).isEqualTo(2L);
    assertThat(count("SELECT count(*) AS n FROM V")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS n FROM F")).isZero();
    assertCheckDatabaseClean();
  }

  /** A projection carrying the @rid of a record deletes that record: one named twice is deleted once. */
  @Test
  void ridProjectionNamingOneRecordTwiceDeletesItOnce() {
    createGraph();
    assertThat(deleted("DELETE FROM (SELECT @rid FROM (SELECT expand(out()) FROM V WHERE k = 0))")).isEqualTo(2L);
    assertThat(count("SELECT count(*) AS n FROM V")).isEqualTo(1L);
    assertCheckDatabaseClean();
  }

  /** A row that is not a record and carries no @rid has nothing to delete: the statement is still refused, deleting nothing. */
  @Test
  void rowThatIsNotARecordIsStillRefused() {
    createGraph();
    assertThatThrownBy(() -> deleted("DELETE FROM (SELECT k FROM V WHERE k > 0)")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not a record");
    assertThat(count("SELECT count(*) AS n FROM V")).isEqualTo(3L);
  }

  /** Parallel lightweight edges share one RID and are still distinct edges: each row deletes one of them. */
  @Test
  void parallelLightweightEdgesAreEachDeleted() {
    createGraph();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE k = 0")) {
        final MutableVertex v0 = rs.next().getVertex().orElseThrow().modify();
        try (final ResultSet targets = database.query("sql", "SELECT FROM V WHERE k = 1")) {
          final MutableVertex v1 = targets.next().getVertex().orElseThrow().modify();
          v0.newEdge("L", v1);
          v0.newEdge("L", v1);
        }
      }
    });
    assertThat(count("SELECT count(*) AS n FROM L")).isEqualTo(2L);
    assertThat(deleted("DELETE FROM (SELECT expand(outE('L')) FROM V WHERE k = 0)")).isEqualTo(2L);
    assertThat(count("SELECT count(*) AS n FROM L")).isZero();
    assertCheckDatabaseClean();
  }
}
