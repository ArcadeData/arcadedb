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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.FutureTask;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12747: a statement that read a record the transaction had updated, and failed, is taken back by its savepoint
 * ({@code StatementSavepoint.rollback}, #10346), which puts the content the record had back in place. A later DELETE of that record
 * in the same transaction was then refused with a {@code ConcurrentModificationException} (the #12472 guard), although no other
 * transaction was involved.
 */
class Issue12747FailedStatementThenDeleteTest extends TestHelper {

  private static final String FAILING_SUBQUERY = "SELECT FROM %s WHERE k = 1 AND 1 = (SELECT 1/0)";

  @Test
  void documentDeletedAfterAFailingSubQuery() {
    createRecords(false, false);
    updateFailDelete("DD", String.format(FAILING_SUBQUERY, "DD"), "DELETE FROM DD WHERE id = 2");
    assertGone("DD", 2);
  }

  @Test
  void vertexDeletedAfterAFailingSubQuery() {
    createRecords(true, false);
    updateFailDelete("VA", String.format(FAILING_SUBQUERY, "VA"), "DELETE VERTEX FROM VA WHERE id = 2");
    assertGone("VA", 2);
  }

  /**
   * The protection of #12472 survives the take-back: a vertex read before a concurrent commit changed it, and given an edge in this
   * transaction, is still refused when it is deleted after a statement that read it failed. The buffer the record was read with is what
   * the delete compares with the slot, restored content or not.
   */
  @Test
  void aDeleteOverARealConcurrentChangeIsStillRefusedAfterAFailedStatement() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("V").set("v", 1).save().getIdentity();
      rids[1] = database.newVertex("V").set("v", 9).save().getIdentity();
    });

    // OUTSIDE ANY TRANSACTION, SO modify() PINS NO PAGE THE CONCURRENT COMMIT COULD BE CHECKED AGAINST
    final MutableVertex readBefore = rids[0].asVertex(true).modify();
    assertThat(readBefore.getInteger("v")).isEqualTo(1);
    database.begin();
    try {
      final FutureTask<Void> concurrent = new FutureTask<>(() -> {
        database.transaction(() -> rids[0].asVertex().modify().set("v", 2).save());
        return null;
      });
      new Thread(concurrent).start();
      concurrent.get();
      readBefore.newEdge("E", rids[1].asVertex());
      assertThat(((DatabaseInternal) database).getTransaction().isUpdateQueued(rids[0])).as("the edge-list write must be queued").isTrue();

      assertThatThrownBy(() -> database.command("sql", "SELECT FROM V WHERE v >= 0 AND 1 = (SELECT 1/0)").close()).isInstanceOf(
          Exception.class);

      assertThatThrownBy(readBefore::delete).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
    assertThat(database.existsRecord(rids[0])).isTrue();
    database.transaction(() -> assertThat(rids[0].asVertex().getInteger("v")).isEqualTo(2));
  }

  @Test
  void vertexDeletedAfterARefusedDropType() {
    createRecords(true, false);
    updateFailDelete("VA", "DROP TYPE VA", "DELETE VERTEX FROM VA WHERE id = 2");
    assertGone("VA", 2);
  }

  @Test
  void documentWithUniqueIndexLeavesNoStaleEntry() {
    createRecords(false, true);
    updateFailDelete("DD", String.format(FAILING_SUBQUERY, "DD"), "DELETE FROM DD WHERE id = 2");
    assertGone("DD", 2);
    // the unique key is free again
    database.transaction(() -> database.command("sql", "INSERT INTO DD SET id = 2").close());
    assertThat(count("DD", 2)).isEqualTo(1);
  }

  @Test
  void vertexWithUniqueIndexLeavesNoStaleEntry() {
    createRecords(true, true);
    updateFailDelete("VA", String.format(FAILING_SUBQUERY, "VA"), "DELETE VERTEX FROM VA WHERE id = 2");
    assertGone("VA", 2);
    database.transaction(() -> database.command("sql", "CREATE VERTEX VA SET id = 2").close());
    assertThat(count("VA", 2)).isEqualTo(1);
  }

  /** The untouched neighbour the failing statement read keeps its content and its index entry. */
  @Test
  void theRecordTheFailedStatementReadIsIntact() {
    createRecords(false, true);
    updateFailDelete("DD", String.format(FAILING_SUBQUERY, "DD"), "DELETE FROM DD WHERE id = 2");
    assertThat(count("DD", 1)).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "SELECT FROM DD WHERE id = 1")) {
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(1);
    }
  }

  /** The record is updated again after the failed statement, then deleted. */
  @Test
  void aRecordUpdatedTwiceAroundTheFailedStatementIsDeleted() {
    createRecords(false, true);
    database.begin();
    database.command("sql", "UPDATE DD SET x = 1 WHERE id = 2").close();
    assertThatThrownBy(() -> database.command("sql", String.format(FAILING_SUBQUERY, "DD")).close()).isInstanceOf(Exception.class);
    database.command("sql", "UPDATE DD SET x = 5 WHERE id = 2").close();
    database.command("sql", "DELETE FROM DD WHERE id = 2").close();
    database.commit();
    assertGone("DD", 2);
  }

  private void createRecords(final boolean vertex, final boolean uniqueIndex) {
    final String type = vertex ? "VA" : "DD";
    database.command("sql", (vertex ? "CREATE VERTEX TYPE " : "CREATE DOCUMENT TYPE ") + type).close();
    database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER").close();
    // NOT INDEXED: the failing statement filters on it, so it scans the type and reads the record the transaction updated
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER").close();
    if (uniqueIndex)
      database.command("sql", "CREATE INDEX ON " + type + " (id) UNIQUE").close();
    database.transaction(() -> {
      for (int id = 1; id <= 2; id++)
        database.command("sql",
            (vertex ? "CREATE VERTEX " + type + " SET k = " + id + ", id = " : "INSERT INTO " + type + " SET k = " + id + ", id = ") + id).close();
    });
  }

  private void updateFailDelete(final String type, final String failing, final String delete) {
    database.begin();
    database.command("sql", "UPDATE " + type + " SET x = 1 WHERE id = 2").close();
    assertThatThrownBy(() -> database.command("sql", failing).close()).isInstanceOf(Exception.class);
    database.command("sql", delete).close();
    database.commit();
  }

  private void assertGone(final String type, final int id) {
    assertThat(count(type, id)).isZero();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
  }

  private long count(final String type, final int id) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE id = ?", id)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
