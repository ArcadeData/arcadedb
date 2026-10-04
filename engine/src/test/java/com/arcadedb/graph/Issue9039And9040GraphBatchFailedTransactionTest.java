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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #9039 (a rolled back {@code createVertex} left stale edge-segment cache entries) and #9040 (a failed
 * {@code createVertices} left its transaction open).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9039And9040GraphBatchFailedTransactionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id INTEGER");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Note");
    database.command("sql", "CREATE EDGE TYPE Link");
  }

  @Test
  void createVertexAfterRolledBackCommitStaysUsable() {
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      final int[] ids = { 1, 1, 2, 3 };
      for (final int id : ids) {
        try {
          database.begin();
          batch.createVertex("Person", "id", id);
          database.commit();
        } catch (final DuplicatedKeyException e) {
          if (database.isTransactionActive())
            database.rollback();
        }
      }
    }
    assertThat(database.countType("Person", true)).isEqualTo(3);
  }

  @Test
  void createVerticesAfterRolledBackCreateVertexThenEdgeStaysUsable() {
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      for (final int id : new int[] { 1, 1 }) {
        try {
          database.begin();
          batch.createVertex("Person", "id", id);
          database.commit();
        } catch (final DuplicatedKeyException e) {
          if (database.isTransactionActive())
            database.rollback();
        }
      }
      // THE POSITION OF THE ROLLED BACK VERTEX IS REUSED BY THE BULK PATH
      final RID[] rids = batch.createVertices("Person", new Object[][] { { "id", 2 }, { "id", 3 } });
      batch.newEdge(rids[0], "Link", rids[1]);
    }
    assertThat(database.countType("Person", true)).isEqualTo(3);
    assertThat(database.countType("Link", true)).isEqualTo(1);
  }

  @Test
  void createVertexAfterRolledBackCommitStaysUsableUnidirectional() {
    try (final GraphBatch batch = GraphBatch.builder(database).withBidirectional(false).build()) {
      final int[] ids = { 1, 1, 2 };
      for (final int id : ids) {
        try {
          database.begin();
          batch.createVertex("Person", "id", id);
          database.commit();
        } catch (final DuplicatedKeyException e) {
          if (database.isTransactionActive())
            database.rollback();
        }
      }
    }
    assertThat(database.countType("Person", true)).isEqualTo(2);
  }

  @Test
  void failedCreateVerticesLeavesNoTransactionOpen() {
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      assertThatThrownBy(() -> batch.createVertices("Person", new Object[][] { { "id", 2 }, { "id", 2 } }))
          .isInstanceOf(DuplicatedKeyException.class);
      assertThat(database.isTransactionActive()).isFalse();

      assertThatThrownBy(() -> batch.createVertices("NoSuchType", 3)).isInstanceOf(SchemaException.class);
      assertThat(database.isTransactionActive()).isFalse();

      // THE BATCH IS STILL USABLE
      final RID[] rids = batch.createVertices("Person", new Object[][] { { "id", 5 }, { "id", 6 } });
      assertThat(rids).hasSize(2);
    }
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countType("Person", true)).isEqualTo(2);
  }

  @Test
  void failedCreateVerticesLeavesACallerOwnedTransactionToTheCaller() {
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      database.begin();
      assertThatThrownBy(() -> batch.createVertices("Person", new Object[][] { { "id", 2 }, { "id", 2 } }))
          .isInstanceOf(DuplicatedKeyException.class);
      // THE TRANSACTION WAS OPENED BY THE CALLER, WHO RESOLVES IT
      assertThat(database.isTransactionActive()).isTrue();
      database.rollback();
    }
    assertThat(database.countType("Person", true)).isEqualTo(0);
  }
}
