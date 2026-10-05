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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #9254: {@code REBUILD INDEX} drops the index and then builds the new one, so a build that failed
 * (a stored row with a null key under an index whose null strategy was switched to ERROR) left the type without the
 * index, and a UNIQUE index lost its constraint. A failed rebuild must leave the index it found.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9254RebuildIndexFailureTest extends TestHelper {

  @Test
  void failedRebuildKeepsLsmUniqueIndex() {
    verifyFailedRebuildKeepsIndexes("L", "UNIQUE", "NOTUNIQUE");
  }

  @Test
  void failedRebuildKeepsHashUniqueIndex() {
    verifyFailedRebuildKeepsIndexes("H", "UNIQUE_HASH", "NOTUNIQUE_HASH");
  }

  @Test
  void failedRebuildOfAllIndexesKeepsTheOnesThatFailed() {
    prepare("A", "UNIQUE", "NOTUNIQUE");

    try (final ResultSet rs = database.command("sql", "REBUILD INDEX *")) {
      assertThat(rs.hasNext()).isTrue();
    }

    assertIndexesKept("A", true);
    assertDuplicateRefused("A");
  }

  private void verifyFailedRebuildKeepsIndexes(final String type, final String unique, final String notUnique) {
    prepare(type, unique, notUnique);

    for (final String property : new String[] { "u", "n" })
      assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `" + type + "[" + property + "]`").close()).isInstanceOf(
          IndexException.class);

    assertIndexesKept(type, true);
    assertDuplicateRefused(type);

    // the same after a reopen (Index.setNullStrategy() is not persisted, so the strategy is not compared)
    reopenDatabase();
    assertIndexesKept(type, false);
    assertDuplicateRefused(type);
  }

  private void prepare(final String type, final String unique, final String notUnique) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type).close();
      database.command("sql", "CREATE PROPERTY " + type + ".u STRING").close();
      database.command("sql", "CREATE PROPERTY " + type + ".n STRING").close();
      database.command("sql", "CREATE INDEX ON " + type + " (u) " + unique + " NULL_STRATEGY SKIP").close();
      database.command("sql", "CREATE INDEX ON " + type + " (n) " + notUnique + " NULL_STRATEGY SKIP").close();
    });
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + type + " SET id = 1, u = 'u1', n = 'u1'").close();
      database.command("sql", "INSERT INTO " + type + " SET id = 2, u = 'u2', n = 'u2'").close();
      database.command("sql", "INSERT INTO " + type + " SET id = 3").close();
    });
    // the stored null row can no longer be indexed, so a rebuild must fail
    for (final String property : new String[] { "u", "n" })
      database.getSchema().getIndexByName(type + "[" + property + "]").setNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
  }

  private void assertIndexesKept(final String type, final boolean checkNullStrategy) {
    assertThat(database.getSchema().getType(type).getAllIndexes(false)).hasSize(2);
    for (final String property : new String[] { "u", "n" }) {
      final Index index = database.getSchema().getIndexByName(type + "[" + property + "]");
      if (checkNullStrategy)
        assertThat(index.getNullStrategy()).isEqualTo(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
      assertThat(index.countEntries()).isEqualTo(2);
    }
    assertThat(database.getSchema().getIndexByName(type + "[u]").isUnique()).isTrue();

    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT FROM " + type + " WHERE u = 'u1'")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("FETCH FROM INDEX");
    }
  }

  private void assertDuplicateRefused(final String type) {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO " + type + " SET id = 9, u = 'u1'").close())).isInstanceOf(
        DuplicatedKeyException.class);
  }
}
