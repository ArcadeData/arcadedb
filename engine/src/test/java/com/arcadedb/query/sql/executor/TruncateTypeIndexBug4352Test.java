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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4352: TRUNCATE TYPE leaves UNIQUE indexes in an inconsistent state.
 * After a TRUNCATE TYPE, all indexes on the type must be empty so that subsequent inserts of the same
 * unique key succeed without a duplicate-key error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TruncateTypeIndexBug4352Test extends TestHelper {

  @Test
  void truncateTypeClearsUniqueIndexOnVertex() {
    database.command("sql", "CREATE VERTEX TYPE Worker");
    database.command("sql", "CREATE PROPERTY Worker.name STRING");
    database.command("sql", "CREATE INDEX ON Worker(name) UNIQUE");

    database.transaction(() ->
      database.command("sql", "INSERT INTO Worker SET name = 'John'"));

    // sanity check: 1 record and 1 index entry
    assertCount("Worker", 1);
    assertIndexSize("Worker[name]", 1);

    database.transaction(() ->
      database.command("sql", "TRUNCATE TYPE Worker UNSAFE"));

    assertCount("Worker", 0);
    assertIndexSize("Worker[name]", 0);

    // this used to fail with "Duplicated key 'John' found on index" because the index still pointed
    // to the (now deleted) record
    database.transaction(() ->
      database.command("sql", "INSERT INTO Worker SET name = 'John'"));

    assertCount("Worker", 1);
    assertIndexSize("Worker[name]", 1);
  }

  @Test
  void truncateTypeClearsUniqueIndexOnDocument() {
    database.command("sql", "CREATE DOCUMENT TYPE Tag");
    database.command("sql", "CREATE PROPERTY Tag.code STRING");
    database.command("sql", "CREATE INDEX ON Tag(code) UNIQUE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Tag SET code = 'A'");
      database.command("sql", "INSERT INTO Tag SET code = 'B'");
    });

    assertCount("Tag", 2);
    assertIndexSize("Tag[code]", 2);

    database.transaction(() ->
      database.command("sql", "TRUNCATE TYPE Tag"));

    assertCount("Tag", 0);
    assertIndexSize("Tag[code]", 0);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Tag SET code = 'A'");
      database.command("sql", "INSERT INTO Tag SET code = 'B'");
    });

    assertCount("Tag", 2);
    assertIndexSize("Tag[code]", 2);
  }

  /**
   * Mimics the console/HTTP flow where each command runs in its own implicit transaction
   * (autoTransaction = true). This is the exact pattern reported in issue #4352.
   */
  @Test
  void truncateTypeWithAutoTransactionClearsUniqueIndex() {
    database.setAutoTransaction(true);
    try {
      database.command("sql", "CREATE VERTEX TYPE Worker");
      database.command("sql", "CREATE PROPERTY Worker.name STRING");
      database.command("sql", "CREATE INDEX ON Worker(name) UNIQUE");

      database.command("sql", "INSERT INTO Worker SET name = 'John'");

      assertCount("Worker", 1);
      assertIndexSize("Worker[name]", 1);

      database.command("sql", "TRUNCATE TYPE Worker UNSAFE");

      assertCount("Worker", 0);
      assertIndexSize("Worker[name]", 0);

      database.command("sql", "INSERT INTO Worker SET name = 'John'");

      assertCount("Worker", 1);
      assertIndexSize("Worker[name]", 1);
    } finally {
      database.setAutoTransaction(false);
    }
  }

  /**
   * Truncate a large type (more records than the truncateBatchSize) so the batched commit/begin runs.
   */
  @Test
  void truncateLargeTypeWithBatchedCommitClearsUniqueIndex() {
    database.command("sql", "CREATE VERTEX TYPE Worker");
    database.command("sql", "CREATE PROPERTY Worker.name STRING");
    database.command("sql", "CREATE INDEX ON Worker(name) UNIQUE");

    final int total = 60_000;
    database.begin();
    for (int i = 0; i < total; i++) {
      database.command("sql", "INSERT INTO Worker SET name = ?", "w" + i);
      if ((i + 1) % 10_000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();

    assertCount("Worker", total);

    database.transaction(() ->
      database.command("sql", "TRUNCATE TYPE Worker UNSAFE"));

    assertCount("Worker", 0);
    // index must be empty too (was failing: infinite-loop iteration on stale tombstones)
    assertIndexSize("Worker[name]", 0);

    // re-insert ALL the same keys: must succeed (used to fail with duplicate-key)
    database.begin();
    for (int i = 0; i < total; i++) {
      database.command("sql", "INSERT INTO Worker SET name = ?", "w" + i);
      if ((i + 1) % 10_000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();

    assertCount("Worker", total);
    assertIndexSize("Worker[name]", total);
  }

  @Test
  void truncateTypePolymorphicClearsUniqueIndexes() {
    database.command("sql", "CREATE VERTEX TYPE Worker");
    database.command("sql", "CREATE PROPERTY Worker.name STRING");
    database.command("sql", "CREATE INDEX ON Worker(name) UNIQUE");

    database.command("sql", "CREATE VERTEX TYPE Manager EXTENDS Worker");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Worker SET name = 'John'");
      database.command("sql", "INSERT INTO Manager SET name = 'Jane'");
    });

    assertCount("Worker", 2);

    database.transaction(() ->
      database.command("sql", "TRUNCATE TYPE Worker POLYMORPHIC UNSAFE"));

    assertCount("Worker", 0);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Worker SET name = 'John'");
      database.command("sql", "INSERT INTO Manager SET name = 'Jane'");
    });

    assertCount("Worker", 2);
  }

  private void assertCount(final String typeName, final long expected) {
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM " + typeName);
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(expected);
    rs.close();
  }

  private void assertIndexSize(final String indexName, final long expected) {
    final RangeIndex index = (RangeIndex) database.getSchema().getIndexByName(indexName);
    long count = 0;
    final IndexCursor cursor = index.iterator(true);
    while (cursor.hasNext()) {
      cursor.next();
      count++;
    }
    assertThat(count).as("Index '%s' size", indexName).isEqualTo(expected);
  }
}
