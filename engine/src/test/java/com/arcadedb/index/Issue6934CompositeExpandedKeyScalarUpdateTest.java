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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6934: a composite index mixing a scalar property with exactly one {@code BY ITEM}/{@code BY KEY}/{@code BY VALUE}
 * property was never updated when only the scalar changed, because the single-expansion update path diffed the collection's
 * element membership instead of the complete key tuples. The record stayed visible under the stale key and invisible under the
 * real one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6934CompositeExpandedKeyScalarUpdateTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.status STRING");
      database.command("sql", "CREATE PROPERTY Doc.tags LIST");
      database.command("sql", "CREATE PROPERTY Doc.attrs MAP");
      database.command("sql", "CREATE PROPERTY Doc.items LIST");
    });
  }

  @Test
  void scalarChangeIsReflectedInListByItemCompositeIndex() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, tags BY ITEM) NOTUNIQUE"));
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x','y']"));
    database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

    assertThat(countWhere("B", "tags CONTAINS 'x'")).isEqualTo(1);
    assertThat(countWhere("B", "tags CONTAINS 'y'")).isEqualTo(1);
    assertThat(countWhere("A", "tags CONTAINS 'x'")).isZero();
    assertThat(countWhere("A", "tags CONTAINS 'y'")).isZero();
    assertThat(indexEntries()).isEqualTo(2);
  }

  @Test
  void scalarAndListChangedTogetherKeepsNoStaleEntry() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, tags BY ITEM) NOTUNIQUE"));
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x','y']"));
    // Partial list change together with the scalar: before the fix the retained element 'x' kept its (A,x) entry while the new
    // element 'z' was keyed with (B,z), so the index held a mix of old and new scalar values for the same record
    database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B', tags = ['x','z']"));

    assertThat(countWhere("B", "tags CONTAINS 'x'")).isEqualTo(1);
    assertThat(countWhere("B", "tags CONTAINS 'z'")).isEqualTo(1);
    assertThat(countWhere("A", "tags CONTAINS 'x'")).isZero();
    assertThat(countWhere("A", "tags CONTAINS 'y'")).isZero();
    assertThat(countWhere("B", "tags CONTAINS 'y'")).isZero();
    assertThat(indexEntries()).isEqualTo(2);
  }

  @Test
  void uniqueIndexReleasesTheStaleKey() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, tags BY ITEM) UNIQUE"));
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x']"));
    database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

    // The key (A,x) is no longer held by any record: a new record can take it
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x']"));
    assertThat(countWhere("A", "tags CONTAINS 'x'")).isEqualTo(1);
    assertThat(countWhere("B", "tags CONTAINS 'x'")).isEqualTo(1);

    // And the key (B,x) is now really taken
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'B', tags = ['x']")))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void scalarChangeIsReflectedInMapByKeyCompositeIndex() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, attrs BY KEY) NOTUNIQUE"));
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', attrs = {'k1': 'v1', 'k2': 'v2'}"));
    database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

    assertThat(countWhere("B", "attrs CONTAINSKEY 'k1'")).isEqualTo(1);
    assertThat(countWhere("A", "attrs CONTAINSKEY 'k1'")).isZero();
    assertThat(indexEntries()).isEqualTo(2);
  }

  @Test
  void scalarChangeIsReflectedInMapByValueCompositeIndex() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, attrs BY VALUE) NOTUNIQUE"));
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', attrs = {'k1': 'v1', 'k2': 'v2'}"));
    database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

    assertThat(countWhere("B", "attrs CONTAINSVALUE 'v1'")).isEqualTo(1);
    assertThat(countWhere("A", "attrs CONTAINSVALUE 'v1'")).isZero();
    assertThat(indexEntries()).isEqualTo(2);
  }

  @Test
  void scalarChangeIsReflectedInNestedListByItemCompositeIndex() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, `items.id` BY ITEM) NOTUNIQUE"));
    database.transaction(() -> database.command("sql",
        "INSERT INTO Doc SET status = 'A', items = [{'id': 1}, {'id': 2}]"));
    database.transaction(() -> database.command("sql", "UPDATE Doc SET status = 'B'"));

    assertThat(lookup("B", 1)).isEqualTo(1);
    assertThat(lookup("B", 2)).isEqualTo(1);
    assertThat(lookup("A", 1)).isZero();
    assertThat(lookup("A", 2)).isZero();
  }

  @Test
  void unchangedRecordTouchesNothing() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Doc (status, tags BY ITEM) NOTUNIQUE"));
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET status = 'A', tags = ['x','y']"));
    // An update that changes neither the scalar nor the list must leave the index as it is
    database.transaction(() -> database.command("sql", "UPDATE Doc SET other = 1"));

    assertThat(countWhere("A", "tags CONTAINS 'x'")).isEqualTo(1);
    assertThat(indexEntries()).isEqualTo(2);
  }

  /**
   * Counts the rows matching {@code status = '<status>' AND <predicate>}, checking every returned row really carries that
   * status: before the fix the index answered a record under a key it no longer held, returning a row with status 'B' for a
   * query asking for 'A'.
   */
  private int countWhere(final String status, final String predicate) {
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE status = '" + status + "' AND " + predicate);
    int count = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("status")).isEqualTo(status);
      ++count;
    }
    return count;
  }

  /** The only index defined on Doc by the test. */
  private TypeIndex docIndex() {
    final TypeIndex[] indexes = database.getSchema().getType("Doc").getAllIndexes(false).toArray(new TypeIndex[0]);
    assertThat(indexes).hasSize(1);
    return indexes[0];
  }

  private long indexEntries() {
    return docIndex().countEntries();
  }

  private int lookup(final Object... keys) {
    final IndexCursor cursor = docIndex().get(keys);
    int count = 0;
    while (cursor.hasNext()) {
      cursor.next();
      ++count;
    }
    return count;
  }
}
