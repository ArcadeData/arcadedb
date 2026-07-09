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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub Issue #5181: a combined FULL_TEXT index on two nested-list properties both using
 * {@code BY ITEM} (e.g. {@code obj.hd BY ITEM, obj.tl BY ITEM}) only indexed the last property, so
 * {@code search_index()} on the first property returned nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextMultiPropertyByItemTest extends TestHelper {

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Pair");
      database.command("sql", "CREATE PROPERTY Pair.hd STRING");
      database.command("sql", "CREATE PROPERTY Pair.tl STRING");

      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.obj LIST OF Pair");
    });
  }

  private void insertTwoDocs() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET obj = [{\"@type\": \"Pair\", \"hd\": \"a1\", \"tl\": \"a2\"},"
          + " {\"@type\": \"Pair\", \"hd\": \"b1\", \"tl\": \"b2\"}]");
      database.command("sql", "INSERT INTO Doc SET obj = [{\"@type\": \"Pair\", \"hd\": \"c1\", \"tl\": \"c2\"},"
          + " {\"@type\": \"Pair\", \"hd\": \"d1\", \"tl\": \"d2\"}]");
    });
  }

  private void createIndex() {
    database.transaction(() ->
        database.command("sql", "CREATE INDEX combo ON Doc (`obj.hd` BY ITEM, `obj.tl` BY ITEM) FULL_TEXT"));
  }

  private void assertBothComponentsIndexed() {
    database.transaction(() -> {
      // hd values (first component) - previously returned nothing (issue #5181)
      assertThat(count("a1")).as("search for hd value 'a1'").isEqualTo(1);
      assertThat(count("b1")).as("search for hd value 'b1'").isEqualTo(1);
      assertThat(count("c1")).as("search for hd value 'c1'").isEqualTo(1);
      assertThat(count("d1")).as("search for hd value 'd1'").isEqualTo(1);

      // tl values (second component) - these always worked
      assertThat(count("a2")).as("search for tl value 'a2'").isEqualTo(1);
      assertThat(count("b2")).as("search for tl value 'b2'").isEqualTo(1);
      assertThat(count("c2")).as("search for tl value 'c2'").isEqualTo(1);
      assertThat(count("d2")).as("search for tl value 'd2'").isEqualTo(1);

      // a value that is not present must not match
      assertThat(count("zz")).as("search for absent value").isEqualTo(0);
    });
  }

  /** Index built AFTER the records exist: exercises the {@code build()} -> {@code addToIndex} scan path. */
  @Test
  void combinedByItemIndexBuiltAfterInsert() {
    createSchema();
    insertTwoDocs();
    createIndex();
    assertBothComponentsIndexed();
  }

  /** Index created BEFORE the records: exercises the live {@code addToIndex} path at insert time. */
  @Test
  void combinedByItemIndexCreatedBeforeInsert() {
    createSchema();
    createIndex();
    insertTwoDocs();
    assertBothComponentsIndexed();
  }

  /** Updating a record must keep both components searchable and drop the old values. */
  @Test
  void combinedByItemIndexUpdate() {
    createSchema();
    createIndex();
    insertTwoDocs();

    // Replace the obj list of the first document (a1/a2, b1/b2) with new pairs (e1/e2).
    database.transaction(() ->
        database.command("sql", "UPDATE Doc SET obj = [{\"@type\": \"Pair\", \"hd\": \"e1\", \"tl\": \"e2\"}]"
            + " WHERE obj.hd CONTAINS 'a1'"));

    database.transaction(() -> {
      // Old values of the updated document are gone.
      assertThat(count("a1")).as("old hd value 'a1' removed").isEqualTo(0);
      assertThat(count("a2")).as("old tl value 'a2' removed").isEqualTo(0);
      assertThat(count("b1")).as("old hd value 'b1' removed").isEqualTo(0);
      // New values are searchable on both components.
      assertThat(count("e1")).as("new hd value 'e1'").isEqualTo(1);
      assertThat(count("e2")).as("new tl value 'e2'").isEqualTo(1);
      // The untouched document is unaffected.
      assertThat(count("d1")).as("untouched hd value 'd1'").isEqualTo(1);
      assertThat(count("d2")).as("untouched tl value 'd2'").isEqualTo(1);
    });
  }

  /** Deleting a record must remove both components from the index. */
  @Test
  void combinedByItemIndexDelete() {
    createSchema();
    createIndex();
    insertTwoDocs();

    database.transaction(() ->
        database.command("sql", "DELETE FROM Doc WHERE obj.hd CONTAINS 'a1'"));

    database.transaction(() -> {
      assertThat(count("a1")).as("deleted hd value 'a1'").isEqualTo(0);
      assertThat(count("a2")).as("deleted tl value 'a2'").isEqualTo(0);
      assertThat(count("b1")).as("deleted hd value 'b1'").isEqualTo(0);
      // The other document remains.
      assertThat(count("c1")).as("surviving hd value 'c1'").isEqualTo(1);
      assertThat(count("d2")).as("surviving tl value 'd2'").isEqualTo(1);
    });
  }

  private long count(final String term) {
    final ResultSet result = database.query("sql", "SELECT expand(search_index('combo', ?)) as r", term);
    return result.stream().count();
  }
}
