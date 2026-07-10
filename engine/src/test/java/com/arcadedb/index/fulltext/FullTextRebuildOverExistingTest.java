/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4732: building/rebuilding a FULL_TEXT index over data that already exists.
 * <ul>
 *   <li>CREATE INDEX ... FULL_TEXT over a populated type must index the pre-existing rows so they match SEARCH_INDEX.</li>
 *   <li>REBUILD INDEX &lt;fulltext&gt; must re-analyze the documents without crashing and must never leave the index dropped.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextRebuildOverExistingTest extends TestHelper {

  private Set<String> searchTitles(final String query) {
    final Set<String> titles = new HashSet<>();
    final ResultSet result = database.query("sql",
        "SELECT title FROM doc WHERE SEARCH_INDEX('doc[title]', '" + query + "') = true");
    while (result.hasNext())
      titles.add(result.next().getProperty("title"));
    return titles;
  }

  @Test
  void createSinglePropertyIndexOverExistingDataMatches() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.title STRING");
      database.command("sql", "INSERT INTO doc SET title = 'java database tutorial'");
    });

    // Build the index over the pre-existing row.
    database.transaction(() -> database.command("sql", "CREATE INDEX ON doc (title) FULL_TEXT"));

    database.transaction(() -> {
      // The pre-existing row must be matchable (issue #4732, part 1).
      assertThat(searchTitles("java")).containsExactly("java database tutorial");

      // A row inserted after creation continues to match too.
      database.command("sql", "INSERT INTO doc SET title = 'another java doc'");
      assertThat(searchTitles("java")).containsExactlyInAnyOrder("java database tutorial", "another java doc");
    });
  }

  @Test
  void rebuildFullTextIndexBM25KeepsIndexAndMatches() {
    rebuildFullTextIndex("BM25");
  }

  @Test
  void rebuildFullTextIndexClassicKeepsIndexAndMatches() {
    rebuildFullTextIndex("CLASSIC");
  }

  private void rebuildFullTextIndex(final String similarity) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.title STRING");
      database.command("sql", "CREATE INDEX ON doc (title) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
      database.command("sql", "INSERT INTO doc SET title = 'java database tutorial'");
      database.command("sql", "INSERT INTO doc SET title = 'python guide'");
    });

    database.transaction(() -> assertThat(searchTitles("java")).containsExactly("java database tutorial"));

    // REBUILD must not crash and must not drop the index (issue #4732, part 2).
    database.transaction(() -> database.command("sql", "REBUILD INDEX `doc[title]`"));

    database.transaction(() -> {
      // The index still exists...
      assertThat(database.getSchema().existsIndex("doc[title]")).isTrue();
      assertThat(database.getSchema().getIndexByName("doc[title]").getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);

      // ...and still matches the documents after the rebuild.
      assertThat(searchTitles("java")).containsExactly("java database tutorial");
      assertThat(searchTitles("python")).containsExactly("python guide");
    });
  }
}
