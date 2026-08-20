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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A BM25 CONTAINSTEXT conjunction over several properties of a multi-property full-text index answers each constrained
 * property as one type-wide scoring pass, intersected. Before the fix, EVERY one of those passes independently scanned
 * document frequencies for the whole corpus - the same, corpus-wide statistics recomputed once per constrained property
 * instead of once for the whole query (issue #6436). This pins two things: the document-frequency scan now runs exactly
 * once per query regardless of how many properties are constrained, and the scored/intersected results are unchanged.
 */
class Issue6436Bm25ConjunctionSharedCorpusScanTest extends TestHelper {

  @Test
  void aThreePropertyConjunctionScansDocumentFrequenciesOnce() {
    createArticles();

    database.transaction(() -> {
      FullTextSearch.resetDocumentFrequencyScanInvocationsForTesting();

      assertThat(idsMatching(
          "SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'jvm' "
              + "AND tags CONTAINSTEXT 'backend'"))
          .containsExactly("a");

      // One shared corpus scan for the whole conjunction, not one per constrained property.
      assertThat(FullTextSearch.getDocumentFrequencyScanInvocationsForTesting()).isEqualTo(1);
    });
  }

  @Test
  void aTwoPropertyConjunctionScansDocumentFrequenciesOnce() {
    createArticles();

    database.transaction(() -> {
      FullTextSearch.resetDocumentFrequencyScanInvocationsForTesting();

      assertThat(idsMatching("SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'jvm'"))
          .containsExactlyInAnyOrder("a", "c");

      assertThat(FullTextSearch.getDocumentFrequencyScanInvocationsForTesting()).isEqualTo(1);
    });
  }

  @Test
  void aSinglePropertyConditionStillScansDocumentFrequenciesOnce() {
    createArticles();

    database.transaction(() -> {
      FullTextSearch.resetDocumentFrequencyScanInvocationsForTesting();

      assertThat(idsMatching("SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java'"))
          .containsExactlyInAnyOrder("a", "c");

      assertThat(FullTextSearch.getDocumentFrequencyScanInvocationsForTesting()).isEqualTo(1);
    });
  }

  /**
   * The scan-sharing refactor must not change what a conjunction answers: same intersected, scored result as the
   * per-property path it replaces.
   */
  @Test
  void theConjunctionResultIsUnaffectedByHowManyPropertiesAreConstrained() {
    createArticles();

    database.transaction(() -> {
      assertThat(idsMatching(
          "SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'jvm' "
              + "AND tags CONTAINSTEXT 'backend'"))
          .containsExactly("a");
      assertThat(idsMatching(
          "SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'jvm' "
              + "AND tags CONTAINSTEXT 'zzz'"))
          .isEmpty();
      assertThat(idsMatching(
          "SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'python' AND content CONTAINSTEXT 'scripting'"))
          .containsExactly("b");
    });
  }

  private void createArticles() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article6436");
      database.command("sql", "CREATE PROPERTY Article6436.title STRING");
      database.command("sql", "CREATE PROPERTY Article6436.content STRING");
      database.command("sql", "CREATE PROPERTY Article6436.tags STRING");
      database.command("sql",
          "CREATE INDEX ON Article6436 (title, content, tags) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");

      database.command("sql",
          "INSERT INTO Article6436 SET id = 'a', title = 'java concurrency', content = 'jvm internals', "
              + "tags = 'backend performance'");
      database.command("sql",
          "INSERT INTO Article6436 SET id = 'b', title = 'python basics', content = 'scripting tips', "
              + "tags = 'frontend tooling'");
      database.command("sql",
          "INSERT INTO Article6436 SET id = 'c', title = 'JAVA rocks', content = 'jvm tuning', tags = 'frontend'");
      database.command("sql",
          "INSERT INTO Article6436 SET id = 'd', title = 'cooking notes', content = 'pasta recipes', tags = 'food'");
    });
  }

  private Set<String> idsMatching(final String query) {
    final Set<String> ids = new LinkedHashSet<>();
    try (final ResultSet rs = database.query("sql", query, Map.of())) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }
}
