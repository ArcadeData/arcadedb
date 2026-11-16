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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #12162: a multi-property full-text index stores every token twice, unprefixed and as {@code field:token}. An unqualified
 * prefix query matched the {@code field:token} keys of every field whose name starts with the prefix, and an unprefixed token
 * that itself reads {@code field:token} (the StandardTokenizer keeps {@code title:java} as one token) was stored under the very
 * key of the field-qualified posting, so {@code title:java} matched a record holding that text in another field.
 */
class Issue12162FullTextFieldKeysTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Article.k INTEGER");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");
      database.command("sql", "INSERT INTO Article SET k = 1, title = 'Running fast', body = 'a slow walk'");
      database.command("sql", "INSERT INTO Article SET k = 2, title = 'Graph databases', body = 'nodes and edges'");
      database.command("sql", "INSERT INTO Article SET k = 3, title = 'Tiger', body = 'big cat'");
      database.command("sql", "INSERT INTO Article SET k = 4, title = 'Notes', body = 'see title:java for details'");
      database.command("sql", "INSERT INTO Article SET k = 5, title = 'Java', body = 'intro'");

      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.k INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.t STRING");
      database.command("sql", "CREATE INDEX ON Doc (t) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET k = 1, t = 'note:this is it'");
      database.command("sql", "INSERT INTO Doc SET k = 2, t = 'notebook'");
    });
  }

  @Test
  void unqualifiedPrefixDoesNotMatchTheFieldNames() {
    assertThat(search("Article", "Article[title,body]", "ti*").keySet()).containsExactlyInAnyOrder(3, 4);
    assertThat(search("Article", "Article[title,body]", "b*").keySet()).containsExactly(3);
    assertThat(search("Article", "Article[title,body]", "bo*").keySet()).isEmpty();
    assertThat(search("Article", "Article[title,body]", "t*").keySet()).containsExactlyInAnyOrder(3, 4);
    // the field-qualified forms are unchanged
    assertThat(search("Article", "Article[title,body]", "title:ti*").keySet()).containsExactly(3);
    assertThat(search("Article", "Article[title,body]", "body:b*").keySet()).containsExactly(3);
  }

  @Test
  void unqualifiedPrefixScoresTheTokensItExpandsTo() {
    // ti* expands to the tokens tiger and title:java: each record scores what its own token alone scores
    final Map<Integer, Float> prefix = search("Article", "Article[title,body]", "ti*");
    assertThat(prefix.get(3)).isCloseTo(search("Article", "Article[title,body]", "tiger").get(3), within(1e-6f));
    assertThat(prefix.get(4)).isCloseTo(search("Article", "Article[title,body]", "title\\:java").get(4), within(1e-6f));
  }

  @Test
  void searchFieldsOverEveryPropertyIsTheSameSearch() {
    assertThat(searchFields("ti*")).isEqualTo(search("Article", "Article[title,body]", "ti*"));
    assertThat(searchFields("b*")).isEqualTo(search("Article", "Article[title,body]", "b*"));
  }

  @Test
  void aFieldQualifiedTermDoesNotMatchTheLiteralTextOfAnotherField() {
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactly(5);
    assertThat(search("Article", "Article[title,body]", "body:java").keySet()).isEmpty();
    assertThat(search("Article", "Article[title,body]", "java").keySet()).containsExactly(5);
    // the literal token stays reachable unqualified, by term, prefix and wildcard, and qualified by its own field
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).containsExactly(4);
    assertThat(search("Article", "Article[title,body]", "title\\:ja*").keySet()).containsExactly(4);
    assertThat(search("Article", "Article[title,body]", "title\\:j?va").keySet()).containsExactly(4);
    assertThat(search("Article", "Article[title,body]", "body:title\\:java").keySet()).containsExactly(4);
    assertThat(search("Article", "Article[title,body]", "\"for title\\:java\"").keySet()).containsExactly(4);
    assertThat(search("Article", "Article[title,body]", "title:\"title\\:java\"").keySet()).isEmpty();
  }

  @Test
  void thePerRecordEvaluationAgreesWithTheIndex() {
    final String rids = database.query("sql", "SELECT @rid AS rid FROM Article").stream()
        .map(r -> r.getProperty("rid").toString()).collect(Collectors.joining(", ", "[", "]"));
    for (final String query : List.of("title:java", "java", "title\\:java", "ti*", "b*"))
      assertThat(search(rids, "Article[title,body]", query)).as(query).isEqualTo(search("Article", "Article[title,body]", query));
  }

  @Test
  void anUpdateAndADeleteLeaveNoStalePosting() {
    database.transaction(() -> database.command("sql", "UPDATE Article SET body = 'nothing here' WHERE k = 4"));
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).isEmpty();
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactly(5);

    database.transaction(() -> database.command("sql", "UPDATE Article SET body = 'back to title:java' WHERE k = 4"));
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).containsExactly(4);
    database.transaction(() -> database.command("sql", "DELETE FROM Article WHERE k = 4"));
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).isEmpty();
    assertThat(search("Article", "Article[title,body]", "t*").keySet()).containsExactly(3);
  }

  @Test
  void aRebuildKeepsTheKeysApart() {
    database.command("sql", "REBUILD INDEX `Article[title,body]`");
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactly(5);
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).containsExactly(4);
    assertThat(search("Article", "Article[title,body]", "ti*").keySet()).containsExactlyInAnyOrder(3, 4);
  }

  @Test
  void aSinglePropertyIndexMatchesItsColonTokensByWildcardToo() {
    // a one-property index stores no field-qualified key, so a token holding a colon is an ordinary token for every query form
    assertThat(search("Doc", "Doc[t]", "note*").keySet()).containsExactlyInAnyOrder(1, 2);
    assertThat(search("Doc", "Doc[t]", "not?*").keySet()).containsExactlyInAnyOrder(1, 2);
    assertThat(search("Doc", "Doc[t]", "note?this").keySet()).containsExactly(1);
  }

  private Map<Integer, Float> search(final String target, final String index, final String query) {
    return collect(database.query("sql", "SELECT k, $score AS s FROM " + target + " WHERE SEARCH_INDEX('" + index + "', :q) = true",
        Map.of("q", query)));
  }

  private Map<Integer, Float> searchFields(final String query) {
    return collect(database.query("sql", "SELECT k, $score AS s FROM Article WHERE SEARCH_FIELDS(['title', 'body'], :q) = true",
        Map.of("q", query)));
  }

  private static Map<Integer, Float> collect(final ResultSet rs) {
    final Map<Integer, Float> scores = new HashMap<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      scores.put(row.<Integer>getProperty("k"), ((Number) row.getProperty("s")).floatValue());
    }
    return scores;
  }
}
