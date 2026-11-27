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
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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
 *
 * @author lvca (l.garulli@arcadedata.com)
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
    // the phrase in the order the body holds it: phrases match by position since issue #12165
    assertThat(search("Article", "Article[title,body]", "\"title\\:java for\"").keySet()).containsExactly(4);
    assertThat(search("Article", "Article[title,body]", "\"for title\\:java\"").keySet()).isEmpty();
    assertThat(search("Article", "Article[title,body]", "title:\"title\\:java\"").keySet()).isEmpty();
  }

  @Test
  void aFuzzyTermReadsTheEscapedTokenAsTheTokenItStandsFor() {
    // the unqualified fuzzy walk measures an escaped key by the token it stands for, and never a field-prefixed key
    assertThat(search("Article", "Article[title,body]", "title\\:jav~1").keySet()).containsExactly(4);
    assertThat(search("Article", "Article[title,body]", "jav~1").keySet()).containsExactly(5);
    assertThat(search("Article", "Article[title,body]", "title:jav~1").keySet()).containsExactly(5);
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

  @Test
  void anUpdateOfOneFieldKeepsTheQualifiedPostingOfTheOtherOne() {
    // k = 6 holds java in its title and title:java in its body: the unprefixed posting of the body token (stored escaped) and the
    // title posting of java are two keys, and the cleanup of an update must not take one for the other
    database.transaction(() -> database.command("sql", "INSERT INTO Article SET k = 6, title = 'java', body = 'see title:java'"));
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactlyInAnyOrder(5, 6);
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).containsExactlyInAnyOrder(4, 6);

    database.transaction(() -> database.command("sql", "UPDATE Article SET body = 'other text' WHERE k = 6"));
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactlyInAnyOrder(5, 6);
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).containsExactly(4);

    database.transaction(() -> database.command("sql", "UPDATE Article SET title = 'other' WHERE k = 6"));
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactly(5);

    database.transaction(() -> database.command("sql", "DELETE FROM Article WHERE k = 5"));
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).isEmpty();
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).containsExactly(4);
  }

  @Test
  void aPostingAnIndexWrittenBeforeTheFixStoredUnescapedGoesWithItsRecord() {
    // An index written before the escape stored the token of the body of k = 4 under title:java, the key of the title posting of
    // java. Until a REBUILD INDEX it still answers a title:java query with k = 4, but an update or a delete of the record removes it.
    final RID rid = ridOf(4);
    database.transaction(() -> ((LSMTreeFullTextIndex) bucketIndexOf(rid)).putReplay(new Object[] { "title:java" }, new RID[] { rid }));
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactlyInAnyOrder(4, 5);

    database.transaction(() -> database.command("sql", "UPDATE Article SET body = 'nothing here' WHERE k = 4"));
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactly(5);
    assertThat(search("Article", "Article[title,body]", "title\\:java").keySet()).isEmpty();

    database.transaction(() -> database.command("sql", "UPDATE Article SET body = 'see title:java again' WHERE k = 4"));
    database.transaction(() -> ((LSMTreeFullTextIndex) bucketIndexOf(rid)).putReplay(new Object[] { "title:java" }, new RID[] { rid }));
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactlyInAnyOrder(4, 5);
    database.transaction(() -> database.command("sql", "DELETE FROM Article WHERE k = 4"));
    assertThat(search("Article", "Article[title,body]", "title:java").keySet()).containsExactly(5);
  }

  @Test
  void theDirectLookupReadsTheKeyTheTokenIsStoredUnder() {
    // Title is not a qualifier (the property names are case sensitive), so the analyzer folds Title:java into the token title:java
    assertThat(direct("Article[title,body]", "Title:java")).containsExactly(4);
    assertThat(direct("Article[title,body]", "title:java")).containsExactly(5);
    assertThat(direct("Article[title,body]", "body:title:java")).containsExactly(4);
  }

  @Test
  void aClassicIndexKeepsTheKeysApartToo() {
    // CLASSIC stores one RID-only posting per token occurrence, through another writer than the BM25 postings with statistics
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE ClassicArticle");
      database.command("sql", "CREATE PROPERTY ClassicArticle.k INTEGER");
      database.command("sql", "CREATE PROPERTY ClassicArticle.title STRING");
      database.command("sql", "CREATE PROPERTY ClassicArticle.body STRING");
      database.command("sql", "CREATE INDEX ON ClassicArticle (title, body) FULL_TEXT METADATA {\"similarity\": \"CLASSIC\"}");
      database.command("sql", "INSERT INTO ClassicArticle SET k = 3, title = 'Tiger', body = 'big cat'");
      database.command("sql", "INSERT INTO ClassicArticle SET k = 4, title = 'Notes', body = 'see title:java for details'");
      database.command("sql", "INSERT INTO ClassicArticle SET k = 5, title = 'Java', body = 'intro'");
    });
    final String index = "ClassicArticle[title,body]";

    assertThat(search("ClassicArticle", index, "ti*").keySet()).containsExactlyInAnyOrder(3, 4);
    assertThat(search("ClassicArticle", index, "b*").keySet()).containsExactly(3);
    assertThat(search("ClassicArticle", index, "title:java").keySet()).containsExactly(5);
    assertThat(search("ClassicArticle", index, "title\\:java").keySet()).containsExactly(4);
    assertThat(search("ClassicArticle", index, "title\\:ja*").keySet()).containsExactly(4);
    assertThat(search("ClassicArticle", index, "title\\:jav~1").keySet()).containsExactly(4);
    assertThat(direct(index, "Title:java")).containsExactly(4);
    assertThat(direct(index, "title:java")).containsExactly(5);

    database.transaction(() -> database.command("sql", "UPDATE ClassicArticle SET body = 'nothing here' WHERE k = 4"));
    assertThat(search("ClassicArticle", index, "title\\:java").keySet()).isEmpty();
    assertThat(search("ClassicArticle", index, "title:java").keySet()).containsExactly(5);

    database.command("sql", "REBUILD INDEX `" + index + "`");
    assertThat(search("ClassicArticle", index, "title:java").keySet()).containsExactly(5);
    assertThat(search("ClassicArticle", index, "ti*").keySet()).containsExactly(3);
  }

  @Test
  void aFuzzyTermReachesTheEscapedAndTheUnescapedKeysItExpandsTo() {
    // title\:jav~1 expands to two keys one edit away: ":title:java" (the escaped token of k = 4) and "titlejav" (a plain token). They
    // sort far apart, and the walk of the expansions must reach both
    database.transaction(() -> database.command("sql", "INSERT INTO Article SET k = 7, title = 'x', body = 'titlejav'"));
    assertThat(search("Article", "Article[title,body]", "title\\:jav~1").keySet()).containsExactlyInAnyOrder(4, 7);
    assertThat(search("Article", "Article[title,body]", "titlejav~2").keySet()).containsExactlyInAnyOrder(4, 7);
    assertThat(search("Article", "Article[title,body]", "title:jav~1").keySet()).containsExactly(5);
  }

  @Test
  void aFieldAnalyzerKeepsTheQualifiedPostingsApartFromTheEscapedUnprefixedOnes() {
    // The qualified postings of title are analyzed by the analyzer of the property (EnglishAnalyzer), the unprefixed ones by the
    // analyzer of the index: the escape applies to the unprefixed postings only, whatever writer stores them
    for (final String similarity : new String[] { "CLASSIC", "BM25" }) {
      final String type = "Stemmed" + similarity;
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".title STRING");
        database.command("sql", "CREATE PROPERTY " + type + ".body STRING");
        database.command("sql", "CREATE INDEX ON " + type + " (title, body) FULL_TEXT METADATA {\"similarity\": \"" + similarity
            + "\", \"title_analyzer\": \"org.apache.lucene.analysis.en.EnglishAnalyzer\"}");
        database.command("sql", "INSERT INTO " + type + " SET k = 1, title = 'Running fast', body = 'see title:java here'");
        database.command("sql", "INSERT INTO " + type + " SET k = 2, title = 'java', body = 'x'");
        database.command("sql", "INSERT INTO " + type + " SET k = 3, title = 'Tiger', body = 'cat'");
      });
      final String index = type + "[title,body]";

      // the stemmed qualified postings are reached by the stem, and title:java is the title posting of java alone
      assertThat(search(type, index, "title:run").keySet()).as(similarity).containsExactly(1);
      assertThat(search(type, index, "title:java").keySet()).as(similarity).containsExactly(2);
      assertThat(search(type, index, "body:java").keySet()).as(similarity).isEmpty();
      // the literal token of the body is an unprefixed posting, found by the analyzer of the index
      assertThat(search(type, index, "title\\:java").keySet()).as(similarity).containsExactly(1);
      assertThat(search(type, index, "title\\:ja*").keySet()).as(similarity).containsExactly(1);
      assertThat(search(type, index, "title\\:jav~1").keySet()).as(similarity).containsExactly(1);
      assertThat(search(type, index, "ti*").keySet()).as(similarity).containsExactlyInAnyOrder(1, 3);
      assertThat(search(type, index, "title:ti*").keySet()).as(similarity).containsExactly(3);

      database.transaction(() -> database.command("sql", "UPDATE " + type + " SET body = 'nothing' WHERE k = 1"));
      assertThat(search(type, index, "title\\:java").keySet()).as(similarity).isEmpty();
      assertThat(search(type, index, "title:java").keySet()).as(similarity).containsExactly(2);
      assertThat(search(type, index, "title:run").keySet()).as(similarity).containsExactly(1);

      database.command("sql", "REBUILD INDEX `" + index + "`");
      assertThat(search(type, index, "title:java").keySet()).as(similarity).containsExactly(2);
      assertThat(search(type, index, "ti*").keySet()).as(similarity).containsExactly(3);

      database.transaction(() -> database.command("sql", "DELETE FROM " + type));
      // not one posting is left behind by a key the removal did not name
      assertThat(((TypeIndex) database.getSchema().getIndexByName(index)).countEntries()).as(similarity).isZero();
    }
  }

  private RID ridOf(final int k) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Article WHERE k = :k", Map.of("k", k))) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  private Index bucketIndexOf(final RID rid) {
    for (final Index bucketIndex : ((TypeIndex) database.getSchema().getIndexByName("Article[title,body]")).getIndexesOnBuckets())
      if (bucketIndex.getAssociatedBucketId() == rid.getBucketId())
        return bucketIndex;
    throw new IllegalStateException("no bucket index for " + rid);
  }

  /** The {@code k} of every record the index lookup of {@code text} finds, the path of CONTAINSTEXT rather than of SEARCH_INDEX. */
  private List<Integer> direct(final String indexName, final String text) {
    final List<Integer> ks = new ArrayList<>();
    try (final IndexCursor cursor = ((TypeIndex) database.getSchema().getIndexByName(indexName)).get(new Object[] { text })) {
      while (cursor.hasNext())
        ks.add(database.lookupByRID(cursor.next().getIdentity(), true).asDocument().getInteger("k"));
    }
    return ks;
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
