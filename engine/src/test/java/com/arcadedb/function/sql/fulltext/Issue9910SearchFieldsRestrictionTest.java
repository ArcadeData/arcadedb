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
package com.arcadedb.function.sql.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9910: {@code SEARCH_FIELDS(['a'], q)} searched every field of the index it picked, so a term found only in another
 * field matched too, and it was evaluated row by row after a scan of the whole type instead of through the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9910SearchFieldsRestrictionTest extends TestHelper {
  private static final int NOTES = 400;

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
  void theSearchIsRestrictedToTheGivenFields(final String variant) {
    createCorpus(variant);

    // 'zebra' is only in field b of one note
    assertThat(count("SEARCH_FIELDS(['a'], 'zebra') = true")).isZero();
    assertThat(count("SEARCH_FIELDS(['b'], 'zebra') = true")).isEqualTo(1);
    assertThat(count("SEARCH_FIELDS(['a', 'b'], 'zebra') = true")).isEqualTo(1);
    assertThat(count("SEARCH_FIELDS(['b', 'a'], 'zebra') = true")).isEqualTo(1);

    // 'fever' is in a for the even notes and in b for every third note: listing both matches either
    final long inA = NOTES / 2;
    final long inB = (NOTES + 2) / 3;
    final long inBoth = (NOTES + 5) / 6;
    assertThat(count("SEARCH_FIELDS(['a'], 'fever') = true")).isEqualTo(inA);
    assertThat(count("SEARCH_FIELDS(['b'], 'fever') = true")).isEqualTo(inB);
    assertThat(count("SEARCH_FIELDS(['a', 'b'], 'fever') = true")).isEqualTo(inA + inB - inBoth);

    // Every kind of clause is restricted, not only a term: a phrase, a prefix, a wildcard, a fuzzy term, a negation
    assertThat(count("SEARCH_FIELDS(['a'], '\"zebra stripes\"') = true")).isZero();
    assertThat(count("SEARCH_FIELDS(['b'], '\"zebra stripes\"') = true")).isEqualTo(1);
    assertThat(count("SEARCH_FIELDS(['a'], 'zeb*') = true")).isZero();
    assertThat(count("SEARCH_FIELDS(['a'], 'z?bra') = true")).isZero();
    assertThat(count("SEARCH_FIELDS(['a'], 'zebre~1') = true")).isZero();
    assertThat(count("SEARCH_FIELDS(['b'], 'zebre~1') = true")).isEqualTo(1);
    assertThat(count("SEARCH_FIELDS(['a'], 'fever AND -cough') = true")).isEqualTo(count("SEARCH_INDEX('Note[a,b]', 'a:fever AND -a:cough') = true"));

    // A clause the query qualifies itself keeps its field
    assertThat(count("SEARCH_FIELDS(['a'], 'b:zebra') = true")).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
  void scoresAreThoseOfTheEquivalentFieldQualifiedSearchIndex(final String variant) {
    createCorpus(variant);
    assertSameScores("SEARCH_FIELDS(['a'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'a:fever')");
    assertSameScores("SEARCH_FIELDS(['a'], 'fever cough')", "SEARCH_INDEX('Note[a,b]', 'a:fever a:cough')");
    assertSameScores("SEARCH_FIELDS(['b'], 'fever OR zebra')", "SEARCH_INDEX('Note[a,b]', 'b:fever OR b:zebra')");
    // Over exactly the fields of the index, it is SEARCH_INDEX on that index
    assertSameScores("SEARCH_FIELDS(['a', 'b'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'fever')");
  }

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_4" })
  void theConditionIsAnsweredByTheIndex(final String variant) {
    createCorpus(variant);

    final String plan = plan("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true");
    assertThat(plan).contains("FETCH FROM INDEXED FUNCTION").doesNotContain("FETCH FROM TYPE");

    // The pushdowns of SEARCH_INDEX apply: the limit, best first, and the lazy loading
    assertThat(plan("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true LIMIT 10")).contains("(best 10 needed)");
    final List<RID> ordered = rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true ORDER BY $score DESC");
    assertThat(rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true LIMIT 10")).isEqualTo(ordered.subList(0, 10));
    assertThat(rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true")).isEqualTo(ordered);

    // The first search computes the index's corpus statistics with a scan of the type: not the subject
    rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'cough') = true LIMIT 1");
    final long before = readRecords();
    assertThat(rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'cough') = true LIMIT 10")).hasSize(10);
    assertThat(readRecords() - before).isEqualTo(10);

    // Inside an OR with a plain condition the type is scanned and the condition evaluated per record: same answer
    assertThat(count("SEARCH_FIELDS(['a'], 'zebra') = true OR n < 0")).isZero();
    assertThat(count("SEARCH_FIELDS(['b'], 'zebra') = true OR n < 0")).isEqualTo(1);
  }

  @Test
  void aNegatedConditionIsNotAnsweredWithTheMatches() {
    createCorpus("BM25_4");
    // The index answers the records that match: '= false' asks for the others, which only a scan can find
    assertThat(count("SEARCH_FIELDS(['b'], 'zebra') = false")).isEqualTo(NOTES - 1);
    assertThat(count("SEARCH_INDEX('Note[a,b]', 'zebra') = false")).isEqualTo(NOTES - 1);
    assertThat(count("SEARCH_INDEX('Note[a,b]', 'zebra') <> true")).isEqualTo(NOTES - 1);
    assertThat(count("SEARCH_INDEX('Note[a,b]', 'zebra') = true")).isEqualTo(1);
    assertThat(plan("SELECT FROM Note WHERE SEARCH_INDEX('Note[a,b]', 'zebra') = false")).doesNotContain("INDEXED FUNCTION");
  }

  @Test
  void theIndexOverTheFewestPropertiesIsSearched() {
    createCorpus("BM25_1");
    database.command("sql", "CREATE INDEX ON Note (a) FULL_TEXT");

    // The single-property index answers the search without restriction, and its own statistics give the scores
    assertSameScores("SEARCH_FIELDS(['a'], 'fever')", "SEARCH_INDEX('Note[a]', 'fever')");
    assertThat(count("SEARCH_FIELDS(['a'], 'zebra') = true")).isZero();
    // b is only in the two-property index
    assertThat(count("SEARCH_FIELDS(['b'], 'zebra') = true")).isEqualTo(1);
  }

  @Test
  void fieldNamesAreASetAndAByItemPropertyIsNamedByItsBaseName() {
    createCorpus("BM25_1");
    // Order and repetitions change nothing
    assertSameScores("SEARCH_FIELDS(['b', 'a', 'a'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'fever')");
    assertSameScores("SEARCH_FIELDS(['a', 'a'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'a:fever')");
    assertThat(catchThrowable(() -> count("SEARCH_FIELDS([null], 'fever') = true"))).hasMessageContaining("cannot be null");

    database.transaction(() -> {
      database.command("sql", "CREATE PROPERTY Note.tags LIST OF STRING");
      database.command("sql", "CREATE INDEX ON Note (a, tags BY ITEM) FULL_TEXT");
      database.newVertex("Note").set("n", NOTES).set("a", "nothing").set("b", "nothing").set("tags", List.of("okapi", "fever"))
          .save();
    });
    assertThat(count("SEARCH_FIELDS(['tags'], 'okapi') = true")).isEqualTo(1);
    assertThat(count("SEARCH_FIELDS(['tags'], 'patient') = true")).isZero();
    assertThat(count("SEARCH_FIELDS(['a'], 'okapi') = true")).isZero();
  }

  @Test
  void amongCoveringIndexesOfTheSameSizeTheFirstByNameIsSearched() {
    createCorpus("BM25_1");
    database.transaction(() -> {
      database.command("sql", "CREATE PROPERTY Note.c STRING");
      database.command("sql", "UPDATE Note SET c = 'filler words for a longer document' WHERE n % 2 = 0");
      database.command("sql", "CREATE INDEX ON Note (a, c) FULL_TEXT");
    });
    // Note[a,b] and Note[a,c] both cover a with two properties: Note[a,b] comes first. Their corpus statistics differ (c
    // makes documents longer), so the scores tell which one was searched
    assertThat(scores("SEARCH_INDEX('Note[a,b]', 'a:fever')")).isNotEqualTo(scores("SEARCH_INDEX('Note[a,c]', 'a:fever')"));
    assertSameScores("SEARCH_FIELDS(['a'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'a:fever')");
    assertSameScores("SEARCH_FIELDS(['c'], 'filler')", "SEARCH_INDEX('Note[a,c]', 'c:filler')");
  }

  @Test
  void aSubtypeIsSearchedThroughTheIndexOfItsSupertype() {
    createCorpus("BM25_1");
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE UrgentNote EXTENDS Note");
      database.newVertex("UrgentNote").set("n", NOTES).set("a", "zebra crossing").set("b", "nothing").save();
    });

    assertThat(count("SEARCH_FIELDS(['a'], 'zebra') = true")).isEqualTo(1);
    assertThat(rids("SELECT FROM UrgentNote WHERE SEARCH_FIELDS(['b'], 'zebra') = true")).isEmpty();

    // The subtype defines its own index over a only. A search FROM the supertype is fetched through the supertype's index, and
    // the subtype's rows are evaluated again on that same index, with its scores, not on the subtype's own
    database.command("sql", "CREATE INDEX ON UrgentNote (a) FULL_TEXT");
    assertThat(rids("SELECT FROM UrgentNote WHERE SEARCH_FIELDS(['a'], 'zebra') = true")).hasSize(1);
    final Map<Object, Float> viaSupertype = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, $score AS score FROM Note WHERE SEARCH_FIELDS(['a'], 'zebra crossing') = true")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        viaSupertype.put(row.getProperty("rid"), row.<Float>getProperty("score"));
      }
    }
    assertThat(viaSupertype).isEqualTo(scores("SEARCH_INDEX('Note[a,b]', 'a:zebra a:crossing')"));
  }

  private void assertSameScores(final String searchFields, final String searchIndex) {
    final Map<Object, Float> expected = scores(searchIndex);
    assertThat(expected).as(searchIndex).isNotEmpty();
    assertThat(scores(searchFields)).as(searchFields).isEqualTo(expected);
  }

  private Map<Object, Float> scores(final String condition) {
    final Map<Object, Float> scores = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, $score AS score FROM Note WHERE " + condition + " = true ORDER BY $score DESC")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        scores.put(row.getProperty("rid"), row.<Float>getProperty("score"));
      }
    }
    return scores;
  }

  private void createCorpus(final String variant) {
    final String similarity = variant.substring(0, variant.indexOf('_'));
    final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Note BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Note.a STRING");
      database.command("sql", "CREATE PROPERTY Note.b STRING");
      database.command("sql", "CREATE PROPERTY Note.n INTEGER");
      database.command("sql", "CREATE INDEX ON Note (a, b) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
      for (int i = 0; i < NOTES; i++) {
        final String a = (i % 2 == 0 ? "fever " : "") + (i % 4 == 0 ? "cough " : "") + "patient ".repeat(1 + i % 3);
        final String b = (i % 3 == 0 ? "fever " : "") + (i == 7 ? "zebra stripes " : "") + "notes ".repeat(1 + i % 2);
        database.newVertex("Note").set("n", i).set("a", a).set("b", b).save();
      }
    });
  }

  private long count(final String condition) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM Note WHERE " + condition)) {
      return rs.next().<Long>getProperty("c");
    }
  }

  private long readRecords() {
    return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<RID> rids(final String query) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rids.add(rs.next().getIdentity().orElseThrow());
    }
    return rids;
  }
}
