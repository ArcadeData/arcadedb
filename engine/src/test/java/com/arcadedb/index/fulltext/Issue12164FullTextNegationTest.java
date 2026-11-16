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
 * Issue #12164: a MUST_NOT clause excludes what it matches from the boolean query it belongs to, as in Lucene. A negated group is
 * decided as the group (the negation of a conjunction excludes the records matching every clause of it), and a negation nested in
 * an optional clause does not reach the records the other clauses match.
 */
class Issue12164FullTextNegationTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Doc.k INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.t STRING");
      database.command("sql", "CREATE INDEX ON Doc (t) FULL_TEXT");
      insert("Doc", 4, "apple cherry");
      insert("Doc", 5, "banana");
      insert("Doc", 6, "java python");
      insert("Doc", 7, "java ruby");
      insert("Doc", 8, "java python ruby");

      database.command("sql", "CREATE DOCUMENT TYPE En");
      database.command("sql", "CREATE PROPERTY En.k INTEGER");
      database.command("sql", "CREATE PROPERTY En.t STRING");
      database.command("sql",
          "CREATE INDEX ON En (t) FULL_TEXT METADATA {\"analyzer\": \"org.apache.lucene.analysis.en.EnglishAnalyzer\"}");
      insert("En", 1, "apple cherry");
      insert("En", 2, "banana");
    });
  }

  @Test
  void theNegationOfAConjunctionExcludesOnlyTheRecordsMatchingAllOfIt() {
    assertThat(search("Doc", "java -(+python +ruby)").keySet()).containsExactlyInAnyOrder(6, 7);
    assertThat(search("Doc", "java -(python AND ruby)").keySet()).containsExactlyInAnyOrder(6, 7);
    assertThat(search("Doc", "java -(python OR ruby)").keySet()).isEmpty();
    // a double negation keeps what the inner negation takes out of the group
    assertThat(search("Doc", "java -(python -ruby)").keySet()).containsExactlyInAnyOrder(7, 8);
  }

  @Test
  void aNegationInsideAnOptionalClauseStaysInsideIt() {
    assertThat(search("Doc", "apple OR (banana -cherry)").keySet()).containsExactlyInAnyOrder(4, 5);
    assertThat(search("Doc", "(apple -cherry) OR banana").keySet()).containsExactly(5);
    // the score of a record comes from the positive terms it matches, as before
    assertThat(search("Doc", "apple OR (banana -cherry)").get(4)).isCloseTo(search("Doc", "apple").get(4), within(1e-6f));
  }

  @Test
  void aNegationBesideClausesThatMatchNothingAnswersNothing() {
    // the optional clause matches no record: there is nothing to take the negation from
    assertThat(search("Doc", "zzz -banana").keySet()).isEmpty();
    assertThat(search("Doc", "+zzz -banana").keySet()).isEmpty();
    // a group made of a negation alone matches nothing, as in Lucene
    assertThat(search("Doc", "+java +(-python)").keySet()).isEmpty();
    assertThat(search("Doc", "banana OR (-java)").keySet()).containsExactly(5);
  }

  @Test
  void aQueryWrittenAsANegationAloneAnswersTheComplement() {
    // kept from before: the query is the negation, so it answers every indexed record the negation does not match
    assertThat(search("Doc", "-banana").keySet()).containsExactlyInAnyOrder(4, 6, 7, 8);
    assertThat(search("Doc", "-java -apple").keySet()).containsExactly(5);
  }

  @Test
  void positiveClausesTheAnalyzerDropsDoNotTurnTheQueryIntoANegationAlone() {
    // EnglishAnalyzer drops the stop words: the user wrote positive clauses, which match nothing
    assertThat(search("En", "the was -cherry").keySet()).isEmpty();
    assertThat(search("En", "+the -cherry").keySet()).isEmpty();
    assertThat(search("En", "-cherry").keySet()).containsExactly(2);
  }

  @Test
  void thePerRecordEvaluationAgreesWithTheIndex() {
    final String rids = database.query("sql", "SELECT @rid AS rid FROM Doc").stream()
        .map(r -> r.getProperty("rid").toString()).collect(Collectors.joining(", ", "[", "]"));
    for (final String query : List.of("java -(+python +ruby)", "apple OR (banana -cherry)", "java -(python -ruby)",
        "+java +(-python)", "zzz -banana", "java python -ruby"))
      assertThat(search(rids, query)).as(query).isEqualTo(search("Doc", query));
  }

  private void insert(final String type, final int k, final String text) {
    database.command("sql", "INSERT INTO " + type + " SET k = ?, t = ?", k, text);
  }

  private Map<Integer, Float> search(final String target, final String query) {
    final String index = target.startsWith("[") ? "Doc[t]" : target + "[t]";
    final ResultSet rs = database.query("sql",
        "SELECT k, $score AS s FROM " + target + " WHERE SEARCH_INDEX('" + index + "', :q) = true", Map.of("q", query));
    final Map<Integer, Float> scores = new HashMap<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      scores.put(row.<Integer>getProperty("k"), ((Number) row.getProperty("s")).floatValue());
    }
    return scores;
  }
}
