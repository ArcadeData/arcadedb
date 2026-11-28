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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.index.fulltext.FullTextQueryParseException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Oracle tests of the SQL read statements an application that stores chunks with an INT8 {@code LSM_VECTOR} index and a
 * {@code FULL_TEXT} index on a 16 bucket type sends: {@code vector.neighbors} with a RID-list, a subquery or a {@code where}
 * allow-list, and {@code SEARCH_INDEX} with {@code ORDER BY $score DESC LIMIT} over a type or over a RID-list target. The model is
 * brute force (cosine) and the documented BM25 formula; a differential fuzz of these statements against the same model (1,000 to
 * 20,000 vectors of 1024 dimensions) found no difference, and these tests pin the behaviours it relied on.
 */
class SqlReadLaneOracleTest extends TestHelper {
  private static final int      DIM    = 8;
  private static final int      CHUNKS = 120;
  private static final String[] WORDS  = { "alpha", "beta", "gamma", "delta", "epsilon", "the", "of", "bank", "zeta" };

  private final List<String>  rids        = new ArrayList<>();
  private final List<String>  externalIds = new ArrayList<>();
  private final List<float[]> vectors     = new ArrayList<>();
  private final List<String>  groups      = new ArrayList<>();
  private final List<String>  texts       = new ArrayList<>();
  private final List<String>  questions   = new ArrayList<>();
  private final List<String>  entityRids  = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Chunk BUCKETS 16");
    database.command("sql", "CREATE VERTEX TYPE Entity BUCKETS 2");
    for (final String type : new String[] { "Chunk", "Entity" }) {
      database.command("sql", "CREATE PROPERTY " + type + ".external_id STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".group_id STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
    }
    database.command("sql", "CREATE PROPERTY Chunk.text STRING");
    database.command("sql", "CREATE PROPERTY Chunk.questions_text STRING");
    database.command("sql", "CREATE PROPERTY Chunk.deleted_at STRING");
    database.command("sql", "CREATE INDEX ON Chunk (external_id) UNIQUE");
    database.command("sql", "CREATE INDEX ON Chunk (text) FULL_TEXT");
    database.command("sql", "CREATE INDEX ON Chunk (questions_text) FULL_TEXT");
    final String meta = " LSM_VECTOR METADATA { \"dimensions\": " + DIM + ", \"similarity\": \"COSINE\", \"quantization\": \"INT8\","
        + " \"storeVectorsInGraph\": true, \"maxConnections\": 16, \"beamWidth\": 80, \"addHierarchy\": false }";
    database.command("sql", "CREATE INDEX ON Chunk (embedding)" + meta);
    database.command("sql", "CREATE INDEX ON Entity (embedding)" + meta);

    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < CHUNKS; i++) {
        final float[] vector = new float[DIM];
        for (int d = 0; d < DIM; d++)
          vector[d] = (float) random.nextGaussian();
        final String text;
        if (i % 10 == 7)
          text = null;                      // no text at all: still a record of the type
        else if (i % 10 == 8)
          text = "";                        // empty text
        else if (i % 10 == 9)
          text = "the of the";              // stop words only: the default analyzer keeps them
        else {
          final StringBuilder b = new StringBuilder();
          final int words = 2 + random.nextInt(9);
          for (int w = 0; w < words; w++)
            b.append(WORDS[random.nextInt(WORDS.length)]).append(w % 3 == 2 ? ", " : " ");
          text = b.toString().trim();
        }
        final String question = i % 4 == 0 ? WORDS[random.nextInt(5)] + " " + WORDS[random.nextInt(5)] : null;
        final var vertex = database.newVertex("Chunk").set("external_id", "c" + i, "group_id", "g" + (i % 4), "embedding", vector);
        if (text != null)
          vertex.set("text", text);
        if (question != null)
          vertex.set("questions_text", question);
        vertex.save();
        rids.add(vertex.getIdentity().toString());
        externalIds.add("c" + i);
        vectors.add(vector);
        groups.add("g" + (i % 4));
        texts.add(text);
        questions.add(question);
      }
      for (int i = 0; i < 10; i++) {
        final float[] vector = new float[DIM];
        for (int d = 0; d < DIM; d++)
          vector[d] = (float) random.nextGaussian();
        final var entity = database.newVertex("Entity").set("external_id", "e" + i, "group_id", "g0", "embedding", vector).save();
        entityRids.add(entity.getIdentity().toString());
      }
    });
  }

  private static double distance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return 1.0 - dot / Math.sqrt(na * nb);
  }

  private List<Result> rows(final String sql, final Map<String, Object> params) {
    final List<Result> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      rs.forEachRemaining(out::add);
    }
    return out;
  }

  private static final String ANN = "SELECT external_id AS external_id, @rid AS rid, distance AS d FROM"
      + " (SELECT expand(vector.neighbors('Chunk[embedding]', :qv, :k, %s)))";

  private static double number(final Result row, final String property) {
    return ((Number) row.getProperty(property)).doubleValue();
  }

  private List<Result> ann(final String options, final int k, final float[] query, final Map<String, Object> extra) {
    final Map<String, Object> params = new HashMap<>(extra);
    params.put("qv", query);
    params.put("k", k);
    return rows(String.format(ANN, options), params);
  }

  private Set<String> eligibleOf(final String group) {
    final Set<String> out = new LinkedHashSet<>();
    for (int i = 0; i < CHUNKS; i++)
      if (groups.get(i).equals(group))
        out.add(rids.get(i));
    return out;
  }

  @Test
  void exhaustiveRidFilterAnswersEveryEligibleRecordDespiteDuplicatesForeignAndDeletedRids() {
    final Set<String> eligible = eligibleOf("g1");
    final List<Object> filter = new ArrayList<>(eligible);
    filter.addAll(new ArrayList<>(eligible).subList(0, 5));         // duplicates
    filter.addAll(entityRids);                                       // RIDs of another type
    filter.add(null);                                                // a null element is skipped
    filter.add("#999:1");                                            // a RID that never existed
    for (final int k : new int[] { eligible.size(), eligible.size() + 1, eligible.size() + 500 }) {
      final List<Result> result = ann("{filter: :rids}", k, vectors.get(0), Map.of("rids", filter));
      final Set<String> got = new HashSet<>();
      for (final Result r : result)
        assertThat(got.add(r.getProperty("rid").toString())).as("no duplicate row").isTrue();
      assertThat(got).as("k=%d", k).isEqualTo(eligible);
      for (final Result r : result)
        assertThat(((Number) r.getProperty("d")).doubleValue())
            .isCloseTo(distance(vectors.get(0), vectors.get(externalIds.indexOf((String) r.getProperty("external_id")))), within(0.02));
    }
  }

  @Test
  void aRidFilterBelowTheEligibleCountAnswersKRowsOfTheFilterOnly() {
    final Set<String> eligible = eligibleOf("g2");
    final List<Result> result = ann("{filter: :rids}", 7, vectors.get(3), Map.of("rids", new ArrayList<>(eligible)));
    assertThat(result).hasSize(7);
    for (final Result r : result)
      assertThat(eligible).contains(r.getProperty("rid").toString());
    for (int i = 1; i < result.size(); i++)
      assertThat(number(result.get(i), "d")).isGreaterThanOrEqualTo(number(result.get(i - 1), "d"));
  }

  @Test
  void anEmptyFilterAnswersNothingAndAFilterOfNullsToo() {
    assertThat(ann("{filter: :rids}", 10, vectors.get(0), Map.of("rids", List.of()))).isEmpty();
    final List<Object> nulls = new ArrayList<>();
    nulls.add(null);
    nulls.add(null);
    assertThat(ann("{filter: :rids}", 10, vectors.get(0), Map.of("rids", nulls))).isEmpty();
    assertThat(ann("{filter: (SELECT @rid FROM Chunk WHERE external_id = 'none')}", 10, vectors.get(0), Map.of())).isEmpty();
  }

  @Test
  void aSubqueryAWhereMapAndARidListOfTheSameRecordsAnswerTheSameRows() {
    final Set<String> eligible = eligibleOf("g3");
    final Set<String> byList = new HashSet<>();
    ann("{filter: :rids}", 1000, vectors.get(5), Map.of("rids", new ArrayList<>(eligible)))
        .forEach(r -> byList.add(r.getProperty("rid").toString()));
    final Set<String> bySubquery = new HashSet<>();
    ann("{filter: (SELECT @rid FROM Chunk WHERE group_id = 'g3')}", 1000, vectors.get(5), Map.of())
        .forEach(r -> bySubquery.add(r.getProperty("rid").toString()));
    final Set<String> byWhere = new HashSet<>();
    ann("{where: {group_id: :g}}", 1000, vectors.get(5), Map.of("g", "g3")).forEach(r -> byWhere.add(r.getProperty("rid").toString()));
    assertThat(byList).isEqualTo(eligible);
    assertThat(bySubquery).isEqualTo(eligible);
    assertThat(byWhere).isEqualTo(eligible);
  }

  @Test
  void aHardDeletedRecordDisappearsFromAFilterListThatStillNamesIt() {
    final String gone = rids.get(4);
    final List<String> filter = new ArrayList<>(eligibleOf("g0"));
    assertThat(filter).contains(gone);
    database.transaction(() -> database.command("sql", "DELETE FROM Chunk WHERE external_id = 'c4'"));
    final List<Result> result = ann("{filter: :rids}", 1000, vectors.get(0), Map.of("rids", filter));
    assertThat(result).hasSize(filter.size() - 1);
    for (final Result r : result)
      assertThat(r.getProperty("rid").toString()).isNotEqualTo(gone);
  }

  @Test
  void orderBySkipAndLimitSliceTheAnswerOfTheSameFilteredSearch() {
    final List<String> filter = new ArrayList<>(eligibleOf("g1"));
    final List<Result> all = ann("{filter: :rids}", 1000, vectors.get(9), Map.of("rids", filter));
    final String sliced = "SELECT @rid AS rid, distance AS d FROM"
        + " (SELECT expand(vector.neighbors('Chunk[embedding]', :qv, :k, {filter: :rids}))) ORDER BY distance ASC SKIP 4 LIMIT 6";
    final List<Result> slice = rows(sliced, Map.of("qv", vectors.get(9), "k", 1000, "rids", filter));
    assertThat(slice).hasSize(6);
    final double[] distances = all.stream().mapToDouble(r -> ((Number) r.getProperty("d")).doubleValue()).sorted().toArray();
    for (int i = 0; i < 6; i++)
      assertThat(((Number) slice.get(i).getProperty("d")).doubleValue()).isCloseTo(distances[4 + i], within(1e-6));
  }

  // ---------------------------------------------------------------- BM25

  private static List<String> tokens(final String text) {
    final List<String> out = new ArrayList<>();
    if (text == null)
      return out;
    for (final String t : text.toLowerCase().split("[^a-z0-9]+"))
      if (!t.isEmpty())
        out.add(t);
    return out;
  }

  /** The documented formula; N counts every record of the type, a document's length counts its stop words. */
  private double[] bm25(final List<String> docs, final String query) {
    final int n = docs.size();
    double sumLength = 0;
    for (final String d : docs)
      sumLength += tokens(d).size();
    final double avg = sumLength / n;
    final double[] scores = new double[n];
    final Set<String> queryTokens = new LinkedHashSet<>(tokens(query));
    for (final String term : queryTokens) {
      int df = 0;
      for (final String d : docs)
        if (tokens(d).contains(term))
          df++;
      final double idf = Math.log((n - df + 0.5) / (df + 0.5) + 1.0);
      for (int i = 0; i < n; i++) {
        final List<String> t = tokens(docs.get(i));
        final long tf = t.stream().filter(term::equals).count();
        if (tf > 0)
          scores[i] += idf * (tf * 2.2) / (tf + 1.2 * (1 - 0.75 + 0.75 * t.size() / avg));
      }
    }
    return scores;
  }

  private boolean matches(final String doc, final String query) {
    final List<String> t = tokens(doc);
    for (final String q : tokens(query))
      if (t.contains(q))
        return true;
    return false;
  }

  @Test
  void bm25ScoresFollowTheFormulaOverEveryRecordOfTheType() {
    for (final String query : new String[] { "bank", "alpha beta", "the", "gamma delta bank", "zeta of" }) {
      final double[] expected = bm25(texts, query);
      final List<Result> result = rows("SELECT external_id AS e, $score AS s FROM Chunk WHERE SEARCH_INDEX('Chunk[text]', :q) = true"
          + " ORDER BY $score DESC LIMIT 1000", Map.of("q", query));
      int matching = 0;
      for (int i = 0; i < CHUNKS; i++)
        if (matches(texts.get(i), query))
          matching++;
      assertThat(result).as(query).hasSize(matching);
      double previous = Double.MAX_VALUE;
      for (final Result r : result) {
        final int i = externalIds.indexOf((String) r.getProperty("e"));
        final double score = ((Number) r.getProperty("s")).doubleValue();
        assertThat(score).as("%s %s", query, r.<String>getProperty("e")).isCloseTo(expected[i], within(1e-4));
        assertThat(score).isLessThanOrEqualTo(previous);
        previous = score;
      }
    }
  }

  @Test
  void aLimitKeepsTheBestMatchesOfWhatTheOtherConditionsLeave() {
    final String query = "alpha bank";
    final double[] expected = bm25(texts, query);
    final List<Double> best = new ArrayList<>();
    for (int i = 0; i < CHUNKS; i++)
      if (groups.get(i).equals("g2") && matches(texts.get(i), query))
        best.add(expected[i]);
    best.sort((a, b) -> Double.compare(b, a));
    final List<Result> result = rows("SELECT external_id AS e, $score AS s FROM Chunk WHERE SEARCH_INDEX('Chunk[text]', :q) = true"
        + " AND group_id = :g ORDER BY $score DESC LIMIT 5", Map.of("q", query, "g", "g2"));
    assertThat(result).hasSize(Math.min(5, best.size()));
    for (int i = 0; i < result.size(); i++) {
      assertThat(((Number) result.get(i).getProperty("s")).doubleValue()).isCloseTo(best.get(i), within(1e-4));
      assertThat(groups.get(externalIds.indexOf((String) result.get(i).getProperty("e")))).isEqualTo("g2");
    }
  }

  @Test
  void aRidListTargetReturnsARecordOncePerOccurrenceAndSkipsForeignAndNeverExistingRids() {
    final String query = "alpha";
    final List<String> list = new ArrayList<>();
    final Set<String> wanted = new HashSet<>();
    for (int i = 0; i < 40; i++) {
      list.add(rids.get(i));
      if (matches(texts.get(i), query))
        wanted.add(rids.get(i));
    }
    final String duplicated = rids.get(0);
    list.add(duplicated);
    list.addAll(entityRids);
    list.add("#999:1");
    final String target = String.join(", ", list);
    final List<Result> result = rows("SELECT @rid AS rid, $score AS s FROM [" + target
        + "] WHERE SEARCH_INDEX('Chunk[text]', :q) = true ORDER BY $score DESC LIMIT 1000", Map.of("q", query));
    final Map<String, Integer> seen = new HashMap<>();
    for (final Result r : result)
      seen.merge(r.getProperty("rid").toString(), 1, Integer::sum);
    assertThat(seen.keySet()).isEqualTo(wanted);
    final double[] expected = bm25(texts, query);
    for (final Result r : result)
      assertThat(number(r, "s")).isCloseTo(expected[rids.indexOf(r.getProperty("rid").toString())], within(1e-4));
    if (wanted.contains(duplicated))
      assertThat(seen.get(duplicated)).isEqualTo(2);
  }

  @Test
  void aPlainRidListTargetSkipsADeletedAndANeverExistingRidAndKeepsDuplicatesAndOtherTypes() {
    database.transaction(() -> database.command("sql", "DELETE FROM Chunk WHERE external_id = 'c4'"));
    final List<String> list = new ArrayList<>(rids.subList(0, 10));
    list.add(rids.get(2));
    list.add(entityRids.get(0));
    list.add("#999:1");
    final String target = "[" + String.join(", ", list) + "]";

    final List<String> expected = new ArrayList<>(list);
    expected.remove(rids.get(4));
    expected.remove("#999:1");
    final List<String> got = new ArrayList<>();
    rows("SELECT @rid AS rid FROM " + target, Map.of()).forEach(r -> got.add(r.getProperty("rid").toString()));
    assertThat(got).containsExactlyElementsOf(expected);

    final List<String> filtered = new ArrayList<>();
    rows("SELECT @rid AS rid FROM " + target + " WHERE group_id = 'g1'", Map.of())
        .forEach(r -> filtered.add(r.getProperty("rid").toString()));
    final List<String> expectedFiltered = new ArrayList<>();
    for (final String rid : expected)
      if (rids.contains(rid) && groups.get(rids.indexOf(rid)).equals("g1"))
        expectedFiltered.add(rid);
    assertThat(filtered).isNotEmpty().containsExactlyElementsOf(expectedFiltered);

    final List<String> paged = new ArrayList<>();
    rows("SELECT @rid AS rid FROM " + target + " SKIP 3 LIMIT 4", Map.of()).forEach(r -> paged.add(r.getProperty("rid").toString()));
    assertThat(paged).containsExactlyElementsOf(expected.subList(3, 7));
  }

  @Test
  void anOrOfTwoFullTextIndexesScoresTheHigherArmForTheTypeAndForARidList() {
    final String query = "alpha beta";
    final double[] byText = bm25(texts, query);
    final double[] byQuestion = bm25(questions, query);
    final Set<String> union = new HashSet<>();
    for (int i = 0; i < CHUNKS; i++)
      if (matches(texts.get(i), query) || matches(questions.get(i), query))
        union.add(rids.get(i));
    final String where = "(SEARCH_INDEX('Chunk[text]', :q) = true OR SEARCH_INDEX('Chunk[questions_text]', :q) = true)";
    final String order = " ORDER BY $score DESC LIMIT 1000";
    final List<Result> overType = rows("SELECT @rid AS rid, $score AS s FROM Chunk WHERE " + where + order, Map.of("q", query));
    final List<Result> overList = rows("SELECT @rid AS rid, $score AS s FROM [" + String.join(", ", rids) + "] WHERE " + where + order,
        Map.of("q", query));
    for (final List<Result> result : List.of(overType, overList)) {
      final Set<String> got = new HashSet<>();
      for (final Result r : result) {
        final String rid = r.getProperty("rid").toString();
        assertThat(got.add(rid)).as("one row per record").isTrue();
        final int i = rids.indexOf(rid);
        final double higher = Math.max(matches(texts.get(i), query) ? byText[i] : 0, matches(questions.get(i), query) ? byQuestion[i] : 0);
        assertThat(((Number) r.getProperty("s")).doubleValue()).isCloseTo(higher, within(1e-4));
      }
      assertThat(got).isEqualTo(union);
    }
  }

  @Test
  void anEmptyQueryAndAnOperatorOnlyQueryAreRefusedAsParseErrorsAndANullQueryMatchesNothing() {
    for (final String bad : new String[] { "", "   ", "!!!", "&&" })
      assertThatThrownBy(() -> rows("SELECT external_id FROM Chunk WHERE SEARCH_INDEX('Chunk[text]', :q) = true LIMIT 5",
          Map.of("q", bad))).as("'%s'", bad).isInstanceOf(FullTextQueryParseException.class);
    final Map<String, Object> nullQuery = new HashMap<>();
    nullQuery.put("q", null);
    assertThat(rows("SELECT external_id FROM Chunk WHERE SEARCH_INDEX('Chunk[text]', :q) = true LIMIT 5", nullQuery)).isEmpty();
  }
}
