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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10921: the {@code where} option of {@code vector.neighbors} and {@code db.index.vector.queryNodes} never took the
 * pre-filter plan of issue #6502, because a predicate has no size to compare with the selectivity threshold. A selective
 * predicate therefore walked most of the graph, a record loaded per candidate, until it had met {@code k} members: 20 matches
 * of 100,000 cost about 220 ms against 1 ms for a broad predicate, with or without an index on the property.
 * <p>
 * A predicate whose equalities bind an index of the searched bucket now lists its candidates through it, and when they are
 * under the threshold the search scores them directly. The index answers the whole predicate exactly when it binds every
 * equality and the predicate holds nothing else: no record is loaded then. Otherwise the candidates are scored, then tested
 * nearest first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10921WhereIndexedPrefilterPlanTest extends TestHelper {
  private static final int     RECORDS = 2_000;
  // one record in 100 has tenant 1: 20 of 2,000, far under the 20% threshold
  private static final int     EVERY   = 100;
  private static final int     K       = 5;
  private static final float[] QUERY   = { 0.9f, 0.1f, 0.3f, 0.5f };

  private void seed() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.tenant INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.color STRING");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.day DATE");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });
    final Random random = new Random(10921);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Doc")
            .set("tenant", i % EVERY == 0 ? 1 : 0)
            .set("color", i % 3 == 0 ? "red" : "blue")
            .set("name", i % 4 == 0 ? "Alpha" : "alpha")
            .set("day", i % EVERY == 0 ? LocalDate.of(2024, 1, 2) : LocalDate.of(2024, 1, 1))
            .set("embedding", new float[] { random.nextFloat(), random.nextFloat(), random.nextFloat(), random.nextFloat() })
            .save();
    });
    // Created over the loaded rows, so the graph is built and the search does not answer from the delta buffer alone
    database.command("sql", "CREATE INDEX ON Doc (tenant) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Doc (name COLLATE CI) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Doc (day) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { dimensions: 4, similarity: 'EUCLIDEAN' }");
  }

  @Test
  void anIndexedEqualityMapTakesThePreFilterPlan() {
    seed();

    final long plansBefore = preFilterSearches();
    final long readsBefore = readRecords();
    final List<Result> rows = neighbors("{ where: { tenant: :tenant } }", Map.of("tenant", 1));
    final long reads = readRecords() - readsBefore;

    assertThat(preFilterSearches() - plansBefore).as("every bucket takes the pre-filter plan").isEqualTo(2);
    assertThat(rids(rows)).containsExactlyElementsOf(rids(byFilter("tenant = 1")));

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 0f);
    try {
      final long walkReadsBefore = readRecords();
      assertThat(rids(neighbors("{ where: { tenant: :tenant } }", Map.of("tenant", 1)))).containsExactlyElementsOf(rids(rows));
      final long walkReads = readRecords() - walkReadsBefore;
      // The walk reads most of the records, since the predicate keeps 1 in 100; the plan gives the walk up after a few dozen
      // candidates and scores the 10 the index lists in each bucket
      assertThat(walkReads).as("the walk the plan replaces").isGreaterThan(RECORDS / 2);
      assertThat(reads * 3).isLessThan(walkReads);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
          GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
    }
  }

  @Test
  void anIndexedSqlEqualityTakesThePreFilterPlan() {
    seed();

    final long plansBefore = preFilterSearches();
    final List<Result> rows = neighbors("{ where: 'tenant = :tenant' }", Map.of("tenant", 1));

    assertThat(preFilterSearches() - plansBefore).isEqualTo(2);
    assertThat(rids(rows)).containsExactlyElementsOf(rids(byFilter("tenant = 1")));
  }

  @Test
  void anIndexedEqualityWithAnotherConditionScoresTheIndexedCandidatesThenTestsThem() {
    seed();

    final long plansBefore = preFilterSearches();
    final long readsBefore = readRecords();
    final List<Result> rows = neighbors("{ where: 'tenant = :tenant AND color = :color' }", Map.of("tenant", 1, "color", "red"));
    final long reads = readRecords() - readsBefore;

    assertThat(preFilterSearches() - plansBefore).isEqualTo(2);
    assertThat(rows).hasSize(K);
    assertThat(rids(rows)).containsExactlyElementsOf(rids(byFilter("tenant = 1 AND color = 'red'")));
    // The walk that the plan replaces reads most of the records, as the predicate keeps 7 in 2,000
    assertThat(reads).isLessThan(RECORDS / 3);

    // the same predicate as a map, with the key no index binds
    assertThat(rids(neighbors("{ where: { tenant: 1, color: 'red' } }", Map.of()))).containsExactlyElementsOf(rids(rows));
  }

  @Test
  void aBroadPredicateStaysOnTheGraphWalk() {
    seed();

    final long plansBefore = preFilterSearches();
    final List<Result> rows = neighbors("{ where: { tenant: 0 } }", Map.of());

    assertThat(preFilterSearches()).as("99% of the records are no pre-filter").isEqualTo(plansBefore);
    assertThat(rows).hasSize(K);
    for (final Result row : rows)
      assertThat(row.<Integer>getProperty("tenant")).isEqualTo(0);
  }

  @Test
  void aPredicateNoRecordSatisfiesAnswersNothing() {
    seed();

    final long plansBefore = preFilterSearches();
    assertThat(neighbors("{ where: { tenant: 7 } }", Map.of())).isEmpty();
    assertThat(preFilterSearches() - plansBefore).isEqualTo(2);
  }

  @Test
  void aCaseInsensitiveIndexListsCandidatesThatAreStillTested() {
    seed();

    final long plansBefore = preFilterSearches();
    // The CI index lists every spelling of the name, all 1,000 records of each bucket, and the equality keeps the 'Alpha' ones
    // only: the threshold is raised so the listing qualifies
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 1f);
    try {
      final List<Result> rows = neighbors("{ where: { name: 'Alpha' } }", Map.of());
      assertThat(preFilterSearches() - plansBefore).isEqualTo(2);
      assertThat(rows).hasSize(K);
      for (final Result row : rows)
        assertThat(row.<String>getProperty("name")).isEqualTo("Alpha");
      assertThat(rids(rows)).containsExactlyElementsOf(rids(byFilter("name = 'Alpha'")));
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
          GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
    }
  }

  @Test
  void theListingReadsTheTransactionsOwnWrites() {
    seed();

    database.transaction(() -> {
      // The nearest record of tenant 0 joins tenant 1, the nearest of tenant 1 leaves it
      final RID joining = rids(neighbors("{ where: { tenant: 0 } }", Map.of())).getFirst();
      final RID leaving = rids(neighbors("{ where: { tenant: 1 } }", Map.of())).getFirst();
      final MutableDocument joined = joining.asDocument().modify();
      joined.set("tenant", 1).save();
      final MutableDocument left = leaving.asDocument().modify();
      left.set("tenant", 0).save();

      final List<RID> rows = rids(neighbors("{ where: { tenant: 1 } }", Map.of()));
      assertThat(rows).contains(joining).doesNotContain(leaving);
      assertThat(rows).containsExactlyElementsOf(rids(byFilter("tenant = 1")));
    }, false);
  }

  @Test
  void aShortFilterWithAnUnindexedPredicateIsScoredThenTested() {
    seed();

    final List<RID> scope = new ArrayList<>();
    for (final Result r : query("SELECT @rid FROM Doc LIMIT 60", Map.of()))
      scope.add(r.getProperty("@rid"));

    final long plansBefore = preFilterSearches();
    final List<Result> rows = neighbors("{ filter: :scope, where: { color: 'red' } }", Map.of("scope", scope));

    assertThat(preFilterSearches() - plansBefore).isEqualTo(2);
    final Set<RID> inScope = new HashSet<>(scope);
    for (final Result row : rows) {
      assertThat(inScope).contains(row.<RID>getProperty("@rid"));
      assertThat(row.<String>getProperty("color")).isEqualTo("red");
    }
    final List<RID> redInScope = new ArrayList<>();
    for (final Result r : query("SELECT @rid FROM Doc WHERE @rid IN :scope AND color = 'red'", Map.of("scope", scope)))
      redInScope.add(r.getProperty("@rid"));
    assertThat(rids(rows)).containsExactlyElementsOf(rids(neighbors("{ filter: :scope }", Map.of("scope", redInScope))));
  }

  @Test
  void theGroupedSearchTakesThePlanToo() {
    seed();

    final long plansBefore = preFilterSearches();
    final List<Result> rows = neighbors("{ where: 'tenant = 1', groupBy: 'color', groupSize: 1 }", Map.of());

    assertThat(preFilterSearches() - plansBefore).isEqualTo(2);
    assertThat(rows).hasSize(2);
    final Set<String> colors = new HashSet<>();
    for (final Result row : rows) {
      assertThat(row.<Integer>getProperty("tenant")).isEqualTo(1);
      colors.add(row.getProperty("color"));
    }
    assertThat(colors).containsExactlyInAnyOrder("red", "blue");
  }

  @Test
  void theCypherWhereOptionTakesThePlan() {
    seed();

    final long plansBefore = preFilterSearches();
    final List<RID> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Doc[embedding]', $k, $q, {where: {tenant: $tenant}}) YIELD node RETURN node",
        Map.of("k", K, "q", QUERY, "tenant", 1L))) {
      rs.forEachRemaining(r -> rows.add(r.<Document>getProperty("node").getIdentity()));
    }

    assertThat(preFilterSearches() - plansBefore).isEqualTo(2);
    assertThat(rows).containsExactlyElementsOf(rids(byFilter("tenant = 1")));
  }

  @Test
  void aValueTheKeyTypeReadsDifferentlyIsTestedNotLookedUp() {
    seed();

    // 1.5 is no INTEGER: the lookup would read it as 1 and answer every tenant 1 record, the equality accepts none
    assertThat(neighbors("{ where: { tenant: 1.5 } }", Map.of())).isEmpty();
    // '1' is SQL-equal to 1: the same rows as the number
    assertThat(rids(neighbors("{ where: { tenant: '1' } }", Map.of()))).containsExactlyElementsOf(rids(byFilter("tenant = 1")));
  }

  /**
   * The index lists the candidates of a value only when SQL {@code =} holds the key it reads equal to the value, and skips the
   * test only when that key is all the predicate asks: whatever the kind of the value, the answer is the one the predicate gives
   * tested on every record, which is what the plan answers when it is disabled. Every member is asked for, so both answers are
   * exact.
   */
  @Test
  void everyValueKindAnswersWhatThePredicateTestedOnEachRecordAnswers() {
    seed();

    final Object[][] cases = {
        { "tenant", 1 }, { "tenant", 1L }, { "tenant", 1.0d }, { "tenant", 1.5d }, { "tenant", "1" }, { "tenant", "x" },
        { "tenant", true }, { "day", LocalDate.of(2024, 1, 2) }, { "day", "2024-01-02" }, { "day", LocalDateTime.of(2024, 1, 2, 0, 0) },
        { "day", LocalDateTime.of(2024, 1, 2, 10, 30) }, { "day", 20240102 } };
    for (final Object[] c : cases) {
      final String options = "{ where: { " + c[0] + ": :value } }";
      final Map<String, Object> params = Map.of("value", c[1]);
      final Set<RID> planned = new HashSet<>(rids(neighbors(options, params, RECORDS / EVERY)));

      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 0f);
      try {
        final Set<RID> tested = new HashSet<>(rids(neighbors(options, params, RECORDS / EVERY)));
        assertThat(planned).as("%s = %s (%s)", c[0], c[1], c[1].getClass().getSimpleName()).isEqualTo(tested);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
            GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
      }
    }
  }

  private List<Result> neighbors(final String options, final Map<String, Object> params) {
    return neighbors(options, params, K);
  }

  private List<Result> neighbors(final String options, final Map<String, Object> params, final int k) {
    final Map<String, Object> all = new HashMap<>(params);
    all.put("q", QUERY);
    return query("SELECT expand(`vector.neighbors`('Doc[embedding]', :q, " + k + ", " + options + "))", all);
  }

  /** The reference answer: the same search restricted by the RIDs the condition selects, the pre-filter plan of a list. */
  private List<Result> byFilter(final String condition) {
    final List<RID> scope = new ArrayList<>();
    for (final Result r : query("SELECT @rid FROM Doc WHERE " + condition, Map.of()))
      scope.add(r.getProperty("@rid"));
    return neighbors("{ filter: :scope }", Map.of("scope", scope));
  }

  private List<Result> query(final String sql, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      rs.forEachRemaining(rows::add);
    }
    return rows;
  }

  private static List<RID> rids(final List<Result> rows) {
    final List<RID> out = new ArrayList<>(rows.size());
    for (final Result r : rows)
      out.add(r.getProperty("@rid"));
    return out;
  }

  private long readRecords() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }

  private long preFilterSearches() {
    long total = 0;
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    for (final IndexInternal bucketIndex : index.getIndexesOnBuckets())
      total += ((Number) bucketIndex.getStats().getOrDefault("preFilterSearches", 0L)).longValue();
    return total;
  }
}
