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
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the vector SQL functions, one nested class per fixed issue.
 */
class VectorNeighborsPrefilterTest {
  /**
   * Issue #10169: restricting {@code vector.neighbors} by a property took a {@code filter} subquery listing every RID in scope,
   * which loads every record in scope and builds a set of them on every call. The {@code where} option tests the predicate on
   * each candidate the search meets instead, as an equality map or as a SQL condition, on {@code vector.neighbors} and
   * {@code vector.sparseNeighbors} alike.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorNeighborsWherePredicateTest extends TestHelper {
    private static final int     RECORDS = 600;
    private static final float[] QUERY   = { 1.0f, 0.2f, 0.1f, 0.0f };

    private void seedDense() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Passage BUCKETS 2");
        database.command("sql", "CREATE PROPERTY Passage.space STRING");
        database.command("sql", "CREATE PROPERTY Passage.year INTEGER");
        database.command("sql", "CREATE PROPERTY Passage.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Passage (embedding) LSM_VECTOR METADATA { dimensions: 4, similarity: 'COSINE' }");
      });
      final Random random = new Random(10169);
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument("Passage")
              .set("space", i % 3 == 0 ? "a" : "b")
              .set("year", 2000 + i % 25)
              .set("embedding", new float[] { random.nextFloat(), random.nextFloat(), random.nextFloat(), random.nextFloat() })
              .save();
      });
    }

    private List<Result> query(final String sql, final Object... args) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql, args)) {
        rs.forEachRemaining(rows::add);
      }
      return rows;
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

    @Test
    void anEqualityMapAnswersWhatTheFilterSubqueryAnswers() {
      seedDense();

      final long beforeFilter = readRecords();
      final List<Result> byFilter = query("""
          SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 10,
            { filter: (SELECT @rid FROM Passage WHERE space = 'a').@rid }))""", QUERY);
      final long filterLoads = readRecords() - beforeFilter;

      final long beforeWhere = readRecords();
      final List<Result> byWhere = query("""
          SELECT expand(`vector.neighbors`('Passage[embedding]', :q, 10, { where: { space: :space } }))""",
          Map.of("q", QUERY, "space", "a"));
      final long whereLoads = readRecords() - beforeWhere;

      assertThat(byWhere).hasSize(10);
      for (final Result r : byWhere)
        assertThat(r.<String>getProperty("space")).isEqualTo("a");
      assertThat(rids(byWhere)).containsExactlyElementsOf(rids(byFilter));
      // The filter loads every record in scope (200 of them) to list their RIDs; the predicate only the candidates the search met
      assertThat(whereLoads).isLessThan(filterLoads);
    }

    @Test
    void aSqlConditionWithAParameterRestrictsTheCandidates() {
      seedDense();

      final List<Result> rows = query("""
          SELECT expand(`vector.neighbors`('Passage[embedding]', :q, 8, { where: 'space = :space AND year >= :minYear' }))""",
          Map.of("q", QUERY, "space", "b", "minYear", 2020));

      assertThat(rows).hasSize(8);
      for (final Result r : rows) {
        assertThat(r.<String>getProperty("space")).isEqualTo("b");
        assertThat(r.<Integer>getProperty("year")).isGreaterThanOrEqualTo(2020);
      }
      final List<Result> reference = query("""
          SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 8,
            { filter: (SELECT @rid FROM Passage WHERE space = 'b' AND year >= 2020).@rid }))""", QUERY);
      assertThat(rids(rows)).containsExactlyElementsOf(rids(reference));
    }

    @Test
    void whereAndFilterTogetherKeepOnlyTheCandidatesBothAccept() {
      seedDense();
      final List<RID> scope = new ArrayList<>();
      for (final Result r : query("SELECT @rid AS rid FROM Passage WHERE year < 2005"))
        scope.add(r.getProperty("rid"));

      final List<Result> rows = query("""
          SELECT expand(`vector.neighbors`('Passage[embedding]', :q, 5, { filter: :scope, where: { space: 'a' } }))""",
          Map.of("q", QUERY, "scope", scope));

      assertThat(rows).hasSize(5);
      for (final Result r : rows) {
        assertThat(r.<String>getProperty("space")).isEqualTo("a");
        assertThat(r.<Integer>getProperty("year")).isLessThan(2005);
      }
    }

    @Test
    void aPredicateNoRecordSatisfiesAnswersNothing() {
      seedDense();

      assertThat(query("""
          SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 5, { where: { space: 'none' } }))""", QUERY)).isEmpty();
    }

    @Test
    void aWhereThatIsNeitherAMapNorAConditionIsRefused() {
      seedDense();

      assertThatThrownBy(() -> query("SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 5, { where: 3 }))", QUERY))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("'where'");
      assertThatThrownBy(() -> query("SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 5, { where: {} }))", QUERY))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("'where'");
    }

    @Test
    void aPositionalParameterInAConditionIsRefused() {
      seedDense();

      // the `?` of the condition would be read as the query's first positional value, the query vector
      assertThatThrownBy(() -> query("SELECT expand(`vector.neighbors`('Passage[embedding]', ?, 5, { where: 'space = ?' }))", QUERY,
          "a")).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("positional");
      assertThat(RecordPredicateRidSet.hasPositionalParameter("space = '?' AND `a?b` = \"x?\"")).isFalse();
      assertThat(RecordPredicateRidSet.hasPositionalParameter("space = 'it\\'s' AND year = ?")).isTrue();
      assertThat(RecordPredicateRidSet.hasPositionalParameter("title = 'it\\'s ?'")).isFalse();
    }

    @Test
    void sparseNeighborsTakesTheSameWhere() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Sparse BUCKETS 2");
        database.command("sql", "CREATE PROPERTY Sparse.space STRING");
        database.command("sql", "CREATE PROPERTY Sparse.tokens ARRAY_OF_INTEGERS");
        database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
      });
      final Random random = new Random(10169);
      database.transaction(() -> {
        for (int i = 0; i < 200; i++)
          database.newDocument("Sparse")
              .set("space", i % 2 == 0 ? "a" : "b")
              .set("tokens", new int[] { 0, 1 + random.nextInt(15) })
              .set("weights", new float[] { random.nextFloat(), random.nextFloat() })
              .save();
      });

      final List<Result> byWhere = query("""
          SELECT expand(`vector.sparseNeighbors`('Sparse[tokens,weights]', [0], [1.0], 6, { where: "space = 'b'" }))""");
      final List<Result> byFilter = query("""
          SELECT expand(`vector.sparseNeighbors`('Sparse[tokens,weights]', [0], [1.0], 6,
            { filter: (SELECT @rid FROM Sparse WHERE space = 'b').@rid }))""");

      assertThat(byWhere).hasSize(6);
      for (final Result r : byWhere)
        assertThat(r.<String>getProperty("space")).isEqualTo("b");
      assertThat(rids(byWhere)).containsExactlyElementsOf(rids(byFilter));
    }
  }

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
  @Nested
  class WhereIndexedPrefilterPlanTest extends TestHelper {
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
        database.command("sql", "CREATE PROPERTY Doc.weight DOUBLE");
        database.command("sql", "CREATE PROPERTY Doc.serial LONG");
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
              .set("weight", i % EVERY == 0 ? 1.0d : 0.5d)
              .set("serial", i % EVERY == 0 ? 7_000_000_000L : 1L)
              .set("embedding", new float[] { random.nextFloat(), random.nextFloat(), random.nextFloat(), random.nextFloat() })
              .save();
      });
      // Created over the loaded rows, so the graph is built and the search does not answer from the delta buffer alone
      database.command("sql", "CREATE INDEX ON Doc (tenant) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Doc (name COLLATE CI) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Doc (day) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Doc (weight) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Doc (serial) NOTUNIQUE");
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
          { "day", LocalDateTime.of(2024, 1, 2, 10, 30) }, { "day", 20240102 },
          { "weight", 1 }, { "weight", 1L }, { "weight", 1.0f }, { "weight", "1" }, { "weight", "1.0" }, { "weight", 0.9999999d },
          { "serial", 7_000_000_000L }, { "serial", 7_000_000_000d }, { "serial", "7000000000" }, { "serial", 7.0e9f },
          { "serial", (int) 7_000_000_000L } };
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

      // A null is no key: `= null` matches no record, whether written in the condition or bound to a parameter
      final Map<String, Object> nullValue = new HashMap<>();
      nullValue.put("value", null);
      assertThat(neighbors("{ where: 'tenant = null' }", Map.of(), RECORDS / EVERY)).isEmpty();
      assertThat(neighbors("{ where: 'tenant = :value' }", nullValue, RECORDS / EVERY)).isEmpty();
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

  /**
   * Issue #10973: {@code vectorNeighbors} read the record of every neighbour to make its row, before the outer query had said
   * which properties it wants, so {@code SELECT @rid FROM (SELECT expand(vectorNeighbors(...)))} filled the page cache with
   * every result record, and a quantized index, whose walk reads no document, saved nothing through SQL. A row now reads its
   * record on the first property asked of it, and a projection of {@code @rid}, {@code @type} and the distance reads none.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorNeighborsDeferredRecordReadTest extends TestHelper {
    private static final int    DIMENSIONS = 16;
    private static final String RID_ONLY   =
        "SELECT @rid AS r, distance FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 10)))";

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // one test leaves index entries pointing at records it removed from the bucket, on purpose
      return false;
    }

    @Test
    void aRowReadsItsRecordOnTheFirstPropertyAskedOfIt() {
      createDocs(50, "", 0);

      final List<ScoredRecordRow> rows = neighbours(query(new Random(1)));
      assertThat(rows).hasSize(10);
      for (final ScoredRecordRow row : rows) {
        assertThat(isUnread(row)).as("a row made by the search reads no record").isTrue();
        assertThat(row.get("@rid")).isEqualTo(row.getRecord().getIdentity());
        assertThat(row.get("@type")).isEqualTo("Doc");
        assertThat(row.get("distance")).isInstanceOf(Float.class);
        assertThat(isUnread(row)).as("@rid, @type and the distance read no record").isTrue();
      }

      final ScoredRecordRow first = rows.getFirst();
      assertThat(first.get("pid")).isInstanceOf(Integer.class);
      assertThat(isUnread(first)).as("a property reads the record").isFalse();
    }

    /** The reporter's measure: the page cache after the queries, against a projection that does read every record. */
    @Test
    void aRidOnlyQueryOnAQuantizedIndexLeavesTheDataPagesOutOfTheCache() {
      // ~16 KB of payload each, so the 400 records spread over ~100 pages, which the INT8 walk never reads
      createDocs(400, ", \"quantization\": \"INT8\"", 16_000);
      indexOf().buildVectorGraphNow();
      reopenDatabase();

      final List<float[]> queries = new ArrayList<>();
      final Random random = new Random(7);
      for (int i = 0; i < 30; i++)
        queries.add(query(random));

      // The walks first, through the Java API: the index pages they read are cached by the time the SQL queries run
      for (final float[] q : queries)
        assertThat(indexOf().findNeighborsFromVector(q, 10)).hasSize(10);

      final long beforeRidOnly = PageManager.INSTANCE.getReadCacheRAM();
      for (final float[] q : queries)
        assertThat(rows(RID_ONLY, q)).hasSize(10).allSatisfy(r -> assertThat(r.<RID>getProperty("r")).isNotNull());
      final long ridOnlyGrowth = PageManager.INSTANCE.getReadCacheRAM() - beforeRidOnly;

      final long beforeReading = PageManager.INSTANCE.getReadCacheRAM();
      for (final float[] q : queries)
        assertThat(rows("SELECT pid FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 10)))", q)).hasSize(10)
            .allSatisfy(r -> assertThat(r.<Integer>getProperty("pid")).isNotNull());
      final long readingGrowth = PageManager.INSTANCE.getReadCacheRAM() - beforeReading;

      // Self-check: a projection that reads the records does fill the cache, so the pages were not cached already
      assertThat(readingGrowth).as("pages the records of the answers occupy").isGreaterThan(10L * 64 * 1024);
      assertThat(ridOnlyGrowth).as("pages the @rid-only queries brought into the cache").isLessThan(readingGrowth / 10);
    }

    /** The agreed trade: a row whose record is gone behind the index's back reads as a record with no properties. */
    @Test
    void aRecordRemovedFromItsBucketBehindTheIndexAnswersNoProperty() {
      createDocs(30, "", 0);
      indexOf().buildVectorGraphNow();
      final float[] q = new float[DIMENSIONS];

      final RID nearest = rows(RID_ONLY, q).getFirst().getProperty("r");
      database.transaction(() -> ((LocalBucket) database.getSchema().getBucketById(nearest.getBucketId())).deleteRecord(nearest));

      // The index still answers it: only reading the record could tell, and nothing reads it
      assertThat(rows(RID_ONLY, q)).extracting(r -> r.<RID>getProperty("r")).contains(nearest);
      // A projection that reads it is answered, not failed
      final List<Result> withPid = rows("SELECT @rid AS r, pid FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 10)))", q);
      assertThat(withPid).hasSize(10);
      assertThat(withPid).filteredOn(r -> nearest.equals(r.getProperty("r"))).singleElement()
          .satisfies(r -> assertThat(r.<Object>getProperty("pid")).isNull());
      assertThat(withPid).filteredOn(r -> !nearest.equals(r.getProperty("r"))).allSatisfy(r -> assertThat(r.<Object>getProperty("pid")).isNotNull());
    }

    /**
     * Issue #11202: the row of a record gone behind the index's back takes one of the {@code limit} places, so the answer holds one
     * real neighbour fewer. It is reported once per row however many properties are read, and {@code REBUILD INDEX}, the repair the
     * report names, gives the real neighbour its place back.
     */
    @Test
    void aRecordGoneBehindTheIndexIsReportedAndRebuildIndexRepairsTheAnswer() {
      createDocs(30, "", 0);
      indexOf().buildVectorGraphNow();
      final float[] q = new float[DIMENSIONS];
      final String withPid = "SELECT @rid AS r, pid, payload, vector FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 10)))";

      final List<RID> before = new ArrayList<>();
      rows(withPid, q).forEach(r -> before.add(r.getProperty("r")));
      final RID nearest = before.getFirst();
      // the neighbour that has the 11th place while every record is there
      final List<RID> eleven = new ArrayList<>();
      rows("SELECT @rid AS r FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 11)))", q).forEach(r -> eleven.add(r.getProperty("r")));
      final RID eleventh = eleven.getLast();
      assertThat(before).doesNotContain(eleventh);

      database.transaction(() -> ((LocalBucket) database.getSchema().getBucketById(nearest.getBucketId())).deleteRecord(nearest));

      final long reportedBefore = StaleVectorEntryReporter.getTotalReported();
      final List<Result> damaged = rows(withPid, q);
      assertThat(damaged).hasSize(10).extracting(r -> r.<RID>getProperty("r")).contains(nearest).doesNotContain(eleventh);
      // three properties read off the row, one report
      assertThat(StaleVectorEntryReporter.getTotalReported() - reportedBefore).isEqualTo(1L);

      database.command("sql", "REBUILD INDEX `Doc[vector]`");
      indexOf().buildVectorGraphNow();

      final List<Result> repaired = rows(withPid, q);
      assertThat(repaired).hasSize(10).allSatisfy(r -> assertThat(r.<Object>getProperty("pid")).isNotNull());
      assertThat(repaired).extracting(r -> r.<RID>getProperty("r")).doesNotContain(nearest).contains(eleventh)
          .containsAll(before.subList(1, 10));
    }

    /**
     * Issue #11202 (found by CodeRabbit): a delete committed after the search made a row and before the row was first read takes the
     * vector out of the index as well, so the row finds its record gone and the index is not damaged: nothing is reported.
     */
    @Test
    void aDeleteCommittedBetweenTheSearchAndTheReadIsNotReportedAsDamage() {
      createDocs(30, "", 0);
      indexOf().buildVectorGraphNow();
      final List<ScoredRecordRow> rows = neighbours(new float[DIMENSIONS]);
      final ScoredRecordRow first = rows.getFirst();
      assertThat(isUnread(first)).isTrue();

      database.transaction(() -> first.getRecord().getIdentity().asDocument().delete());

      final long reportedBefore = StaleVectorEntryReporter.getTotalReported();
      assertThat(first.get("pid")).isNull();
      assertThat(StaleVectorEntryReporter.getTotalReported()).isEqualTo(reportedBefore);
    }

    /**
     * Issue #11202 (found by CodeRabbit): a record the open transaction deleted is not gone yet. It is not reported (the committed index
     * still holds the vector until the commit), and once the transaction rolls back the row reads the record again.
     */
    @Test
    void aDeleteOfTheOpenTransactionIsNotReportedAndARollbackBringsTheRowBack() {
      createDocs(30, "", 0);
      indexOf().buildVectorGraphNow();

      database.begin();
      final List<ScoredRecordRow> rows = neighbours(new float[DIMENSIONS]);
      final ScoredRecordRow first = rows.getFirst();
      assertThat(isUnread(first)).isTrue();
      first.getRecord().getIdentity().asDocument().delete();

      final long reportedBefore = StaleVectorEntryReporter.getTotalReported();
      assertThat(first.get("pid")).isNull();
      assertThat(StaleVectorEntryReporter.getTotalReported()).isEqualTo(reportedBefore);
      database.rollback();

      assertThat(first.get("pid")).isInstanceOf(Integer.class);
    }

    /** A delete through the API takes the vector out of the index in its own commit: the deferred row never sees it. */
    @Test
    void aCommittedDeleteCostsNoAnswerRow() {
      createDocs(30, "", 0);
      indexOf().buildVectorGraphNow();
      final float[] q = new float[DIMENSIONS];
      final List<RID> before = new ArrayList<>();
      rows(RID_ONLY, q).forEach(r -> before.add(r.getProperty("r")));

      database.transaction(() -> before.getFirst().asDocument().delete());

      assertThat(indexOf().hasLiveVector(before.getFirst())).isFalse();
      assertThat(indexOf().hasLiveVector(before.get(1))).isTrue();
      final List<RID> after = new ArrayList<>();
      rows(RID_ONLY, q).forEach(r -> after.add(r.getProperty("r")));
      assertThat(after).hasSize(10).doesNotContain(before.getFirst()).containsAll(before.subList(1, 10));
    }

    /** A read listener sees, and can veto, every record the search hands out: its records are read as before. */
    @Test
    void aReadListenerKeepsTheRecordsRead() {
      createDocs(30, "", 0);
      final AfterRecordReadListener listener = record -> record;
      database.getSchema().getType("Doc").getEvents().registerListener(listener);
      try {
        assertThat(neighbours(new float[DIMENSIONS])).hasSize(10).allSatisfy(row -> assertThat(isUnread(row)).isFalse());
      } finally {
        database.getSchema().getType("Doc").getEvents().unregisterListener(listener);
      }
    }

    /** The calling transaction answers its own rows from what it queued: they are read as before. */
    @Test
    void anIndexTheTransactionWroteToKeepsTheRecordsRead() {
      createDocs(30, "", 0);
      database.transaction(() -> {
        database.newDocument("Doc").set("pid", 1_000, "payload", "", "vector", new float[DIMENSIONS]).save();
        final List<ScoredRecordRow> rows = neighbours(new float[DIMENSIONS]);
        assertThat(rows).hasSize(10).allSatisfy(row -> assertThat(isUnread(row)).isFalse());
        assertThat(rows).extracting(row -> row.get("pid")).contains(1_000);
      });
    }

    private void createDocs(final int count, final String extraMetadata, final int payloadSize) {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.payload STRING");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\"" + extraMetadata + " }");
      final String payload = "x".repeat(payloadSize);
      final Random random = new Random(42);
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          database.newDocument("Doc").set("pid", i, "payload", payload, "vector", query(random)).save();
      });
    }

    private List<ScoredRecordRow> neighbours(final float[] q) {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final Object result = new SQLFunctionVectorNeighbors().execute(null, null, null, new Object[] { "Doc[vector]", q, 10 }, context);
      final List<ScoredRecordRow> rows = new ArrayList<>();
      for (final Object row : (List<?>) result)
        rows.add((ScoredRecordRow) row);
      return rows;
    }

    private List<Result> rows(final String sql, final float[] q) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql, q)) {
        rs.forEachRemaining(rows::add);
      }
      return rows;
    }

    private LSMVectorIndex indexOf() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
    }

    private static boolean isUnread(final ScoredRecordRow row) {
      return ((BaseRecord) row.getRecord()).getBuffer() == null;
    }

    private static float[] query(final Random random) {
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = random.nextFloat();
      return v;
    }
  }
}
