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
package com.arcadedb.query.facet;

import com.arcadedb.TestHelper;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final review B-I1: a bucket whose type has a READ trigger or listener (a super type's trigger included), or a database with a read
 * listener of its own, is never answered from the projection. Such a trigger can hide a record and a listener rewrite one, which only a
 * read of the record applies: every facet kind (TERMS, RANGE, HISTOGRAM, HIERARCHY, DISTINCT, STATS, a FILTER) and {@code facet.searchValues} then
 * answer what the normal plan answers ({@link FacetOracle}, SQL rows read with the events firing), from the records ({@code scan}),
 * decided at each execution so a cached plan follows a trigger created or dropped after it. A type with no read event is still answered
 * from the projection ({@code index}).
 */
class FacetReadEventsTest extends TestHelper {
  private static final String TYPE    = AdvancedFacetsFixture.TYPE;
  private static final long   BIN     = 1L << 20;
  private static final String WHERE   = "fileSize > 1000";
  private static final String FACETS  = " FACETS b: brand TERMS TOP 1000, a: author TERMS TOP 1000, t: tags HIERARCHY TOP 1000,"
      + " d: author DISTINCT, s: fileSize STATS, r: fileSize RANGES ([*, 1000000), [1000000, *)), h: fileSize HISTOGRAM FIXED " + BIN;
  private static final String TRIGGER = "CREATE TRIGGER hideTenant BEFORE READ ON TYPE " + TYPE + " EXECUTE JAVA '"
      + AdvancedFacetsFixture.HideTenantTrigger.class.getName() + "'";
  private static final String MASKED  = "masked";
  /** A full-text search the index answers by RID, without reading a record: the records of {@code author-12}, a hidden tenant. */
  private static final String SEARCH  = "SELECT FROM " + TYPE + " WHERE search_fields(['author'], '12') FACETS b: brand TERMS TOP 1000";
  private static final Pattern MATCHES = Pattern.compile("matchSource=(\\S+) matches=(\\d+)");

  private FacetResults facets(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getFacets();
    }
  }

  /** The match source and the match count PROFILE prints for the statement. */
  private String[] profiledMatches(final String sql) {
    final String plan;
    try (final ResultSet rs = database.query("sql", "PROFILE " + sql)) {
      plan = rs.next().getProperty("executionPlanAsString");
    }
    final Matcher matcher = MATCHES.matcher(plan);
    assertThat(matcher.find()).as(plan).isTrue();
    return new String[] { matcher.group(1), matcher.group(2) };
  }

  /** The records of the type the normal plan reads, every read event applied. */
  private long visible(final String type) {
    final FacetStats stats = FacetOracle.stats(database, type, "fileSize", null);
    return stats.count() + stats.missing();
  }

  private static String where(final String where) {
    return where == null ? "" : " WHERE " + where;
  }

  /** The buckets as value to count, the matches without a value under {@link FacetOracle#MISSING}, as the oracle keys them. */
  private static Map<Object, Long> toMap(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      assertThat(counts.put(bucket.value(), bucket.count())).as("value " + bucket.value() + " returned twice").isNull();
    if (result.missing() > 0)
      counts.put(FacetOracle.MISSING, result.missing());
    assertThat(result.otherCount()).isZero();
    return counts;
  }

  /**
   * Every facet kind over {@code type} equals the oracle's answer, which reads the records through SQL with the read events firing, and
   * is answered from {@code source}.
   */
  private void assertEveryKindIsTheNormalPlans(final String type, final String where, final String source) {
    final FacetResults facets = facets("SELECT FROM " + type + where(where) + FACETS);
    assertThat(toMap(facets.get("b"))).as("TERMS brand" + where(where)).isEqualTo(FacetOracle.expectedCounts(database, type, "brand", where));
    assertThat(toMap(facets.get("a"))).as("TERMS author" + where(where)).isEqualTo(FacetOracle.expectedCounts(database, type, "author", where));
    assertThat(toMap(facets.get("t"))).as("HIERARCHY tags" + where(where)).isEqualTo(
        FacetOracle.hierarchicalCounts(database, type, "tags", where, ColumnarTestData.TAG_TYPE, "path", ColumnarTestData.PARENT_EDGE));
    assertThat(facets.get("d").distinct().value()).as("DISTINCT author" + where(where))
        .isEqualTo(FacetOracle.distinctCount(database, type, "author", where));
    assertThat(facets.get("s").stats()).as("STATS fileSize" + where(where)).isEqualTo(FacetOracle.stats(database, type, "fileSize", where));
    // RANGE and HISTOGRAM (round 2, N4): the fileSize values the normal plan reads, bucketed here
    final List<Long> sizes = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT fileSize AS v FROM " + type + where(where))) {
      while (rs.hasNext()) {
        final Object value = rs.next().getProperty("v");
        if (value != null)
          sizes.add(((Number) value).longValue());
      }
    }
    final List<Long> ranges = new ArrayList<>(List.of(0L, 0L));
    final Map<Object, Long> bins = new TreeMap<>();
    for (final long size : sizes) {
      ranges.set(size < 1_000_000 ? 0 : 1, ranges.get(size < 1_000_000 ? 0 : 1) + 1);
      bins.merge(Math.floorDiv(size, BIN) * BIN, 1L, Long::sum);
    }
    final List<Long> rangeCounts = new ArrayList<>();
    for (final FacetBucket bucket : facets.get("r").buckets())
      rangeCounts.add(bucket.count());
    assertThat(rangeCounts).as("RANGES fileSize" + where(where)).isEqualTo(ranges);
    final Map<Object, Long> histogram = new TreeMap<>();
    for (final FacetBucket bucket : facets.get("h").buckets())
      if (bucket.count() > 0)
        histogram.put(((Number) bucket.from()).longValue(), bucket.count());
    assertThat(histogram).as("HISTOGRAM fileSize" + where(where)).isEqualTo(bins);
    for (final String alias : List.of("b", "a", "t", "d", "s", "r", "h"))
      assertThat(facets.get(alias).source()).as(type + where(where) + " " + alias + ": " + facets.get(alias).reason()).isEqualTo(source);
  }

  /** The authors the normal plan counts: the hidden tenant's are among them only when no read event hides them. */
  private Set<Object> authors(final String type) {
    final Set<Object> authors = new HashSet<>(FacetOracle.expectedCounts(database, type, "author", null).keySet());
    authors.remove(FacetOracle.MISSING);
    return authors;
  }

  /** The rows of {@code facet.searchValues(Asset, author, prefix, 100)} as value to count, in their order. */
  private Map<Object, Long> searchValues(final String prefix) {
    final Map<Object, Long> rows = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(facet.searchValues('" + TYPE + "', 'author', :x, 100))", Map.of("x",
        prefix))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.put(row.getProperty("value"), row.<Long>getProperty("count"));
      }
    }
    return rows;
  }

  /** What {@code facet.searchValues} answers by the normal plan: the authors the oracle counts that start with the prefix, in value order. */
  private Map<Object, Long> expectedSearchValues(final String prefix) {
    final TreeMap<String, Long> sorted = new TreeMap<>();
    for (final Map.Entry<Object, Long> entry : FacetOracle.expectedCounts(database, TYPE, "author", null).entrySet())
      if (entry.getKey() instanceof String value && value.startsWith(prefix))
        sorted.put(value, entry.getValue());
    return new LinkedHashMap<>(sorted);
  }

  private void assertMeasuresAnsweredFrom(final String source) {
    final FacetResults measures = facets(AdvancedFacetsFixture.MEASURE_QUERY);
    for (final String alias : measures.aliases())
      assertThat(measures.get(alias).source()).as(alias).isEqualTo(source);
    AdvancedFacetsFixture.assertStatsEqualRows("measures", measures, AdvancedFacetsFixture.measureAggregates(database));
  }

  @Test
  @Timeout(300)
  void beforeReadTriggerHidesTheTenantFromEveryKind() {
    AdvancedFacetsFixture.load(database, 7L, 1500);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (author) FULL_TEXT");
    final String noWhere = "SELECT FROM " + TYPE + " FACETS b: brand TERMS TOP 1000";
    assertThat(profiledMatches(noWhere)).containsExactly("projection", String.valueOf(visible(TYPE)));
    assertThat(profiledMatches(SEARCH)[0]).isEqualTo("index");
    assertThat(Long.parseLong(profiledMatches(SEARCH)[1])).isGreaterThan(0);
    // the plan of these statements is cached before the trigger exists: the projection answers them now
    for (final String where : new String[] { null, WHERE })
      assertEveryKindIsTheNormalPlans(TYPE, where, "index");
    assertThat(authors(TYPE)).as("the data holds the tenant the trigger hides").anyMatch(AdvancedFacetsFixture::isHiddenTenant);
    assertThat(searchValues(AdvancedFacetsFixture.HIDDEN_TENANT)).isNotEmpty();

    database.command("sql", TRIGGER);
    assertThat(authors(TYPE)).as("the normal plan no longer reads the hidden tenant").noneMatch(AdvancedFacetsFixture::isHiddenTenant);
    for (final String where : new String[] { null, WHERE })
      assertEveryKindIsTheNormalPlans(TYPE, where, "scan");
    assertThat(searchValues(AdvancedFacetsFixture.HIDDEN_TENANT)).as("no value only hidden records hold").isEmpty();
    assertThat(searchValues("author-")).isNotEmpty().containsExactlyEntriesOf(expectedSearchValues("author-"));
    // a type without read events keeps the projection
    assertMeasuresAnsweredFrom("index");
    // the match set holds no hidden record either: PROFILE counts the records the normal plan reads, and the index (which adds its
    // matches by RID) is not the source
    assertThat(profiledMatches(noWhere)).containsExactly("rows", String.valueOf(visible(TYPE)));
    assertThat(profiledMatches(SEARCH)).containsExactly("rows", "0");
    assertThat(facets(SEARCH).get("b").buckets()).isEmpty();

    database.command("sql", "DROP TRIGGER hideTenant");
    for (final String where : new String[] { null, WHERE })
      assertEveryKindIsTheNormalPlans(TYPE, where, "index");
    assertThat(searchValues(AdvancedFacetsFixture.HIDDEN_TENANT)).isNotEmpty();
  }

  /** A trigger on the super type fires for the records of its subtypes: a query on the subtype reads them too. */
  @Test
  @Timeout(300)
  void superTypeTriggerHidesTheTenantOfTheSubtype() {
    AdvancedFacetsFixture.load(database, 11L, 600);
    database.command("sql", "CREATE DOCUMENT TYPE Clip EXTENDS " + TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newDocument("Clip").set("brand", "clip-" + (i % 9)).set("author", "author-" + (i % 30)).set("fileSize", (long) i * 977)
            .set("tags", List.of("t/" + (1 + i % 3), "t/1/" + (1 + i % 3))).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertEveryKindIsTheNormalPlans("Clip", null, "index");

    database.command("sql", TRIGGER);
    assertThat(authors("Clip")).noneMatch(AdvancedFacetsFixture::isHiddenTenant);
    for (final String type : List.of("Clip", TYPE))
      for (final String where : new String[] { null, WHERE })
        assertEveryKindIsTheNormalPlans(type, where, "scan");
  }

  /**
   * An AFTER READ listener of the type that filters the hidden tenant's records out (null) and rewrites the brand of others: the facets
   * count the rewritten records, a FILTER included, as the normal plan does.
   */
  @Test
  @Timeout(300)
  void afterReadListenerFiltersAndRewritesRecords() {
    AdvancedFacetsFixture.load(database, 5L, 1500);
    // a FILTER applies to the other facets (disjunctive faceting): f counts the matches whose brand passes x's
    final String filtered = "SELECT FROM " + TYPE + " FACETS x: brand TERMS TOP 10 FILTER (brand = '" + MASKED + "'),"
        + " f: author TERMS TOP 1000";
    assertThat(facets(filtered).get("f").source()).isEqualTo("index");

    final AfterRecordReadListener listener = record -> {
      final Object author = record.asDocument().get("author");
      if (AdvancedFacetsFixture.isHiddenTenant(author))
        return null;
      if (author instanceof String name && name.startsWith("author-2"))
        return record.asDocument().modify().set("brand", MASKED);
      return record;
    };
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    try {
      assertThat(authors(TYPE)).noneMatch(AdvancedFacetsFixture::isHiddenTenant);
      assertThat(FacetOracle.expectedCounts(database, TYPE, "brand", null)).as("the normal plan reads the rewritten brand")
          .containsKey(MASKED);
      for (final String where : new String[] { null, WHERE })
        assertEveryKindIsTheNormalPlans(TYPE, where, "scan");

      final FacetResult masked = facets(filtered).get("f");
      assertThat(masked.source()).isEqualTo("scan");
      assertThat(toMap(masked)).isNotEmpty().isEqualTo(FacetOracle.expectedCounts(database, TYPE, "author", "brand = '" + MASKED + "'"));
      assertMeasuresAnsweredFrom("index");
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
    assertEveryKindIsTheNormalPlans(TYPE, null, "index");
    assertThat(facets(filtered).get("f").buckets()).isEmpty();
  }

  /** A read listener of the database fires for every record of every type: no type is answered from the projection meanwhile. */
  @Test
  @Timeout(300)
  void databaseReadListenerReadsEveryType() {
    AdvancedFacetsFixture.load(database, 3L, 1000);
    final Set<Integer> assetBuckets = new HashSet<>(database.getSchema().getType(TYPE).getBucketIds(true));
    final BeforeRecordReadListener listener = rid -> !assetBuckets.contains(rid.getBucketId())
        || !AdvancedFacetsFixture.isHiddenTenant(database.lookupByRID(rid, true).asDocument().get("author"));
    database.getEvents().registerListener(listener);
    try {
      assertThat(authors(TYPE)).noneMatch(AdvancedFacetsFixture::isHiddenTenant);
      assertEveryKindIsTheNormalPlans(TYPE, null, "scan");
      assertMeasuresAnsweredFrom("scan");
    } finally {
      database.getEvents().unregisterListener(listener);
    }
    assertEveryKindIsTheNormalPlans(TYPE, null, "index");
    assertMeasuresAnsweredFrom("index");
  }
}
