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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
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

  /**
   * An AFTER READ listener of the type that counts the reads of the calling thread (not those of the background apply) and leaves every
   * record as it is.
   */
  private static AfterRecordReadListener counting(final AtomicLong reads) {
    final Thread caller = Thread.currentThread();
    return record -> {
      if (Thread.currentThread() == caller)
        reads.incrementAndGet();
      return record;
    };
  }

  /** The rows of the statement, the reads counted by {@code reads} while it runs reset first. */
  private long rows(final String sql, final AtomicLong reads) {
    reads.set(0);
    long rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  /** The facets of the statement, the reads counted by {@code reads} while it runs reset first. */
  private FacetResults facets(final String sql, final AtomicLong reads) {
    reads.set(0);
    return facets(sql);
  }

  /**
   * Plan 7 Task 13: the facets of a type whose records fire read events are taken from the rows the WHERE read, so an AFTER READ
   * listener sees each matched record once, as the normal plan does (it saw each twice: once for the row, once more for the scan that
   * counted it). The WHERE is answered by an LSM index, so the normal plan reads the matches only.
   */
  @Test
  @Timeout(300)
  void afterReadListenerFiresOncePerMatchedRecordWithWhere() {
    AdvancedFacetsFixture.load(database, 13L, 1500);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (fileSize) NOTUNIQUE");
    // a quarter of the records, read through the index
    final String narrow = "fileSize > 3000000";
    FacetOracle.awaitCaughtUp(database);
    final AtomicLong reads = new AtomicLong();
    final AfterRecordReadListener listener = counting(reads);
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    try {
      final long records = visible(TYPE);
      final long matches = rows("SELECT FROM " + TYPE + where(narrow), reads);
      assertThat(matches).isGreaterThan(0).isLessThan(records);
      assertThat(reads.get()).as("the normal plan reads each match once").isEqualTo(matches);

      final FacetResults facets = facets("SELECT FROM " + TYPE + where(narrow) + FACETS, reads);
      assertThat(reads.get()).as("reads of the FACETS statement").isEqualTo(matches);
      for (final String alias : List.of("b", "a", "t", "d", "s", "r", "h"))
        assertThat(facets.get(alias).source()).as(alias + ": " + facets.get(alias).reason()).isEqualTo("scan");
      assertThat(profiledMatches("SELECT FROM " + TYPE + where(narrow) + FACETS)).containsExactly("rows", String.valueOf(matches));
      assertEveryKindIsTheNormalPlans(TYPE, narrow, "scan");
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
  }

  /** The same without WHERE: the rows are every record of the type, each read once. */
  @Test
  @Timeout(300)
  void afterReadListenerFiresOncePerRecordWithoutWhere() {
    AdvancedFacetsFixture.load(database, 17L, 1200);
    final AtomicLong reads = new AtomicLong();
    final AfterRecordReadListener listener = counting(reads);
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    try {
      final long records = rows("SELECT FROM " + TYPE, reads);
      assertThat(reads.get()).as("the normal plan reads each record once").isEqualTo(records);

      final FacetResults facets = facets("SELECT FROM " + TYPE + FACETS, reads);
      assertThat(reads.get()).as("reads of the FACETS statement").isEqualTo(records);
      assertThat(facets.get("b").source()).isEqualTo("scan");
      // LIMIT: the rows past it are read once too, to count the facets, and none twice
      facets("SELECT FROM " + TYPE + FACETS + " LIMIT 10", reads);
      assertThat(reads.get()).as("reads of the FACETS statement with LIMIT").isEqualTo(records);
      assertEveryKindIsTheNormalPlans(TYPE, null, "scan");
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
  }

  /**
   * Disjunctive faceting (spec 7.5) over a type with an AFTER READ listener that hides the hidden tenant and rewrites brands: each facet
   * counts the rows every other FILTER passes (its own one ignored), from the records as the listener left them, read once.
   */
  @Test
  @Timeout(300)
  void disjunctiveFiltersOnReadEventTypeEqualNormalPlan() {
    AdvancedFacetsFixture.load(database, 19L, 1500);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (fileSize) NOTUNIQUE");
    FacetOracle.awaitCaughtUp(database);
    final AtomicLong reads = new AtomicLong();
    final Thread caller = Thread.currentThread();
    final AfterRecordReadListener listener = record -> {
      if (Thread.currentThread() == caller)
        reads.incrementAndGet();
      final Object author = record.asDocument().get("author");
      if (AdvancedFacetsFixture.isHiddenTenant(author))
        return null;
      if (author instanceof String name && name.startsWith("author-2"))
        return record.asDocument().modify().set("brand", MASKED);
      return record;
    };
    final String brand = "brand = '" + MASKED + "'";
    final String size = "fileSize > 500000";
    final String author = "author <> 'author-3'";
    final String sql = "SELECT FROM " + TYPE + where(WHERE) + " FACETS x: brand TERMS TOP 1000 FILTER (" + brand + "),"
        + " y: author TERMS TOP 1000 FILTER (" + size + "), f: author TERMS TOP 1000, s: fileSize STATS FILTER (" + author + ")";
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    try {
      rows("SELECT FROM " + TYPE + where(WHERE), reads);
      // a rewrite reloads the record (modify), which the listener sees too: the normal plan's reads are the reference
      final long normalReads = reads.get();
      final String all = WHERE + " AND " + brand + " AND " + size + " AND " + author;
      final long passing = rows("SELECT FROM " + TYPE + where(all), reads);
      assertThat(passing).as("rows pass every filter").isGreaterThan(0);

      reads.set(0);
      final FacetResults facets;
      long forwarded = 0;
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext()) {
          rs.next();
          forwarded++;
        }
        facets = rs.getFacets();
      }
      assertThat(reads.get()).as("reads of the FACETS statement").isEqualTo(normalReads);
      assertThat(forwarded).as("the rows every FILTER passes").isEqualTo(passing);
      assertThat(toMap(facets.get("x"))).as("x: brand, y's and s's filters").hasSizeGreaterThan(1)
          .isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", WHERE + " AND " + size + " AND " + author));
      assertThat(toMap(facets.get("y"))).as("y: author, x's and s's filters").isNotEmpty()
          .isEqualTo(FacetOracle.expectedCounts(database, TYPE, "author", WHERE + " AND " + brand + " AND " + author));
      assertThat(toMap(facets.get("f"))).as("f: author, every filter").isNotEmpty()
          .isEqualTo(FacetOracle.expectedCounts(database, TYPE, "author", all));
      assertThat(facets.get("s").stats()).as("s: fileSize, x's and y's filters")
          .isEqualTo(FacetOracle.stats(database, TYPE, "fileSize", WHERE + " AND " + brand + " AND " + size));
      for (final String alias : List.of("x", "y", "f", "s"))
        assertThat(facets.get(alias).source()).as(alias).isEqualTo("scan");
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
  }

  /**
   * No fallback-scan permit: the facets that read a bucket with read events are {@code unavailable / fallback scans capped}, as before,
   * the rows are still read once, and the permit count is unchanged afterwards.
   */
  @Test
  @Timeout(300)
  void readEventsWithoutAScanPermitAreCapped() {
    AdvancedFacetsFixture.load(database, 23L, 800);
    final AtomicLong reads = new AtomicLong();
    final AfterRecordReadListener listener = counting(reads);
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    final Semaphore permits = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager().fallbackScans();
    try {
      final long matches = rows("SELECT FROM " + TYPE + where(WHERE), reads);
      final int held = permits.drainPermits();
      try {
        final FacetResults facets = facets("SELECT FROM " + TYPE + where(WHERE) + " FACETS b: brand TERMS TOP 10, s: fileSize STATS", reads);
        assertThat(reads.get()).as("reads of the FACETS statement").isEqualTo(matches);
        for (final String alias : List.of("b", "s")) {
          assertThat(facets.get(alias).source()).as(alias).isEqualTo(FacetResult.UNAVAILABLE);
          assertThat(facets.get(alias).reason()).as(alias).isEqualTo(FacetExecutor.FALLBACK_SCANS_CAPPED);
        }
      } finally {
        permits.release(held);
      }
      assertThat(permits.availablePermits()).isEqualTo(database.getConfiguration().getValueAsInteger(
          GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS));
      assertThat(facets("SELECT FROM " + TYPE + where(WHERE) + " FACETS b: brand TERMS TOP 10", reads).get("b").source()).isEqualTo("scan");
      assertThat(permits.availablePermits()).isEqualTo(database.getConfiguration().getValueAsInteger(
          GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS));
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
  }

  /**
   * A result set closed after its first rows, before its facets were asked: the rows' scan of the buckets with read events gives its
   * scan permit and its counting memory back.
   */
  @Test
  @Timeout(300)
  void aRowScanClosedBeforeItsFacetsGivesItsPermitAndMemoryBack() {
    AdvancedFacetsFixture.load(database, 29L, 800);
    final AtomicLong reads = new AtomicLong();
    final AfterRecordReadListener listener = counting(reads);
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    final Semaphore permits = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager().fallbackScans();
    final int allPermits = database.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS);
    // the first query publishes the hierarchy views, whose heap stays charged to the budget while they are published (issue #11368):
    // the budget to come back to is the one with them in place
    try (final ResultSet warm = database.query("sql", "SELECT FROM " + TYPE + FACETS)) {
      warm.stream().count();
    }
    final long fullBudget = FacetMemoryBudget.instance().available();
    try {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + FACETS)) {
        for (int i = 0; i < 5; i++)
          rs.next();
        assertThat(permits.availablePermits()).as("the rows' scan holds the permit").isEqualTo(allPermits - 1);
      }
      assertThat(permits.availablePermits()).isEqualTo(allPermits);
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(fullBudget);
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
  }

  /**
   * Fix round 1, the projection+scan source: a query on the super type, no WHERE, whose subtype has a BEFORE READ listener. The super
   * type's bucket is answered from its projection, the subtype's bucket is counted from the rows, so the listener sees each subtype
   * record once (it saw each twice: once by the scan of the uncovered buckets, once by the rows), with or without a LIMIT.
   */
  @Test
  @Timeout(300)
  void subtypeListenerUnderTheProjectionSourceFiresOncePerRecord() {
    AdvancedFacetsFixture.load(database, 31L, 800);
    database.command("sql", "CREATE DOCUMENT TYPE Clip EXTENDS " + TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Clip").set("brand", "clip-" + (i % 9)).set("author", "author-" + (i % 30)).set("fileSize", (long) i * 977)
            .set("tags", List.of("t/" + (1 + i % 3), "t/1/" + (1 + i % 3))).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final AtomicLong reads = new AtomicLong();
    // every thread's reads: a scan of two buckets reads them in parallel (the projection is caught up, the apply reads nothing)
    final BeforeRecordReadListener listener = rid -> {
      reads.incrementAndGet();
      return true;
    };
    database.getSchema().getType("Clip").getEvents().registerListener(listener);
    try {
      rows("SELECT FROM " + TYPE, reads);
      final long clips = reads.get();
      assertThat(clips).as("the normal plan reads each subtype record once").isEqualTo(300);
      final String sql = "SELECT FROM " + TYPE + FACETS;
      assertThat(profiledMatches(sql)[0]).as("the shape under test").startsWith("projection");

      rows(sql, reads);
      assertThat(reads.get()).as("reads of the FACETS statement").isEqualTo(clips);
      facets(sql, reads);
      assertThat(reads.get()).as("reads of the FACETS statement, facets asked before the rows").isEqualTo(clips);
      facets(sql + " LIMIT 10", reads);
      assertThat(reads.get()).as("reads of the FACETS statement with LIMIT").isEqualTo(clips);
      assertEveryKindIsTheNormalPlans(TYPE, null, "scan");
    } finally {
      database.getSchema().getType("Clip").getEvents().unregisterListener(listener);
    }
    assertEveryKindIsTheNormalPlans(TYPE, null, "index");
  }

  /**
   * Fix round 1, the projection+scan source of a hidden bucket: a type with an AFTER READ listener and two buckets, one of which the
   * user may not read. The readable bucket is counted from the rows, so the FACETS statement fires the listener as often as the same
   * statement without FACETS (it read the readable bucket a second time), and the facet counts the readable bucket's records.
   */
  @Test
  @Timeout(300)
  void readEventTypeWithAHiddenBucketFiresOncePerRecord() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Doc.brand STRING");
    database.command("sql", "CREATE INDEX ON Doc (brand) COLUMNAR");
    database.transaction(() -> {
      for (int i = 0; i < 600; i++)
        database.newDocument("Doc").set("brand", "brand-" + (i % 7)).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final int hiddenBucket = database.getSchema().getType("Doc").getBuckets(false).getFirst().getFileId();
    final String readableBucket = database.getSchema().getType("Doc").getBuckets(false).get(1).getName();
    final AtomicLong reads = new AtomicLong();
    // every thread's reads: a scan of two buckets reads them in parallel (the projection is caught up, the apply reads nothing)
    final AfterRecordReadListener listener = record -> {
      reads.incrementAndGet();
      return record;
    };
    database.getSchema().getType("Doc").getEvents().registerListener(listener);
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
    context.setCurrentUser(userWithoutReadOn(hiddenBucket));
    try {
      // an embedded query applies no record-level check of this user: the rows hold the hidden bucket's records too, the facets do not
      rows("SELECT FROM Doc", reads);
      final long normalReads = reads.get();
      assertThat(normalReads).isEqualTo(600);
      final Map<Object, Long> readable = new HashMap<>();
      try (final ResultSet rs = database.query("sql", "SELECT brand AS v FROM BUCKET:" + readableBucket)) {
        while (rs.hasNext())
          readable.merge(rs.next().getProperty("v"), 1L, Long::sum);
      }
      reads.set(0);
      assertThat(readable.values().stream().mapToLong(Long::longValue).sum()).as("both buckets hold records").isBetween(1L, 599L);
      final String sql = "SELECT FROM Doc FACETS b: brand TERMS TOP 100";
      assertThat(profiledMatches(sql)[0]).as("the shape under test").startsWith("projection");

      final FacetResults facets = facets(sql, reads);
      assertThat(reads.get()).as("reads of the FACETS statement").isEqualTo(normalReads);
      assertThat(facets.get("b").source()).isEqualTo("scan");
      assertThat(toMap(facets.get("b"))).as("the readable bucket's brands").isEqualTo(readable);
      facets(sql + " LIMIT 5", reads);
      assertThat(reads.get()).as("reads of the FACETS statement with LIMIT").isEqualTo(normalReads);
    } finally {
      context.setCurrentUser(null);
      database.getSchema().getType("Doc").getEvents().unregisterListener(listener);
    }
  }

  /** A user who may do everything but read the records of {@code hiddenBucket}. */
  private static SecurityDatabaseUser userWithoutReadOn(final int hiddenBucket) {
    return new SecurityDatabaseUser() {
      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.READ_RECORD || fileId != hiddenBucket;
      }

      @Override
      public String getName() {
        return "noBucket";
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
