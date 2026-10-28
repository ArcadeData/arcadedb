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
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetStats;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;

import java.lang.reflect.Array;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Test oracle of the columnar projection (shared by plans 3 to 6): the facet counts SQL GROUP BY gives over the records against the
 * counts the projection answers through a {@link ColumnarSnapshot} of every polymorphic bucket of the type.
 * <p>
 * Both sides key a value by {@link Type#normalizeForKey} and count a record with no value, or with a value the column kind cannot
 * hold, under {@link #MISSING}. A LONG column is keyed by its epoch count, computed here from the schema property type with
 * {@code java.time} (DATE days, DATETIME millis, DATETIME_SECOND seconds, DATETIME_MICROS micros, DATETIME_NANOS nanos, an integral
 * type its {@code longValue}), independently of {@link ColumnarValueExtractor#toColumnLong}, which only decides what is rejected and
 * must agree wherever both convert. A property indexed BY ITEM counts documents per item, items normalized first and deduplicated per
 * record, so an item repeated inside one record counts once.
 * <p>
 * The SQL side reads the match set as {@code SELECT @rid}, which only an oracle may do (no production path builds a match set from SQL
 * rows). With no WHERE, the projection side also asserts that the rows each bucket's snapshot holds live are exactly the bucket's
 * records (pending pages aside), so a stale live row of a deleted record fails even where no value differs.
 */
public final class FacetOracle {
  /** The key under which records with no (convertible) value are counted. */
  public static final Object MISSING = new Object() {
    @Override
    public String toString() {
      return "<missing>";
    }
  };

  private static final long CAUGHT_UP_TIMEOUT_MS = 30_000;
  private static final int  MAX_DIFFS_REPORTED   = 20;

  private FacetOracle() {
  }

  /**
   * The facet counts of GROUP BY over the type, polymorphic. BY ITEM is inferred from the COLUMNAR index on the property.
   *
   * @param where a SQL condition, null for every record
   */
  public static Map<Object, Long> expectedCounts(final Database db, final String type, final String property, final String where) {
    final ColumnDefinition column = column(db, type, property);
    final Map<Object, Long> counts = new HashMap<>();
    if (column.byItem()) {
      // per record: the items' keys, normalized THEN deduplicated; a record with no countable item is one missing document
      final Set<Object> keys = new HashSet<>();
      try (final ResultSet rs = db.query("sql", "SELECT " + quote(property) + " AS v FROM " + quote(type) + where(where))) {
        while (rs.hasNext()) {
          keys.clear();
          collectItemKeys(rs.next().getProperty("v"), keys);
          if (keys.isEmpty())
            add(counts, MISSING, 1L);
          for (final Object key : keys)
            add(counts, key, 1L);
        }
      }
      // cross-check against SQL's own unwinding: the same distinct values (counts can differ only by asSet() deduplicating before
      // normalization, which the pass above does after)
      final Set<Object> unwound = new HashSet<>();
      final String items = "SELECT v, count(*) AS c FROM (SELECT " + quote(property) + ".asSet() AS v FROM " + quote(type) + where(where)
          + " UNWIND v) GROUP BY v";
      try (final ResultSet rs = db.query("sql", items)) {
        while (rs.hasNext()) {
          final Object key = ordinalKey(rs.next().getProperty("v"));
          if (key != MISSING)
            unwound.add(key);
        }
      }
      final Set<Object> counted = new HashSet<>(counts.keySet());
      counted.remove(MISSING);
      if (!counted.equals(unwound))
        throw new AssertionError("Oracle: the items of " + type + "." + property + " read per record " + counted.size()
            + " distinct values, SQL UNWIND " + unwound.size());
      return counts;
    }

    final ColumnarValueExtractor extractor = column.kind() == ColumnKind.LONG ? extractor(db, type) : null;
    final Type propertyType = column.kind() == ColumnKind.LONG ? propertyType(db, type, column) : null;
    final String terms = "SELECT " + quote(property) + " AS v, count(*) AS c FROM " + quote(type) + where(where) + " GROUP BY " + quote(
        property);
    try (final ResultSet rs = db.query("sql", terms)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        add(counts, scalarKey(column, propertyType, extractor, row.getProperty("v")), row.<Number>getProperty("c").longValue());
      }
    }
    return counts;
  }

  /**
   * The facet counts the projection answers, through a snapshot of every polymorphic bucket of the type. With {@code patchPending},
   * matches on pages not applied yet are counted by reading the record instead.
   *
   * @throws AssertionError when a bucket holding a match has no projection, a projection is not usable, a position is counted by two
   *                        views, or (with no WHERE) a bucket's live rows outside the pending pages are not exactly its records
   */
  public static Map<Object, Long> projectionCounts(final Database db, final String type, final String property, final String where,
      final boolean patchPending) {
    final ColumnDefinition column = column(db, type, property);
    final ColumnarIndexManager manager = manager(db);
    final Map<Object, Long> counts = new HashMap<>();
    final Map<Integer, PositionBitmap> matches = matches(db, type, where);
    // every bucket, not only those SQL matched: a bucket whose records are all gone must not hold live rows either
    for (final Bucket bucket : db.getSchema().getType(type).getBuckets(true)) {
      final int bucketId = bucket.getFileId();
      final PositionBitmap bucketMatches = matches.containsKey(bucketId) ? matches.get(bucketId)
          : new PositionBitmap(((LocalBucket) bucket).getMaxRecordsInPage());
      final ColumnarProjection projection = manager.projection(bucketId);
      if (projection == null) {
        if (!bucketMatches.isEmpty())
          throw new AssertionError("Bucket " + bucketId + " holds records of " + type + " but has no columnar projection");
        continue;
      }
      try (final ColumnarSnapshot snapshot = projection.acquire()) {
        if (!snapshot.isUsable())
          throw new AssertionError("The columnar projection of bucket " + bucketId + " is not usable");
        if (where == null)
          assertLiveRowsAreTheRecords(snapshot, bucketMatches);
        countBucket(db, manager, snapshot, column, bucketMatches, patchPending, counts);
      }
    }
    return counts;
  }

  /**
   * Reference of a HIERARCHY facet: per record of {@code SELECT <property> FROM <type> WHERE <where>} (polymorphic), the set of its
   * values (normalized, deduplicated) plus every ancestor reached from the vertices of {@code vertexType} keyed by each value
   * ({@link Database#lookupByKey}) through {@code getVertices(IN, parentEdge)}, walked with a visited set of its own; a value no vertex
   * has counts only itself. Each value of the set counts the record once, and a record with no value counts once under
   * {@link #MISSING}. Shares no code with the production path: no view, no expander, no projection.
   *
   * @param where a SQL condition, null for every record
   */
  public static Map<Object, Long> hierarchicalCounts(final Database db, final String type, final String property, final String where,
      final String vertexType, final String key, final String parentEdge) {
    final Map<Object, Long> counts = new HashMap<>();
    final Set<Object> values = new HashSet<>();
    final Set<Object> closure = new HashSet<>();
    final Set<RID> visited = new HashSet<>();
    final ArrayDeque<Vertex> pending = new ArrayDeque<>();
    try (final ResultSet rs = db.query("sql", "SELECT " + quote(property) + " AS v FROM " + quote(type) + where(where))) {
      while (rs.hasNext()) {
        values.clear();
        closure.clear();
        visited.clear();
        collectItemKeys(rs.next().getProperty("v"), values);
        if (values.isEmpty()) {
          add(counts, MISSING, 1L);
          continue;
        }
        for (final Object value : values) {
          closure.add(value);
          final IndexCursor tags = db.lookupByKey(vertexType, key, value);
          while (tags.hasNext())
            pending.add(tags.next().asVertex());
        }
        while (!pending.isEmpty()) {
          final Vertex tag = pending.poll();
          if (!visited.add(tag.getIdentity()))
            continue;
          final Object tagKey = ordinalKey(tag.get(key));
          // a vertex without a key is no node of the hierarchy: nothing is reached through it
          if (tagKey == MISSING)
            continue;
          closure.add(tagKey);
          for (final Vertex parent : tag.getVertices(Vertex.DIRECTION.IN, parentEdge))
            pending.add(parent);
        }
        for (final Object node : closure)
          add(counts, node, 1L);
      }
    }
    return counts;
  }

  /**
   * The answer of a DISTINCT facet by SQL alone: {@code SELECT count(DISTINCT p)} for a single-valued property, and for a LIST property
   * the number of non-null groups of {@code GROUP BY p} over its items ({@code UNWIND p}). Shares no code with the collector.
   *
   * @param where a SQL condition, null for every record
   */
  public static long distinctCount(final Database db, final String type, final String property, final String where) {
    final Property declared = db.getSchema().getType(type).getPolymorphicPropertyIfExists(property);
    if (declared != null && declared.getType() == Type.LIST) {
      long groups = 0;
      try (final ResultSet rs = db.query("sql",
          "SELECT " + quote(property) + " AS v, count(*) AS c FROM (SELECT " + quote(property) + " FROM " + quote(type) + where(where)
              + " UNWIND " + quote(property) + ") GROUP BY " + quote(property))) {
        while (rs.hasNext())
          if (rs.next().getProperty("v") != null)
            groups++;
      }
      return groups;
    }
    try (final ResultSet rs = db.query("sql",
        "SELECT count(DISTINCT " + quote(property) + ") AS c FROM " + quote(type) + where(where))) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  /**
   * The answer of a STATS facet by SQL alone: {@code count(p)}, {@code min(p)}, {@code max(p)}, {@code sum(p)} and {@code avg(p)} as
   * SQL returns them (value and Java type), missing = {@code count(@rid) - count(p)}. Over dates (or when no record has a value) sum and
   * average are null, which SQL refuses to compute. Shares no code with the collector.
   *
   * @param where a SQL condition, null for every record
   */
  public static FacetStats stats(final Database db, final String type, final String property, final String where) {
    final String p = quote(property);
    final long count;
    final long total;
    final Object min;
    final Object max;
    try (final ResultSet rs = db.query("sql",
        "SELECT count(" + p + ") AS c, min(" + p + ") AS mn, max(" + p + ") AS mx, count(@rid) AS t FROM " + quote(type) + where(where))) {
      final Result row = rs.next();
      count = row.<Number>getProperty("c").longValue();
      total = row.<Number>getProperty("t").longValue();
      min = row.getProperty("mn");
      max = row.getProperty("mx");
    }
    if (min == null || DateUtils.isDate(min))
      return new FacetStats(count, total - count, min, max, null, null);
    try (final ResultSet rs = db.query("sql", "SELECT sum(" + p + ") AS s, avg(" + p + ") AS a FROM " + quote(type) + where(where))) {
      final Result row = rs.next();
      return new FacetStats(count, total - count, min, max, row.getProperty("s"), row.getProperty("a"));
    }
  }

  /** {@link ColumnarIndexManager#awaitCaughtUp} of the database's embedded instance, 30 s. */
  public static void awaitCaughtUp(final Database db) {
    try {
      if (!manager(db).awaitCaughtUp(CAUGHT_UP_TIMEOUT_MS))
        throw new AssertionError("The columnar projections of database '" + db.getName() + "' did not catch up within "
            + CAUGHT_UP_TIMEOUT_MS + " ms");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while waiting for the columnar projections to catch up", e);
    }
  }

  /** Waits for the projections to catch up, then asserts that the projection counts are the GROUP BY counts. */
  public static void assertEquivalent(final Database db, final String type, final String property, final String where) {
    awaitCaughtUp(db);
    assertSame(type, property, where, expectedCounts(db, type, property, where), projectionCounts(db, type, property, where, false));
  }

  /** Asserts the same without waiting: the pages not applied yet are patched from the records. */
  public static void assertEquivalentWhileLagging(final Database db, final String type, final String property, final String where) {
    assertSame(type, property, where, expectedCounts(db, type, property, where), projectionCounts(db, type, property, where, true));
  }

  /**
   * Runs {@code SELECT FROM <type> WHERE <where> FACETS f: <property> TERMS TOP <topMax>} and asserts that its counts are the GROUP BY
   * counts of {@link #expectedCounts}: the {@code missing} member is compared under {@link #MISSING}, a date value by its column's epoch
   * count. Never waits for the projection to catch up, so it can run inside a {@code suspendApply()} hold.
   *
   * @param where a SQL condition, null for every record
   */
  public static void assertFacetsEqualGroupBy(final Database db, final String type, final String property, final String where) {
    final int topMax = GlobalConfiguration.COLUMNAR_TOP_MAX.getValueAsInteger();
    final Map<Object, Long> expected = expectedCounts(db, type, property, where);
    final long distinct = expected.keySet().stream().filter(k -> k != MISSING).count();
    if (distinct > topMax)
      throw new AssertionError("Oracle: " + type + "." + property + " WHERE " + where + " has " + distinct
          + " distinct values, more than TOP " + topMax + " can return: the comparison would be truncated");

    final FacetResults facets;
    try (final ResultSet rs = db.query("sql",
        "SELECT FROM " + quote(type) + where(where) + " FACETS f: " + quote(property) + " TERMS TOP " + topMax)) {
      facets = rs.getFacets();
    }
    if (facets == null)
      throw new AssertionError("The FACETS query over " + type + " WHERE " + where + " returned no facets");
    final FacetResult result = facets.get("f");
    if (!"index".equals(result.source()) && !"scan".equals(result.source()))
      throw new AssertionError("Facet " + property + " WHERE " + where + " is " + result.source() + ": " + result.reason());

    final ColumnarValueExtractor extractor = extractor(db, type);
    final Map<Object, Long> actual = new HashMap<>();
    for (final FacetBucket bucket : result.buckets()) {
      if (bucket.isMissing())
        continue;
      final Object value = bucket.value();
      final Object key = value == null || value instanceof String || value instanceof Number || value instanceof Boolean ? value
          : extractor.toColumnLong(property, value);
      if (actual.put(key, bucket.count()) != null)
        throw new AssertionError("Facet " + property + " WHERE " + where + " returned the value " + value + " twice");
    }
    if (result.missing() > 0)
      actual.put(MISSING, result.missing());
    if (result.otherCount() != 0)
      throw new AssertionError("Facet " + property + " WHERE " + where + " left " + result.otherCount() + " values out of TOP " + topMax);
    assertSame(type, property, where, expected, actual);
  }

  /** LONG columns: stored value to count, without the missing records. */
  public static Map<Long, Long> expectedHistogram(final Database db, final String type, final String property, final String where) {
    requireLongColumn(db, type, property);
    return histogram(expectedCounts(db, type, property, where));
  }

  /** LONG columns: the histogram the projection answers, once caught up. */
  public static Map<Long, Long> projectionHistogram(final Database db, final String type, final String property, final String where) {
    requireLongColumn(db, type, property);
    return histogram(projectionCounts(db, type, property, where, false));
  }

  // ----------------------------------------------------------------------------------------------------------- projection side

  private static void countBucket(final Database db, final ColumnarIndexManager manager, final ColumnarSnapshot snapshot,
      final ColumnDefinition column, final PositionBitmap bucketMatches, final boolean patchPending, final Map<Object, Long> counts) {
    final String property = column.property();
    final GlobalDictionary dictionary = snapshot.dictionary(property);
    PositionBitmap segmentMatches = bucketMatches;
    PositionBitmap patched = null;
    if (patchPending) {
      final PositionBitmap pending = snapshot.pendingPositions();
      patched = bucketMatches.copy();
      patched.and(pending);
      segmentMatches = bucketMatches.copy();
      segmentMatches.andNot(pending);
    }

    final int maxRecordsInPage = bucketMatches.maxRecordsInPage();
    final PositionBitmap seen = new PositionBitmap(maxRecordsInPage);
    for (final ColumnarSegmentView view : snapshot.segments()) {
      final PositionBitmap rows = segmentMatches.copy();
      rows.and(view.live());
      if (rows.isEmpty())
        continue;
      rows.forEach(position -> {
        if (seen.get(position))
          throw new AssertionError("Position " + position + " of bucket " + snapshot.bucketId() + " is live in two views");
        seen.set(position);
      });

      final ColumnReader reader = view.column(property);
      if (reader == null) {
        add(counts, MISSING, rows.cardinality());
        continue;
      }
      switch (column.kind()) {
      case ORDINAL, ORDINAL_LIST -> {
        final PositionBitmap valued = new PositionBitmap(maxRecordsInPage);
        reader.forEachOrdinal(segmentMatches, (position, ordinal) -> {
          if (!rows.get(position))
            throw new AssertionError("forEachOrdinal visited position " + position + ", outside matches AND live()");
          if (dictionary == null)
            throw new AssertionError("Ordinal " + ordinal + " of '" + property + "' with no dictionary in the snapshot");
          valued.set(position);
          add(counts, dictionary.valueOf(ordinal), 1L);
        });
        add(counts, MISSING, rows.cardinality() - valued.cardinality());
      }
      case LONG -> rows.forEach(position -> add(counts, reader.isPresent(position) ? reader.longValue(position) : MISSING, 1L));
      case DOUBLE -> rows.forEach(
          position -> add(counts, reader.isPresent(position) ? Type.normalizeForKey(reader.doubleValue(position)) : MISSING, 1L));
      }
    }

    if (patched != null && !patched.isEmpty()) {
      final GlobalDictionary decoding = dictionary != null ? dictionary : GlobalDictionary.createInMemory();
      final CountingSink sink = new CountingSink(decoding, counts);
      final int bucketId = snapshot.bucketId();
      patched.forEach(position -> {
        final Document record = db.lookupByRID(new RID(bucketId, position), true).asDocument();
        final ColumnarValueExtractor extractor = manager.extractor(record.getTypeName());
        if (extractor == null)
          throw new AssertionError("Type " + record.getTypeName() + " has no columnar extractor");
        extractor.extract(record, property, decoding, sink);
      });
    }
  }

  /**
   * The union of the views' live rows must be the bucket's records, both outside the pending pages (whose rows may legitimately lag):
   * a live row with no record is a stale row a count without WHERE would report.
   */
  private static void assertLiveRowsAreTheRecords(final ColumnarSnapshot snapshot, final PositionBitmap records) {
    final PositionBitmap pending = snapshot.pendingPositions();
    final PositionBitmap live = new PositionBitmap(records.maxRecordsInPage());
    for (final ColumnarSegmentView view : snapshot.segments())
      live.or(view.live());
    live.andNot(pending);
    final PositionBitmap expected = records.copy();
    expected.andNot(pending);
    final PositionBitmap stale = live.copy();
    stale.andNot(expected);
    final PositionBitmap absent = expected.copy();
    absent.andNot(live);
    if (!stale.isEmpty() || !absent.isEmpty())
      throw new AssertionError("The columnar projection of bucket " + snapshot.bucketId() + " holds " + stale.cardinality()
          + " live rows with no record " + firstPositions(stale) + " and misses " + absent.cardinality() + " records " + firstPositions(
          absent));
  }

  private static List<Long> firstPositions(final PositionBitmap positions) {
    final List<Long> first = new ArrayList<>();
    positions.forEach(position -> {
      if (first.size() < MAX_DIFFS_REPORTED)
        first.add(position);
    });
    return first;
  }

  /** Counts what a read-only extraction reports, decoding ordinals with the snapshot's dictionary. */
  private static final class CountingSink implements ValueSink {
    private final GlobalDictionary  dictionary;
    private final Map<Object, Long> counts;

    CountingSink(final GlobalDictionary dictionary, final Map<Object, Long> counts) {
      this.dictionary = dictionary;
      this.counts = counts;
    }

    @Override
    public void ordinal(final int o) {
      add(counts, dictionary.valueOf(o), 1L);
    }

    @Override
    public void ordinals(final int[] sorted, final int count) {
      for (int i = 0; i < count; i++)
        add(counts, dictionary.valueOf(sorted[i]), 1L);
    }

    @Override
    public void longValue(final long v) {
      add(counts, v, 1L);
    }

    @Override
    public void doubleValue(final double v) {
      add(counts, Type.normalizeForKey(v), 1L);
    }

    @Override
    public void missing() {
      add(counts, MISSING, 1L);
    }

    @Override
    public void unknownValue(final Object normalized) {
      add(counts, normalized, 1L);
    }
  }

  /** The positions of the records SQL matches, one bitmap per bucket. */
  private static Map<Integer, PositionBitmap> matches(final Database db, final String type, final String where) {
    final Map<Integer, PositionBitmap> byBucket = new TreeMap<>();
    try (final ResultSet rs = db.query("sql", "SELECT @rid AS rid FROM " + quote(type) + where(where))) {
      while (rs.hasNext()) {
        final RID rid = rs.next().<Identifiable>getProperty("rid").getIdentity();
        byBucket.computeIfAbsent(rid.getBucketId(),
                bucketId -> new PositionBitmap(((LocalBucket) db.getSchema().getBucketById(bucketId)).getMaxRecordsInPage()))
            .set(rid.getPosition());
      }
    }
    return byBucket;
  }

  // ------------------------------------------------------------------------------------------------------------------ SQL side

  private static Object scalarKey(final ColumnDefinition column, final Type propertyType, final ColumnarValueExtractor extractor,
      final Object raw) {
    if (raw == null)
      return MISSING;
    return switch (column.kind()) {
      case ORDINAL, ORDINAL_LIST -> ordinalKey(raw);
      case LONG -> longKey(column.property(), propertyType, extractor, raw);
      // the extractor's DOUBLE rule: any Number, a Float through its decimal form; anything else is missing
      case DOUBLE -> raw instanceof Number number ? Type.normalizeForKey(raw instanceof Float f ? Type.widenFloat(f) : number.doubleValue())
          : MISSING;
    };
  }

  /**
   * The epoch count (or integer) a LONG column stores for the value, computed without the extractor; the extractor only decides
   * whether the value is rejected, and must give the same key wherever both convert. A value this oracle has no rule for (a number or
   * a string in a date column, reachable only through schemaless data) takes the extractor's key.
   */
  private static Object longKey(final String property, final Type propertyType, final ColumnarValueExtractor extractor, final Object raw) {
    final long converted;
    try {
      converted = extractor.toColumnLong(property, raw);
    } catch (final IllegalArgumentException e) {
      return MISSING;
    }
    final Long independent = independentLong(propertyType, raw);
    if (independent == null)
      return converted;
    if (independent != converted)
      throw new AssertionError("Oracle: " + raw + " (" + raw.getClass().getSimpleName() + ") of " + propertyType + " column '" + property
          + "' keys as " + independent + " by java.time, " + converted + " by the extractor");
    return independent;
  }

  private static Long independentLong(final Type propertyType, final Object raw) {
    try {
      return switch (propertyType) {
        case BYTE, SHORT, INTEGER, LONG -> raw instanceof Number number ? number.longValue() : null;
        case DATE -> raw instanceof LocalDate date ? date.toEpochDay() : epochIn(raw, ChronoUnit.DAYS);
        case DATETIME_SECOND -> epochIn(raw, ChronoUnit.SECONDS);
        case DATETIME -> epochIn(raw, ChronoUnit.MILLIS);
        case DATETIME_MICROS -> epochIn(raw, ChronoUnit.MICROS);
        case DATETIME_NANOS -> epochIn(raw, ChronoUnit.NANOS);
        default -> null;
      };
    } catch (final ArithmeticException e) {
      return null;
    }
  }

  /** The instant of a date value counted in {@code unit} since the epoch, floored; null for a value that is not a date. */
  private static Long epochIn(final Object raw, final ChronoUnit unit) {
    final Instant instant = switch (raw) {
      case Date date -> date.toInstant();
      case Calendar calendar -> calendar.toInstant();
      case Instant i -> i;
      case LocalDateTime dateTime -> dateTime.toInstant(ZoneOffset.UTC);
      case LocalDate date -> date.atStartOfDay(ZoneOffset.UTC).toInstant();
      case ZonedDateTime dateTime -> dateTime.toInstant();
      case OffsetDateTime dateTime -> dateTime.toInstant();
      default -> null;
    };
    if (instant == null)
      return null;
    final long seconds = instant.getEpochSecond();
    final long nanos = instant.getNano();
    return switch (unit) {
      case DAYS -> Math.floorDiv(seconds, 86_400L);
      case SECONDS -> seconds;
      case MILLIS -> Math.addExact(Math.multiplyExact(seconds, 1_000L), nanos / 1_000_000L);
      case MICROS -> Math.addExact(Math.multiplyExact(seconds, 1_000_000L), nanos / 1_000L);
      case NANOS -> Math.addExact(Math.multiplyExact(seconds, 1_000_000_000L), nanos);
      default -> throw new IllegalArgumentException("Unit " + unit);
    };
  }

  /** The schema type of the property, as declared on the type or a super type; the index key type when it is not declared. */
  private static Type propertyType(final Database db, final String type, final ColumnDefinition column) {
    final Property declared = db.getSchema().getType(type).getPolymorphicPropertyIfExists(column.property());
    return declared != null ? declared.getType() : column.propertyType();
  }

  /** The dictionary key of a value, {@link #MISSING} when a dictionary cannot hold it. */
  private static Object ordinalKey(final Object raw) {
    if (raw == null)
      return MISSING;
    final Object value = raw instanceof Identifiable identifiable && !(raw instanceof RID) ? identifiable.getIdentity() : raw;
    if (value == null)
      return MISSING;
    final Object key = Type.normalizeForKey(value);
    return DictionaryValueCodec.isSupported(key) ? key : MISSING;
  }

  /** Adds the keys of the countable items of a list value (a lone value is unwound to itself, as UNWIND and the extractor do). */
  private static void collectItemKeys(final Object raw, final Set<Object> keys) {
    if (raw == null)
      return;
    if (raw instanceof Iterable<?> items) {
      for (final Object item : items)
        addItemKey(item, keys);
    } else if (raw.getClass().isArray()) {
      for (int i = 0, length = Array.getLength(raw); i < length; i++)
        addItemKey(Array.get(raw, i), keys);
    } else
      addItemKey(raw, keys);
  }

  private static void addItemKey(final Object item, final Set<Object> keys) {
    final Object key = ordinalKey(item);
    if (key != MISSING)
      keys.add(key);
  }

  // ------------------------------------------------------------------------------------------------------------------ helpers

  private static void assertSame(final String type, final String property, final String where, final Map<Object, Long> expected,
      final Map<Object, Long> projected) {
    if (expected.equals(projected))
      return;
    final List<String> diffs = new ArrayList<>();
    final Set<Object> keys = new HashSet<>(expected.keySet());
    keys.addAll(projected.keySet());
    for (final Object key : keys) {
      final long e = expected.getOrDefault(key, 0L);
      final long p = projected.getOrDefault(key, 0L);
      if (e != p && diffs.size() < MAX_DIFFS_REPORTED)
        diffs.add(key + ": GROUP BY " + e + ", projection " + p);
    }
    throw new AssertionError("Columnar projection of " + type + "." + property + (where != null ? " WHERE " + where : "")
        + " diverges from GROUP BY (" + keys.size() + " keys, first differences): " + diffs);
  }

  /**
   * The column of the COLUMNAR index on {@code property} the type declares or inherits, resolved as the schema resolves it (the type's
   * own first, then its super types in declaration order), so the answer never depends on hash iteration order.
   */
  private static ColumnDefinition column(final Database db, final String type, final String property) {
    final TypeIndex index = db.getSchema().getType(type).getPolymorphicColumnarIndex(property);
    if (index != null)
      for (final IndexInternal sub : index.getSubIndexes())
        if (sub instanceof ColumnarIndex columnar)
          return columnar.columnDefinition();
    throw new IllegalArgumentException("Type " + type + " has no COLUMNAR index on '" + property + "'");
  }

  private static void requireLongColumn(final Database db, final String type, final String property) {
    if (column(db, type, property).kind() != ColumnKind.LONG)
      throw new IllegalArgumentException("'" + property + "' of " + type + " is not a LONG column");
  }

  private static Map<Long, Long> histogram(final Map<Object, Long> counts) {
    final Map<Long, Long> histogram = new HashMap<>();
    for (final Map.Entry<Object, Long> entry : counts.entrySet())
      if (entry.getKey() != MISSING)
        histogram.put((Long) entry.getKey(), entry.getValue());
    return histogram;
  }

  private static ColumnarValueExtractor extractor(final Database db, final String type) {
    final ColumnarValueExtractor extractor = manager(db).extractor(type);
    if (extractor == null)
      throw new IllegalArgumentException("Type " + type + " has no columnar extractor");
    return extractor;
  }

  private static ColumnarIndexManager manager(final Database db) {
    return ((LocalDatabase) ((DatabaseInternal) db).getEmbedded()).getColumnarIndexManager();
  }

  private static void add(final Map<Object, Long> counts, final Object key, final long count) {
    if (count != 0)
      counts.merge(key, count, Long::sum);
  }

  private static String where(final String where) {
    return where == null ? "" : " WHERE " + where;
  }

  private static String quote(final String name) {
    return "`" + name + "`";
  }
}
