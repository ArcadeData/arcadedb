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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.index.columnar.ColumnarValueExtractor;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;

import java.lang.reflect.Array;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;

/**
 * The fallback scan (spec 7.8, D9): counts a bucket the projection cannot answer for a facet from its matched records, each read once
 * for every facet scanned in the bucket (and for the facets patched in it, when there are both). How each facet reads its value is
 * decided once, when the query is planned ({@link #sourceOf}), and handed to {@link #scan}; the collector that counts it was built for
 * that same {@link Source}. The values are those GROUP BY keys on:
 * <ul>
 *   <li>{@link Read#COLUMN}: a property that is a column of the record's type, extracted as the projection would store it
 *   ({@link ColumnarValueExtractor#extract} against one in-memory dictionary that is never grown, so every dictionary value arrives as
 *   {@link PatchedValue#unknown}, a list item once per distinct value), merged with the projection's counts by the collectors'
 *   converted merge;</li>
 *   <li>any other property is its raw value ({@link DocumentIndexer#getPropertyValue}), an expression facet the value of its expression
 *   on the record, and so is the property of a HIERARCHY facet (and of a DISTINCT facet over a dictionary column, or over a property
 *   that is no number and no date). For a terms, hierarchy or distinct facet ({@link Read#TERMS}) a list (or array) counts each
 *   distinct item once per document, nulls left out. A distinct facet over a date property reads the epoch counts of its declared
 *   type and precision ({@link Read#EPOCH}: days for a DATE, down to nanoseconds for a DATETIME_NANOS). For a range or histogram facet
 *   (and a distinct one over a number) the property's schema type decides: a date or datetime is read as epoch milliseconds
 *   ({@link Read#EPOCH_MS}), an integral type as a long ({@link Read#INTEGER}), anything else (a floating type, an undeclared property,
 *   an expression) as a double ({@link Read#NUMBER}); a value that does not convert counts as missing. A type that does not declare the
 *   property reads it as dates when another type of the facet holds it as a date ({@link #asDates}). Dates are read at millisecond
 *   precision whatever the declared precision: a DATETIME_MICROS or DATETIME_NANOS property bucketed by an interval or bounded by a
 *   literal finer than 1 ms can differ from its indexed answer.</li>
 * </ul>
 * A position whose record is gone is skipped. Not thread safe: driven by the query thread.
 */
public final class FacetFallbackScanner {
  /** Above this many items a list is deduplicated with a hash set instead of a linear scan of the items kept. */
  private static final int LINEAR_DEDUP_ITEMS = 16;

  /** How a scanned facet reads the value of a record. */
  enum Read {COLUMN, TERMS, NUMBER, INTEGER, EPOCH_MS, EPOCH}

  /**
   * How one facet is scanned in one bucket.
   *
   * @param column    the column the value is extracted for ({@link Read#COLUMN}), or the synthetic column the raw values are counted
   *                  as; null for {@link Read#TERMS}
   * @param extractor the extractor of the bucket's type ({@link Read#COLUMN} only)
   */
  record Source(FacetSpec spec, Read read, ColumnDefinition column, ColumnarValueExtractor extractor, ColumnConversion conversion) {
  }

  /** Receives the value of one scanned record for one facet; the value is reused for the next one. */
  @FunctionalInterface
  public interface ScanSink {
    void accept(int facetIndex, long position, PatchedValue value);
  }

  private FacetFallbackScanner() {
  }

  /**
   * Decides how the facet is scanned in a bucket of {@code type}: the property's column when it is one of the type's (and its
   * extractor knows it), else its raw value, read by the property's schema type. A HIERARCHY facet, and a DISTINCT facet over a
   * dictionary column, read the raw values ({@link Read#TERMS}): they compare values, never ordinals, so the extraction against a
   * dictionary buys them nothing. Without a column, a DISTINCT facet reads a date property as the epoch counts of its declared type and
   * precision ({@link Read#EPOCH}), an integral or a floating property by its declared type as a range facet does, and any other (a
   * string, a list, a DECIMAL, an undeclared property) by its raw values.
   */
  static Source sourceOf(final DatabaseInternal db, final DocumentType type, final FacetSpec spec) {
    final String property = spec.property();
    final ColumnDefinition column = column(type, property);
    if (spec instanceof HierarchyFacetSpec || spec instanceof DistinctFacetSpec && column != null && (column.kind() == ColumnKind.ORDINAL
        || column.kind() == ColumnKind.ORDINAL_LIST))
      return new Source(spec, Read.TERMS, null, null, null);
    if (column != null) {
      final ColumnarValueExtractor extractor = ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager().extractor(type.getName());
      if (extractor != null && extractor.hasColumn(property))
        return new Source(spec, Read.COLUMN, column, extractor, ColumnConversion.of(extractor, column));
    }
    if (spec instanceof TermsFacetSpec)
      return new Source(spec, Read.TERMS, null, null, null);
    final String name = property != null ? property : spec.alias();
    final Property declared = type == null || property == null ? null : type.getPolymorphicPropertyIfExists(property);
    final Type propertyType = declared == null ? null : declared.getType();
    if (propertyType != null && propertyType.isDateOrDateTime()) {
      if (spec instanceof DistinctFacetSpec)
        // the declared type and precision: a DATE is no instant, two instants a millisecond apart or less are two values
        return new Source(spec, Read.EPOCH, new ColumnDefinition(-1, name, propertyType, ColumnKind.LONG, false,
            propertyType == Type.DATE ? ChronoUnit.DAYS : DateUtils.getPrecisionFromType(propertyType)), null,
            ColumnConversion.declaredDates(db, propertyType));
      return dates(db, spec, name);
    }
    if (propertyType == Type.BYTE || propertyType == Type.SHORT || propertyType == Type.INTEGER || propertyType == Type.LONG)
      return new Source(spec, Read.INTEGER, new ColumnDefinition(-1, name, Type.LONG, ColumnKind.LONG, false, null), null,
          ColumnConversion.integers());
    if (spec instanceof DistinctFacetSpec && propertyType != Type.FLOAT && propertyType != Type.DOUBLE)
      // a string, a list, a DECIMAL, an undeclared property: distinct raw values (a number canonicalized by the collector)
      return new Source(spec, Read.TERMS, null, null, null);
    return new Source(spec, Read.NUMBER, new ColumnDefinition(-1, name, Type.DOUBLE, ColumnKind.DOUBLE, false, null), null, null);
  }

  /**
   * Whether the source reads, as numbers, a property the bucket's type does not declare: when other types of the facet hold it as a
   * date ({@link #asDates}), such a bucket reads it as dates too, rather than refusing a time histogram for the whole query.
   */
  static boolean isUndeclared(final DocumentType type, final Source source) {
    final String property = source.spec().property();
    return source.read() == Read.NUMBER && property != null && (type == null || type.getPolymorphicPropertyIfExists(property) == null);
  }

  /** The source reading the same property as dates: a temporal value (or a date string, or epoch ms) converted, anything else missing. */
  static Source asDates(final DatabaseInternal db, final Source source) {
    return dates(db, source.spec(), source.column().property());
  }

  private static Source dates(final DatabaseInternal db, final FacetSpec spec, final String name) {
    return new Source(spec, Read.EPOCH_MS, new ColumnDefinition(-1, name, Type.DATETIME, ColumnKind.LONG, false, ChronoUnit.MILLIS), null,
        ColumnConversion.epochMillis(db));
  }

  /**
   * Reads every matched record of the bucket once and hands its value to the sink, for every facet scanned here whose own matches
   * ({@link MatchSet#facetMatches}) hold the record.
   *
   * @param sources  the facets of the query by index, null for a facet that is not scanned in this bucket
   * @param ctx      the context the expressions of expression facets are evaluated in
   * @param patch    the bucket's patch set for the facets that are not scanned in it, null for none: its records (all of them matches
   *                 of the bucket, D11) are handed to {@code patching} from the same read
   * @param patching the patching of those facets, null with {@code patch}
   * @param guard    the query's deadline and termination, checked every few positions
   *
   * @return the records read for the scanned facets
   */
  static long scan(final DatabaseInternal db, final MatchSet matches, final int bucketId, final List<Source> sources,
      final CommandContext ctx, final ScanSink sink, final PositionBitmap patch, final FacetPatcher.Patching patching,
      final WorkGuard guard) {
    final PositionBitmap base = matches.forBucket(bucketId);
    if (base == null || base.isEmpty())
      return 0;
    final int facetCount = sources.size();
    final MatchSet.FacetMatches[] facetMatches = new MatchSet.FacetMatches[facetCount];
    GlobalDictionary noValues = null;
    for (int f = 0; f < facetCount; f++) {
      final Source source = sources.get(f);
      if (source == null)
        continue;
      facetMatches[f] = matches.facetMatches(f, bucketId);
      if (source.read() == Read.COLUMN && noValues == null)
        // never grown: a dictionary value is reported by value, which the collectors merge with the projection's ordinals
        noValues = GlobalDictionary.createInMemory();
    }
    final GlobalDictionary dictionary = noValues;

    final PatchedValue value = new PatchedValue();
    final HashSet<Object> seen = new HashSet<>();
    final long[] read = new long[1];
    final int[] ticks = new int[1];
    base.forEach(position -> {
      guard.checkPeriodically(++ticks[0]);
      final boolean patched = patch != null && patch.get(position);
      boolean scanned = false;
      for (int f = 0; f < facetCount && !scanned; f++)
        scanned = facetMatches[f] != null && facetMatches[f].get(position);
      if (!scanned && !patched)
        return;
      final Document record = FacetPatcher.recordAt(db, new RID(bucketId, position));
      if (record == null)
        return;
      if (patched)
        patching.accept(record, position);
      if (!scanned)
        return;
      read[0]++;
      ResultInternal row = null;
      for (int f = 0; f < facetCount; f++) {
        if (facetMatches[f] == null || !facetMatches[f].get(position))
          continue;
        final Source source = sources.get(f);
        final FacetSpec spec = source.spec();
        value.reset();
        if (source.read() == Read.COLUMN)
          source.extractor().extract(record, spec.property(), dictionary, value);
        else {
          final Object raw;
          if (spec.property() != null)
            raw = DocumentIndexer.getPropertyValue(record, spec.property());
          else {
            // evaluated on the row, as the projection of a GROUP BY evaluates it
            if (row == null)
              row = new ResultInternal(record);
            raw = spec.expression().execute(row, ctx);
          }
          switch (source.read()) {
          case TERMS -> termsOf(raw, value, seen);
          case NUMBER -> numberOf(raw, value);
          default -> longOf(raw, source.conversion(), value);
          }
        }
        sink.accept(f, position, value);
      }
    });
    return read[0];
  }

  /**
   * The column of the property in the type (its own COLUMNAR index or its super types'), null when the property is not a column of it:
   * a record of the type is then scanned by its raw value.
   */
  static ColumnDefinition column(final DocumentType type, final String property) {
    if (type == null || property == null)
      return null;
    final TypeIndex index = type.getPolymorphicColumnarIndex(property);
    if (index != null)
      for (final IndexInternal sub : index.getSubIndexes())
        if (sub instanceof ColumnarIndex columnar)
          return columnar.columnDefinition();
    return null;
  }

  /** A raw value read as a double: a number, anything else is missing. */
  private static void numberOf(final Object raw, final PatchedValue value) {
    if (!(raw instanceof Number number)) {
      value.missing();
      return;
    }
    final double d = raw instanceof Float f ? Type.widenFloat(f) : number.doubleValue();
    // -0.0 and 0.0 are one value, as for a DOUBLE column
    value.doubleValue(d == 0d ? 0d : d);
  }

  /** A raw value read as a long by the synthetic column's conversion (an integral number, a date as epoch ms), else missing. */
  private static void longOf(final Object raw, final ColumnConversion conversion, final PatchedValue value) {
    if (raw == null) {
      value.missing();
      return;
    }
    try {
      value.longValue(conversion.toColumnLong(null, raw));
    } catch (final IllegalArgumentException e) {
      value.missing();
    }
  }

  /** A raw value of a terms facet: each distinct item of a list or array once, a scalar as itself, both normalized. */
  private static void termsOf(final Object raw, final PatchedValue value, final HashSet<Object> seen) {
    if (raw instanceof Iterable<?> items) {
      for (final Object item : items)
        addDistinct(item, value, seen);
    } else if (raw instanceof Object[] items) {
      for (final Object item : items)
        addDistinct(item, value, seen);
    } else if (raw != null && raw.getClass().isArray()) {
      final int length = Array.getLength(raw);
      for (int i = 0; i < length; i++)
        addDistinct(Array.get(raw, i), value, seen);
    } else
      addDistinct(raw, value, seen);
    seen.clear();
    if (value.unknownCount() == 0)
      value.missing();
  }

  /** Keeps the item unless an equal one is kept: a linear scan of a short list, the hash set from {@link #LINEAR_DEDUP_ITEMS} items on. */
  private static void addDistinct(final Object item, final PatchedValue value, final HashSet<Object> seen) {
    if (item == null)
      return;
    final Object normalized = FacetValueComparator.normalize(item);
    final int kept = value.unknownCount();
    if (kept < LINEAR_DEDUP_ITEMS) {
      for (int i = 0; i < kept; i++)
        if (value.unknown(i).equals(normalized))
          return;
    } else {
      if (seen.isEmpty())
        for (int i = 0; i < kept; i++)
          seen.add(value.unknown(i));
      if (!seen.add(normalized))
        return;
    }
    value.unknownValue(normalized);
  }
}
