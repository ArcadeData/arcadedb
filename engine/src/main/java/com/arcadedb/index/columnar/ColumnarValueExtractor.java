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

import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;

import java.lang.reflect.Array;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Turns the property values of a record into column values with the rules GROUP BY keys by: the raw value is read as an index reads
 * it ({@link DocumentIndexer#getPropertyValue}) and numbers are normalized by {@link Type#normalizeNumberForKey}, so the ordinals
 * fed to a {@link GlobalDictionary} are exactly the group keys of the same data. A value the column kind cannot hold is reported as
 * {@link ValueSink#missing()} and counted in {@link ColumnarStats#conversionFailure} by {@code extractForApply} only (once per projected record
 * and failing value); read-only extractions (queries, patching) report {@code missing} but do not count, so a failure is not counted
 * once per query. Null or an absent property is missing without a failure.
 * <p>
 * Where this deliberately differs from GROUP BY (all reachable only through schemaless or mistyped data, the plan accepts them): a
 * DATE or DATETIME column also accepts the numbers and strings {@code DateUtils} converts, and a {@code Date} and a
 * {@code LocalDateTime} of the same instant merge into one long; a DOUBLE column keeps a double, so integers above 2^53 that GROUP BY
 * keeps apart can merge; a LocalDate in a DATETIME_NANOS column saturates; a Float NaN in an ORDINAL column is refused where GROUP BY
 * keys it. A DOUBLE column stores -0.0 as 0.0, as GROUP BY keys both as Long 0.
 * <p>
 * A sink must not call {@code extract} re-entrantly on the same thread: the ordinal buffer is per thread.
 * <p>
 * One extractor per type; dictionaries are per bucket and property, supplied by a {@link DictionaryResolver}. Read-only extraction
 * ({@code extract}) is safe from any thread and never grows a dictionary; {@code extractForApply} adds values and is for the
 * single apply thread of the dictionary's projection.
 */
public final class ColumnarValueExtractor {
  /**
   * The dictionary of a bucket and property, as the projection of that bucket currently writes it; throws
   * {@link IllegalArgumentException} for a bucket it has no projection of.
   */
  @FunctionalInterface
  public interface DictionaryResolver {
    GlobalDictionary dictionary(int bucketId, String property);
  }

  /** Thrown inside the extractor to say "not convertible" without a stack trace; never leaves the class. */
  private static final class Unconvertible extends RuntimeException {
    Unconvertible() {
      super(null, null, false, false);
    }
  }

  private static final Unconvertible UNCONVERTIBLE = new Unconvertible();

  private static final int INITIAL_CAPACITY      = 16;
  private static final int MAX_RETAINED_CAPACITY = 4096;

  /** Growable per-thread buffer of the ordinals of one list value. */
  private static final class OrdinalBuffer {
    int[]    ordinals = new int[INITIAL_CAPACITY];
    int      count;
    Object[] unknown  = new Object[INITIAL_CAPACITY];
    int      unknownCount;

    /** Adds a value the dictionary does not hold unless an equal one is already buffered. */
    void addUnknown(final Object value) {
      for (int i = 0; i < unknownCount; i++)
        if (unknown[i].equals(value))
          return;
      if (unknownCount == unknown.length)
        unknown = Arrays.copyOf(unknown, unknownCount * 2);
      unknown[unknownCount++] = value;
    }

    /** Drops the references and gives back an array that grew for one huge list. */
    void reset() {
      Arrays.fill(unknown, 0, unknownCount, null);
      unknownCount = 0;
      count = 0;
      if (ordinals.length > MAX_RETAINED_CAPACITY)
        ordinals = new int[INITIAL_CAPACITY];
      if (unknown.length > MAX_RETAINED_CAPACITY)
        unknown = new Object[INITIAL_CAPACITY];
    }

    void add(final int ordinal) {
      if (count == ordinals.length)
        ordinals = Arrays.copyOf(ordinals, count * 2);
      ordinals[count++] = ordinal;
    }

    /** Sorts the ordinals and removes duplicates in place. */
    void sortAndDeduplicate() {
      Arrays.sort(ordinals, 0, count);
      int unique = 0;
      for (int i = 0; i < count; i++)
        if (unique == 0 || ordinals[i] != ordinals[unique - 1])
          ordinals[unique++] = ordinals[i];
      count = unique;
    }
  }

  private final String                        typeName;
  private final Map<String, ColumnDefinition> columns = new HashMap<>();
  private final DictionaryResolver            dictionaries;
  private final ColumnarStats                 stats;
  private final ThreadLocal<OrdinalBuffer>    buffers = ThreadLocal.withInitial(OrdinalBuffer::new);

  ColumnarValueExtractor(final String typeName, final ColumnDefinition[] columns, final DictionaryResolver dictionaries,
      final ColumnarStats stats) {
    this.typeName = typeName;
    for (final ColumnDefinition column : columns)
      this.columns.put(column.property(), column);
    this.dictionaries = dictionaries;
    this.stats = stats;
  }

  /**
   * Read-only extraction against the dictionary of the record's bucket: a value the dictionary does not hold is reported through
   * {@link ValueSink#unknownValue}, the dictionary is never grown.
   *
   * @throws IllegalArgumentException for a dictionary-encoded column, when the record has no identity yet (it has no bucket), or when
   *                                  the {@link DictionaryResolver} refuses its bucket
   */
  public void extract(final Document record, final String property, final ValueSink sink) {
    final ColumnDefinition column = column(property);
    final boolean ordinal = column.kind() == ColumnKind.ORDINAL || column.kind() == ColumnKind.ORDINAL_LIST;
    GlobalDictionary dictionary = null;
    if (ordinal) {
      final RID rid = record.getIdentity();
      if (rid == null)
        throw new IllegalArgumentException("A record with no identity has no bucket to resolve the dictionary of '" + property + "' from");
      dictionary = dictionaries.dictionary(rid.getBucketId(), property);
    }
    extract(record, column, dictionary, false, sink);
  }

  /**
   * Read-only extraction against an explicit dictionary generation (the one of the snapshot being read). For a list a value the
   * dictionary does not hold is reported by {@link ValueSink#unknownValue}, once per distinct value, before the known ordinals reach
   * {@link ValueSink#ordinals}. Failures are not counted.
   */
  public void extract(final Document record, final String property, final GlobalDictionary dictionary, final ValueSink sink) {
    extract(record, column(property), dictionary, false, sink);
  }

  /** Apply thread only: adds the values the dictionary does not hold yet. */
  void extractForApply(final Document record, final ColumnDefinition column, final GlobalDictionary dictionary, final ValueSink sink) {
    extract(record, column, dictionary, true, sink);
  }

  /**
   * The LONG-column encoding of a literal (a range or histogram bound), by the same rules as extraction.
   *
   * @throws IllegalArgumentException when the column is not a LONG column or the value is not convertible
   */
  public long toColumnLong(final String property, final Object value) {
    final ColumnDefinition column = column(property);
    if (column.kind() != ColumnKind.LONG)
      throw new IllegalArgumentException("Property '" + property + "' of type " + typeName + " is not a LONG column");
    if (value == null)
      throw new IllegalArgumentException("Cannot encode null as a value of column '" + property + "'");
    try {
      return toLong(column.propertyType(), value);
    } catch (final Unconvertible e) {
      throw new IllegalArgumentException("Value of " + value.getClass().getName() + " is not convertible to column '" + property + "' ("
          + column.propertyType() + ")");
    }
  }

  /** The unit of the epoch counts a date column stores, null for a column that is not a date. */
  public TimeUnit dateUnit(final String property) {
    return switch (column(property).propertyType()) {
      case DATE -> TimeUnit.DAYS;
      case DATETIME_SECOND -> TimeUnit.SECONDS;
      case DATETIME -> TimeUnit.MILLISECONDS;
      case DATETIME_MICROS -> TimeUnit.MICROSECONDS;
      case DATETIME_NANOS -> TimeUnit.NANOSECONDS;
      default -> null;
    };
  }

  private ColumnDefinition column(final String property) {
    final ColumnDefinition column = columns.get(property);
    if (column == null)
      throw new IllegalArgumentException("Property '" + property + "' is not a columnar column of type " + typeName);
    return column;
  }

  private void extract(final Document record, final ColumnDefinition column, final GlobalDictionary dictionary, final boolean grow,
      final ValueSink sink) {
    final Object raw = DocumentIndexer.getPropertyValue(record, column.property());
    if (raw == null) {
      sink.missing();
      return;
    }
    switch (column.kind()) {
    case ORDINAL -> {
      final Object normalized = normalize(raw);
      if (!DictionaryValueCodec.isSupported(normalized)) {
        failed(column, grow, sink);
        return;
      }
      final int ordinal = grow ? dictionary.getOrAdd(normalized) : dictionary.ordinalOf(normalized);
      if (ordinal >= 0)
        sink.ordinal(ordinal);
      else
        sink.unknownValue(normalized);
    }
    case ORDINAL_LIST -> extractList(raw, column, dictionary, grow, sink);
    case LONG -> {
      final long value;
      try {
        value = toLong(column.propertyType(), raw);
      } catch (final Unconvertible e) {
        failed(column, grow, sink);
        return;
      }
      sink.longValue(value);
    }
    case DOUBLE -> {
      if (raw instanceof Number number) {
        final double value = raw instanceof Float f ? Type.widenFloat(f) : number.doubleValue();
        // -0.0 and 0.0 are one GROUP BY key (Long 0)
        sink.doubleValue(value == 0d ? 0d : value);
      } else
        failed(column, grow, sink);
    }
    }
  }

  private void extractList(final Object raw, final ColumnDefinition column, final GlobalDictionary dictionary, final boolean grow,
      final ValueSink sink) {
    final OrdinalBuffer buffer = buffers.get();
    buffer.reset();
    if (raw instanceof Iterable<?> items) {
      for (final Object item : items)
        addItem(item, column, dictionary, grow, buffer);
    } else if (raw instanceof Object[] items) {
      for (final Object item : items)
        addItem(item, column, dictionary, grow, buffer);
    } else if (raw.getClass().isArray()) {
      final int length = Array.getLength(raw);
      for (int i = 0; i < length; i++)
        addItem(Array.get(raw, i), column, dictionary, grow, buffer);
    } else
      // a lone value is unwound to itself, as UNWIND does
      addItem(raw, column, dictionary, grow, buffer);

    try {
      for (int i = 0; i < buffer.unknownCount; i++)
        sink.unknownValue(buffer.unknown[i]);
      if (buffer.count > 0) {
        buffer.sortAndDeduplicate();
        sink.ordinals(buffer.ordinals, buffer.count);
      } else if (buffer.unknownCount == 0)
        sink.missing();
    } finally {
      buffer.reset();
    }
  }

  /** Buffers the ordinal of the item, or the item itself (once per distinct value) when the dictionary does not hold it. */
  private void addItem(final Object item, final ColumnDefinition column, final GlobalDictionary dictionary, final boolean grow,
      final OrdinalBuffer buffer) {
    if (item == null)
      return;
    final Object normalized = normalize(item);
    if (!DictionaryValueCodec.isSupported(normalized)) {
      if (grow)
        stats.conversionFailure(typeName, column.property());
      return;
    }
    final int ordinal = grow ? dictionary.getOrAdd(normalized) : dictionary.ordinalOf(normalized);
    if (ordinal >= 0)
      buffer.add(ordinal);
    else
      buffer.addUnknown(normalized);
  }

  private void failed(final ColumnDefinition column, final boolean grow, final ValueSink sink) {
    if (grow)
      stats.conversionFailure(typeName, column.property());
    sink.missing();
  }

  /** What GROUP BY keys a scalar as: numbers are canonicalized, anything else is kept (an unsupported kind is refused by the caller). */
  private static Object normalize(final Object value) {
    return value instanceof Number ? Type.normalizeNumberForKey(value) : value;
  }

  private static long toLong(final Type type, final Object raw) {
    final Long converted;
    try {
      converted = switch (type) {
        case DATE -> DateUtils.dateToEpochDays(raw);
        case DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> DateUtils.dateTimeToTimestamp(raw, DateUtils.getPrecisionFromType(type));
        default -> normalize(raw) instanceof Long number ? number : null;
      };
    } catch (final IllegalArgumentException | DateTimeException | ArithmeticException | SerializationException e) {
      throw UNCONVERTIBLE;
    }
    if (converted == null)
      throw UNCONVERTIBLE;
    return converted;
  }
}
