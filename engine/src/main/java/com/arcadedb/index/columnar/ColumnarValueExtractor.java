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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Dictionary;
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
 * {@link ValueSink#missing()} and counted in {@link ColumnarStats#conversionFailure} on the apply path only: {@code extractForApply}
 * counts at once, the apply pass uses {@code extractForApplyUncounted} and counts once per staged record version and failing value.
 * Read-only extractions (queries, patching) report {@code missing} but do not count, so a failure is not counted once per query. Null or an absent property is missing without a failure.
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

  /**
   * {@link #extract(Document, String, GlobalDictionary, ValueSink)} with an explicit column definition instead of this extractor's own:
   * a reader decoding segments written with that definition (a query that planned its counters before the column was redefined) reads
   * the record in the same encoding.
   */
  public void extract(final Document record, final ColumnDefinition column, final GlobalDictionary dictionary, final ValueSink sink) {
    extract(record, column, dictionary, false, sink);
  }

  /** Apply thread only: adds the values the dictionary does not hold yet and counts the failing values. */
  void extractForApply(final Document record, final ColumnDefinition column, final GlobalDictionary dictionary, final ValueSink sink) {
    countFailures(column, extract(record, column, dictionary, true, sink));
  }

  /**
   * {@link #extractForApply} without counting: returns the number of failing values (a scalar the column kind cannot hold, or an item
   * of a list), which the apply pass counts with {@link #countFailures} only when it stages the record, so a re-read of an unchanged
   * record counts nothing.
   */
  int extractForApplyUncounted(final Document record, final ColumnDefinition column, final GlobalDictionary dictionary,
      final ValueSink sink) {
    return extract(record, column, dictionary, true, sink);
  }

  /**
   * {@link #extractForApplyUncounted} of a raw value already read from the record ({@link RecordReader#raw}): null or absent is
   * missing.
   */
  int extractValueForApplyUncounted(final Object raw, final ColumnDefinition column, final GlobalDictionary dictionary,
      final ValueSink sink) {
    return extractRaw(raw, column, dictionary, true, sink);
  }

  /** Counts {@code failures} conversion failures of the column. */
  void countFailures(final ColumnDefinition column, final int failures) {
    for (int i = 0; i < failures; i++)
      stats.conversionFailure(typeName, column.property());
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

  /** Whether the property is a column of this extractor's type. */
  public boolean hasColumn(final String property) {
    return columns.containsKey(property);
  }

  /** The unit of the epoch counts a date column stores, null for a column that is not a date. */
  public TimeUnit dateUnit(final String property) {
    return dateUnit(column(property).propertyType());
  }

  /** The unit of the epoch counts a date column of the type stores, null for a type that is not a date. */
  public static TimeUnit dateUnit(final Type propertyType) {
    return switch (propertyType) {
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

  /** Returns the number of failing values; counts nothing. */
  private int extract(final Document record, final ColumnDefinition column, final GlobalDictionary dictionary, final boolean grow,
      final ValueSink sink) {
    return extractRaw(DocumentIndexer.getPropertyValue(record, column.property()), column, dictionary, grow, sink);
  }

  private int extractRaw(final Object raw, final ColumnDefinition column, final GlobalDictionary dictionary, final boolean grow,
      final ValueSink sink) {
    if (raw == null) {
      sink.missing();
      return 0;
    }
    switch (column.kind()) {
    case ORDINAL -> {
      final Object normalized = normalize(raw);
      if (!DictionaryValueCodec.isSupported(normalized)) {
        return failed(sink);
      }
      final int ordinal = grow ? dictionary.getOrAdd(normalized) : dictionary.ordinalOf(normalized);
      if (ordinal >= 0)
        sink.ordinal(ordinal);
      else
        sink.unknownValue(normalized);
      return 0;
    }
    case ORDINAL_LIST -> {
      return extractList(raw, dictionary, grow, sink);
    }
    case LONG -> {
      final long value;
      try {
        value = toLong(column.propertyType(), raw);
      } catch (final Unconvertible e) {
        return failed(sink);
      }
      sink.longValue(value);
      return 0;
    }
    case DOUBLE -> {
      if (!(raw instanceof Number number))
        return failed(sink);
      final double value = raw instanceof Float f ? Type.widenFloat(f) : number.doubleValue();
      // -0.0 and 0.0 are one GROUP BY key (Long 0)
      sink.doubleValue(value == 0d ? 0d : value);
      return 0;
    }
    default -> throw new IllegalStateException("Unknown column kind " + column.kind());
    }
  }

  private int extractList(final Object raw, final GlobalDictionary dictionary, final boolean grow, final ValueSink sink) {
    final OrdinalBuffer buffer = buffers.get();
    buffer.reset();
    int failures = 0;
    if (raw instanceof Iterable<?> items) {
      for (final Object item : items)
        failures += addItem(item, dictionary, grow, buffer);
    } else if (raw instanceof Object[] items) {
      for (final Object item : items)
        failures += addItem(item, dictionary, grow, buffer);
    } else if (raw.getClass().isArray()) {
      final int length = Array.getLength(raw);
      for (int i = 0; i < length; i++)
        failures += addItem(Array.get(raw, i), dictionary, grow, buffer);
    } else
      // a lone value is unwound to itself, as UNWIND does
      failures += addItem(raw, dictionary, grow, buffer);

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
    return failures;
  }

  /**
   * Buffers the ordinal of the item, or the item itself (once per distinct value) when the dictionary does not hold it; returns 1 for
   * an item the dictionary cannot hold, 0 otherwise.
   */
  private static int addItem(final Object item, final GlobalDictionary dictionary, final boolean grow, final OrdinalBuffer buffer) {
    if (item == null)
      return 0;
    final Object normalized = normalize(item);
    if (!DictionaryValueCodec.isSupported(normalized))
      return 1;
    final int ordinal = grow ? dictionary.getOrAdd(normalized) : dictionary.ordinalOf(normalized);
    if (ordinal >= 0)
      buffer.add(ordinal);
    else
      buffer.addUnknown(normalized);
    return 0;
  }

  private static int failed(final ValueSink sink) {
    sink.missing();
    return 1;
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
  /**
   * Reads the raw values of the columns of one record with ONE walk of its header (final review C, ingest cost): the per-column read
   * ({@link DocumentIndexer#getPropertyValue}) walks the header twice per column ({@code has}, then {@code get}). For the apply pass and
   * the rebuild, one instance per pass and thread. A column is read through the walk when its property is a plain name the schema
   * dictionary knew at {@link #prepare} (a nested path, an {@code @} property, or a name first written later takes the per-column read),
   * and the record is an {@link ImmutableDocument} whose header reads; anything else takes the per-column read, which answers exactly as
   * before. The record's content is left at the position it had.
   */
  static final class RecordReader {
    private final ColumnDefinition[] columns;
    private final int[]              positions;
    /** Whether column k is read through the walk, decided by {@link #prepare}. */
    private final boolean[]          walked;
    private int[]                    slotByNameId = new int[0];
    private int                      wanted;
    // the record being read, between begin and end
    private Document                 document;
    private ImmutableDocument        immutable;
    private Binary                   located;
    private int                      before;

    RecordReader(final ColumnDefinition[] columns) {
      this.columns = columns;
      this.positions = new int[columns.length];
      this.walked = new boolean[columns.length];
    }

    /** Resolves the dictionary ids of the column properties, once per pass. */
    void prepare(final Dictionary dictionary) {
      final int[] ids = new int[columns.length];
      int length = 0;
      for (int k = 0; k < columns.length; k++) {
        final String property = columns[k].property();
        ids[k] = property.indexOf('.') >= 0 || property.startsWith("@") ? -1 : dictionary.getIdByName(property, false);
        length = Math.max(length, ids[k] + 1);
      }
      if (slotByNameId.length < length)
        slotByNameId = new int[length];
      Arrays.fill(slotByNameId, -1);
      wanted = 0;
      for (int k = 0; k < columns.length; k++) {
        // a second column on the same property (refused by the schema) would share the slot: it reads on its own
        walked[k] = ids[k] >= 0 && slotByNameId[ids[k]] < 0;
        if (walked[k]) {
          slotByNameId[ids[k]] = k;
          wanted++;
        }
      }
    }

    /** Walks the header of the record for the columns of {@link #prepare}; pair with {@link #end()}. */
    void begin(final Document record) {
      document = record;
      if (wanted == 0 || !(record instanceof ImmutableDocument doc))
        return;
      final Binary content = doc.getBuffer();
      if (content == null)
        return;
      before = content.position();
      content.position(doc.getPropertiesStartingPosition());
      located = doc.locateProperties(slotByNameId, positions, wanted);
      if (located == null)
        content.position(before);
      else
        immutable = doc;
    }

    /** The raw value of column k of the record, as {@link DocumentIndexer#getPropertyValue} reads it: null when absent. */
    Object raw(final int k) {
      if (located == null || !walked[k])
        return DocumentIndexer.getPropertyValue(document, columns[k].property());
      final int position = positions[k];
      return position < 0 ? null : immutable.getPropertyAt(located, columns[k].property(), position, null);
    }

    /** Leaves the record's content at the position it had before {@link #begin}. */
    void end() {
      if (located != null)
        located.position(before);
      located = null;
      immutable = null;
      document = null;
    }
  }
}
