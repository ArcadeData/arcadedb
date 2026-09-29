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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.Temporal;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;

/**
 * Estimates the heap a value held by a query buffer takes, for the {@link QueryHeapBudget} (issue #8591).
 * <p>
 * An estimate, cheap by design rather than exact: the sizes are those of a 64-bit JVM with compressed references, a
 * string is counted as Latin-1, a record as its serialized buffer plus the objects that wrap it, and a collection or a
 * map larger than {@link #SAMPLED_ELEMENTS} is extrapolated from its first elements. Nesting deeper than
 * {@link #MAX_DEPTH} is counted flat. The keys of a result row or a map are the property names every row shares, so
 * only the entries that hold them are counted, not the names themselves.
 * <p>
 * A record several rows hold is counted once per row, so the estimate errs on the high side for rows that repeat a
 * record (the start node of an expansion); rows of distinct records - the ones that fill a heap - are counted as
 * they are.
 * <p>
 * It reads a value through its ordinary accessors, so a row that is not a {@link ResultInternal} may load its record
 * to answer a property and a lazy collection may count or iterate its elements: the operations estimate the first
 * elements they hold and then one in several (see {@link OperationHeapLimit}), which keeps that cost to a sample.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class HeapEstimator {
  /** A reference, plus the slack of the array growth it lives in. */
  public static final int REFERENCE_BYTES  = 8;
  /** An entry of a hash map or a hash set, with its share of the table. */
  public static final int HASH_ENTRY_BYTES = 48;
  /** The smallest object: a header and a field. */
  public static final int OBJECT_BYTES     = 16;

  static final         int SAMPLED_ELEMENTS = 16;
  static final         int MAX_DEPTH        = 4;
  // A String object and the header of its byte array
  private static final int STRING_BYTES     = 40;
  /** A ResultInternal and the LinkedHashMap it keeps its properties in, with its table and its key set. */
  public static final  int RESULT_BYTES     = 56 + 136;
  // A ResultInternal that only wraps a record, with no map of its own
  private static final int ELEMENT_RESULT_BYTES = 56;
  // A record read from storage: the document or graph element, its RID, the Binary and the ByteBuffer that wrap its bytes
  private static final int RECORD_BYTES     = 176;
  private static final int LIST_BYTES       = 40;
  private static final int MAP_BYTES        = 64;
  private static final int ARRAY_BYTES      = 16;

  private HeapEstimator() {
  }

  /** The estimated heap {@code value} takes, what it references included. */
  public static long estimate(final Object value) {
    return estimate(value, 0);
  }

  private static long estimate(final Object value, final int depth) {
    if (value == null || value instanceof Boolean)
      // NULL, OR ONE OF THE TWO SHARED BOOLEANS
      return 0L;
    if (value instanceof String string)
      return STRING_BYTES + string.length();
    if (value instanceof Integer || value instanceof Float || value instanceof Short || value instanceof Byte
        || value instanceof Character)
      return OBJECT_BYTES;
    if (value instanceof Long || value instanceof Double)
      return 24L;
    if (depth >= MAX_DEPTH)
      return OBJECT_BYTES;

    if (value instanceof ResultInternal result)
      return estimateResult(result, depth);
    if (value instanceof Document document)
      return estimateDocument(document, depth);
    if (value instanceof RID)
      return 24L;
    if (value instanceof Result result)
      return estimateOtherResult(result, depth);
    if (value instanceof Collection<?> collection)
      return estimateCollection(collection, depth);
    if (value instanceof Map<?, ?> map)
      return MAP_BYTES + estimateEntries(map, depth);
    if (value instanceof byte[] bytes)
      return ARRAY_BYTES + bytes.length;
    if (value instanceof Object[] array)
      return estimateArray(array, depth);
    if (value instanceof float[] floats)
      return ARRAY_BYTES + 4L * floats.length;
    if (value instanceof int[] ints)
      return ARRAY_BYTES + 4L * ints.length;
    if (value instanceof long[] longs)
      return ARRAY_BYTES + 8L * longs.length;
    if (value instanceof double[] doubles)
      return ARRAY_BYTES + 8L * doubles.length;
    if (value instanceof CharSequence chars)
      return STRING_BYTES + 2L * chars.length();
    if (value instanceof BigDecimal || value instanceof BigInteger)
      return 64L;
    if (value instanceof Temporal || value instanceof Date)
      // A LocalDateTime holds a LocalDate and a LocalTime, a zoned one a zone on top: the common temporals average here
      return 48L;
    return 32L;
  }

  private static long estimateResult(final ResultInternal result, final int depth) {
    long bytes = result.content != null ? RESULT_BYTES : ELEMENT_RESULT_BYTES;
    if (result.content != null && !result.content.isEmpty())
      bytes += estimateEntries(result.content, depth);
    // A CYPHER ROW OF ONE ENTITY KEEPS IT BOTH AS ITS ELEMENT AND AS A PROPERTY: IT IS HELD ONCE
    if (result.element != null && (result.content == null || !result.content.containsValue(result.element)))
      bytes += estimateDocument(result.element, depth);
    if (result.temporaryContent != null && !result.temporaryContent.isEmpty())
      bytes += MAP_BYTES + estimateEntries(result.temporaryContent, depth);
    if (result.metadata != null && !result.metadata.isEmpty())
      bytes += MAP_BYTES + estimateEntries(result.metadata, depth);
    if (result.value != null)
      bytes += estimate(result.value, depth + 1);
    return bytes;
  }

  private static long estimateOtherResult(final Result result, final int depth) {
    long bytes = OBJECT_BYTES;
    if (result.isElement())
      bytes += result.getElement().map(document -> estimateDocument(document, depth)).orElse(0L);
    else {
      final Set<String> names = result.getPropertyNames();
      int counted = 0;
      long sampled = 0L;
      for (final String name : names) {
        if (counted == SAMPLED_ELEMENTS)
          break;
        sampled += REFERENCE_BYTES + estimate(result.getProperty(name), depth + 1);
        ++counted;
      }
      bytes += counted == 0 ? 0L : sampled * names.size() / counted;
    }
    return bytes;
  }

  private static long estimateDocument(final Document document, final int depth) {
    if (document instanceof MutableDocument mutable && mutable.getBuffer() == null) {
      // A DOCUMENT BUILT IN MEMORY, NOT SERIALIZED YET: ITS PROPERTIES ARE WHAT IT HOLDS
      final Set<String> names = mutable.getPropertyNames();
      int counted = 0;
      long sampled = 0L;
      for (final String name : names) {
        if (counted == SAMPLED_ELEMENTS)
          break;
        sampled += HASH_ENTRY_BYTES + estimate(mutable.get(name), depth + 1);
        ++counted;
      }
      return RECORD_BYTES + MAP_BYTES + (counted == 0 ? 0L : sampled * names.size() / counted);
    }
    if (document instanceof BaseRecord record) {
      final Binary buffer = record.getBuffer();
      return RECORD_BYTES + (buffer != null ? buffer.size() : 0);
    }
    return RECORD_BYTES;
  }

  private static long estimateCollection(final Collection<?> collection, final int depth) {
    final int size = collection.size();
    if (size == 0)
      return LIST_BYTES;
    long sampled = 0L;
    int counted = 0;
    if (collection instanceof List<?> list && collection instanceof RandomAccess) {
      counted = Math.min(size, SAMPLED_ELEMENTS);
      for (int i = 0; i < counted; i++)
        sampled += estimate(list.get(i), depth + 1);
    } else {
      final Iterator<?> iterator = collection.iterator();
      while (counted < SAMPLED_ELEMENTS && iterator.hasNext()) {
        sampled += estimate(iterator.next(), depth + 1);
        ++counted;
      }
    }
    // A SET HOLDS EVERY ELEMENT IN A HASH ENTRY, A LIST IN AN ARRAY SLOT
    final long perSlot = collection instanceof Set ? HASH_ENTRY_BYTES : REFERENCE_BYTES;
    return LIST_BYTES + perSlot * size + (counted == 0 ? 0L : sampled * size / counted);
  }

  private static long estimateArray(final Object[] array, final int depth) {
    final int counted = Math.min(array.length, SAMPLED_ELEMENTS);
    long sampled = 0L;
    for (int i = 0; i < counted; i++)
      sampled += estimate(array[i], depth + 1);
    return ARRAY_BYTES + (long) REFERENCE_BYTES * array.length + (counted == 0 ? 0L : sampled * array.length / counted);
  }

  /** The entries of a map and the values they hold; the keys are counted only when they are not strings. */
  private static long estimateEntries(final Map<?, ?> map, final int depth) {
    final int size = map.size();
    if (size == 0)
      return 0L;
    long sampled = 0L;
    int counted = 0;
    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      if (counted == SAMPLED_ELEMENTS)
        break;
      final Object key = entry.getKey();
      if (!(key instanceof String))
        sampled += estimate(key, depth + 1);
      sampled += estimate(entry.getValue(), depth + 1);
      ++counted;
    }
    return (long) HASH_ENTRY_BYTES * size + sampled * size / counted;
  }
}
