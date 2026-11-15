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
import java.util.IdentityHashMap;
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

  /**
   * The large collections, maps and arrays the elements of one buffer hold, so that the elements that share one are
   * charged for it once (issue #11812): a list collected once and carried by every row of an UNWIND was counted again for
   * each row, and an ORDER BY over a few hundred such rows was refused for asking more than the whole budget although it holds the list
   * once. Only the containers above {@link #SAMPLED_ELEMENTS} are remembered: the small ones are cheap to count again, and
   * the large ones are what an overcount is made of.
   * <p>
   * Each container is remembered with the elements that hold it, so a buffer that lets an element go ({@link #remove}) gives
   * back a container with the last element that held it, and forgets it then.
   * <p>
   * Not thread-safe, as the operation that owns it is not.
   */
  public static final class Shared {
    // Created at the first large container: most operations never meet one
    private IdentityHashMap<Object, Held> held;
    // The bytes charged so far to the containers met for the first time, each counted once, nested ones included
    private long    heldBytes;
    // The bytes of the containers a sampled element met first that a later sampled element turned out to share
    private long    sharedSampleBytes;
    private int     walk;
    private boolean sample;
    // The containers met for the first time that the current walk is inside of
    private int     firstSightDepth;
    private boolean releasing;
    private long    releasedBytes;

    /**
     * The estimated heap {@code value} takes, counting a large container an element held already as the reference to it,
     * and remembering the large containers met for the first time.
     */
    public long add(final Object value) {
      return walk(value, false);
    }

    /**
     * {@link #add(Object)} for an element whose estimate stands for the elements that are not estimated. A container it
     * meets first is part of what it holds on its own until another sampled element shares it: then its bytes move to
     * {@link #sharedSampleBytes()}.
     */
    public long addSample(final Object value) {
      return walk(value, true);
    }

    /**
     * The estimated heap {@code value} stops taking when the buffer lets it go: what {@link #add(Object)} charged for it,
     * the containers it was the last element to hold included, which are forgotten.
     */
    public long remove(final Object value) {
      releasing = true;
      releasedBytes = 0L;
      try {
        return estimate(value, 0, this) + releasedBytes;
      } finally {
        releasing = false;
      }
    }

    /**
     * The bytes of the containers a sampled element met for the first time and a later sampled element held as well: no
     * part of what one element holds on its own, as the elements that share one are charged for it once.
     */
    public long sharedSampleBytes() {
      return sharedSampleBytes;
    }

    private long walk(final Object value, final boolean sample) {
      ++walk;
      this.sample = sample;
      firstSightDepth = 0;
      return estimate(value, 0, this);
    }

    private long hold(final Object container, final int depth) {
      if (held == null)
        held = new IdentityHashMap<>();
      final Held known = held.get(container);
      if (known != null) {
        ++known.holders;
        if (sample && known.walk != walk) {
          sharedSampleBytes += known.sampleBytes;
          known.sampleBytes = 0L;
        }
        return REFERENCE_BYTES;
      }
      final Held entry = new Held(depth, walk);
      held.put(container, entry);
      final long before = heldBytes;
      ++firstSightDepth;
      final long bytes = contents(container, depth, this);
      --firstSightDepth;
      // The containers nested in this one met for the first time are charged on their own: this one is charged the rest
      final long charged = bytes - REFERENCE_BYTES;
      entry.bytes = charged - (heldBytes - before);
      heldBytes = before + charged;
      if (sample && firstSightDepth == 0)
        entry.sampleBytes = charged;
      return bytes;
    }

    private long release(final Object container, final int depth) {
      final Held known = held == null ? null : held.get(container);
      if (known == null) {
        // Not met when it was added, or forgotten since: it was charged in full
        releasedBytes += contents(container, depth, null) - REFERENCE_BYTES;
        return REFERENCE_BYTES;
      }
      if (--known.holders > 0)
        return REFERENCE_BYTES;
      held.remove(container);
      releasedBytes += known.bytes;
      // The containers it holds lose it as a holder, walked as they were when it was met
      contents(container, known.depth, this);
      return REFERENCE_BYTES;
    }
  }

  /** A large container a buffer holds, with the bytes charged to it alone and the number of holders. */
  private static final class Held {
    private final int  depth;
    private final int  walk;
    private int        holders = 1;
    private long       bytes;
    private long       sampleBytes;

    private Held(final int depth, final int walk) {
      this.depth = depth;
      this.walk = walk;
    }
  }

  /** The estimated heap {@code value} takes, what it references included. */
  public static long estimate(final Object value) {
    return estimate(value, 0, null);
  }

  private static long estimate(final Object value, final int depth, final Shared shared) {
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
      return estimateResult(result, depth, shared);
    if (value instanceof Document document)
      return estimateDocument(document, depth, shared);
    if (value instanceof RID)
      return 24L;
    if (value instanceof Result result)
      return estimateOtherResult(result, depth, shared);
    if (value instanceof Collection<?> collection)
      return countedOnce(collection, collection.size(), depth, shared);
    if (value instanceof Map<?, ?> map)
      return countedOnce(map, map.size(), depth, shared);
    if (value instanceof byte[] bytes)
      return ARRAY_BYTES + bytes.length;
    if (value instanceof Object[] array)
      return countedOnce(array, array.length, depth, shared);
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

  /** The estimate of a container: in full the first time {@code shared} meets a large one, as a reference afterwards. */
  private static long countedOnce(final Object container, final int size, final int depth, final Shared shared) {
    if (shared == null || size <= SAMPLED_ELEMENTS)
      return contents(container, depth, shared);
    return shared.releasing ? shared.release(container, depth) : shared.hold(container, depth);
  }

  private static long contents(final Object container, final int depth, final Shared shared) {
    if (container instanceof Collection<?> collection)
      return estimateCollection(collection, depth, shared);
    if (container instanceof Map<?, ?> map)
      return MAP_BYTES + estimateEntries(map, depth, shared);
    return estimateArray((Object[]) container, depth, shared);
  }

  private static long estimateResult(final ResultInternal result, final int depth, final Shared shared) {
    long bytes = result.content != null ? RESULT_BYTES : ELEMENT_RESULT_BYTES;
    if (result.content != null && !result.content.isEmpty())
      bytes += estimateEntries(result.content, depth, shared);
    // A CYPHER ROW OF ONE ENTITY KEEPS IT BOTH AS ITS ELEMENT AND AS A PROPERTY: IT IS HELD ONCE
    if (result.element != null && (result.content == null || !result.content.containsValue(result.element)))
      bytes += estimateDocument(result.element, depth, shared);
    if (result.temporaryContent != null && !result.temporaryContent.isEmpty())
      bytes += MAP_BYTES + estimateEntries(result.temporaryContent, depth, shared);
    if (result.metadata != null && !result.metadata.isEmpty())
      bytes += MAP_BYTES + estimateEntries(result.metadata, depth, shared);
    if (result.value != null)
      bytes += estimate(result.value, depth + 1, shared);
    return bytes;
  }

  private static long estimateOtherResult(final Result result, final int depth, final Shared shared) {
    long bytes = OBJECT_BYTES;
    if (result.isElement())
      bytes += result.getElement().map(document -> estimateDocument(document, depth, shared)).orElse(0L);
    else {
      final Set<String> names = result.getPropertyNames();
      int counted = 0;
      long sampled = 0L;
      for (final String name : names) {
        if (counted == SAMPLED_ELEMENTS)
          break;
        sampled += REFERENCE_BYTES + estimate(result.getProperty(name), depth + 1, shared);
        ++counted;
      }
      bytes += counted == 0 ? 0L : sampled * names.size() / counted;
    }
    return bytes;
  }

  private static long estimateDocument(final Document document, final int depth, final Shared shared) {
    if (document instanceof MutableDocument mutable && mutable.getBuffer() == null) {
      // A DOCUMENT BUILT IN MEMORY, NOT SERIALIZED YET: ITS PROPERTIES ARE WHAT IT HOLDS
      final Set<String> names = mutable.getPropertyNames();
      int counted = 0;
      long sampled = 0L;
      for (final String name : names) {
        if (counted == SAMPLED_ELEMENTS)
          break;
        sampled += HASH_ENTRY_BYTES + estimate(mutable.get(name), depth + 1, shared);
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

  private static long estimateCollection(final Collection<?> collection, final int depth, final Shared shared) {
    final int size = collection.size();
    if (size == 0)
      return LIST_BYTES;
    long sampled = 0L;
    int counted = 0;
    if (collection instanceof List<?> list && collection instanceof RandomAccess) {
      counted = Math.min(size, SAMPLED_ELEMENTS);
      for (int i = 0; i < counted; i++)
        sampled += estimate(list.get(i), depth + 1, shared);
    } else {
      final Iterator<?> iterator = collection.iterator();
      while (counted < SAMPLED_ELEMENTS && iterator.hasNext()) {
        sampled += estimate(iterator.next(), depth + 1, shared);
        ++counted;
      }
    }
    // A SET HOLDS EVERY ELEMENT IN A HASH ENTRY, A LIST IN AN ARRAY SLOT
    final long perSlot = collection instanceof Set ? HASH_ENTRY_BYTES : REFERENCE_BYTES;
    return LIST_BYTES + perSlot * size + (counted == 0 ? 0L : sampled * size / counted);
  }

  private static long estimateArray(final Object[] array, final int depth, final Shared shared) {
    final int counted = Math.min(array.length, SAMPLED_ELEMENTS);
    long sampled = 0L;
    for (int i = 0; i < counted; i++)
      sampled += estimate(array[i], depth + 1, shared);
    return ARRAY_BYTES + (long) REFERENCE_BYTES * array.length + (counted == 0 ? 0L : sampled * array.length / counted);
  }

  /** The entries of a map and the values they hold; the keys are counted only when they are not strings. */
  private static long estimateEntries(final Map<?, ?> map, final int depth, final Shared shared) {
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
        sampled += estimate(key, depth + 1, shared);
      sampled += estimate(entry.getValue(), depth + 1, shared);
      ++counted;
    }
    return (long) HASH_ENTRY_BYTES * size + sampled * size / counted;
  }
}
