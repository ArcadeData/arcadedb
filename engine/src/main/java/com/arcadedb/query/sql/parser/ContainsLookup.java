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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.QueryOperatorEquals;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A hash over the items of a {@code CONTAINSANY} / {@code CONTAINSALL} operand that is the same on every row (a parameter, a
 * literal, or one object met again), so that a row costs the size of its own list instead of that size times the operand's
 * (issue #11703). It answers only where its answer is exactly what {@link QueryOperatorEquals} would say for every item:
 * <ul>
 *   <li>items that are all strings, compared with strings only (a number equals the text it converts to, which no hash of the
 *   text can answer);</li>
 *   <li>items that are all integral numbers of magnitude at most 2^53 (a {@code Byte}, {@code Short}, {@code Integer},
 *   {@code Long}, or a {@code Float} / {@code Double} holding an integer), kept as a {@code Long}, compared with numbers of the
 *   same kind; within 2^53 a double holds every such integer exactly, so 1, 1L and 1.0 are one key as they are one value.</li>
 * </ul>
 * Any other operand (a null, a fraction, NaN, a collection, a map, a record or RID, a date, a {@code byte[]}, a mix of strings
 * and numbers) has no lookup, and any other probe is answered {@link #UNKNOWN}: the caller walks the operand as before.
 */
final class ContainsLookup {
  /** Below this size the operand is walked: the walk costs less than the hash, and nothing is remembered for it. */
  static final int MIN_SIZE = 16;

  /** A probe the lookup cannot answer exactly. */
  static final Object UNKNOWN = new Object();

  /** The key of a {@code null} probe: equal to no item, as no item of a hashed operand is null. */
  private static final Object NULL_KEY = new Object();

  private static final long MAX_EXACT = 1L << 53;

  private final boolean strings;
  private final Set<Object> keys;

  private ContainsLookup(final boolean strings, final Set<Object> keys) {
    this.strings = strings;
    this.keys = keys;
  }

  /**
   * @return the lookup over the items of {@code operand}, or null when it is no collection or array of at least {@link #MIN_SIZE}
   *     items, or holds an item the hash cannot answer for exactly
   */
  static ContainsLookup of(final Object operand) {
    final int size;
    final Iterator<?> items;
    if (operand instanceof Collection<?> collection) {
      size = collection.size();
      items = collection.iterator();
    } else if (MultiValue.isSequenceArray(operand)) {
      size = Array.getLength(operand);
      items = MultiValue.getMultiValueIterator(operand);
    } else
      return null;

    if (size < MIN_SIZE)
      return null;

    final Set<Object> keys = new HashSet<>(size * 2);
    final Object first = items.hasNext() ? items.next() : null;
    final boolean strings = first instanceof String;
    Object item = first;
    while (true) {
      final Object key = strings ? (item instanceof String ? item : UNKNOWN) : numberKey(item);
      if (key == UNKNOWN)
        return null;
      keys.add(key);
      if (!items.hasNext())
        break;
      item = items.next();
    }
    return new ContainsLookup(strings, keys);
  }

  /**
   * @return the key the probe is equal to when it is an item, a key equal to no item for {@code null}, or {@link #UNKNOWN} when
   *     the probe is of a kind the hash cannot answer for
   */
  Object key(final Object probe) {
    if (probe == null)
      return NULL_KEY;
    if (strings)
      return probe instanceof String ? probe : UNKNOWN;
    return numberKey(probe);
  }

  /** @return TRUE or FALSE when the probe is answered exactly, null when the caller has to walk the operand. */
  Boolean contains(final Object probe) {
    final Object key = key(probe);
    return key == UNKNOWN ? null : keys.contains(key);
  }

  boolean containsKey(final Object key) {
    return keys.contains(key);
  }

  /** The number of distinct keys: items that are equal under {@code =} share one. */
  int size() {
    return keys.size();
  }

  private static Object numberKey(final Object value) {
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
      final long l = ((Number) value).longValue();
      return l >= -MAX_EXACT && l <= MAX_EXACT ? (Object) l : UNKNOWN;
    }
    if (value instanceof Double || value instanceof Float) {
      final double d = ((Number) value).doubleValue();
      // NaN and the infinities fail the range test; -0.0 becomes 0, which "=" also equals it to
      if (d >= -MAX_EXACT && d <= MAX_EXACT && d == Math.rint(d))
        return (long) d;
    }
    return UNKNOWN;
  }

  /**
   * One operand as the last row met it, and its lookup. Immutable, and held in a volatile field of the condition, because the
   * node of a cached plan is shared by concurrent executions and the rows of one execution can be evaluated by several threads.
   *
   * @param context the execution whose literal or parameter this is, or null when the memo is keyed by the identity of value
   * @param params  the input parameters of that execution when the memo was written
   * @param built   whether {@code lookup} was decided; an identity memo builds it only when the same object is met again
   */
  record Memo(CommandContext context, Map<String, Object> params, Object value, ContainsLookup lookup, boolean built) {
    /** @return the memo when it holds the value of a row-independent operand of this execution, otherwise null */
    static Memo constant(final Memo memo, final CommandContext context) {
      return memo != null && memo.context != null && memo.context == context && memo.params == context.getInputParameters() ?
          memo : null;
    }

    /**
     * The memo for an operand just evaluated to {@code value}. A literal or an input parameter has one value for the whole
     * execution, so its lookup is built at once and later rows skip evaluating it. Any other operand is recognized by identity
     * only, and hashed the second time the same object is met, so an operand built per row pays nothing but this check.
     */
    static Memo of(final Memo memo, final Expression expression, final Object value, final CommandContext context) {
      // an iterator is read once, so a later row cannot be given the one this row consumed
      if (context != null && !(value instanceof Iterator<?>) && expression.isLiteral(true))
        return new Memo(context, context.getInputParameters(), value, ContainsLookup.of(value), true);
      if (memo != null && memo.context == null && memo.value == value)
        return memo.built ? memo : new Memo(null, null, value, ContainsLookup.of(value), true);
      if (!largeEnough(value))
        return memo;
      return new Memo(null, null, value, null, false);
    }

    private static boolean largeEnough(final Object value) {
      if (value instanceof Collection<?> collection)
        return collection.size() >= MIN_SIZE;
      return MultiValue.isSequenceArray(value) && Array.getLength(value) >= MIN_SIZE;
    }

    /** @return the lookup when this memo holds {@code value} */
    ContainsLookup lookupFor(final Object value) {
      return this.value == value ? lookup : null;
    }
  }
}
