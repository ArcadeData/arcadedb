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
package com.arcadedb.function;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.LightEdgeRID;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.LongRangeList;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Canonicalizes values for duplicate-elimination purposes (Cypher {@code UNION},
 * {@code RETURN DISTINCT}, {@code count(DISTINCT ...)}, {@code collect(DISTINCT ...)}) so that
 * values which compare equal under Cypher's {@code =} operator also collapse to the same key.
 * <p>
 * <b>Numeric values:</b> finite numeric values which compare equal (e.g. {@code 1 = 1.0}) canonicalize
 * to the same key. Without this, duplicate-elimination paths that key a {@link java.util.HashSet} /
 * {@link java.util.HashMap} directly on the boxed value see {@code Integer.valueOf(1)},
 * {@code Long.valueOf(1)} and {@code Double.valueOf(1.0)} as distinct, because Java's per-type
 * {@code equals()}/{@code hashCode()} never compares across boxed numeric types. See issue #5789.
 * A value that is an exact-integer Java numeric type ({@link Long}, {@link Integer}, {@link Short},
 * {@link Byte}) canonicalizes to its {@code long} value, which is always exact. Any other
 * {@link Number} (typically {@link Double}, {@link Float}, or {@link java.math.BigDecimal})
 * canonicalizes to the same {@code long} value when it represents a finite integer within the range
 * a {@code double} represents integers exactly (|value| &lt;= 2^53), so it lines up with the
 * exact-integer-type canonicalization above; otherwise it canonicalizes to its {@code double} value.
 * <p>
 * <b>Graph elements:</b> a document/vertex/edge canonicalizes to its {@link RID}. Some callers build
 * their duplicate-elimination key by calling {@code toString()} on the canonicalized value (e.g. to
 * concatenate it into a composite string key); {@link com.arcadedb.database.BaseRecord#toString()} is
 * RID-based and stable, but document/vertex/edge implementations that decorate it with the record's
 * deserialized properties are not - a not-yet-loaded property buffer renders as a placeholder (e.g.
 * {@code #1:0[?]}) instead of the actual property values, so two references to the very same record
 * can render two different strings depending on load state alone, even though they represent one
 * value. Canonicalizing to the identity ({@link RID}) up front, whose own {@code toString()} is just
 * the RID text, sidesteps that instability. See issue #6488. A lightweight edge canonicalizes to
 * {@link LightEdgeRID#distinctKey()} instead: parallel lightweight edges of one type over the same
 * ordered pair share their RID, and are still two relationships (issue #10008).
 * <p>
 * <p>
 * <b>Collections:</b> a list or map canonicalizes to a copy whose elements (map values) are canonicalized, so {@code [1]} and
 * {@code [1.0]} share a key; one holding nothing to change is returned as is. A lazy range is never walked.
 * <p>
 * <b>Arrays:</b> a Java array has the identity {@code equals()} / {@code hashCode()}, so two arrays holding the same values are two
 * keys unless something stands in for them (issue #10380). A BINARY value ({@code byte[]}) is one value, compared by its bytes, and
 * canonicalizes to a {@link BinaryKey}. Any other array ({@code ARRAY_OF_SHORTS}, {@code ARRAY_OF_INTEGERS}, ...) is a sequence,
 * which Cypher's {@code =} compares element by element as it does a list, so it canonicalizes to the list of its canonicalized
 * elements and shares a key with the list of the same numbers. That costs one boxed element per array element and per row, which is
 * the price of the shared key: a {@code DISTINCT} or {@code GROUP BY} over long vectors pays it, and a key made of the primitive array
 * itself would not share a key with the equal list or with the array of another number type, as {@code =} says they are.
 * <p>
 * <b>Temporals:</b> a temporal canonicalizes to the {@code java.time} value {@code =} reads it as, see
 * {@link TemporalUtil#distinctKey(Object)}, so a stored DATETIME ({@code LocalDateTime}) and the {@code datetime()} of the same instant,
 * which {@code =} reports equal, share a key although {@code LocalDateTime.equals(ZonedDateTime)} is false for every pair (issue
 * #10526). A collection that is no list, a {@code Set} reaching a query as a parameter, canonicalizes to the list of its elements in
 * the order it iterates: {@code =} compares collections element by element in that order, so two sets holding the same elements in a
 * different order are two values to {@code =} and two keys here, and a {@code LinkedHashSet} or an immutable set keeps one order.
 * <p>
 * Other values pass through unchanged.
 */
public final class DistinctNumericKey {
  /** 2^53: the largest integer magnitude a double represents exactly. */
  private static final long MAX_EXACT_DOUBLE_INTEGER = 1L << 53;

  private DistinctNumericKey() {
  }

  /**
   * Whether {@link #canonicalize(Object)} may answer something other than {@code value} itself: the values that Java's own
   * {@code equals()} / {@code hashCode()} cannot key, or key apart from what Cypher's {@code =} says. A caller that wraps only
   * the values that need it, to keep the rest as they are, asks here instead of repeating the list.
   */
  public static boolean needsKey(final Object value) {
    return value instanceof Number || value instanceof Identifiable || value instanceof Collection || value instanceof Map
        || value != null && value.getClass().isArray() || TemporalUtil.isTemporalValue(value);
  }

  /**
   * Returns a canonical key for {@code value} suitable for use in a hash-based set/map, or to be
   * rendered with {@code toString()} into a composite string key, so that values which are equal
   * under Cypher's {@code =} operator collapse to the same key regardless of their Java
   * representation or in-memory load state. Values that are neither numeric nor a graph element
   * ({@link Identifiable}) are returned unchanged.
   */
  public static Object canonicalize(final Object value) {
    // The commonest group key, and none of the kinds below
    if (value instanceof String)
      return value;

    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
      return ((Number) value).longValue();

    // A float goes through its decimal form, as Cypher's = reads it (issue #7609): 0.1f and 0.1 are equal there, so they share a key
    if (value instanceof Float f)
      return canonicalDouble(Type.widenFloat(f));
    if (value instanceof Number number)
      return canonicalDouble(number.doubleValue());

    if (value instanceof Identifiable identifiable) {
      final RID rid = identifiable.getIdentity();
      // Two parallel lightweight edges of one type over the same ordered pair share their RID but are two
      // relationships: their key tells them apart, resolving which twin is which only when two of them meet (#10008)
      if (rid instanceof LightEdgeRID light)
        return light.distinctKey();
      if (rid != null)
        return rid;
    }

    // Lists and maps are equal under Cypher's = when their elements are, so [1] and [1.0] share a key (issue #8561). A lazy
    // range is left alone: walking it would materialise what it exists not to, and its elements are all Longs already
    if (value instanceof List<?> list && !(value instanceof LongRangeList)) {
      // Copied only when an element changes: the usual list of strings or already canonical numbers is its own key
      List<Object> key = null;
      for (int i = 0; i < list.size(); i++) {
        final Object element = list.get(i);
        final Object canonical = canonicalize(element);
        if (key == null && !Objects.equals(canonical, element))
          key = new ArrayList<>(list);
        if (key != null)
          key.set(i, canonical);
      }
      return key != null ? key : value;
    }
    // A collection that is no list - a Set a parameter or a function can hand over - is read as a sequence by =, which says it equals
    // the list of the same elements, and is the list of its canonicalized elements here for the same reason (issue #10526)
    if (value instanceof Collection<?> collection && !(value instanceof List)) {
      final List<Object> key = new ArrayList<>(collection.size());
      for (final Object element : collection)
        key.add(canonicalize(element));
      return key;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> key = null;
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        final Object canonical = canonicalize(entry.getValue());
        if (key == null && !Objects.equals(canonical, entry.getValue()))
          key = new LinkedHashMap<>(map);
        if (key != null)
          key.put(entry.getKey(), canonical);
      }
      return key != null ? key : value;
    }

    if (value instanceof byte[] bytes)
      return new BinaryKey(bytes);
    if (value != null && value.getClass().isArray())
      return canonicalizeSequence(value);

    // Temporals last: they are the rarest group key, and what they cost is paid by the values that reach here
    final Object temporal = TemporalUtil.distinctKey(value);
    return temporal != null ? temporal : value;
  }

  /** The canonical form of a number held as a double, see the numeric rule of the class. */
  private static Object canonicalDouble(final double d) {
    if (!Double.isNaN(d) && !Double.isInfinite(d) && d == Math.rint(d) && Math.abs(d) <= MAX_EXACT_DOUBLE_INTEGER)
      return (long) d;
    return d;
  }

  /**
   * The list of the canonicalized elements of an array that is a sequence. The arrays of numbers, which a group key made of a
   * vector or a series can be a long one, are read by their type and not through reflection, which boxes every element once more.
   */
  private static List<Object> canonicalizeSequence(final Object array) {
    final List<Object> key;
    switch (array) {
    case long[] longs -> {
      key = new ArrayList<>(longs.length);
      for (final long element : longs)
        key.add(element);
    }
    case int[] ints -> {
      key = new ArrayList<>(ints.length);
      for (final int element : ints)
        key.add((long) element);
    }
    case short[] shorts -> {
      key = new ArrayList<>(shorts.length);
      for (final short element : shorts)
        key.add((long) element);
    }
    case double[] doubles -> {
      key = new ArrayList<>(doubles.length);
      for (final double element : doubles)
        key.add(canonicalDouble(element));
    }
    case float[] floats -> {
      key = new ArrayList<>(floats.length);
      for (final float element : floats)
        key.add(canonicalDouble(Type.widenFloat(element)));
    }
    default -> {
      final int length = Array.getLength(array);
      key = new ArrayList<>(length);
      for (int i = 0; i < length; i++)
        key.add(canonicalize(Array.get(array, i)));
    }
    }
    return key;
  }

  /**
   * A BINARY value as a key: its bytes decide, as they do for {@code =} and for the order of {@code ORDER BY}, where the identity of
   * the array cannot (issue #10380). Holds the array it was made for, without a copy: a stored value is not changed afterwards.
   */
  private static final class BinaryKey {
    private final byte[] bytes;

    private BinaryKey(final byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public boolean equals(final Object obj) {
      return this == obj || obj instanceof BinaryKey other && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    /** The bytes, not the identity of the array, for a caller that renders a key into text. */
    @Override
    public String toString() {
      return "BINARY[" + HexFormat.of().formatHex(bytes) + "]";
    }
  }

  /**
   * Builds a composite key from {@code names}, in iteration order, as a flat {@code [name, value, name,
   * value, ...]} list of each name paired with its canonicalized value. Shared by every Cypher
   * duplicate-elimination step (RETURN DISTINCT, WITH DISTINCT, UNION) that dedups on a set of named
   * values, so the {@link #canonicalize(Object)} behavior - including the RID canonicalization above -
   * only needs to be applied in one place. Callers control ordering (and hence key stability across
   * rows) by choosing what {@code names} they pass; this method does not sort or otherwise reorder it.
   * <p>
   * The key is a {@link List}, not a delimited string: {@link List#equals(Object)} compares elements
   * structurally, so a value whose canonicalized {@code toString()} happens to contain a delimiter
   * character can never collide with a differently-shaped row the way a rendered {@code name=value|}
   * string could (issue #6540) - e.g. one property {@code a = "1|b=2"} and two properties
   * {@code a=1, b=2} used to both render as {@code "a=1|b=2|"}.
   */
  public static List<Object> buildKey(final Iterable<String> names, final Function<String, Object> valueOf) {
    final List<Object> key = new ArrayList<>();
    for (final String name : names) {
      key.add(name);
      key.add(canonicalize(valueOf.apply(name)));
    }
    return key;
  }
}
