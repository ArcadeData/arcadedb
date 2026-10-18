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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.function.DistinctNumericKey;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Represents the values of one or more grouping keys, used as a {@code HashMap} key by the
 * aggregation steps (issue #6629: a row-multiplying clause upstream of a GROUP BY, such as
 * UNWIND, must still collapse into one output row per distinct key combination). Caches its
 * hash code since the same instance is hashed on every row of its group.
 * <p>
 * Equality/hashing is computed on {@link DistinctNumericKey#canonicalize(Object)} of each value rather than on the
 * raw boxed value, so that the same logical number represented with different Java numeric types (e.g. Integer 1 vs
 * Long 1 vs Double 1.0) groups together instead of splitting, matching Cypher's own {@code =} operator and the
 * sibling DISTINCT paths that already canonicalize this way (issue #6676, following #5789). {@link #values} itself
 * stays the raw, first-seen values, since those - not the canonical form - are what the group's output row reports.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GroupKeyValues {
  final Object[] values;
  private final Object[] canonicalValues;
  private final int      hash;

  GroupKeyValues(final Object[] values) {
    this.values = values;
    this.canonicalValues = new Object[values.length];
    int h = 1;
    for (int i = 0; i < values.length; i++) {
      final Object canonical = DistinctNumericKey.canonicalize(values[i]);
      canonicalValues[i] = canonical;
      h = 31 * h + (canonical == null ? 0 : canonical.hashCode());
    }
    this.hash = h;
  }

  private GroupKeyValues(final Object[] values, final Object[] canonicalValues, final int hash) {
    this.values = values;
    this.canonicalValues = canonicalValues;
    this.hash = hash;
  }

  /**
   * Builds the keys of consecutive rows, on one thread. Canonicalizing and hashing a value costs as much as the value is
   * long, and a list carried next to an aggregate ({@code WITH r1, collect(DISTINCT b) AS r2}) is the very same object on
   * every row, built once by the {@code collect()} upstream: walking it again per row made the grouping quadratic (issue
   * #10254). A value that is the object the row before held reuses that row's canonical form and hash, and the canonical
   * form being the same object too, the key equals the group's own in one reference comparison per value.
   * <p>
   * The memory is by reference, which holds because a value a row carries is not changed afterwards: a list or a map is
   * built by the clause that produces it (a {@code collect()} hands over its list when the group is complete, a literal or a
   * parameter is never written to), so the same object is the same value on every row it reaches.
   */
  static final class Builder {
    private static final Object   NONE = new Object();
    private final        Object[] lastValues;
    private final        Object[] lastCanonical;
    private final        int[]    lastHash;

    Builder(final int size) {
      lastValues = new Object[size];
      lastCanonical = new Object[size];
      lastHash = new int[size];
      Arrays.fill(lastValues, NONE);
    }

    /** Whether every value is the very object the last key built held: the row is that key's group. */
    boolean sameAsLast(final Object[] values) {
      if (values.length != lastValues.length)
        return false;
      for (int i = 0; i < values.length; i++)
        if (values[i] != lastValues[i])
          return false;
      return true;
    }

    /**
     * The canonical form of the value of a single key, the one the row before computed when it is the same object. A builder
     * serves either this or {@link #build(Object[])}, never both: this one keeps no hash, which a single key leaves to the map.
     */
    Object canonical(final Object value) {
      if (value != lastValues[0]) {
        lastValues[0] = value;
        lastCanonical[0] = DistinctNumericKey.canonicalize(value);
      }
      return lastCanonical[0];
    }

    /**
     * The key of the named values of a row, as {@code [name, value, name, value, ...]} in the order of {@code names}: the
     * key of a DISTINCT, where a list carried on every row costs the same per row as in a grouping (issue #10254).
     */
    GroupKeyValues build(final List<String> names, final Function<String, Object> valueOf) {
      final Object[] values = new Object[names.size() * 2];
      int i = 0;
      for (final String name : names) {
        values[i++] = name;
        values[i++] = valueOf.apply(name);
      }
      return build(values);
    }

    GroupKeyValues build(final Object[] values) {
      // A ROW OF ANOTHER SHAPE THAN THE MEMORY (A DISTINCT * OVER ROWS WITH DIFFERENT NAMES) IS KEYED WITHOUT IT
      if (values.length != lastValues.length)
        return new GroupKeyValues(values);
      final Object[] canonical = new Object[values.length];
      int h = 1;
      for (int i = 0; i < values.length; i++) {
        if (values[i] != lastValues[i]) {
          final Object c = DistinctNumericKey.canonicalize(values[i]);
          lastValues[i] = values[i];
          lastCanonical[i] = c;
          lastHash[i] = c == null ? 0 : c.hashCode();
        }
        canonical[i] = lastCanonical[i];
        h = 31 * h + lastHash[i];
      }
      return new GroupKeyValues(values, canonical, h);
    }
  }

  /** What the key holds on to besides the raw values, for the heap accounting of a set of keys. */
  Object[] canonicalValues() {
    return canonicalValues;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof GroupKeyValues that))
      return false;
    if (hash != that.hash || canonicalValues.length != that.canonicalValues.length)
      return false;
    for (int i = 0; i < canonicalValues.length; i++)
      if (!Objects.equals(canonicalValues[i], that.canonicalValues[i]))
        return false;
    return true;
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
