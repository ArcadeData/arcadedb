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
package com.arcadedb.function.sql.coll;

import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.query.sql.executor.MultiValue;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;

/**
 * A {@link Set} implementation that canonicalizes keys using {@link DistinctNumericKey#canonicalize(Object)},
 * ensuring that numerically equal values (such as {@link Long} 1 and {@link Integer} 1, or 1.0 and 1)
 * map to the same set entry while preserving the first-encountered original value for iteration and retrieval.
 * Insertion order is preserved via {@link LinkedHashMap}.
 *
 * @author ArcadeData team
 */
public class CanonicalSet extends AbstractSet<Object> {
  private final Map<Object, Object> map;

  public CanonicalSet() {
    this.map = new LinkedHashMap<>();
  }

  public CanonicalSet(final int initialCapacity) {
    this.map = new LinkedHashMap<>(initialCapacity);
  }

  public CanonicalSet(final Collection<?> c) {
    this.map = new LinkedHashMap<>(c != null ? c.size() : 16);
    if (c != null)
      addAll(c);
  }

  /**
   * Converts {@code value} into a {@link CanonicalSet}. If {@code value} is already a {@link CanonicalSet},
   * returns it directly; otherwise unpacks multi-values and adds each element.
   */
  public static CanonicalSet from(final Object value) {
    if (value instanceof CanonicalSet canonicalSet)
      return canonicalSet;
    final CanonicalSet set = new CanonicalSet();
    if (value != null) {
      if (value instanceof Iterator<?> iterator) {
        while (iterator.hasNext())
          set.add(iterator.next());
      } else if (MultiValue.isMultiValue(value) && !(value instanceof Map<?, ?>)) {
        for (final Object o : MultiValue.getMultiValueIterable(value, false))
          set.add(o);
      } else {
        set.add(value);
      }
    }
    return set;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean contains(final Object o) {
    return map.containsKey(DistinctNumericKey.canonicalize(o));
  }

  @Override
  public boolean add(final Object o) {
    final Object key = DistinctNumericKey.canonicalize(o);
    if (map.containsKey(key))
      return false;
    map.put(key, o);
    return true;
  }

  @Override
  public boolean remove(final Object o) {
    final Object key = DistinctNumericKey.canonicalize(o);
    final boolean present = map.containsKey(key);
    map.remove(key);
    return present;
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    Objects.requireNonNull(c);
    boolean modified = false;
    for (final Object e : c)
      modified |= remove(e);
    return modified;
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    Objects.requireNonNull(c);
    final CanonicalSet retainKeys = from(c);
    final Iterator<Map.Entry<Object, Object>> it = map.entrySet().iterator();
    boolean modified = false;
    while (it.hasNext()) {
      final Map.Entry<Object, Object> entry = it.next();
      if (!retainKeys.map.containsKey(entry.getKey())) {
        it.remove();
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Iterator<Object> iterator() {
    return map.values().iterator();
  }

  @Override
  public Spliterator<Object> spliterator() {
    return map.values().spliterator();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this)
      return true;
    if (!(o instanceof Set<?> set))
      return false;
    if (set.size() != size())
      return false;
    if (set instanceof CanonicalSet other)
      return map.keySet().equals(other.map.keySet());
    return containsAll(set) && set.containsAll(this);
  }

  @Override
  public int hashCode() {
    int h = 0;
    for (final Object key : map.keySet()) {
      if (key != null)
        h += key.hashCode();
    }
    return h;
  }
}
