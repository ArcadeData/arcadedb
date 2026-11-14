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

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A set whose membership is keyed on {@link DistinctNumericKey#canonicalize}, so values that compare equal although their
 * Java classes differ ({@code 1} as an {@code Integer} and as a {@code Long}, {@code [1]} and {@code [1L]}) are one element.
 * The first value added under a key is the one kept and iterated. The SQL set functions ({@code set()},
 * {@code intersect()}, {@code difference()}, {@code symmetricDifference()}) use it so a list written by openCypher, whose
 * integers are {@code Long}, and one written by SQL, whose integers are {@code Integer}, hold the same items (issue #11581).
 */
public final class DistinctValueSet extends AbstractSet<Object> {
  private final HashMap<Object, Object> values = new HashMap<>();

  private static Object keyOf(final Object value) {
    return value == null ? null : DistinctNumericKey.canonicalize(value);
  }

  @Override
  public boolean add(final Object value) {
    final Object key = keyOf(value);
    if (values.containsKey(key))
      return false;
    values.put(key, value);
    return true;
  }

  @Override
  public boolean contains(final Object value) {
    return values.containsKey(keyOf(value));
  }

  @Override
  public boolean remove(final Object value) {
    final Object key = keyOf(value);
    if (!values.containsKey(key))
      return false;
    values.remove(key);
    return true;
  }

  /**
   * Removes each element of {@code collection} by its key. {@link AbstractSet#removeAll} may instead ask
   * {@code collection.contains()}, which compares by the argument's own {@code equals()}.
   */
  @Override
  public boolean removeAll(final Collection<?> collection) {
    boolean modified = false;
    for (final Object value : collection)
      modified |= remove(value);
    return modified;
  }

  @Override
  public Iterator<Object> iterator() {
    return values.values().iterator();
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public void clear() {
    values.clear();
  }
}
