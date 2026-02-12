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
package com.arcadedb.utility;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Lightweight immutable map with exactly one entry.
 * Optimized for common cases like duration({seconds: value}) to avoid LinkedHashMap overhead.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SingletonMap<K, V> extends AbstractMap<K, V> {
  private final     K                    key;
  private final     V                    value;
  private transient Set<Map.Entry<K, V>> entrySet;

  public SingletonMap(final K key, final V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean containsKey(final Object key) {
    return this.key == null ? key == null : this.key.equals(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return this.value == null ? value == null : this.value.equals(value);
  }

  @Override
  public V get(final Object key) {
    return containsKey(key) ? value : null;
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    if (entrySet == null) {
      entrySet = new AbstractSet<Map.Entry<K, V>>() {
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
          return new Iterator<Map.Entry<K, V>>() {
            private boolean hasNext = true;

            @Override
            public boolean hasNext() {
              return hasNext;
            }

            @Override
            public Map.Entry<K, V> next() {
              if (!hasNext)
                throw new NoSuchElementException();
              hasNext = false;
              return new AbstractMap.SimpleImmutableEntry<>(key, value);
            }
          };
        }

        @Override
        public int size() {
          return 1;
        }
      };
    }
    return entrySet;
  }

  @Override
  public int hashCode() {
    return key.hashCode() ^ value.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Map))
      return false;
    final Map<?, ?> other = (Map<?, ?>) o;
    if (other.size() != 1)
      return false;
    final Map.Entry<?, ?> entry = other.entrySet().iterator().next();
    return (key == null ? entry.getKey() == null : key.equals(entry.getKey())) &&
        (value == null ? entry.getValue() == null : value.equals(entry.getValue()));
  }

  @Override
  public String toString() {
    return "{" + key + "=" + value + "}";
  }
}
