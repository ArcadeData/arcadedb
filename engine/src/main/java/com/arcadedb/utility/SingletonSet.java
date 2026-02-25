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

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Lightweight immutable set with exactly one element.
 * Avoids the overhead of Set.of() which creates a wrapper object.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SingletonSet<E> extends AbstractSet<E> {
  private final E element;

  public SingletonSet(final E element) {
    this.element = element;
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
  public boolean contains(final Object o) {
    return element == null ? o == null : element.equals(o);
  }

  @Override
  public Iterator<E> iterator() {
    return new Iterator<>() {
      private boolean hasNext = true;

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public E next() {
        if (!hasNext)
          throw new NoSuchElementException();
        hasNext = false;
        return element;
      }
    };
  }

  @Override
  public int hashCode() {
    return element == null ? 0 : element.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Set<?> other))
      return false;
    if (other.size() != 1)
      return false;
    return contains(other.iterator().next());
  }

  @Override
  public String toString() {
    return "[" + element + "]";
  }
}
