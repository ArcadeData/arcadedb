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

import com.arcadedb.exception.TimeoutException;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Iterator that allow to iterate against multiple collection of elements.
 */
public class MultiIterator<T> implements Iterator<T>, Iterable<T> {
  private List<Object> sources;
  private Iterator<?>  sourcesIterator;
  private Iterator<T>  partialIterator;

  private long    browsed   = 0L;
  private long    skip      = -1L;
  private long    limit     = -1L;
  private long    timeout   = -1L;
  private boolean embedded  = false;
  private int     skipped   = 0;
  private final long    beginTime = System.currentTimeMillis();

  public MultiIterator() {
    sources = new ArrayList<>();
  }

  public MultiIterator(final Iterator<? extends Collection<?>> iterator) {
    sourcesIterator = iterator;
    getNextPartial();
  }

  @Override
  public boolean hasNext() {
    if (timeout > -1L && System.currentTimeMillis() - beginTime > timeout)
      throw new TimeoutException("Timeout on iteration");

    while (skipped < skip) {
      if (!hasNextInternal()) {
        return false;
      }
      partialIterator.next();
      skipped++;
    }
    return hasNextInternal();
  }

  private boolean hasNextInternal() {
    if (timeout > -1L && System.currentTimeMillis() - beginTime > timeout)
      throw new TimeoutException("Timeout on iteration");

    if (sourcesIterator == null) {
      if (sources == null || sources.isEmpty())
        return false;

      // THE FIRST TIME CREATE THE ITERATOR
      sourcesIterator = sources.iterator();
      getNextPartial();
    }

    if (partialIterator == null)
      return false;

    if (limit > -1 && browsed >= limit)
      return false;

    if (partialIterator.hasNext())
      return true;
    else if (sourcesIterator.hasNext())
      return getNextPartial();

    return false;
  }

  @Override
  public T next() {
    if (!hasNext())
      throw new NoSuchElementException();

    browsed++;
    return partialIterator.next();
  }

  @Override
  public Iterator<T> iterator() {
    reset();
    return this;
  }

  public void reset() {
    sourcesIterator = null;
    partialIterator = null;
    browsed = 0;
    skipped = 0;
  }

  public MultiIterator<T> addIterator(final Object iValue) {
    if (iValue != null) {
      if (sourcesIterator != null)
        throw new IllegalStateException("MultiCollection iterator is in use and new collections cannot be added");
      sources.add(iValue);
    }
    return this;
  }

  public int countEntries() {
    // SUM ALL THE COLLECTION SIZES
    int size = 0;
    final int totSources = sources.size();
    for (int i = 0; i < totSources; ++i) {
      if (timeout > -1L && System.currentTimeMillis() - beginTime > timeout)
        throw new TimeoutException("Timeout on iteration");

      final Object o = sources.get(i);

      if (o != null)
        if (o instanceof Collection<?>)
          size += ((Collection<?>) o).size();
        else if (o instanceof Map<?, ?>)
          size += ((Map<?, ?>) o).size();
        else if (o.getClass().isArray())
          size += Array.getLength(o);
        else
          size++;
    }
    return size;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("PMultiIterator.remove()");
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(final long limit) {
    this.limit = limit;
  }

  public void setTimeout(long readTimeout) {
    this.timeout = readTimeout;
  }

  public long getSkip() {
    return skip;
  }

  public void setSkip(final long skip) {
    this.skip = skip;
  }

  public boolean contains(final Object value) {
    final int totSources = sources.size();
    for (int i = 0; i < totSources; ++i) {
      Object o = sources.get(i);

      if (o != null) {
        if (o instanceof Collection<?>) {
          if (((Collection<?>) o).contains(value))
            return true;
        }
      }
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  protected boolean getNextPartial() {
    if (timeout > -1L && System.currentTimeMillis() - beginTime > timeout)
      throw new TimeoutException("Timeout on iteration");

    if (sourcesIterator != null)
      while (sourcesIterator.hasNext()) {
        Object next = sourcesIterator.next();
        if (next != null) {

          if (next instanceof Iterable<?>)
            next = ((Iterable) next).iterator();

          if (next instanceof Iterator<?>) {
            if (((Iterator<T>) next).hasNext()) {
              partialIterator = (Iterator<T>) next;
              return true;
            }
          } else if (next instanceof Collection<?>) {
            if (!((Collection<T>) next).isEmpty()) {
              partialIterator = ((Collection<T>) next).iterator();
              return true;
            }
          } else if (next.getClass().isArray()) {
            final int arraySize = Array.getLength(next);
            if (arraySize > 0) {
              if (arraySize == 1)
                partialIterator = new IterableObject<>((T) Array.get(next, 0));
              else
                partialIterator = new IterableObjectArray<T>(next).iterator();
              return true;
            }
          } else {
            partialIterator = new IterableObject<>((T) next);
            return true;
          }
        }
      }

    return false;
  }

  public boolean isEmbedded() {
    return embedded;
  }

  public MultiIterator<T> setEmbedded(final boolean embedded) {
    this.embedded = embedded;
    return this;
  }

  @Override
  public String toString() {
    return "[" + countEntries() + "]";
  }
}
