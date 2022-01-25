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

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Allow to iterate over the array casted to Object.
 **/
public class IterableObjectArray<T> implements Iterable<T> {

  private final Object object;
  private final int    length;

  public IterableObjectArray(Object o) {
    object = o;
    length = Array.getLength(o);
  }

  /**
   * Returns an iterator over a set of elements of type T.
   *
   * @return an Iterator.
   */
  public Iterator<T> iterator() {
    return new ObjIterator();
  }

  private class ObjIterator implements Iterator<T> {
    private int p = 0;

    /**
     * Returns <tt>true</tt> if the iteration has more elements. (In other words, returns <tt>true</tt> if <tt>next</tt> would
     * return an element rather than throwing an exception.)
     *
     * @return <tt>true</tt> if the iterator has more elements.
     */
    public boolean hasNext() {
      return p < length;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration.
     *
     * @throws NoSuchElementException iteration has no more elements.
     */
    @SuppressWarnings("unchecked")
    public T next() {
      if (p < length) {
        return (T) Array.get(object, p++);
      } else {
        throw new NoSuchElementException();
      }
    }

    /**
     * Removes from the underlying collection the last element returned by the iterator (optional operation). This method can be
     * called only once per call to <tt>next</tt>. The behavior of an iterator is unspecified if the underlying collection is
     * modified while the iteration is in progress in any way other than by calling this method.
     *
     * @throws UnsupportedOperationException if the <tt>remove</tt> operation is not supported by this Iterator.
     * @throws IllegalStateException         if the <tt>next</tt> method has not yet been called, or the <tt>remove</tt> method has
     *                                       already been called after the last call to the <tt>next</tt> method.
     */
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
