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
package com.arcadedb.graph;

import com.arcadedb.database.Document;
import com.arcadedb.utility.MultiIterator;

import java.util.*;

/**
 * Iterable implementation with utility methods to browse and count elements.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 */
public interface IterableGraph<T> extends Iterable<T> {
  Class<? extends Document> getEntryType();

  default T getFirstOrNull() {
    if (this instanceof List<?> list)
      return list.isEmpty() ? null : (T) list.getFirst();

    for (final T item : this)
      return (T) item;
    return null;
  }

  default List<T> toList() {
    final List<T> result = new ArrayList<>();
    for (final T item : this) {
      result.add(item);
    }
    return result;
  }

  default int size() {
    if (this instanceof Collection<?> coll)
      return coll.size();
    else if (this instanceof MultiIterator<T> it)
      return (int) it.countEntries();

    int count = 0;
    for (final T item : this)
      count++;
    return count;
  }
}
