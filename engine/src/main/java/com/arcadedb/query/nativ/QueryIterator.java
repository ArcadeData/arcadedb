package com.arcadedb.query.nativ;/*
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
 */

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;

import java.util.*;

/**
 * Query iterator returned from queries. Extends the base Java iterator with convenient methods.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryIterator<T extends Identifiable> implements Iterator<T> {
  private final NativeSelectExecutor   executor;
  private final Iterator<Identifiable> iterator;
  private       HashSet<RID>           filterOutRecords;
  private       T                      next     = null;
  private       long                   returned = 0;

  protected QueryIterator(final NativeSelectExecutor executor, final Iterator<Identifiable> iterator, final boolean uniqueResult) {
    this.executor = executor;
    this.iterator = iterator;
    if (uniqueResult)
      this.filterOutRecords = new HashSet<>();
  }

  @Override
  public boolean hasNext() {
    if (executor.select.limit > -1 && returned >= executor.select.limit)
      return false;
    if (next != null)
      return true;
    if (!iterator.hasNext())
      return false;

    next = fetchNext();
    return next != null;
  }

  @Override
  public T next() {
    if (next == null && !hasNext())
      throw new NoSuchElementException();
    try {
      return next;
    } finally {
      next = null;
    }
  }

  private T fetchNext() {
    do {
      final Document record = iterator.next().asDocument();

      if (filterOutRecords != null && filterOutRecords.contains(record.getIdentity()))
        // ALREADY RETURNED, AVOID DUPLICATES IN THE RESULTSET
        continue;

      if (executor.evaluateWhere(record)) {
        ++returned;

        if (filterOutRecords != null)
          filterOutRecords.add(record.getIdentity());

        return (T) record;
      }

    } while (iterator.hasNext());

    // NOT FOUND
    return null;
  }

  public T nextOrNull() {
    return hasNext() ? next() : null;
  }

  public List<T> toList() {
    final List<T> result = new ArrayList<>();
    while (hasNext())
      result.add(next());
    return result;
  }

  public Map<String, Object> getMetrics() {
    return executor.metrics();
  }
}
