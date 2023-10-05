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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.MultiIterator;

import java.util.*;
import java.util.stream.*;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeSelectExecutor {
  private final NativeSelect select;

  public NativeSelectExecutor(final NativeSelect select) {
    this.select = select;
  }

  <T extends Document> QueryIterator<T> execute() {
    final int[] returned = new int[] { 0 };
    final Iterator<Record> iterator;

    if (select.fromType != null)
      iterator = select.database.iterateType(select.fromType.getName(), select.polymorphic);
    else if (select.fromBuckets.size() == 1)
      iterator = select.database.iterateBucket(select.fromBuckets.get(0).getName());
    else {
      final MultiIterator multiIterator = new MultiIterator<>();
      for (Bucket b : select.fromBuckets)
        multiIterator.addIterator(b.iterator());
      iterator = multiIterator;
    }

    return new QueryIterator<>() {
      private T next = null;

      @Override
      public boolean hasNext() {
        if (select.limit > -1 && returned[0] >= select.limit)
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
          if (evaluateWhere(record)) {
            ++returned[0];
            return (T) record;
          }

        } while (iterator.hasNext());

        // NOT FOUND
        return null;
      }
    };
  }

  private boolean evaluateWhere(final Document record) {
    final Object result = select.rootTreeElement.eval(record);
    if (result instanceof Boolean)
      return (Boolean) result;
    throw new IllegalArgumentException("A boolean result was expected but '" + result + "' was returned");
  }
}
