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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Result set returned from queries. This class implements can be used as an Iterator of Result.
 */
public interface ResultSet extends Spliterator<Result>, Iterator<Result>, AutoCloseable {
  EmptyResult EMPTY_RESULT = new EmptyResult();

  @Override
  boolean hasNext();

  @Override
  Result next();

  default void remove() {
    throw new UnsupportedOperationException();
  }

  default void close() {
    // NO ACTIONS
  }

  /**
   * Returns the first element of the resultset if any, otherwise an empty Result object. This allows to write code without null check. Example:<br>
   * <code>
   * int updated = result.first().getProperty("count", 0);
   * </code>
   */
  default Result nextIfAvailable() {
    return hasNext() ? next() : EMPTY_RESULT;
  }

  default Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  default Map<String, Long> getQueryStats() {
    return null;
  }

  default void reset() {
    throw new UnsupportedOperationException("Implement RESET on " + getClass().getSimpleName());
  }

  default boolean tryAdvance(final Consumer<? super Result> action) {
    if (hasNext()) {
      action.accept(next());
      return true;
    }
    return false;
  }

  default void forEachRemaining(final Consumer<? super Result> action) {
    Spliterator.super.forEachRemaining(action);
  }

  default ResultSet trySplit() {
    return null;
  }

  default long estimateSize() {
    return Long.MAX_VALUE;
  }

  default int characteristics() {
    return ORDERED;
  }

  /**
   * Returns the result set as a stream. IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<Result> stream() {
    return StreamSupport.stream(this, false);
  }

  /**
   * Returns the result set as a stream of elements (filters only the results that are elements - where the isElement() method
   * returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */

  default Stream<Record> elementStream() {
    return StreamSupport.stream(new Spliterator<>() {
      @Override
      public boolean tryAdvance(final Consumer<? super Record> action) {
        while (hasNext()) {
          final Result elem = next();
          if (elem.isElement()) {
            action.accept(elem.getElement().get());
            return true;
          }
        }
        return false;
      }

      @Override
      public Spliterator<Record> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return ORDERED;
      }
    }, false);
  }

  /**
   * Returns the result set as a stream of vertices (filters only the results that are vertices - where the isVertex() method
   * returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */

  default Stream<Vertex> vertexStream() {
    return StreamSupport.stream(new Spliterator<>() {
      @Override
      public boolean tryAdvance(final Consumer<? super Vertex> action) {
        while (hasNext()) {
          final Result elem = next();
          if (elem.isVertex()) {
            action.accept(elem.getVertex().get());
            return true;
          }
        }
        return false;
      }

      @Override
      public Spliterator<Vertex> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return ORDERED;
      }
    }, false);
  }

  /**
   * Returns the result set as a stream of vertices (filters only the results that are edges - where the isEdge() method
   * returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */

  default Stream<Edge> edgeStream() {
    return StreamSupport.stream(new Spliterator<>() {
      @Override
      public boolean tryAdvance(final Consumer<? super Edge> action) {
        while (hasNext()) {
          final Result nextElem = next();
          if (nextElem != null && nextElem.isEdge()) {
            action.accept(nextElem.getEdge().get());
            return true;
          }
        }
        return false;
      }

      @Override
      public Spliterator<Edge> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return ORDERED;
      }
    }, false);
  }

  /**
   * Creates a copy of the resultset. If the resultset is not resettable, the iteration will be completely consumed.
   */
  default ResultSet copy(){
    return new InternalResultSet(this);
  }
}
