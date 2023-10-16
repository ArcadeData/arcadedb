package com.arcadedb.query.select;/*
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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.util.concurrent.*;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectWhereAfterFirstBlock {
  private final Select select;

  public SelectWhereAfterFirstBlock(final Select select) {
    this.select = select;
  }

  public SelectWhereLeftBlock and() {
    return select.setLogic(SelectOperator.and);
  }

  public SelectWhereLeftBlock or() {
    return select.setLogic(SelectOperator.or);
  }

  public SelectIterator<Vertex> vertices() {
    return select.run();
  }

  public SelectIterator<Edge> edges() {
    return select.run();
  }

  public SelectIterator<Document> documents() {
    return select.run();
  }

  public SelectCompiled compile() {
    return select.compile();
  }

  public Select limit(final int limit) {
    return select.limit(limit);
  }

  public Select skip(final int skip) {
    return select.skip(skip);
  }

  public Select timeout(final long timeoutValue, final TimeUnit timeoutUnit, final boolean exceptionOnTimeout) {
    return select.timeout(timeoutValue, timeoutUnit, exceptionOnTimeout);
  }

  public Select orderBy(final String property, final boolean order) {
    return select.orderBy(property, order);
  }
}
