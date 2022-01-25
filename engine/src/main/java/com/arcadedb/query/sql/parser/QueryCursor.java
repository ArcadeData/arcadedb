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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by luigidellaquila on 02/10/15.
 */
public class QueryCursor implements Iterator<Identifiable> {
  private int                    limit;
  private int                    skip;
  private WhereClause            filter;
  private Iterator<Identifiable> iterator;
  private OrderBy                orderBy;
  private CommandContext         ctx;

  private Identifiable next         = null;
  private long         countFetched = 0;

  public QueryCursor() {

  }

  public QueryCursor(Iterator<Identifiable> PIdentifiableIterator, WhereClause filter, OrderBy orderBy, int skip, int limit, CommandContext ctx) {
    this.iterator = PIdentifiableIterator;
    this.filter = filter;
    this.skip = skip;
    this.limit = limit;
    this.orderBy = orderBy;
    this.ctx = ctx;
    loadNext();
  }

  private void loadNext() {
    if (iterator == null) {
      next = null;
      return;
    }
    if (limit > 0 && countFetched >= limit) {
      next = null;
      return;
    }
    if (countFetched == 0 && skip > 0) {
      for (int i = 0; i < skip; i++) {
        next = getNextFromIterator();
        if (next == null) {
          return;
        }
      }
    }
    next = getNextFromIterator();
    countFetched++;
  }

  private Identifiable getNextFromIterator() {
    while (true) {
      if (iterator == null || !iterator.hasNext()) {
        return null;
      }

      Identifiable result = iterator.next();
      if (filter == null || filter.matchesFilters(result, ctx)) {
        return result;
      }
    }
  }

  public boolean hasNext() {
    return next != null;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  public Identifiable next() {
    Identifiable result = next;
    if (result == null) {
      throw new NoSuchElementException();
    }
    loadNext();
    return result;
  }
}
