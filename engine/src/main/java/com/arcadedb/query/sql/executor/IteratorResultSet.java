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

import com.arcadedb.database.Document;

import java.util.Iterator;
import java.util.Optional;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class IteratorResultSet implements ResultSet {
  protected final Iterator iterator;
  protected QueryStatistics statistics;

  public IteratorResultSet(final Iterator iter) {
    this.iterator = iter;
  }

  public void setStatistics(final QueryStatistics statistics) {
    this.statistics = statistics;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Result next() {
    final Object val = iterator.next();
    if (val instanceof Result result)
      return result;

    return val instanceof Document d ?
        new ResultInternal(d) :
        new ResultInternal().setProperty("value", val);
  }

  @Override
  public Optional<QueryStatistics> getStatistics() {
    return Optional.ofNullable(statistics);
  }
}
