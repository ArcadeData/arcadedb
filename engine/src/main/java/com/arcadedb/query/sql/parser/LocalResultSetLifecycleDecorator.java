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

import com.arcadedb.query.sql.executor.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by luigidellaquila on 05/12/16.
 */
public class LocalResultSetLifecycleDecorator implements ResultSet {

  private static final AtomicLong counter = new AtomicLong(0);

  private final ResultSet                    entity;
  private final List<QueryLifecycleListener> lifecycleListeners = new ArrayList<>();
  private final String                       queryId;

  private boolean hasNextPage;

  public LocalResultSetLifecycleDecorator(ResultSet entity) {
    this.entity = entity;
    queryId = "" + System.currentTimeMillis() + "_" + counter.incrementAndGet();
  }

  public LocalResultSetLifecycleDecorator(InternalResultSet rsCopy, String queryId) {
    this.entity = rsCopy;
    this.queryId = queryId;
  }

  public void addLifecycleListener(QueryLifecycleListener db) {
    this.lifecycleListeners.add(db);
  }

  @Override
  public boolean hasNext() {
    return entity.hasNext();
  }

  @Override
  public Result next() {
    return entity.next();
  }

  @Override
  public void close() {
    entity.close();
    this.lifecycleListeners.forEach(x -> x.queryClosed(this.getQueryId()));
    this.lifecycleListeners.clear();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return entity.getExecutionPlan();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return entity.getQueryStats();
  }

  public String getQueryId() {
    return queryId;
  }

  public boolean hasNextPage() {
    return hasNextPage;
  }

  public void setHasNextPage(boolean b) {
    this.hasNextPage = b;
  }

  public boolean isDetached() {
    return entity instanceof InternalResultSet;
  }
}
