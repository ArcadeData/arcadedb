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

import com.arcadedb.utility.ResettableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class InternalResultSet implements ResultSet, ResettableIterator<Result> {
  private   List<Result>  content = new ArrayList<>();
  private   int           next    = 0;
  protected ExecutionPlan plan;

  public InternalResultSet() {
  }

  public InternalResultSet(final ResultInternal result) {
    add(result);
  }

  /**
   * Copy constructor.
   */
  public InternalResultSet(final ResultSet resultSet) {
    while (resultSet.hasNext())
      add(resultSet.next());
  }

  @Override
  public boolean hasNext() {
    return content.size() > next;
  }

  @Override
  public Result next() {
    if (!hasNext())
      throw new NoSuchElementException();
    return content.get(next++);
  }

  @Override
  public void close() {
    this.content.clear();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.ofNullable(plan);
  }

  public void setPlan(final ExecutionPlan plan) {
    this.plan = plan;
  }

  public InternalResultSet add(final Result nextResult) {
    content.add(nextResult);
    return this;
  }

  public void addAll(final List<ResultInternal> list) {
    content.addAll(list);
  }

  @Override
  public void reset() {
    this.next = 0;
  }

  @Override
  public long countEntries() {
    return content.size();
  }

  @Override
  public long getBrowsed() {
    return next;
  }

  @Override
  public long estimateSize() {
    return content.size();
  }

  @Override
  public InternalResultSet copy() {
    final InternalResultSet copy = new InternalResultSet();
    copy.content = this.content;
    return copy;
  }

  @Override
  public String toString() {
    return content.toString();
  }
}
