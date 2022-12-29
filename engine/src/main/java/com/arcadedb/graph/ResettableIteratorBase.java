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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.ResettableIterator;

import java.util.concurrent.atomic.*;

public abstract class ResettableIteratorBase<T> implements ResettableIterator<T>, Iterable<T> {
  protected final DatabaseInternal database;
  private         EdgeSegment      initialContainer;
  protected       EdgeSegment      currentContainer;
  protected final AtomicInteger    currentPosition = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);

  protected ResettableIteratorBase(final DatabaseInternal database, final EdgeSegment current) {
    if (current == null)
      throw new IllegalArgumentException("Edge chunk is null");
    this.database = database;
    this.initialContainer = current;
    this.currentContainer = current;
  }

  @Override
  public void reset() {
    this.currentContainer = initialContainer;
    currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
  }

  @Override
  public int countEntries() {
    int total = 0;
    while (hasNext()) {
      next();
      ++total;
    }
    reset();
    return total;
  }
}
