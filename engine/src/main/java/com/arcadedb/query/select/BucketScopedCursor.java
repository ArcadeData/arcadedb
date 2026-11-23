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
package com.arcadedb.query.select;

import com.arcadedb.database.Identifiable;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.utility.IntHashSet;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * #12701: the entries of an index cursor that belong to the buckets of a select's type scope, nothing else. An index declared on a
 * supertype holds a sub-index per bucket of the whole hierarchy, so the cursor of a select on a subtype (or a non-polymorphic one)
 * also delivers the records of the sibling types and of the subtypes. They are dropped here, by the bucket of their RID and
 * before the record is loaded, so that skip, limit, order by, count and exists of the select all see the scope's records only.
 * <p>
 * The run of entries discarded by one {@link #hasNext()} has no bound of its own: a range over a small subtype under a large
 * supertype index skips every sibling entry in between, and the consumers check the select's deadline only once this cursor
 * hands them a record. So the deadline is checked here for every discarded entry: an expired one ends the stream (a select
 * that asked not to be thrown at keeps what it has) or throws, as {@link SelectExecutor#checkForTimeout()} decides.
 */
final class BucketScopedCursor implements Iterator<Identifiable>, AutoCloseable {
  private final MultiIndexCursor source;
  private final IntHashSet       bucketIds;
  private final SelectExecutor   executor;
  private       Identifiable     pending;

  BucketScopedCursor(final MultiIndexCursor source, final IntHashSet bucketIds, final SelectExecutor executor) {
    this.source = source;
    this.bucketIds = bucketIds;
    this.executor = executor;
  }

  @Override
  public boolean hasNext() {
    while (pending == null) {
      if (!source.hasNext())
        return false;
      final Identifiable candidate = source.next();
      if (candidate != null && bucketIds.contains(candidate.getIdentity().getBucketId()))
        pending = candidate;
      else if (executor.checkForTimeout())
        return false;
    }
    return true;
  }

  @Override
  public Identifiable next() {
    if (!hasNext())
      throw new NoSuchElementException();
    final Identifiable result = pending;
    pending = null;
    return result;
  }

  @Override
  public void close() {
    pending = null;
    source.close();
  }
}
