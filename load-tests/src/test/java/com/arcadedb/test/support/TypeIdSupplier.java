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
package com.arcadedb.test.support;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Supplier of existing ids for a given vertex/document type. Fetches ids from the server in
 * batches via SELECT ORDER BY id SKIP ? LIMIT ?. When the end of the type is reached, the
 * supplier cycles back to skip=0 so callers that consume more ids than the type holds (e.g. a
 * createFriendships loop that requests 1000 edges from 500 users) keep getting valid ids rather
 * than running forever on empty SELECTs.
 * <p>
 * Prior behavior returned {@code null} once the type was exhausted, which combined with the
 * {@code continue} in {@link DatabaseWrapper#createFriendships} and
 * {@link DatabaseWrapper#createLike} produced an infinite loop of empty-batch SELECTs that
 * looked like an HTTP client hang. This cycling behavior is the intended read-pick-write
 * traffic pattern for a mixed workload and matches how a real consumer would use a random
 * subset of existing ids.
 */
public class TypeIdSupplier implements Supplier<Integer> {

  private final RemoteDatabase    db;
  private final String            query;
  private final int               batchSize;
  private       Iterator<Integer> idsIt;
  private       int               skip;
  private       boolean           exhaustedOnce;

  public TypeIdSupplier(RemoteDatabase db, String type) {
    this.db = db;
    query = String.format("SELECT id FROM %s ORDER BY id SKIP ? LIMIT ?", type);
    skip = 0;
    batchSize = 100;
  }

  @Override
  public Integer get() {
    if (idsIt == null || !idsIt.hasNext())
      fetchNextBatch();
    if (idsIt.hasNext())
      return idsIt.next();
    return null;
  }

  private void fetchNextBatch() {
    ResultSet resultSet = db.query("sql", query, skip, batchSize);
    List<Integer> ids = resultSet.stream()
        .map(r -> r.<Integer>getProperty("id"))
        .toList();
    if (ids.isEmpty()) {
      // End of the type: cycle from the beginning. If skip was already 0 the type is truly
      // empty and we'll return an empty iterator so the caller sees get() == null; this is
      // the only way out of the feedback loop when no ids exist at all.
      if (skip == 0 || exhaustedOnce) {
        idsIt = ids.iterator();
        return;
      }
      exhaustedOnce = true;
      skip = 0;
      resultSet = db.query("sql", query, skip, batchSize);
      ids = resultSet.stream().map(r -> r.<Integer>getProperty("id")).toList();
    } else {
      exhaustedOnce = false;
    }
    idsIt = ids.iterator();
    if (!ids.isEmpty())
      skip += ids.size();
  }
}
