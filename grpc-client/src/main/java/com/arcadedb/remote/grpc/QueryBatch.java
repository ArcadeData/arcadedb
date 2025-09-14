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
package com.arcadedb.remote.grpc;

import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;

public final class QueryBatch {
  private final List<Result> results; // Changed from List<Record>
  private final int          totalInBatch;
  private final long         runningTotal;
  private final boolean      lastBatch;

  public QueryBatch(List<Result> results, int totalInBatch, long runningTotal, boolean lastBatch) {
    this.results = results;
    this.totalInBatch = totalInBatch;
    this.runningTotal = runningTotal;
    this.lastBatch = lastBatch;
  }

  public List<Result> results() {
    return results;
  }

  // Backward compatibility: provide records() method that extracts Records from
  // Results
  @Deprecated
  public List<com.arcadedb.database.Record> records() {
    List<Record> records = new ArrayList<>(results.size());
    for (Result result : results) {
      if (result.isElement()) {
        result.getRecord().ifPresent(records::add);
      }
    }
    return records;
  }

  public int totalInBatch() {
    return totalInBatch;
  }

  public long runningTotal() {
    return runningTotal;
  }

  public boolean isLastBatch() {
    return lastBatch;
  }
}
