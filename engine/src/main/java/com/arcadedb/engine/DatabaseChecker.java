/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;

import java.util.Map;
import java.util.logging.Level;

public class DatabaseChecker {
  public void check(final Database database) {
    LogManager.instance().log(this, Level.INFO, "Starting checking database '%s'...", null, database.getName());

    long autofix = 0;
    long warnings = 0;
    long errors = 0;

    long pageSize = 0;
    long totalPages = 0;
    long totalRecords = 0;
    long totalActiveRecords = 0;
    long totalPlaceholderRecords = 0;
    long totalSurrogateRecords = 0;
    long totalDeletedRecords = 0;
    long totalMaxOffset = 0;

    for (Bucket b : database.getSchema().getBuckets()) {
      final Map<String, Long> stats = b.check();

      pageSize += stats.get("pageSize");
      totalPages += stats.get("totalPages");
      totalRecords += stats.get("totalRecords");
      totalActiveRecords += stats.get("totalActiveRecords");
      totalPlaceholderRecords += stats.get("totalPlaceholderRecords");
      totalSurrogateRecords += stats.get("totalSurrogateRecords");
      totalDeletedRecords += stats.get("totalDeletedRecords");
      totalMaxOffset += stats.get("totalMaxOffset");
    }

    final float avgPageUsed = totalPages > 0 ? (float) (totalMaxOffset / totalPages) * 100f / pageSize : 0;

    LogManager.instance().log(this, Level.INFO, "Total records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) avgPageUsed=%.2f%%", null, totalRecords,
        totalActiveRecords, totalDeletedRecords, totalPlaceholderRecords, totalSurrogateRecords, avgPageUsed);

    LogManager.instance().log(this, Level.INFO, "Completed checking of database '%s':", null, database.getName());
    LogManager.instance().log(this, Level.INFO, "- warning %d", null, warnings);
    LogManager.instance().log(this, Level.INFO, "- auto-fix %d", null, autofix);
    LogManager.instance().log(this, Level.INFO, "- errors %d", null, errors);
  }
}
