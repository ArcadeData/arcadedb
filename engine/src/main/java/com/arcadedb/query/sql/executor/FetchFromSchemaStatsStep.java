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

import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * Returns a single result document containing per-database runtime statistics.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromSchemaStatsStep extends AbstractExecutionStep {

  private boolean served = false;

  public FetchFromSchemaStatsStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (served)
      return new InternalResultSet();

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Database database = context.getDatabase();
      final Map<String, Object> stats = database.getStats();

      final ResultInternal r = new ResultInternal(database);
      r.setProperty("databaseName", database.getName());

      for (final Map.Entry<String, Object> entry : stats.entrySet())
        r.setProperty(entry.getKey(), entry.getValue());

      served = true;

      return new InternalResultSet(r);
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE STATS";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }
}
