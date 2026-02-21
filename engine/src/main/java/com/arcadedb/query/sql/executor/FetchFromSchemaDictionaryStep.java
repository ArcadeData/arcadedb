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

import com.arcadedb.engine.Dictionary;
import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * Returns a single result containing the database dictionary metadata.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromSchemaDictionaryStep extends AbstractExecutionStep {

  private boolean served = false;

  public FetchFromSchemaDictionaryStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (served)
      return new InternalResultSet();

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Dictionary dictionary = context.getDatabase().getSchema().getDictionary();
      final Map<String, Integer> map = dictionary.getDictionaryMap();

      final ResultInternal r = new ResultInternal(context.getDatabase());
      r.setProperty("totalEntries", map.size());
      r.setProperty("totalPages", dictionary.getTotalPages());
      r.setProperty("entries", map);

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
    String result = spaces + "+ FETCH DATABASE DICTIONARY";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }
}
