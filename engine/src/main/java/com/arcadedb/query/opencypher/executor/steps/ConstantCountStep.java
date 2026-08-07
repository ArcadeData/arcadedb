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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Collections;
import java.util.List;

/**
 * Emits a single row holding a count the planner already knows, without touching the database.
 * <p>
 * Used when a mandatory element of the matched pattern - a node label or a relationship type - is absent from the
 * schema or holds no record, which makes the count 0 whatever the graph contains. The alternative is a push-down
 * that scans both endpoint label sets to conclude the same thing (issue #5715).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ConstantCountStep extends AbstractExecutionStep {
  private final long    count;
  private final String  countAlias;
  private       boolean executed = false;

  public ConstantCountStep(final long count, final String countAlias, final CommandContext context) {
    super(context);
    this.count = count;
    this.countAlias = countAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (executed)
      return new IteratorResultSet(Collections.<Result>emptyList().iterator());

    executed = true;
    if (context.isProfiling())
      rowCount = 1;

    final ResultInternal result = new ResultInternal();
    result.setProperty(countAlias, count);
    return new IteratorResultSet(List.of((Result) result).iterator());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent))
        + "+ CONSTANT COUNT (" + count + ": the pattern has an empty or undeclared type)";
  }
}
