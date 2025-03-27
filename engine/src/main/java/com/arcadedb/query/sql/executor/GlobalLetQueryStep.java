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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.Statement;

import java.util.*;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class GlobalLetQueryStep extends AbstractExecutionStep {
  private final Identifier            varName;
  private final InternalExecutionPlan subExecutionPlan;
  boolean executed = false;

  public GlobalLetQueryStep(final Identifier varName, final Statement query, final CommandContext context,
      final List<String> scriptVars) {
    super(context);
    this.varName = varName;

    final BasicCommandContext subCtx = new BasicCommandContext();

    if (scriptVars != null)
      scriptVars.forEach(subCtx::declareScriptVariable);

    subCtx.setDatabase(context.getDatabase());
    subCtx.setParent(context);

    final boolean useCache = !query.toString().contains("?");
    // with positional parameters, you cannot know if a parameter has the same ordinal as the one
    // cached
    subExecutionPlan = query.resolvePlan(useCache, subCtx);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    calculate(context);
    return new InternalResultSet();
  }

  private void calculate(final CommandContext context) {
    if (executed)
      return;

    context.setVariable(varName.getStringValue(), toList(new LocalResultSet(subExecutionPlan)));
    executed = true;
  }

  private List<Result> toList(final LocalResultSet oLocalResultSet) {
    final List<Result> result = new ArrayList<>();
    while (oLocalResultSet.hasNext())
      result.add(oLocalResultSet.next());

    oLocalResultSet.close();
    return result;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (once)\n" + spaces + "  " + varName + " = \n" + box(spaces + "    ",
        this.subExecutionPlan.prettyPrint(0, indent));
  }

  @Override
  public List<ExecutionPlan> getSubExecutionPlans() {
    return List.of(this.subExecutionPlan);
  }

  private String box(final String spaces, final String s) {
    final String[] rows = s.split("\n");
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+-------------------------\n");
    for (final String row : rows) {
      result.append(spaces);
      result.append("| ");
      result.append(row);
      result.append("\n");
    }
    result.append(spaces);
    result.append("+-------------------------");
    return result.toString();
  }
}
