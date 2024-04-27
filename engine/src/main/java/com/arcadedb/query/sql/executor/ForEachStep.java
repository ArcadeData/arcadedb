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
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.ForEachBlock;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.IfStatement;
import com.arcadedb.query.sql.parser.ReturnStatement;
import com.arcadedb.query.sql.parser.Statement;

import java.util.*;

/**
 * Created by luigidellaquila on 19/09/16.
 */
public class ForEachStep extends AbstractExecutionStep {
  private final Identifier      loopVariable;
  private final Expression      source;
  public final  List<Statement> body;

  Iterator iterator;
  private ExecutionStepInternal finalResult = null;
  private boolean               initiated   = false;

  public ForEachStep(final Identifier loopVariable, final Expression oExpression, final List<Statement> statements,
      final CommandContext context) {
    super(context);
    this.loopVariable = loopVariable;
    this.source = oExpression;
    this.body = statements;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious();

    prev.syncPull(context, nRecords);
    if (finalResult != null)
      return finalResult.syncPull(context, nRecords);

    init(context);
    while (iterator.hasNext()) {
      context.setVariable(loopVariable.getStringValue(), iterator.next());
      final ScriptExecutionPlan plan = initPlan(context);
      final ExecutionStepInternal result = plan.executeFull();
      if (result != null) {
        this.finalResult = result;
        return result.syncPull(context, nRecords);
      }
    }
    finalResult = new EmptyStep(context);
    return finalResult.syncPull(context, nRecords);

  }

  protected void init(final CommandContext context) {
    if (!this.initiated) {
      final Object val = source.execute(new ResultInternal(context.getDatabase()), context);
      this.iterator = MultiValue.getMultiValueIterator(val);
      this.initiated = true;
    }
  }

  public ScriptExecutionPlan initPlan(final CommandContext context) {
    final BasicCommandContext subCtx1 = new BasicCommandContext();
    subCtx1.setParent(context);
    final ScriptExecutionPlan plan = new ScriptExecutionPlan(subCtx1);
    for (final Statement stm : body)
      plan.chain(stm.createExecutionPlan(subCtx1));

    return plan;
  }

  public boolean containsReturn() {
    for (final Statement stm : this.body) {
      if (stm instanceof ReturnStatement)
        return true;

      if (stm instanceof ForEachBlock block && block.containsReturn())
        return true;

      if (stm instanceof IfStatement statement && statement.containsReturn())
        return true;
    }
    return false;
  }
}
