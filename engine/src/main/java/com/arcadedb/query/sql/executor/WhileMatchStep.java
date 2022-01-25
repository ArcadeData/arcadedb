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

import com.arcadedb.query.sql.parser.WhereClause;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by luigidellaquila on 13/10/16.
 */
public class WhileMatchStep extends AbstractUnrollStep {

  private final InternalExecutionPlan body;
  private final WhereClause           condition;

  public WhileMatchStep(CommandContext ctx, WhereClause condition, InternalExecutionPlan body, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.body = body;
    this.condition = condition;
  }

  @Override protected Collection<Result> unroll(Result doc, CommandContext iContext) {
    body.reset(iContext);
    List<Result> result = new ArrayList<>();
    ResultSet block = body.fetchNext(100);
    while(block.hasNext()){
      while(block.hasNext()){
        result.add(block.next());
      }
      block = body.fetchNext(100);
    }
    return result;
  }

  @Override public String prettyPrint(int depth, int indent) {
    String indentStep = ExecutionStepInternal.getIndent(1, indent);
    String spaces = ExecutionStepInternal.getIndent(depth, indent);

    String result =
        spaces + "+ WHILE\n" + spaces + indentStep + condition.toString() + "\n" + spaces + "  DO\n" + body.prettyPrint(depth + 1, indent) + "\n" + spaces
            + "  END\n";

    return result;
  }

}
