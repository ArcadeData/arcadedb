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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class LetQueryStep extends AbstractExecutionStep {

  private final Identifier varName;
  private final Statement  query;

  public LetQueryStep(Identifier varName, Statement query, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varName = varName;
    this.query = query;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (getPrev().isEmpty()) {
      throw new CommandExecutionException("Cannot execute a local LET on a query without a target");
    }
    return new ResultSet() {
      final ResultSet source = getPrev().get().syncPull(ctx, nRecords);

      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public Result next() {
        ResultInternal result = (ResultInternal) source.next();
        if (result != null) {
          calculate(result, ctx);
        }
        return result;
      }

      private void calculate(ResultInternal result, CommandContext ctx) {
        BasicCommandContext subCtx = new BasicCommandContext();
        subCtx.setDatabase(ctx.getDatabase());
        subCtx.setParentWithoutOverridingChild(ctx);
        InternalExecutionPlan subExecutionPlan = query.createExecutionPlan(subCtx, profilingEnabled);
        result.setMetadata(varName.getStringValue(), toList(new LocalResultSet(subExecutionPlan)));
      }

      private List<Result> toList(LocalResultSet oLocalResultSet) {
        List<Result> result = new ArrayList<>();
        while (oLocalResultSet.hasNext()) {
          result.add(oLocalResultSet.next());
        }
        oLocalResultSet.close();
        return result;
      }

      @Override
      public void close() {
        source.close();
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varName + " = (" + query + ")";
  }
}
