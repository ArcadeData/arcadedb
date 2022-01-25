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
import com.arcadedb.query.sql.parser.LocalResultSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class MatchFirstStep extends AbstractExecutionStep {
  private final PatternNode           node;
  final         InternalExecutionPlan executionPlan;

  Iterator<Result> iterator;
  ResultSet        subResultSet;

  public MatchFirstStep(CommandContext context, PatternNode node, boolean profilingEnabled) {
    this(context, node, null, profilingEnabled);
  }

  public MatchFirstStep(CommandContext context, PatternNode node, InternalExecutionPlan subPlan, boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.node = node;
    this.executionPlan = subPlan;
  }

  @Override
  public void reset() {
    this.iterator = null;
    this.subResultSet = null;
    if (executionPlan != null) {
      executionPlan.reset(this.getContext());
    }
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init(ctx);
    return new ResultSet() {

      int currentCount = 0;

      @Override
      public boolean hasNext() {
        if (currentCount >= nRecords) {
          return false;
        }
        if (iterator != null) {
          return iterator.hasNext();
        } else {
          return subResultSet.hasNext();
        }
      }

      @Override
      public Result next() {
        if (currentCount >= nRecords) {
          throw new IllegalStateException();
        }
        ResultInternal result = new ResultInternal();
        if (iterator != null) {
          result.setProperty(getAlias(), iterator.next());
        } else {
          result.setProperty(getAlias(), subResultSet.next());
        }
        ctx.setVariable("$matched", result);
        currentCount++;
        return result;
      }

      @Override
      public void close() {

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

//  private Object toResult(Document nextElement) {
//    ResultInternal result = new ResultInternal();
//    result.setElement(nextElement);
//    return result;
//  }

  private void init(CommandContext ctx) {
    if (iterator == null && subResultSet == null) {
      String alias = getAlias();
      Object matchedNodes = ctx.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        initFromPrefetch(matchedNodes);
      } else {
        initFromExecutionPlan(ctx);
      }
    }
  }

  private void initFromExecutionPlan(CommandContext ctx) {
    this.subResultSet = new LocalResultSet(executionPlan);
  }

  private void initFromPrefetch(Object matchedNodes) {
    Iterable possibleResults;
    if (matchedNodes instanceof Iterable) {
      possibleResults = (Iterable) matchedNodes;
    } else {
      possibleResults = Collections.singleton(matchedNodes);
    }
    iterator = possibleResults.iterator();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SET \n");
    result.append(spaces);
    result.append("   ");
    result.append(getAlias());
    if (executionPlan != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  AS\n");
      result.append(executionPlan.prettyPrint(depth + 1, indent));
    }

    return result.toString();
  }

  private String getAlias() {
    return this.node.alias;
  }

}
