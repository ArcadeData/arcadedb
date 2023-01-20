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

import java.util.*;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class MatchFirstStep extends AbstractExecutionStep {
  private final PatternNode           node;
  final         InternalExecutionPlan executionPlan;

  Iterator<Result> iterator;
  ResultSet        subResultSet;

  public MatchFirstStep(final CommandContext context, final PatternNode node, final boolean profilingEnabled) {
    this(context, node, null, profilingEnabled);
  }

  public MatchFirstStep(final CommandContext context, final PatternNode node, final InternalExecutionPlan subPlan, final boolean profilingEnabled) {
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
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(context, nRecords));
    init(context);
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
          throw new NoSuchElementException();
        }
        final ResultInternal result = new ResultInternal();
        if (iterator != null) {
          result.setProperty(getAlias(), iterator.next());
        } else {
          result.setProperty(getAlias(), subResultSet.next());
        }
        context.setVariable("matched", result);
        currentCount++;
        return result;
      }






    };
  }

//  private Object toResult(Document nextElement) {
//    ResultInternal result = new ResultInternal();
//    result.setElement(nextElement);
//    return result;
//  }

  private void init(final CommandContext context) {
    if (iterator == null && subResultSet == null) {
      final String alias = getAlias();
      final Object matchedNodes = context.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        initFromPrefetch(matchedNodes);
      } else {
        initFromExecutionPlan(context);
      }
    }
  }

  private void initFromExecutionPlan(final CommandContext context) {
    this.subResultSet = new LocalResultSet(executionPlan);
  }

  private void initFromPrefetch(final Object matchedNodes) {
    final Iterable possibleResults;
    if (matchedNodes instanceof Iterable) {
      possibleResults = (Iterable) matchedNodes;
    } else {
      possibleResults = Collections.singleton(matchedNodes);
    }
    iterator = possibleResults.iterator();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
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
