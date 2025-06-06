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
/* Generated By:JJTree: Do not edit this line. OArrayConcatExpression.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.AggregationContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ArrayConcatExpression extends SimpleNode {

  List<ArrayConcatExpressionElement> childExpressions = new ArrayList<>();

  public ArrayConcatExpression(final int id) {
    super(id);
  }

  public List<ArrayConcatExpressionElement> getChildExpressions() {
    return childExpressions;
  }

  public void setChildExpressions(final List<ArrayConcatExpressionElement> childExpressions) {
    this.childExpressions = childExpressions;
  }

  public Object apply(final Object left, final Object right) {

    if (left == null && right == null) {
      return null;
    }

    if (right == null) {
      if (MultiValue.isMultiValue(left)) {
        return left;
      } else {
        return List.of(left);
      }
    }

    if (left == null) {
      if (MultiValue.isMultiValue(right)) {
        return right;
      } else {
        return List.of(right);
      }
    }

    final List<Object> result = new ArrayList<>();
    if (MultiValue.isMultiValue(left)) {
      final Iterator<?> leftIter = MultiValue.getMultiValueIterator(left);
      while (leftIter.hasNext()) {
        result.add(leftIter.next());
      }
    } else {
      result.add(left);
    }

    if (MultiValue.isMultiValue(right)) {
      final Iterator<?> rightIter = MultiValue.getMultiValueIterator(right);
      while (rightIter.hasNext()) {
        result.add(rightIter.next());
      }
    } else {
      result.add(right);
    }

    return result;
  }

  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    Object result = childExpressions.getFirst().execute(currentRecord, context);
    for (int i = 1; i < childExpressions.size(); i++) {
      result = apply(result, childExpressions.get(i).execute(currentRecord, context));
    }
    return result;
  }

  public Object execute(final Result currentRecord, final CommandContext context) {
    Object result = childExpressions.getFirst().execute(currentRecord, context);
    for (int i = 1; i < childExpressions.size(); i++) {
      result = apply(result, childExpressions.get(i).execute(currentRecord, context));
    }
    return result;
  }

  public boolean isEarlyCalculated(final CommandContext context) {
    for (final ArrayConcatExpressionElement element : childExpressions) {
      if (!element.isEarlyCalculated(context)) {
        return false;
      }
    }
    return true;
  }

  public boolean isAggregate(final CommandContext context) {
    for (final ArrayConcatExpressionElement expr : this.childExpressions) {
      if (expr.isAggregate(context)) {
        return true;
      }
    }
    return false;
  }

  public SimpleNode splitForAggregation(final CommandContext context) {
    if (isAggregate(context)) {
      throw new CommandExecutionException("Cannot use aggregate functions in array concatenation");
    } else {
      return this;
    }
  }

  public AggregationContext getAggregationContext(final CommandContext context) {
    throw new UnsupportedOperationException("array concatenation expressions do not allow plain aggregation");
  }

  public ArrayConcatExpression copy() {
    final ArrayConcatExpression result = new ArrayConcatExpression(-1);
    this.childExpressions.forEach(x -> result.childExpressions.add(x.copy()));
    return result;
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    for (final ArrayConcatExpressionElement expr : this.childExpressions) {
      expr.extractSubQueries(collector);
    }
  }

  public List<String> getMatchPatternInvolvedAliases() {
    final List<String> result = new ArrayList<>();
    for (final ArrayConcatExpressionElement exp : childExpressions) {
      final List<String> x = exp.getMatchPatternInvolvedAliases();
      if (x != null) {
        result.addAll(x);
      }
    }
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    for (int i = 0; i < childExpressions.size(); i++) {
      if (i > 0) {
        builder.append(" || ");
      }
      childExpressions.get(i).toString(params, builder);
    }
  }

  @Override
  protected Object[] getIdentityElements() {
    return getCacheableElements();
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    return childExpressions.toArray(new SimpleNode[childExpressions.size()]);
  }
}
/* JavaCC - OriginalChecksum=8d976a02f84460bf21c4304009135345 (do not edit this line) */
