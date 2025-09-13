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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.executor.SQLFunctionFiltered;
import com.arcadedb.query.sql.executor.SQLMethod;

import java.util.*;
import java.util.stream.*;

public class MethodCall extends SimpleNode {

  static final Map<String, String> bidirectionalMethods = Map.of(//
      "out", "in",//
      "in", "out", //
      "both", "both", //
      "oute", "outv", //
      "ine", "inv", //
      "bothe", "bothe", //
      "bothv", "bothv", //
      "outv", "oute", //
      "inv", "ine");

  protected Identifier       methodName;
  protected List<Expression> params = new ArrayList<Expression>();

  private Boolean calculatedIsGraph = null;

  public MethodCall(final int id) {
    super(id);
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append(".");
    methodName.toString(params, builder);
    builder.append("(");
    boolean first = true;
    for (final Expression param : this.params) {
      if (!first) {
        builder.append(", ");
      }
      param.toString(params, builder);
      first = false;
    }
    builder.append(")");
  }

  public boolean isBidirectional() {
    return bidirectionalMethods.containsKey(methodName.getStringValue().toLowerCase(Locale.ENGLISH));
  }

  public Object execute(final Object targetObjects, final CommandContext context) {
    return execute(targetObjects, context, methodName.getStringValue(), params, null);
  }

  public Object execute(final Object targetObjects, final Iterable<Identifiable> iPossibleResults, final CommandContext context) {
    return execute(targetObjects, context, methodName.getStringValue(), params, iPossibleResults);
  }

  private Object execute(final Object targetObjects, final CommandContext context, final String name, final List<Expression> params,
      final Iterable<Identifiable> iPossibleResults) {
    final List<Object> paramValues = new ArrayList<Object>();
    Object val = context.getVariable("current");
    if (val == null && targetObjects == null) {
      return null;
    }
    for (final Expression expr : params) {
      if (val instanceof Identifiable identifiable) {
        paramValues.add(expr.execute(identifiable, context));
      } else if (val instanceof Result result) {
        paramValues.add(expr.execute(result, context));
      } else if (targetObjects instanceof Identifiable identifiable) {
        paramValues.add(expr.execute(identifiable, context));
      } else if (targetObjects instanceof Result result) {
        paramValues.add(expr.execute(result, context));
      } else {
        throw new CommandExecutionException("Invalid value for $current: " + val);
      }
    }
    if (isGraphFunction()) {
      final SQLFunction function = ((SQLQueryEngine) context.getDatabase().getQueryEngine("sql")).getFunction(name);
      if (function instanceof SQLFunctionFiltered filtered) {
        Object current = context.getVariable("current");
        if (current instanceof Result result) {
          current = result.getElement().orElse(null);
        }
        return filtered.execute(targetObjects, (Identifiable) current, null, paramValues.toArray(), iPossibleResults, context);
      } else {
        final Object current = context.getVariable("current");
        if (current instanceof Identifiable identifiable) {
          return function.execute(targetObjects, identifiable, null, paramValues.toArray(), context);
        } else if (current instanceof Result result) {
          return function.execute(targetObjects, result.getElement().orElse(null), null, paramValues.toArray(), context);
        } else {
          return function.execute(targetObjects, null, null, paramValues.toArray(), context);
        }
      }

    }

    final SQLMethod method = ((SQLQueryEngine) context.getDatabase().getQueryEngine("sql")).getMethod(name);
    if (method != null) {
      if (val instanceof Result result)
        val = result.getElement().orElse(null);

      return method.execute(targetObjects, (Identifiable) val, context, paramValues.toArray());
    }
    throw new UnsupportedOperationException("OMethod call, something missing in the implementation...?");
  }

  public Object executeReverse(final Object targetObjects, final CommandContext context) {
    final String straightName = methodName.getStringValue().toLowerCase(Locale.ENGLISH);
    final String inverseMethodName = bidirectionalMethods.get(straightName);

    if (inverseMethodName != null)
      return execute(targetObjects, context, inverseMethodName, params, null);

    throw new UnsupportedOperationException("Invalid reverse traversal: " + methodName);
  }

  public MethodCall copy() {
    final MethodCall result = new MethodCall(-1);
    result.methodName = methodName.copy();
    result.params = params.stream().map(x -> x.copy()).collect(Collectors.toList());
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { methodName, params };
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    if (params != null) {
      for (final Expression param : params) {
        param.extractSubQueries(collector);
      }
    }
  }

  public boolean refersToParent() {
    if (params != null) {
      for (final Expression exp : params) {
        if (exp.refersToParent()) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean isCacheable() {
    return isGraphFunction();
  }

  private boolean isGraphFunction() {
    if (calculatedIsGraph != null)
      return calculatedIsGraph;

    final String methodNameLC = methodName.getStringValue().toLowerCase();

    for (final String graphMethod : bidirectionalMethods.keySet()) {
      if (graphMethod.equals(methodNameLC)) {
        calculatedIsGraph = true;
        break;
      }
    }

    if (calculatedIsGraph == null)
      calculatedIsGraph = false;

    return calculatedIsGraph;
  }
}
/* JavaCC - OriginalChecksum=da95662da21ceb8dee3ad88c0d980413 (do not edit this line) */
