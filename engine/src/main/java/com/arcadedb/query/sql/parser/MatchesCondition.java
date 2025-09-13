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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.*;

public class MatchesCondition extends BooleanExpression {
  protected Expression     expression;
  protected String         right;
  public    Expression     rightExpression;
  protected InputParameter rightParam;

  public MatchesCondition(final int id) {
    super(id);
  }

  @Override
  public Boolean evaluate(final Identifiable currentRecord, final CommandContext context) {
    String regex = right;
    if (regex != null) {
      regex = regex.substring(1, regex.length() - 1);
    } else if (rightExpression != null) {
      final Object val = rightExpression.execute(currentRecord, context);
      if (val instanceof String string) {
        regex = string;
      } else {
        return false;
      }
    } else {
      final Object paramVal = rightParam.getValue(context.getInputParameters());
      if (paramVal instanceof String string) {
        regex = string;
      } else {
        return false;
      }
    }
    final Object value = expression.execute(currentRecord, context);

    return matches(value, regex, context);
  }

  private boolean matches(final Object value, final String regex, final CommandContext context) {
    final String key = "MATCHES_" + regex.hashCode();
    java.util.regex.Pattern p = (java.util.regex.Pattern) context.getVariable(key);
    if (p == null) {
      p = java.util.regex.Pattern.compile(regex);
      context.setVariable(key, p);
    }

    if (value instanceof CharSequence sequence) {
      return p.matcher(sequence).matches();
    } else {
      return false;
    }
  }

  @Override
  public Boolean evaluate(final Result currentRecord, final CommandContext context) {
    String regex = right;
    if (regex != null) {
      regex = regex.substring(1, regex.length() - 1);
    } else if (rightExpression != null) {
      final Object val = rightExpression.execute(currentRecord, context);
      if (val instanceof String string) {
        regex = string;
      } else {
        return false;
      }
    } else {
      final Object paramVal = rightParam.getValue(context.getInputParameters());
      if (paramVal instanceof String string) {
        regex = string;
      } else {
        return false;
      }
    }
    final Object value = expression.execute(currentRecord, context);

    return matches(value, regex, context);
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    expression.toString(params, builder);
    builder.append(" MATCHES ");
    if (right != null) {
      builder.append(right);
    } else if (rightExpression != null) {
      rightExpression.toString(params, builder);
    } else {
      rightParam.toString(params, builder);
    }
  }

  @Override
  public MatchesCondition copy() {
    final MatchesCondition result = new MatchesCondition(-1);
    result.expression = expression == null ? null : expression.copy();
    result.right = right;
    result.rightParam = rightParam == null ? null : rightParam.copy();
    result.rightExpression = rightExpression == null ? null : rightExpression.copy();
    return result;
  }

  @Override
  public void extractSubQueries(final SubQueryCollector collector) {
    expression.extractSubQueries(collector);
    if (rightExpression != null) {
      rightExpression.extractSubQueries(collector);
    }
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { expression, right, rightExpression, rightParam };
  }

  @Override
  public List<String> getMatchPatternInvolvedAliases() {
    final List<String> result = new ArrayList<>(expression.getMatchPatternInvolvedAliases());
    if (rightExpression != null) {
      result.addAll(rightExpression.getMatchPatternInvolvedAliases());
    }
    return result;
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    return new SimpleNode[] { expression, rightExpression };
  }
}
/* JavaCC - OriginalChecksum=68712f476e2e633c2bbfc34cb6c39356 (do not edit this line) */
