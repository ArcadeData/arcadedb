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

public class RightBinaryCondition extends SimpleNode {
  BinaryCompareOperator operator;
  boolean               not = false;
  InOperator            inOperator;
  Expression            right;

  public RightBinaryCondition(final int id) {
    super(id);
  }

  @Override
  public RightBinaryCondition copy() {
    final RightBinaryCondition result = new RightBinaryCondition(-1);
    result.operator = operator == null ? null : operator.copy();
    result.not = not;
    result.inOperator = inOperator == null ? null : inOperator.copy();
    result.right = right == null ? null : right.copy();
    return result;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    if (operator != null) {
      builder.append(operator);
      builder.append(" ");
      right.toString(params, builder);
    } else if (inOperator != null) {
      if (not) {
        builder.append("NOT ");
      }
      builder.append("IN ");
      right.toString(params, builder);
    }
  }

  public Object execute(final Result currentRecord, final Object elementToFilter, final CommandContext context) {
    if (elementToFilter == null) {
      return null;
    }
    final Iterator iterator;
    if (elementToFilter instanceof Identifiable) {
      iterator = Set.of(elementToFilter).iterator();
    } else if (elementToFilter instanceof Iterable iterable) {
      iterator = iterable.iterator();
    } else if (elementToFilter instanceof Iterator iterator1) {
      iterator = iterator1;
    } else {
      iterator = Set.of(elementToFilter).iterator();
    }

    final List result = new ArrayList();
    while (iterator.hasNext()) {
      final Object element = iterator.next();
      if (matchesFilters(currentRecord, element, context)) {
        result.add(element);
      }
    }
    return result;
  }

  public Object execute(final Identifiable currentRecord, final Object elementToFilter, final CommandContext context) {
    if (elementToFilter == null) {
      return null;
    }
    final Iterator iterator;
    if (elementToFilter instanceof Identifiable) {
      iterator = Set.of(elementToFilter).iterator();
    } else if (elementToFilter instanceof Iterable iterable) {
      iterator = iterable.iterator();
    } else if (elementToFilter instanceof Iterator iterator1) {
      iterator = iterator1;
    } else {
      iterator = Set.of(elementToFilter).iterator();
    }

    final List result = new ArrayList();
    while (iterator.hasNext()) {
      final Object element = iterator.next();
      if (matchesFilters(currentRecord, element, context)) {
        result.add(element);
      }
    }
    return result;
  }

  private boolean matchesFilters(final Identifiable currentRecord, final Object element, final CommandContext context) {
    if (operator != null) {
      operator.execute(context.getDatabase(), element, right.execute(currentRecord, context));
    } else if (inOperator != null) {

      final Object rightVal = evaluateRight(currentRecord, context);
      if (rightVal == null) {
        return false;
      }
      boolean result = InCondition.evaluateExpression(element, rightVal);
      if (not) {
        result = !result;
      }
      return result;
    }
    return false;
  }

  private boolean matchesFilters(final Result currentRecord, final Object element, final CommandContext context) {
    if (operator != null) {
      return operator.execute(context.getDatabase(), element, right.execute(currentRecord, context));
    } else if (inOperator != null) {

      final Object rightVal = evaluateRight(currentRecord, context);
      if (rightVal == null) {
        return false;
      }
      boolean result = InCondition.evaluateExpression(element, rightVal);
      if (not) {
        result = !result;
      }
      return result;
    }
    return false;
  }

  public Object evaluateRight(final Identifiable currentRecord, final CommandContext context) {
    return right.execute(currentRecord, context);
  }

  public Object evaluateRight(final Result currentRecord, final CommandContext context) {
    return right.execute(currentRecord, context);
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    if (right != null) {
      right.extractSubQueries(collector);
    }
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { operator, not, inOperator, right };
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    return new SimpleNode[] { right };
  }
}
/* JavaCC - OriginalChecksum=29d59ae04778eb611547292a27863da4 (do not edit this line) */
