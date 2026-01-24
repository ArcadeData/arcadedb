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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.*;

/**
 * Represents an array literal expression like ['foo', 'bar', 123].
 * Created for ANTLR parser migration.
 */
public class ArrayLiteralExpression extends MathExpression {

  public List<Expression> items = new ArrayList<>();

  public ArrayLiteralExpression(final int id) {
    super(id);
  }

  public void addItem(final Expression item) {
    if (item != null) {
      items.add(item);
    }
  }

  @Override
  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    final List<Object> result = new ArrayList<>(items.size());
    for (final Expression item : items) {
      result.add(item.execute(currentRecord, context));
    }
    return result;
  }

  @Override
  public Object execute(final Result currentRecord, final CommandContext context) {
    final List<Object> result = new ArrayList<>(items.size());
    for (final Expression item : items) {
      result.add(item.execute(currentRecord, context));
    }
    return result;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("[");
    boolean first = true;
    for (final Expression item : items) {
      if (!first) {
        builder.append(", ");
      }
      item.toString(params, builder);
      first = false;
    }
    builder.append("]");
  }

  @Override
  public boolean isBaseIdentifier() {
    return false;
  }

  @Override
  public boolean isEarlyCalculated(final CommandContext context) {
    for (final Expression item : items) {
      if (!item.isEarlyCalculated(context)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isAggregate(final CommandContext context) {
    for (final Expression item : items) {
      if (item.isAggregate(context)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public MathExpression copy() {
    final ArrayLiteralExpression result = new ArrayLiteralExpression(-1);
    for (final Expression item : items) {
      result.items.add(item.copy());
    }
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { items };
  }

  @Override
  public boolean isCacheable() {
    for (final Expression item : items) {
      if (!item.isCacheable()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean refersToParent() {
    for (final Expression item : items) {
      if (item.refersToParent()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public SimpleNode splitForAggregation(final AggregateProjectionSplit aggregateProj, final CommandContext context) {
    if (isAggregate(context)) {
      final ArrayLiteralExpression result = new ArrayLiteralExpression(-1);

      for (final Expression item : items) {
        final SimpleNode splitResult = item.splitForAggregation(aggregateProj, context);

        if (splitResult instanceof Expression expr) {
          // Check if the result is either early calculated or aggregate
          if (expr.isEarlyCalculated(context) || expr.isAggregate(context)) {
            result.items.add(expr);
          } else {
            // This shouldn't happen, but follow the same pattern as MathExpression
            throw new CommandExecutionException(
                "Cannot mix aggregate and single record attribute values in the same array projection");
          }
        } else {
          // Shouldn't happen, but be defensive
          throw new IllegalStateException("Unexpected split result type: " + splitResult.getClass());
        }
      }

      return result;
    } else {
      return this;
    }
  }
}
