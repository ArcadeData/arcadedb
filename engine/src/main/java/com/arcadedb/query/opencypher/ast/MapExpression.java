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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Expression representing a map literal.
 * Example: {name: 'Alice', age: 30}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MapExpression implements Expression {
  private final Map<String, Expression> entries;
  private final String text;

  public MapExpression(final Map<String, Expression> entries, final String text) {
    this.entries = entries;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Map<String, Object> map = new LinkedHashMap<>();
    for (final Map.Entry<String, Expression> entry : entries.entrySet())
      map.put(entry.getKey(), OpenCypherQueryEngine.getExpressionEvaluator().evaluate(entry.getValue(), result, context));
    return map;
  }

  @Override
  public boolean isAggregation() {
    for (final Expression expr : entries.values())
      if (expr.isAggregation())
        return true;
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public Map<String, Expression> getEntries() {
    return entries;
  }
}
