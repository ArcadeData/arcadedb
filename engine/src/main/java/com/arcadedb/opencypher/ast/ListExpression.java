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
package com.arcadedb.opencypher.ast;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * Expression representing a list literal.
 * Example: [1, 2, 3], ['a', 'b', 'c'], [n.name, n.age]
 */
public class ListExpression implements Expression {
  private final List<Expression> elements;
  private final String text;

  public ListExpression(final List<Expression> elements, final String text) {
    this.elements = elements;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final List<Object> values = new ArrayList<>();
    for (final Expression element : elements) {
      values.add(element.evaluate(result, context));
    }
    return values;
  }

  @Override
  public boolean isAggregation() {
    // A list is an aggregation if any of its elements are aggregations
    for (final Expression element : elements) {
      if (element.isAggregation()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public List<Expression> getElements() {
    return elements;
  }
}
