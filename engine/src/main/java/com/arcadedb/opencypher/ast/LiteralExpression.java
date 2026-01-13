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

/**
 * Expression representing a literal value (number, string, boolean, etc.).
 * Example: 42, "hello", true, 3.14
 */
public class LiteralExpression implements Expression {
  private final Object value;
  private final String text;

  public LiteralExpression(final Object value, final String text) {
    this.value = value;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    return value;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public Object getValue() {
    return value;
  }
}
