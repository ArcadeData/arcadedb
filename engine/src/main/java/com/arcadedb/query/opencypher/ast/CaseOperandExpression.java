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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

/**
 * Stands for the already evaluated operand of an extended CASE inside its comparison-form WHEN predicates
 * ({@code CASE x WHEN > 5, < 0 THEN ...}), so the operand is evaluated once per row however many WHEN forms follow and
 * a non-deterministic or aggregate operand keeps CASE semantics. The value is bound by {@link CaseExpression} for the
 * duration of one evaluation, per thread, so a cached AST can be evaluated concurrently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CaseOperandExpression implements Expression {
  /** Name the operand goes by in the predicate text a comparison-form WHEN is parsed from. */
  public static final String PLACEHOLDER = "__caseOperand__";

  private final ThreadLocal<Object> value = new ThreadLocal<>();

  void bind(final Object operandValue) {
    value.set(operandValue);
  }

  void unbind() {
    value.remove();
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    return value.get();
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return PLACEHOLDER;
  }
}
