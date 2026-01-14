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
 * Base interface for boolean expressions used in WHERE clauses.
 * Boolean expressions evaluate to true/false for filtering results.
 */
public interface BooleanExpression {
  /**
   * Evaluate this boolean expression against a result row.
   *
   * @param result  The result row containing variable bindings
   * @param context The command execution context
   * @return true if the condition is met, false otherwise
   */
  boolean evaluate(Result result, CommandContext context);

  /**
   * Get the text representation of this expression.
   *
   * @return The text representation
   */
  String getText();
}
