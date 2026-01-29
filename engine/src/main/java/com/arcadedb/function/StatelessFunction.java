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
package com.arcadedb.function;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Interface for stateless functions that don't need record context.
 * <p>
 * Stateless functions are pure transformations that take input arguments and produce
 * output values without needing access to the current record being processed.
 * They are thread-safe and can be reused across queries.
 * </p>
 * <p>
 * Examples include:
 * <ul>
 *   <li>Text functions: text.indexOf, text.join, text.split</li>
 *   <li>Math functions: math.sigmoid, math.tanh</li>
 *   <li>Conversion functions: convert.toJson, convert.toMap</li>
 *   <li>Utility functions: util.md5, util.sha256</li>
 * </ul>
 * </p>
 * <p>
 * These functions are available in both Cypher and SQL query engines.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see Function
 * @see RecordFunction
 */
public interface StatelessFunction extends Function {
  /**
   * Executes the function with the given arguments.
   * <p>
   * This method is called with pre-evaluated arguments and should return the
   * computed result. Implementations should be thread-safe and should not
   * maintain internal state between calls.
   * </p>
   *
   * @param args    the function arguments (already evaluated)
   * @param context the command execution context (provides database access if needed)
   * @return the function result
   */
  Object execute(Object[] args, CommandContext context);
}
