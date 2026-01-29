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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Interface for namespaced Cypher functions (e.g., text.indexOf, map.merge).
 * <p>
 * Functions are stateless operations that transform input values to output values.
 * For operations that modify the database or return multiple rows, use {@link com.arcadedb.query.opencypher.procedures.CypherProcedure} instead.
 * </p>
 *
 * @author ArcadeDB Team
 */
public interface CypherFunction {
  /**
   * Returns the fully qualified function name (e.g., "text.indexOf").
   *
   * @return the function name including namespace
   */
  String getName();

  /**
   * Returns the minimum number of arguments required.
   *
   * @return minimum argument count
   */
  int getMinArgs();

  /**
   * Returns the maximum number of arguments allowed.
   *
   * @return maximum argument count
   */
  int getMaxArgs();

  /**
   * Returns a description of the function for documentation.
   *
   * @return function description
   */
  String getDescription();

  /**
   * Executes the function with the given arguments.
   *
   * @param args    the function arguments (already evaluated)
   * @param context the command execution context
   * @return the function result
   */
  Object execute(Object[] args, CommandContext context);

  /**
   * Validates the arguments before execution.
   * Default implementation checks argument count.
   *
   * @param args the arguments to validate
   * @throws IllegalArgumentException if arguments are invalid
   */
  default void validateArgs(final Object[] args) {
    if (args.length < getMinArgs() || args.length > getMaxArgs()) {
      if (getMinArgs() == getMaxArgs()) {
        throw new IllegalArgumentException(
            getName() + "() requires exactly " + getMinArgs() + " argument(s), got " + args.length);
      } else {
        throw new IllegalArgumentException(
            getName() + "() requires " + getMinArgs() + " to " + getMaxArgs() + " arguments, got " + args.length);
      }
    }
  }
}
