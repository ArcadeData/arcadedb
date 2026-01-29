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

/**
 * Base interface for all ArcadeDB functions.
 * <p>
 * Functions are organized into two main categories:
 * <ul>
 *   <li>{@link StatelessFunction} - Functions that don't need record context (pure transformations)</li>
 *   <li>{@link RecordFunction} - Functions that operate on records (need current record context)</li>
 * </ul>
 * </p>
 * <p>
 * All functions are registered in the {@link FunctionRegistry} and can be used from both
 * Cypher and SQL query engines.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see StatelessFunction
 * @see RecordFunction
 * @see FunctionRegistry
 */
public interface Function {
  /**
   * Returns the function name.
   * <p>
   * Names can be simple (e.g., "abs", "count") or namespaced (e.g., "text.indexOf", "map.merge").
   * Namespaced functions are accessible with or without the "apoc." prefix for Neo4j compatibility.
   * </p>
   *
   * @return the function name, never null
   */
  String getName();

  /**
   * Returns the minimum number of arguments required.
   *
   * @return minimum argument count (>= 0)
   */
  int getMinArgs();

  /**
   * Returns the maximum number of arguments allowed.
   *
   * @return maximum argument count (>= getMinArgs())
   */
  int getMaxArgs();

  /**
   * Returns a description of the function for documentation.
   *
   * @return function description
   */
  String getDescription();

  /**
   * Returns the syntax documentation string for this function.
   * <p>
   * Example: "myFunction(param1, param2, [optionalParam3])"
   * </p>
   *
   * @return syntax documentation string
   */
  default String getSyntax() {
    return getName() + "(...)";
  }

  /**
   * Validates the arguments before execution.
   * Default implementation checks argument count against min/max bounds.
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
