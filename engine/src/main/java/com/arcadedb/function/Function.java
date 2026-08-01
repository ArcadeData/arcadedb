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

import com.arcadedb.exception.CommandSemanticException;

import java.util.List;

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
   * <p>
   * <b>This is the function's argument-count contract, not documentation.</b> {@link #checkArity} enforces it at
   * runtime, and {@code CypherFunctionArityRegistryTest} holds the parser's declaration in
   * {@code FunctionValidator} to it, so a registry entry narrower than what the function really accepts fails the
   * build instead of the user's query - the shape the {@code distance()} bug of #5484 had. Declare what
   * {@link StatelessFunction#execute} actually reads, including the optional arguments.
   *
   * @return minimum argument count (>= 0)
   */
  int getMinArgs();

  /**
   * Returns the maximum number of arguments allowed, or {@link Integer#MAX_VALUE} when the function is variadic.
   * See {@link #getMinArgs()} for what this declaration is used for.
   *
   * @return maximum argument count (>= getMinArgs())
   */
  int getMaxArgs();

  /**
   * Rejects a call whose argument count falls outside {@link #getMinArgs()}..{@link #getMaxArgs()}.
   * <p>
   * Functions call this instead of hand-writing their own count check, so each one declares its arity exactly once:
   * a second hand-written copy inside {@code execute()} is free to drift from the declared bounds, and the drift
   * would be invisible to the registry guard, which reads the bounds. The message is the one the parse-time gate
   * uses, so a client is told the same thing whichever path caught the mistake (#5484, #5602).
   * <p>
   * A wrong argument count is the caller's mistake, so this is a {@link CommandSemanticException} (HTTP 400) rather
   * than an internal failure.
   *
   * @param args the arguments the call carried, {@code null} counting as none - a couple of executors used to defend
   *             against a null array by hand, and folding that in here keeps the count check in one place. Note that
   *             this only rejects the null array for a function that requires at least one argument: an executor
   *             declaring {@code getMinArgs() == 0} is handed it unchanged and must still tolerate it.
   */
  default void checkArity(final Object[] args) {
    FunctionArity.check("Function", getName(), getMinArgs(), getMaxArgs(), args);
  }

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
   * Returns an optional alias name for backward compatibility.
   * <p>
   * When a function provides an alias, it will be registered under both
   * its primary name and alias name. This allows for backward compatibility
   * when renaming functions (e.g., vectorCosineSimilarity -> vector.cosineSimilarity).
   * </p>
   *
   * @return the alias name, or null if no alias is provided
   */
  default String getAlias() {
    return null;
  }

  /**
   * Returns additional alias names beyond the primary {@link #getAlias()}.
   * <p>
   * A function may expose several backward-compatible or synonymous names (e.g.
   * {@code vector.magnitude} is also registered as {@code vector.l2Norm} / {@code vectorL2Norm}).
   * Each name returned here is registered as an extra lookup key for the same function instance.
   * </p>
   *
   * @return an unmodifiable list of additional names, or an empty list if none
   */
  default List<String> getAliases() {
    return List.of();
  }

  /**
   * Validates the arguments before execution. Kept as the name the {@code CALL} path calls
   * ({@code CallStep.executeFunction}); the check itself is {@link #checkArity}, so a wrong argument count is
   * reported identically however the function was invoked.
   * <p>
   * It used to raise its own {@link IllegalArgumentException} with its own wording, which meant the same mistake
   * read one way through an expression and another through {@code CALL} - and, because {@code CallStep} wraps what
   * it catches, surfaced over HTTP as 500 rather than the 400 the expression path gave (issue #5602).
   * <p>
   * Note that {@code Procedure} declares a separate {@code validateArgs} of its own, over its own
   * {@code checkArity}: procedures are not functions - they have their own registry and their own {@code CALL}
   * handling - so the two guards stay separate. What they share is {@link FunctionArity}, so the same mistake reads
   * the same way and carries the same status whichever kind the name resolved to (#5627).
   *
   * @param args the arguments to validate
   * @throws CommandSemanticException if the argument count is outside the declared bounds
   */
  default void validateArgs(final Object[] args) {
    checkArity(args);
  }
}
