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
package com.arcadedb.function.procedure;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.List;
import java.util.stream.Stream;

/**
 * Base interface for procedures that can return multiple rows and modify the database.
 * <p>
 * Procedures differ from functions in that they:
 * <ul>
 *   <li>Can return multiple rows (via Stream)</li>
 *   <li>Support the YIELD clause for selecting output fields</li>
 *   <li>Can modify the database (create nodes, relationships, etc.)</li>
 *   <li>Can access the input row context for per-row execution</li>
 * </ul>
 * </p>
 * <p>
 * All procedures are registered in the {@link ProcedureRegistry} and can be used from
 * query engines that support procedure calls (e.g., Cypher CALL statements).
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see ProcedureRegistry
 */
public interface Procedure {
  /**
   * Returns the fully qualified procedure name (e.g., "merge.relationship").
   *
   * @return the procedure name including namespace
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
   * Returns a description of the procedure for documentation.
   *
   * @return procedure description
   */
  String getDescription();

  /**
   * Returns the names of fields that can be yielded from this procedure.
   * <p>
   * Example: For merge.relationship, this would return ["rel"].
   * For algo.dijkstra, this would return ["path", "weight"].
   * </p>
   *
   * @return list of yield field names
   */
  List<String> getYieldFields();

  /**
   * Executes the procedure with the given arguments.
   * <p>
   * The procedure returns a Stream of Results, where each Result contains
   * the yield fields. For single-result procedures (like merge.relationship),
   * the stream will contain one element. For multi-result procedures
   * (like algo.allSimplePaths), it may contain many elements.
   * </p>
   *
   * @param args     the procedure arguments (already evaluated)
   * @param inputRow the current input row (may be null for standalone CALL)
   * @param context  the command execution context
   * @return stream of results, each containing the yield fields
   */
  Stream<Result> execute(Object[] args, Result inputRow, CommandContext context);

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

  /**
   * Returns whether this procedure modifies the database.
   * <p>
   * Write procedures (like merge.relationship) should return true.
   * Read-only procedures (like algo.dijkstra) should return false.
   * </p>
   *
   * @return true if the procedure can modify the database
   */
  default boolean isWriteProcedure() {
    return false;
  }
}
