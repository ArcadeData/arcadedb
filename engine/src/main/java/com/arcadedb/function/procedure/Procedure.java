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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.FunctionArity;
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
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
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
   * Rejects a call whose argument count falls outside {@link #getMinArgs()}..{@link #getMaxArgs()}.
   * <p>
   * The counterpart of {@code Function.checkArity}, over the same {@link FunctionArity#check} body: a wrong argument
   * count is the same mistake whether the name resolved to a function or to a procedure, so it reads the same way
   * and carries the same status, differing only in the noun. It used to raise an {@link IllegalArgumentException}
   * with wording of its own, which {@code CallStep.executeProcedure} then wrapped - surfacing over HTTP as 500 where
   * the function path already gave 400 (issue #5627).
   * <p>
   * A wrong argument count is the caller's mistake, so this is a {@link CommandSemanticException} (HTTP 400) rather
   * than an internal failure. {@code CallStep} rethrows {@code CommandParsingException}, which this extends,
   * untouched.
   *
   * @param args the arguments the call carried, {@code null} counting as none
   *
   * @throws CommandSemanticException if the argument count is outside the declared bounds
   */
  default void checkArity(final Object[] args) {
    FunctionArity.check("Procedure", getName(), getMinArgs(), getMaxArgs(), args);
  }

  /**
   * Validates the arguments before execution. Kept as the name the {@code CALL} path calls
   * ({@code CallStep.executeProcedure}) and the name each implementation calls at the top of its {@code execute()};
   * the check itself is {@link #checkArity}.
   * <p>
   * The per-implementation calls are deliberately kept rather than folded into the {@code CALL} path alone: they are
   * not a second hand-written count check that can drift from the declared bounds - they run this very method - and
   * {@link #execute} is public, so a direct caller would otherwise index past the end of the array instead of being
   * told what it got wrong.
   *
   * @param args the arguments to validate
   *
   * @throws CommandSemanticException if the argument count is outside the declared bounds
   */
  default void validateArgs(final Object[] args) {
    checkArity(args);
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
