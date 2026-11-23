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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.FunctionArity;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Methods can be used on various objects with different number of arguments. SQL syntax: {@literal <object_name>.<method_name>([parameters])}
 *
 * @author Johann Sorel (Geomatys)
 */
@ExcludeFromJacocoGeneratedReport
public interface SQLMethod extends Comparable<SQLMethod> {

  /**
   * @return method name
   */
  String getName();

  /**
   * Returns a convenient SQL String representation of the method.
   * <p>
   * Example :
   *
   * <pre>
   *  field.myMethod( param1, param2, [optionalParam3])
   * </pre>
   * <p>
   * This text will be used in exception messages.
   *
   * @return String , never null.
   */
  String getSyntax();

  /**
   * Returns the minimum number of parameters required.
   *
   * @return minimum parameter count (>= 0)
   */
  int getMinParams();

  /**
   * Returns the maximum number of parameters allowed, or {@link Integer#MAX_VALUE} when the method is variadic.
   *
   * @return maximum parameter count (>= getMinParams())
   */
  int getMaxParams();

  /**
   * Rejects a call whose argument count falls outside {@link #getMinParams()}..{@link #getMaxParams()}, mirroring
   * {@link com.arcadedb.function.Function#checkArity} on the SQL-function side (#5884/#5885): a wrong argument count
   * is the caller's mistake, so it is reported as a {@link com.arcadedb.exception.CommandSemanticException} (HTTP
   * 400) rather than surfacing as whatever raw JDK exception the unguarded {@link #execute} happened to throw.
   *
   * @param params the arguments the call carried
   */
  default void checkArity(final Object[] params) {
    FunctionArity.check("Method", getName(), getMinParams(), getMaxParams(), params);
  }

  /**
   * Whether two calls to this method on the same value and arguments always return the same result and never read or write
   * anything outside them: no wall clock, no random source, no database setting, no schema lookup. The planner reads it, without
   * invoking the method, to decide whether a statement containing the call can have its execution plan cached, the same question
   * {@link SQLFunction#isDeterministic()} answers for functions (#6190). A method that does not say so is assumed not to be, so a
   * statement calling it is planned again on every execution.
   * <p>
   * The value it is applied to is part of what it is a function of, and that may be a record: {@code size()} on a record or a
   * RID reads the record it names, as it does at any other time. What the marker promises is that the call is evaluated against
   * the row of every execution and never in place of it, so a cached plan holds the call and not its result.
   * <p>
   * Only the implementations {@code DefaultSQLMethodFactory} registers itself are asked: a method an application adds or a built-in
   * it replaces is never trusted to be deterministic, whatever this answers. Answering {@code true} wrongly is silent and reaches
   * user data: a plan that holds a call whose result changes between executions would be reused. Defaults to {@code false}.
   *
   * @return {@code true} only when the method is a pure, total function of the value it is applied to and its arguments
   */
  default boolean isDeterministic() {
    return false;
  }

  /**
   * Process a record.
   *
   * @param self          current object
   * @param currentRecord : current record
   * @param context       execution context
   * @param params        : function parameters, number is ensured to be within minParams and maxParams.
   *
   * @return evaluation result
   */
  Object execute(Object self, Identifiable currentRecord, CommandContext context, Object[] params);

  boolean evaluateParameters();
}
