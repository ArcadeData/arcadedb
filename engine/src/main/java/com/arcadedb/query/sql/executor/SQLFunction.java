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

import com.arcadedb.function.RecordFunction;

/**
 * Interface that defines a SQL Function. Functions can be state-less if registered as instance, or state-full when registered as
 * class. State-less function are reused across queries, so don't keep any run-time information inside of it. State-full function,
 * instead, stores Implement it and register it with: {@literal OSQLParser.getInstance().registerFunction()} to being used by the
 * SQL engine.
 * <p>
 * This interface extends {@link RecordFunction} making all SQL functions available
 * in the unified {@link com.arcadedb.function.FunctionRegistry}.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see RecordFunction
 * @see com.arcadedb.function.FunctionRegistry
 */
public interface SQLFunction extends RecordFunction {

  /**
   * Configure the function.
   * <p>
   * Returns the same function for chaining.
   * </p>
   *
   * @param configuredParameters the parameters to configure
   * @return this function (for chaining)
   */
  @Override
  SQLFunction config(Object[] configuredParameters);

  /**
   * Returns a convenient SQL String representation of the function.
   * <p>
   * Example :
   *
   * <pre>
   *  myFunction( param1, param2, [optionalParam3])
   * </pre>
   * <p>
   * This text will be used in exception messages.
   *
   * @return String , never null.
   */
  @Override
  String getSyntax();

  /**
   * Whether two calls to this function with the same arguments always return the same result and never read or
   * write anything outside those arguments - no wall clock, no random source, no counter, no schema/index/record
   * lookup keyed on something other than a literal argument.
   * <p>
   * Defaults to {@code false}. The planner uses this to decide, without ever invoking the function, whether a call
   * over constant arguments can be folded at plan time ({@code WHERE 1 = abs(-1)}), whether a statement containing
   * the call can have its execution plan cached and reused across executions, and whether a call in an indexed
   * equality can be evaluated once at plan time instead of once per invocation site. Getting this wrong in the
   * {@code true} direction is silent and reaches user data - a function that reads the clock, a random source, or
   * database state would be baked into a cached plan or invoked at the wrong time - so a function (built-in or
   * user-defined) opts in explicitly rather than being assumed pure. See issue #6190.
   *
   * @return {@code true} only when the function is a pure, total function of its arguments alone
   */
  default boolean isDeterministic() {
    return false;
  }
}
