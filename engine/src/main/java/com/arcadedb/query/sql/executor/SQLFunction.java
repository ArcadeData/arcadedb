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
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
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
}
