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
package com.arcadedb.query.sql.executor;/*
 *
 *  *  Copyright 2021 Arcade Data Ltd
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: https://arcadedb.com
 *
 */

import com.arcadedb.database.Identifiable;

/**
 * Interface that defines a SQL Function. Functions can be state-less if registered as instance, or state-full when registered as
 * class. State-less function are reused across queries, so don't keep any run-time information inside of it. State-full function,
 * instead, stores Implement it and register it with: {@literal OSQLParser.getInstance().registerFunction()} to being used by the
 * SQL engine.
 * <p>
 * ??? could it be possible to have a small piece of code here showing where to register a function using services ???
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public interface SQLFunction {

  /**
   * Process a record.
   *
   * @param iThis
   * @param iCurrentRecord : current record
   * @param iCurrentResult TODO
   * @param iParams        : function parameters, number is ensured to be within minParams and maxParams.
   * @param iContext       : object calling this function
   *
   * @return function result, can be null. Special cases : can be null if function aggregate results, can be null if function filter
   * results : this mean result is excluded
   */
  Object execute(Object iThis, Identifiable iCurrentRecord, Object iCurrentResult, Object[] iParams, CommandContext iContext);

  /**
   * Configure the function.
   *
   * @param configuredParameters
   */
  void config(Object[] configuredParameters);

  /**
   * A function can make calculation on several records before returning a result.
   * <p>
   * Example of such function : sum, count, max, min ...
   * <p>
   * The final result of the aggregation is obtained by calling {@link #getResult() }
   *
   * @return true if function aggregate results
   */
  boolean aggregateResults();

  /**
   * Function name, the name is used by the sql parser to identify a call this function.
   *
   * @return String , function name, never null or empty.
   */
  String getName();

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
  String getSyntax();

  /**
   * Only called when function aggregates results after all records have been passed to the function.
   *
   * @return Aggregation result
   */
  Object getResult();
}
