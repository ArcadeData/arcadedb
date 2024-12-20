package com.arcadedb.function;/*
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
 */

import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Defines a function with a name and an entrypoint for execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface FunctionDefinition {
  /**
   * Returns the name of the function.
   */
  String getName();

  /**
   * Executes the function passing optional parameters.
   *
   * @param parameters are optional and can be positional or a map of key/value pairs.
   *
   * @return The result of the function
   */
  Object execute(Object... parameters);
}
