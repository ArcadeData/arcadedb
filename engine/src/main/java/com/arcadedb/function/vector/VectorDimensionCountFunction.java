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
package com.arcadedb.function.vector;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;

/**
 * vector_dimension_count(vector) - returns the number of dimensions in a vector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorDimensionCountFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "vector_dimension_count";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("vector_dimension_count() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof float[])
      return (long) ((float[]) args[0]).length;
    if (args[0] instanceof double[])
      return (long) ((double[]) args[0]).length;
    if (args[0] instanceof List)
      return (long) ((List<?>) args[0]).size();
    throw new CommandExecutionException("vector_dimension_count() requires a vector argument");
  }
}
