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
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * vector_create(values, dimension) - converts a list of numbers into a float array.
 * Used by the Cypher vector() function to create typed vector values.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorCreateFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "vector_create";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 1)
      throw new CommandExecutionException("vector_create() requires at least 1 argument (values)");
    if (args[0] == null)
      return null;

    final float[] result = VectorUtils.toFloatArray(args[0]);

    // Validate dimension if provided
    if (args.length >= 2 && args[1] != null) {
      final int expectedDimension = ((Number) args[1]).intValue();
      if (result.length != expectedDimension)
        throw new CommandExecutionException(
            "Vector dimension mismatch: expected " + expectedDimension + " but got " + result.length);
    }

    return result;
  }
}
