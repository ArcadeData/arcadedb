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
package com.arcadedb.function.convert;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Locale;

/**
 * toBoolean() function - converts a value to a boolean.
 * Numbers: 0 is false, non-zero is true.
 * Strings: "true" is true, "false" is false (case-insensitive).
 */
public class ToBooleanFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "toBoolean";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("toBoolean() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof Boolean)
      return args[0];
    if (args[0] instanceof Float || args[0] instanceof Double)
      // Neo4j's toBoolean() rejects FLOAT: a client-side type error, so surface it as a 400 via
      // CommandSemanticException instead of a 500 transaction-commit failure. See issue #5294.
      throw new CommandSemanticException("TypeError: InvalidArgumentValue - toBoolean() cannot convert " + args[0].getClass().getSimpleName());
    if (args[0] instanceof Number)
      return ((Number) args[0]).longValue() != 0L;
    if (args[0] instanceof String) {
      final String str = ((String) args[0]).toLowerCase(Locale.ROOT);
      if ("true".equals(str))
        return Boolean.TRUE;
      else if ("false".equals(str))
        return Boolean.FALSE;
      return null;
    }
    // Unsupported types: List, Map, Node, Relationship, Path. A client-side type error, so throw a
    // CommandSemanticException to surface it as a 400 client error instead of a 500 transaction-commit
    // failure. Same pattern as toString() (see issue #5203). See issue #5294.
    throw new CommandSemanticException("TypeError: InvalidArgumentValue - toBoolean() cannot convert " + args[0].getClass().getSimpleName());
  }
}
