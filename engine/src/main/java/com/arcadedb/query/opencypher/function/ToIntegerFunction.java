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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * toInteger() function - converts a value to an integer.
 */
public class ToIntegerFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "toInteger";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("toInteger() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof Boolean)
      return ((Boolean) args[0]) ? 1L : 0L;
    if (args[0] instanceof Number)
      return ((Number) args[0]).longValue();
    if (args[0] instanceof String) {
      try {
        // Try integer first, then float (truncating)
        final String s = ((String) args[0]).trim();
        if (s.contains(".") || s.contains("e") || s.contains("E"))
          return (long) Double.parseDouble(s);
        return Long.parseLong(s);
      } catch (final NumberFormatException e) {
        return null;
      }
    }
    throw new CommandExecutionException("TypeError: InvalidArgumentValue - toInteger() cannot convert " + args[0].getClass().getSimpleName());
  }
}
