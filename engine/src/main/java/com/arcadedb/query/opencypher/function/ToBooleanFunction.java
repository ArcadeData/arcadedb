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
    if (args[0] instanceof String) {
      final String str = ((String) args[0]).toLowerCase();
      if ("true".equals(str))
        return Boolean.TRUE;
      else if ("false".equals(str))
        return Boolean.FALSE;
      return null;
    }
    throw new CommandExecutionException("TypeError: InvalidArgumentValue - toBoolean() cannot convert " + args[0].getClass().getSimpleName());
  }
}
