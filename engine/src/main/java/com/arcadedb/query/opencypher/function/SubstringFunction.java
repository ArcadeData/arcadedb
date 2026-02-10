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
 * substring() function - returns a substring of the original string.
 */
public class SubstringFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "substring";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 2 || args.length > 3)
      throw new CommandExecutionException("substring() requires 2 or 3 arguments: substring(string, start[, length])");
    if (args[0] == null)
      return null;
    final String str = args[0].toString();
    final int start = ((Number) args[1]).intValue();
    if (start < 0 || start > str.length())
      return "";
    if (args.length == 3 && args[2] != null) {
      final int length = ((Number) args[2]).intValue();
      return str.substring(start, Math.min(start + length, str.length()));
    }
    return str.substring(start);
  }
}
