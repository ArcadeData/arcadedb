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
package com.arcadedb.function.text;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * lTrim() function - strips leading whitespace from a string.
 */
public class LTrimFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "lTrim";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length == 1) {
      if (args[0] == null)
        return null;
      return args[0].toString().stripLeading();
    }
    if (args.length == 2) {
      if (args[0] == null || args[1] == null)
        return null;
      final String source = args[0].toString();
      final String trimChar = args[1].toString();
      if (trimChar.isEmpty())
        return source.stripLeading();
      return stripLeading(source, trimChar);
    }
    throw new CommandExecutionException("lTrim() requires 1 or 2 arguments");
  }

  private static String stripLeading(final String source, final String trimChars) {
    return TrimFunction.stripLeading(source, trimChars);
  }
}
