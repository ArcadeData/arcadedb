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
import com.arcadedb.function.cypher.CypherFunctionHelper;
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
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args.length == 1) {
      final String source = CypherFunctionHelper.requireStringArgument(args[0], getName());
      if (source == null)
        return null;
      return source.stripLeading();
    }
    if (args.length == 2) {
      // Both arguments are STRING-typed and type-checked before either being null decides the answer, so an
      // out-of-domain argument is still reported regardless of which position happens to be null (issue
      // #5798 review: lTrim(5, null) must still be a type error, not a silent null). The trim-character
      // argument is STRING-typed too, same as the primary one - issue #6608: #5798 only covered this
      // function's primary argument position, leaving the trim character to silently coerce via toString().
      final String source = CypherFunctionHelper.requireStringArgument(args[0], getName());
      final String trimChar = CypherFunctionHelper.requireStringArgument(args[1], getName());
      if (source == null || trimChar == null)
        return null;
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
