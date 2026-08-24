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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Cypher trim() and btrim() functions.
 * Supports multiple forms:
 * - trim(source) / btrim(source) - strips leading and trailing whitespace
 * - btrim(source, trimCharacter) - strips specified character from both sides
 * - trim(BOTH/LEADING/TRAILING char FROM string) - SQL-style trim syntax
 * Returns null if any argument is null (Cypher behavior).
 */
public class CypherTrimFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "trim";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args.length == 1) {
      // Simple form: trim(source) or btrim(source)
      final String source = CypherFunctionHelper.requireStringArgument(args[0], getName());
      if (source == null)
        return null;
      return source.strip();
    }

    if (args.length == 2) {
      // 2-arg form: btrim(source, trimCharacter). Both arguments are STRING-typed and type-checked before
      // either being null decides the answer, so an out-of-domain argument is still reported regardless of
      // which position happens to be null (issue #5798 review: btrim(5, null) must still be a type error,
      // not a silent null). The trim-character argument is STRING-typed too, same as the primary one -
      // issue #6608: #5798 only covered this function's primary argument position, leaving the trim
      // character to silently coerce via toString().
      final String source = CypherFunctionHelper.requireStringArgument(args[0], getName());
      final String trimChar = CypherFunctionHelper.requireStringArgument(args[1], getName());
      if (source == null || trimChar == null)
        return null;
      if (trimChar.isEmpty())
        return source.strip();
      return stripLeading(stripTrailing(source, trimChar), trimChar);
    }

    if (args.length == 3) {
      // SQL-style: trim(BOTH/LEADING/TRAILING char FROM string). The mode is not user data: the parser
      // (CypherExpressionBuilder#parseTrimFunction) always supplies one of the three literal mode strings,
      // never an arbitrary expression, so it needs no type check. The source (primary) argument is
      // type-checked first, same ordering convention as every other function in this family, followed by
      // the trim character - STRING-typed too, issue #6608: #5798 only reached this function's primary
      // argument, at position 2 in this form.
      final String mode = args[0] != null ? args[0].toString() : null;
      final String source = CypherFunctionHelper.requireStringArgument(args[2], getName());
      final String trimChar = CypherFunctionHelper.requireStringArgument(args[1], getName());
      if (source == null || trimChar == null)
        return null;
      if (trimChar.isEmpty()) {
        return switch (mode) {
          case "LEADING" -> source.stripLeading();
          case "TRAILING" -> source.stripTrailing();
          default -> source.strip();
        };
      }

      return switch (mode) {
        case "LEADING" -> stripLeading(source, trimChar);
        case "TRAILING" -> stripTrailing(source, trimChar);
        default -> stripLeading(stripTrailing(source, trimChar), trimChar);
      };
    }

    throw new CommandExecutionException("trim() and btrim() require 1, 2, or 3 arguments");
  }

  private static String stripLeading(final String source, final String trimChars) {
    int start = 0;
    while (start < source.length() && trimChars.indexOf(source.charAt(start)) >= 0)
      start++;
    return source.substring(start);
  }

  private static String stripTrailing(final String source, final String trimChars) {
    int end = source.length();
    while (end > 0 && trimChars.indexOf(source.charAt(end - 1)) >= 0)
      end--;
    return source.substring(0, end);
  }
}
