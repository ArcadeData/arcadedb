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

import com.arcadedb.function.FunctionArity;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * trim() function - strips characters from a string.
 * <p>
 * Supports two forms:
 * <ul>
 *   <li>trim(source) - strips leading and trailing whitespace</li>
 *   <li>trim(mode, trimCharacter, source) - strips specified character from leading/trailing/both sides</li>
 * </ul>
 * Mode values: "BOTH", "LEADING", "TRAILING"
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TrimFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "trim";
  }

  /**
   * One argument, or three. Two is inside the declared range but is not a form this function has, so it is rejected
   * separately at the end of {@code execute()}; the range is what the parse-time gate needs and is deliberately the
   * wider of the two statements.
   */
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
      // Simple form: trim(source)
      if (args[0] == null)
        return null;
      return args[0].toString().strip();
    }

    if (args.length == 3) {
      // Extended form: trim(mode, trimCharacter, source)
      final String mode = args[0].toString();
      final String source = args[2] != null ? args[2].toString() : null;
      if (source == null)
        return null;

      final String trimChar = args[1] != null ? args[1].toString() : null;
      if (trimChar == null || trimChar.isEmpty())
        return switch (mode) {
          case "LEADING" -> source.stripLeading();
          case "TRAILING" -> source.stripTrailing();
          default -> source.strip();
        };

      return switch (mode) {
        case "LEADING" -> stripLeading(source, trimChar);
        case "TRAILING" -> stripTrailing(source, trimChar);
        default -> stripLeading(stripTrailing(source, trimChar), trimChar);
      };
    }

    throw FunctionArity.mismatch(getName(), "1 or 3 arguments", args.length);
  }

  static String stripLeading(final String source, final String trimChars) {
    int start = 0;
    while (start < source.length() && trimChars.indexOf(source.charAt(start)) >= 0)
      start++;
    return source.substring(start);
  }

  static String stripTrailing(final String source, final String trimChars) {
    int end = source.length();
    while (end > 0 && trimChars.indexOf(source.charAt(end - 1)) >= 0)
      end--;
    return source.substring(0, end);
  }
}
