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
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length == 1) {
      // Simple form: trim(source) or btrim(source)
      if (args[0] == null)
        return null;
      return args[0].toString().strip();
    }

    if (args.length == 2) {
      // 2-arg form: btrim(source, trimCharacter)
      if (args[0] == null || args[1] == null)
        return null;
      final String source = args[0].toString();
      final String trimChar = args[1].toString();
      if (trimChar.isEmpty())
        return source.strip();
      return stripLeading(stripTrailing(source, trimChar), trimChar);
    }

    if (args.length == 3) {
      // SQL-style: trim(BOTH/LEADING/TRAILING char FROM string)
      final String mode = args[0] != null ? args[0].toString() : null;
      final String trimChar = args[1] != null ? args[1].toString() : null;
      final String source = args[2] != null ? args[2].toString() : null;

      if (source == null)
        return null;
      if (trimChar == null)
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
