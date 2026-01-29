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
package com.arcadedb.query.opencypher.functions.text;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * text.snakeCase(string) - Convert to snake_case.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextSnakeCase extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "snakeCase";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Convert string to snake_case (lowercase with underscores)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null || str.isEmpty())
      return str;

    final StringBuilder result = new StringBuilder(str.length() + 10);
    boolean lastWasUnderscore = false;

    for (int i = 0; i < str.length(); i++) {
      final char c = str.charAt(i);
      if (!Character.isLetterOrDigit(c)) {
        if (!lastWasUnderscore && result.length() > 0) {
          result.append('_');
          lastWasUnderscore = true;
        }
      } else if (Character.isUpperCase(c)) {
        if (!lastWasUnderscore && result.length() > 0) {
          result.append('_');
        }
        result.append(Character.toLowerCase(c));
        lastWasUnderscore = false;
      } else {
        result.append(c);
        lastWasUnderscore = false;
      }
    }

    return result.toString();
  }
}
