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

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * text.decapitalizeAll(string) - Lowercase first letter of each word.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextDecapitalizeAll extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "decapitalizeAll";
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
    return "Lowercase the first letter of each word in the string";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null || str.isEmpty())
      return str;

    final StringBuilder result = new StringBuilder(str.length());
    boolean decapitalizeNext = true;

    // By code point (issue #10484): a cased letter outside the BMP is two UTF-16 units, and neither of them has a case
    for (int i = 0; i < str.length(); ) {
      final int c = str.codePointAt(i);
      i += Character.charCount(c);
      if (Character.isWhitespace(c)) {
        decapitalizeNext = true;
        result.appendCodePoint(c);
      } else if (decapitalizeNext) {
        result.appendCodePoint(Character.toLowerCase(c));
        decapitalizeNext = false;
      } else {
        result.appendCodePoint(c);
      }
    }

    return result.toString();
  }
}
