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
 * text.capitalizeAll(string) - Capitalize the first letter of each word, as {@code apoc.text.capitalizeAll} does (issue #12104): the
 * words are separated by spaces, their first character goes to title case, and the result is trimmed, so {@code '  padded  '} is
 * {@code 'Padded'}.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextCapitalizeAll extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "capitalizeAll";
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
    return "Capitalize the first letter of each space-separated word in the string, then trim it";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    // By code point (issue #10484): a cased letter outside the BMP is two UTF-16 units, and neither of them has a case
    final StringBuilder result = new StringBuilder(str.length());
    for (final String word : str.split(" "))
      result.append(titleCaseFirst(word)).append(' ');
    return result.toString().trim();
  }
}
