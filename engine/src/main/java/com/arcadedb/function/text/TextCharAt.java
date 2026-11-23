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

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * text.charAt(string, index) - The code point (INTEGER) of the character at an index, as {@code apoc.text.charAt} answers
 * ({@code 'hello', 1} is 101, issue #12104); null for a null argument or an index outside the string.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextCharAt extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "charAt";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Get the Unicode code point (INTEGER) of the character at the specified index (0-based)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null || args[1] == null)
      return null;

    // A character is a Unicode code point, never half of a surrogate pair (issue #10378): APOC counts UTF-16 units and answers the
    // halves of an emoji. An index past the int range is refused, not wrapped (issue #10850)
    final String character = CypherFunctionHelper.charAtCodePoint(str, asInt(args[1], 0, "index"));
    return character == null ? null : (long) character.codePointAt(0);
  }
}
