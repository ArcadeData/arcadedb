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
 * text.indexOf(string, substring, [from], [to]) - Find position of substring.
 * Returns -1 if not found. {@code to}, APOC's fourth argument, bounds the search as in APOC: a match must end at or
 * before position {@code to}; -1 (the default) or a {@code to} past the end searches the whole string, and a {@code to}
 * at or before {@code from} finds nothing (issue #12105).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextIndexOf extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "indexOf";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Find position of substring in string, optionally starting from specified position";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final String substring = asString(args[1]);
    if (substring == null)
      return null;

    final int start = args.length > 2 ? asInt(args[2], 0, "start") : 0;
    final int end = args.length > 3 ? asInt(args[3], -1, "to") : -1;
    // The position of a character, not of a UTF-16 code unit (issue #10378)
    if (end == -1 || end > str.codePointCount(0, str.length()))
      return (long) CypherFunctionHelper.indexOfCodePoints(str, substring, start);
    if (end <= start || end < 0)
      return -1L;
    return (long) CypherFunctionHelper.indexOfCodePoints(str.substring(0, str.offsetByCodePoints(0, end)), substring, start);
  }
}
