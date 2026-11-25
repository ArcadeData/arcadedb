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
 * text.code(codepoint) - The one-character string of a Unicode code point, as {@code apoc.text.code} answers ({@code 65} is
 * {@code 'A'}, issue #12104); null for a null argument or a number that is not a code point.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextCode extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "code";
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
    return "Get the one-character string of a Unicode code point";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    // Every code point, as text.charAt() answers them (issue #10378): APOC stops at U+FFFF, the last one a Java char holds
    final long codePoint = CypherFunctionHelper.requireWholeNumberOrTextArgument(args[0], getName(), "codepoint");
    if (codePoint < 0 || codePoint > Character.MAX_CODE_POINT)
      return null;
    return Character.toString((int) codePoint);
  }
}
