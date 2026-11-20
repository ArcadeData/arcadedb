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

import java.util.regex.Matcher;

/**
 * text.replace(string, regex, replacement) - Replace every match of a regular expression, as {@code apoc.text.replace} does
 * ({@code String.replaceAll}): the replacement refers to groups as {@code $1} or {@code ${name}}, and a null argument answers null
 * (issue #12104).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextReplace extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "replace";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Replace every match of a regular expression with the replacement, which can refer to groups as $1 or ${name}";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    final String regex = asString(args[1]);
    final String replacement = asString(args[2]);
    if (str == null || regex == null || replacement == null)
      return null;

    // A match never starts or ends between the two halves of a surrogate pair (issue #10485): '' puts the replacement before every
    // character and after the last, an emoji being one character, and the text of a match that does is kept as it is
    return applyRegex(regex, str, context, (pattern, input) -> {
      try {
        return pattern.matcher(input).replaceAll(
            match -> splitsSurrogatePair(str, match.start(), match.end()) ? Matcher.quoteReplacement(match.group()) : replacement);
      } catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
        // a group the pattern does not have ($2), or a lone '$' or '\' at the end of the replacement
        throw new IllegalArgumentException("text.replace() cannot apply the replacement '" + replacement + "': " + e.getMessage(), e);
      }
    });
  }
}
