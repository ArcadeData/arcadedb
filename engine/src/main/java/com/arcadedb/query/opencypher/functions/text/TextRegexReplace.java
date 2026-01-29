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

import java.util.regex.Pattern;

/**
 * text.regexReplace(string, regex, replace) - Replace using regular expression.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextRegexReplace extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "regexReplace";
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
    return "Replace all matches of a regular expression with replacement";
  }

  private static final int MAX_PATTERN_LENGTH = 500;

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final String regex = asString(args[1]);
    final String replacement = asString(args[2]);

    if (regex == null)
      return str;

    // Validate pattern length to prevent ReDoS attacks
    if (regex.length() > MAX_PATTERN_LENGTH) {
      throw new IllegalArgumentException(
          "Regex pattern exceeds maximum allowed length (" + MAX_PATTERN_LENGTH + "): " + regex.length());
    }

    try {
      return Pattern.compile(regex).matcher(str).replaceAll(replacement == null ? "" : replacement);
    } catch (final java.util.regex.PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regex pattern: " + e.getMessage(), e);
    } catch (final StackOverflowError e) {
      // Catastrophic backtracking can cause stack overflow
      throw new IllegalArgumentException(
          "Regex pattern caused stack overflow (possible catastrophic backtracking): " + regex, e);
    }
  }
}
