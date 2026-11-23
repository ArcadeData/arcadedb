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
import java.util.regex.Pattern;

/**
 * text.slug(string, [delimiter]) - Replace every run of characters other than letters, ASCII digits and '_' with the delimiter
 * (default '-') after trimming the string, as {@code apoc.text.slug} does (issue #12104): the case and the letters are kept, so
 * {@code 'Hello World'} is {@code 'Hello-World'}. A null delimiter answers null.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextSlug extends AbstractTextFunction {
  private static final Pattern NON_WORD = Pattern.compile("[^\\p{L}0-9_]+");

  @Override
  protected String getSimpleName() {
    return "slug";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Trim the string and replace every run of characters other than letters, digits and '_' with the delimiter (default '-')";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    final String delimiter = args.length > 1 ? asString(args[1]) : "-";
    if (str == null || delimiter == null)
      return null;

    // The delimiter is literal text: APOC reads '$1' or '\' in it as a replacement pattern and fails
    return NON_WORD.matcher(str.trim()).replaceAll(Matcher.quoteReplacement(delimiter));
  }
}
