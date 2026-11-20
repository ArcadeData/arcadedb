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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * text.split(string, regex, [limit]) - Split a string around the matches of a regular expression, as {@code apoc.text.split} does
 * ({@code String.split(regex, limit)}, issue #12104): the limit caps the number of pieces when positive, keeps the trailing empty
 * pieces when negative, and drops them when 0, the default. A null argument answers null.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextSplit extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "split";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Split a string around the matches of a regular expression; a positive limit caps the number of pieces, a negative one "
        + "keeps the trailing empty pieces and 0 (the default) drops them";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    final String regex = asString(args[1]);
    if (str == null || regex == null || args.length > 2 && args[2] == null)
      return null;

    // APOC truncates the INTEGER limit to an int, so 4294967298 is 2; a limit past the int range is read as the nearest int instead
    final int limit = args.length > 2 ?
        Math.clamp(CypherFunctionHelper.requireWholeNumberOrTextArgument(args[2], getName(), "limit"), Integer.MIN_VALUE,
            Integer.MAX_VALUE) :
        0;
    return applyRegex(regex, str, context, (pattern, input) -> split(pattern.matcher(input), str, limit));
  }

  /**
   * {@code Pattern.split(str, limit)}, except that a match that starts or ends between the two halves of a surrogate pair is not a
   * separator: the empty regex splits {@code 'a😀'} into {@code ['a', '😀']}, not into the two halves of the emoji (issue #10485).
   */
  private static List<String> split(final Matcher matcher, final String str, final int limit) {
    final List<String> pieces = new ArrayList<>();
    int index = 0;
    while (matcher.find()) {
      final int start = matcher.start();
      final int end = matcher.end();
      // no leading empty piece for a match of no characters at the start, as Pattern.split
      if (start == end && start == 0 || splitsSurrogatePair(str, start, end))
        continue;
      if (limit > 0 && pieces.size() == limit - 1) {
        pieces.add(str.substring(index));
        index = end;
        break;
      }
      pieces.add(str.substring(index, start));
      index = end;
    }

    if (index == 0)
      return List.of(str);

    if (limit <= 0 || pieces.size() < limit)
      pieces.add(str.substring(index));

    if (limit == 0)
      while (!pieces.isEmpty() && pieces.get(pieces.size() - 1).isEmpty())
        pieces.remove(pieces.size() - 1);
    return pieces;
  }
}
