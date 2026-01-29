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
 * text.rpad(string, length, padChar) - Right pad string to specified length.
 *
 * @author ArcadeDB Team
 */
public class TextRpad extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "rpad";
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
    return "Right pad string to the specified length with the specified character";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final int length = asInt(args[1], 0);
    final String padStr = asString(args[2]);
    final char padChar = (padStr != null && !padStr.isEmpty()) ? padStr.charAt(0) : ' ';

    if (str.length() >= length)
      return str;

    final StringBuilder result = new StringBuilder(length);
    result.append(str);
    for (int i = str.length(); i < length; i++) {
      result.append(padChar);
    }

    return result.toString();
  }
}
