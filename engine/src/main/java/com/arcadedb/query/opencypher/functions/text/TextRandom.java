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

import java.util.concurrent.ThreadLocalRandom;

/**
 * text.random(length, [chars]) - Generate random string.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextRandom extends AbstractTextFunction {
  private static final String DEFAULT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  @Override
  protected String getSimpleName() {
    return "random";
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
    return "Generate a random string of the specified length using optional character set";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final int length = asInt(args[0], 0);
    if (length <= 0)
      return "";

    final String chars = args.length > 1 ? asString(args[1]) : DEFAULT_CHARS;
    final String charSet = (chars != null && !chars.isEmpty()) ? chars : DEFAULT_CHARS;

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final StringBuilder result = new StringBuilder(length);

    for (int i = 0; i < length; i++) {
      result.append(charSet.charAt(random.nextInt(charSet.length())));
    }

    return result.toString();
  }
}
