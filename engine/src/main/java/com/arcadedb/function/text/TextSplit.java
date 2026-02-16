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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * text.split(string, delimiter) - Split string into list.
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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Split string into a list using the specified delimiter";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final String delimiter = asString(args[1]);
    if (delimiter == null || delimiter.isEmpty()) {
      // Split into individual characters
      return Arrays.asList(str.split(""));
    }

    return Arrays.asList(str.split(Pattern.quote(delimiter)));
  }
}
