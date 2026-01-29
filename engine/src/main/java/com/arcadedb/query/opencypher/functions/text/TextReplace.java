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
 * text.replace(string, search, replace) - Replace all occurrences of search with replace.
 *
 * @author ArcadeDB Team
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
    return "Replace all occurrences of search string with replacement";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final String search = asString(args[1]);
    final String replacement = asString(args[2]);

    if (search == null)
      return str;

    return str.replace(search, replacement == null ? "" : replacement);
  }
}
