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
import com.arcadedb.utility.StringUtils;

import java.util.List;
import java.util.Locale;

/**
 * text.format(format, params, [language]) - Format a string with Java's {@code String.format}, as {@code apoc.text.format} does
 * (issue #12104): the parameters are ONE list, and the language (default {@code 'en'}) decides the decimal and grouping separators.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextFormat extends AbstractTextFunction {
  private static final String DEFAULT_LANGUAGE = "en";

  @Override
  protected String getSimpleName() {
    return "format";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Format string using Java's String.format() with the parameters given as one list, in the language given (default 'en')";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String format = asString(args[0]);
    if (format == null)
      return null;

    if (args.length < 2 || args[1] == null)
      return format;

    // A value that is not a list is the one parameter
    final List<Object> params = CypherFunctionHelper.asListOrNull(args[1]);
    final Object[] values = params != null ? params.toArray() : new Object[] { args[1] };

    // APOC fails on a null language; it is the default one here
    final String language = args.length > 2 && args[2] != null ? args[2].toString() : DEFAULT_LANGUAGE;
    return StringUtils.format(getName(), Locale.of(language), format, values);
  }
}
