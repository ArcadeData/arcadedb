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
package com.arcadedb.function.number;

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * number.format(number, pattern) - Formats a number to a string using a {@link DecimalFormat} pattern.
 * <p>
 * Always uses {@link Locale#ROOT} symbols (','  grouping, '.' decimal separator) regardless of the JVM
 * default locale, matching the convention the rest of the codebase follows for locale-sensitive
 * formatting/parsing.
 */
public class NumberFormat extends AbstractNumberFunction {
  private static final String DEFAULT_PATTERN = "#,##0.###";

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Formats a number to a string using an optional DecimalFormat pattern";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final Number number = CypherFunctionHelper.requireNumberArgument(args[0], getName());
    // An explicitly-written null pattern propagates, same convention round()'s mode argument follows: only an
    // omitted pattern selects the default, an explicit null is not distinguishable from "the caller wants no result".
    final boolean nullPattern = CypherFunctionHelper.isExplicitNull(args, 1);
    if (number == null || nullPattern)
      return null;

    final String pattern = args.length > 1 && args[1] != null ? args[1].toString() : DEFAULT_PATTERN;

    final DecimalFormat format = new DecimalFormat(pattern, DecimalFormatSymbols.getInstance(Locale.ROOT));
    return format.format(number.doubleValue());
  }
}
