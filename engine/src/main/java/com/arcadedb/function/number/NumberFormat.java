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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;

/**
 * number.format(number, pattern, language) - Formats a number to a string using a {@link DecimalFormat} pattern.
 * <p>
 * Without a language it always uses {@link Locale#ROOT} symbols (','  grouping, '.' decimal separator) regardless of the
 * JVM default locale, matching the convention the rest of the codebase follows for locale-sensitive formatting/parsing.
 * The optional language is APOC's third argument: a bare ISO 639 language code ({@code 'de'}, not {@code 'de-DE'})
 * selects that language's symbols, and an empty pattern its default pattern; any other text answers null, as in APOC
 * (issue #12105).
 */
public class NumberFormat extends AbstractNumberFunction {
  private static final String      DEFAULT_PATTERN = "#,##0.###";
  // copyOf, not of: the list may name a code twice
  private static final Set<String> ISO_LANGUAGES   = Set.copyOf(Arrays.asList(Locale.getISOLanguages()));

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
    return "Formats a number to a string using an optional DecimalFormat pattern and an optional ISO 639 language";
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

    final Object language = args.length > 2 ? args[2] : null;
    final DecimalFormat format;
    if (language == null || "".equals(language))
      format = new DecimalFormat(pattern, DecimalFormatSymbols.getInstance(Locale.ROOT));
    else if (!(language instanceof String code))
      throw CypherFunctionHelper.typeMismatch(getName(), "a STRING language", language);
    else if (!ISO_LANGUAGES.contains(code))
      return null;
    else
      format = localized(Locale.of(code), pattern);
    // Exact types are handed to DecimalFormat as they are instead of being rounded to the nearest double. Anything else
    // (Float, Double and any other Number subclass, which DecimalFormat.format(Object) would refuse) goes through double.
    if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte)
      return format.format(number.longValue());
    if (number instanceof BigInteger || number instanceof BigDecimal)
      return format.format(number);
    return format.format(number.doubleValue());
  }

  /**
   * The language's own number format, as APOC builds it: its symbols, and its default pattern unless one is given.
   */
  private static DecimalFormat localized(final Locale locale, final String pattern) {
    final DecimalFormat format = DecimalFormat.getInstance(locale) instanceof DecimalFormat decimal ?
        decimal :
        new DecimalFormat(DEFAULT_PATTERN, DecimalFormatSymbols.getInstance(locale));
    if (!pattern.isEmpty())
      format.applyPattern(pattern);
    return format;
  }
}
