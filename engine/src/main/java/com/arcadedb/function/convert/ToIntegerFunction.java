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
package com.arcadedb.function.convert;

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.math.BigDecimal;

/**
 * toInteger() function - converts a value to an integer.
 */
public class ToIntegerFunction implements StatelessFunction {
  private static final BigDecimal LONG_MAX = new BigDecimal(Long.MAX_VALUE);
  private static final BigDecimal LONG_MIN = new BigDecimal(Long.MIN_VALUE);

  @Override
  public String getName() {
    return "toInteger";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args[0] == null)
      return null;
    if (args[0] instanceof Boolean b)
      return b ? 1L : 0L;
    if (args[0] instanceof Number)
      return ((Number) args[0]).longValue();
    if (args[0] instanceof String s)
      return parseInteger(s);
    // Unsupported types: List, Map, Node, Relationship, Path. A client-side type error, so throw a
    // CommandSemanticException to surface it as a 400 client error instead of a 500 transaction-commit
    // failure. Same pattern as toString() (see issue #5203). See issue #5294.
    throw new CommandSemanticException("TypeError: InvalidArgumentValue - toInteger() cannot convert " + args[0].getClass().getSimpleName());
  }

  /**
   * Parses a text as an integer, as Neo4j 5 does (issue #12400): the whole text must be the number, so a text with
   * surrounding whitespace is not a number at all, a fraction is truncated towards zero and a number out of the 64-bit
   * range is a client error instead of a value that does not fit anymore.
   * <p>
   * The range is checked on the value as it is written, before the truncation: {@code '9223372036854775807.9'} does not
   * fit a long even though its truncation would.
   *
   * @return the integer, or {@code null} when the text is not a number
   *
   * @throws CommandSemanticException when the number does not fit in a long
   */
  private static Object parseInteger(final String text) {
    final BigDecimal value;
    try {
      value = new BigDecimal(text);
    } catch (final NumberFormatException e) {
      return null;
    }

    if (value.compareTo(LONG_MAX) > 0 || value.compareTo(LONG_MIN) < 0)
      throw new CommandSemanticException("TypeError: integer, " + text + ", is too large");

    return value.toBigInteger().longValue();
  }
}
