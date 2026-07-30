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
package com.arcadedb.function.math;

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;

/**
 * round() function - rounds a number to the nearest integer or to a specified number of decimal places.
 * <p>
 * Supports:
 * <ul>
 *   <li>round(value) - rounds to nearest integer (HALF_UP)</li>
 *   <li>round(value, precision) - rounds to the given number of decimal places (HALF_UP)</li>
 *   <li>round(value, precision, mode) - rounds to the given number of decimal places using the
 *       specified rounding mode (UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN, HALF_EVEN)</li>
 * </ul>
 */
public class RoundFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "round";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 1 || args.length > 3)
      throw new CommandSemanticException("round() requires 1, 2 or 3 arguments");

    // Rejects anything outside INTEGER | FLOAT as a client-facing type error rather than a 500 (issue #5484).
    final Number number = CypherFunctionHelper.requireNumberArgument(args[0], "round");
    if (number == null)
      return null;

    final double value = number.doubleValue();

    if (Double.isNaN(value) || Double.isInfinite(value))
      return value;

    if (args.length == 1) {
      // round(value) - round to nearest integer
      return (double) Math.round(value);
    }

    // round(value, precision) or round(value, precision, mode)
    final Number precisionArg = CypherFunctionHelper.requireNumberArgument(args[1], "round");
    if (precisionArg == null)
      return null;

    final int precision = precisionArg.intValue();

    RoundingMode mode = RoundingMode.HALF_UP;
    if (args.length == 3 && args[2] != null) {
      final String modeStr = args[2].toString().toUpperCase(Locale.ROOT).replace(" ", "_");
      mode = switch (modeStr) {
        case "UP" -> RoundingMode.UP;
        case "DOWN" -> RoundingMode.DOWN;
        case "CEILING" -> RoundingMode.CEILING;
        case "FLOOR" -> RoundingMode.FLOOR;
        case "HALF_UP" -> RoundingMode.HALF_UP;
        case "HALF_DOWN" -> RoundingMode.HALF_DOWN;
        case "HALF_EVEN" -> RoundingMode.HALF_EVEN;
        // An unusable mode name is the caller's mistake too, so it must not surface as a 500 either (issue #5484).
        default -> throw new CommandSemanticException("round() unknown rounding mode: " + args[2]
            + ". Valid modes are UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN and HALF_EVEN");
      };
    }

    final BigDecimal bd = BigDecimal.valueOf(value).setScale(precision, mode);
    return bd.doubleValue();
  }
}
