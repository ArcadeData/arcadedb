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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * round() function - rounds a number to the nearest integer or to a specified number of decimal places.
 * <p>
 * Supports:
 * <ul>
 *   <li>round(value) - rounds to nearest integer (HALF_UP)</li>
 *   <li>round(value, precision) - rounds to the given number of decimal places (HALF_UP)</li>
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
      throw new CommandExecutionException("round() requires one or two arguments");

    if (args[0] == null)
      return null;

    if (!(args[0] instanceof Number))
      throw new CommandExecutionException("round() requires a numeric argument");

    final double value = ((Number) args[0]).doubleValue();

    if (Double.isNaN(value) || Double.isInfinite(value))
      return value;

    if (args.length == 1) {
      // round(value) - round to nearest integer
      return (double) Math.round(value);
    }

    // round(value, precision) or round(value, precision, mode)
    if (args[1] == null)
      return null;

    if (!(args[1] instanceof Number))
      throw new CommandExecutionException("round() precision must be a numeric value");

    final int precision = ((Number) args[1]).intValue();

    RoundingMode mode = RoundingMode.HALF_UP;
    if (args.length == 3 && args[2] != null) {
      final String modeStr = args[2].toString().toUpperCase().replace(" ", "_");
      mode = switch (modeStr) {
        case "UP" -> RoundingMode.UP;
        case "DOWN" -> RoundingMode.DOWN;
        case "CEILING" -> RoundingMode.CEILING;
        case "FLOOR" -> RoundingMode.FLOOR;
        case "HALF_UP" -> RoundingMode.HALF_UP;
        case "HALF_DOWN" -> RoundingMode.HALF_DOWN;
        case "HALF_EVEN" -> RoundingMode.HALF_EVEN;
        default -> throw new CommandExecutionException("round() unknown rounding mode: " + args[2]);
      };
    }

    final BigDecimal bd = BigDecimal.valueOf(value).setScale(precision, mode);
    return bd.doubleValue();
  }
}
