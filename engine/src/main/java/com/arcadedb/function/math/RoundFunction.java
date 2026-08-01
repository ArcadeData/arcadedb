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
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);

    // Every argument is checked before null propagation decides the answer, so an out-of-domain precision or an unusable
    // mode is reported even when the value is null - the same ordering MathBinaryFunction uses, and the one the
    // parse-time check applies, which examines each argument independently (issue #5484).
    final Number number = CypherFunctionHelper.requireNumberArgument(args[0], "round");
    final Number precisionArg = args.length > 1 ? CypherFunctionHelper.requireNumberArgument(args[1], "round") : null;
    // An explicitly written null mode propagates, as every argument before it already does; only an omitted mode selects
    // HALF_UP (issue #5629). It is not parsed on the way: parsing it only to discard the HALF_UP it would yield would
    // also make parseRoundingMode's contract untrue. A mode that is present and not null is still parsed before
    // propagation decides the answer, so round(null, 2, 'SIDEWAYS') reports the unusable mode rather than answering null.
    final boolean nullMode = CypherFunctionHelper.isExplicitNull(args, 2);
    final RoundingMode mode = args.length == 3 && !nullMode ? parseRoundingMode(args[2]) : RoundingMode.HALF_UP;

    if (number == null || nullMode)
      return null;

    final double value = number.doubleValue();

    if (Double.isNaN(value) || Double.isInfinite(value))
      return value;

    if (args.length == 1) {
      // round(value) - round to nearest integer
      return (double) Math.round(value);
    }

    // round(value, precision) or round(value, precision, mode)
    if (precisionArg == null)
      return null;

    final BigDecimal bd = BigDecimal.valueOf(value).setScale(precisionArg.intValue(), mode);
    return bd.doubleValue();
  }

  /**
   * Resolves the optional third argument of {@code round()} to a rounding mode, defaulting to HALF_UP when it is absent.
   * Shared with the parse-time check in {@code CypherSemanticValidator}, which applies it to a mode written as a
   * literal so that the two paths accept exactly the same set of names and word an unknown one identically.
   * <p>
   * A {@code null} argument means the mode was omitted, and yields HALF_UP. {@code execute} does not call this method
   * for a mode written as an explicit {@code null}: that propagates instead, per
   * {@link CypherFunctionHelper#isExplicitNull} (issue #5629). The parse-time check in {@code CypherSemanticValidator}
   * likewise skips null literals, so the {@code null} branch below serves callers that pass the omitted-argument
   * sentinel directly.
   *
   * @throws CommandSemanticException when the name is not one of the supported modes: an unusable mode is the caller's
   *                                  mistake, so it must not surface as an internal 500 either (issue #5484)
   */
  public static RoundingMode parseRoundingMode(final Object mode) {
    if (mode == null)
      return RoundingMode.HALF_UP;

    return switch (mode.toString().toUpperCase(Locale.ROOT).replace(" ", "_")) {
      case "UP" -> RoundingMode.UP;
      case "DOWN" -> RoundingMode.DOWN;
      case "CEILING" -> RoundingMode.CEILING;
      case "FLOOR" -> RoundingMode.FLOOR;
      case "HALF_UP" -> RoundingMode.HALF_UP;
      case "HALF_DOWN" -> RoundingMode.HALF_DOWN;
      case "HALF_EVEN" -> RoundingMode.HALF_EVEN;
      default -> throw new CommandSemanticException("round() unknown rounding mode: " + mode
          + ". Valid modes are UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN and HALF_EVEN");
    };
  }
}
