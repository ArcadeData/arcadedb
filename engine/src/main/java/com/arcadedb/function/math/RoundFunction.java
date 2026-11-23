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
 *   <li>round(value) - rounds to nearest integer</li>
 *   <li>round(value, precision) - rounds to the given number of decimal places</li>
 *   <li>round(value, precision, mode) - rounds to the given number of decimal places using the
 *       specified rounding mode (UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN, HALF_EVEN, UNNECESSARY)</li>
 * </ul>
 * The default mode is not a single one: a value rounded to a whole number ties toward positive infinity, like
 * {@code round(value)} does ({@code round(-2.5)} and {@code round(-2.5, 0)} both answer -2.0), while a value
 * rounded to one or more decimals ties away from zero ({@code round(-2.25, 1)} answers -2.3). Neo4j answers the
 * same, so the two forms of the function agree with it and with each other (issue #12398).
 */
public class RoundFunction implements StatelessFunction {
  /**
   * Below this scale the rounding unit 10^-scale is more than twice Double.MAX_VALUE (below 10^309), so any finite double
   * rounds to 0 or to plus or minus 10^-scale, which converts to an infinity: every scale below it answers alike.
   */
  private static final int MIN_EFFECTIVE_SCALE = -400;

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
    // An explicitly written null mode propagates, as every argument before it already does; only an omitted mode lets
    // the default apply (issue #5629). It is not parsed on the way: parsing it only to discard the mode it would yield
    // would also make parseRoundingMode's contract untrue. A mode that is present and not null is still parsed before
    // propagation decides the answer, so round(null, 2, 'SIDEWAYS') reports the unusable mode rather than answering null.
    final boolean nullMode = CypherFunctionHelper.isExplicitNull(args, 2);
    final RoundingMode explicitMode = args.length == 3 && !nullMode ? parseRoundingMode(args[2]) : null;

    if (number == null || nullMode)
      return null;

    final double value = number.doubleValue();

    if (Double.isNaN(value) || Double.isInfinite(value))
      return value;

    if (args.length == 1)
      // round(value) - round to nearest integer, with a tie going toward positive infinity
      return roundToNearestInteger(value);

    // round(value, precision) or round(value, precision, mode)
    if (precisionArg == null)
      return null;

    // Issue #10850: a whole number in the int range BigDecimal's scale takes - intValue() read 2.9 as 2 and wrapped
    // 4294967298 to 2
    final int precision = CypherFunctionHelper.requireWholeIntArgument(precisionArg, "round", "precision");
    final BigDecimal bd = BigDecimal.valueOf(value);
    // setScale() materializes 10^|precision - scale| as a BigInteger: a precision of 10^8 ran for minutes and one near the
    // int range overflowed BigInteger (an internal error). Clamping does not change the answer: a scale past the value's
    // own only appends zeros, and below MIN_EFFECTIVE_SCALE every finite double rounds to 0 or to a power of ten no double
    // can hold (an infinity)
    final int scale = Math.max(Math.min(precision, bd.scale()), MIN_EFFECTIVE_SCALE);
    // A whole number is rounded the way round(value) rounds it, so that the two forms of the function cannot disagree
    // on the same value: the tie goes toward positive infinity and round(-2.5) and round(-2.5, 0) both answer -2.0,
    // as they do in Neo4j (issue #12398). One or more decimals tie away from zero, the way they always did
    // (round(-2.25, 1) answers -2.3). An explicitly written mode always wins.
    if (explicitMode == null && precision == 0)
      return roundToNearestInteger(value);
    final RoundingMode mode = explicitMode != null ? explicitMode : RoundingMode.HALF_UP;
    try {
      return bd.setScale(scale, mode).doubleValue();
    } catch (final ArithmeticException e) {
      // The UNNECESSARY mode refuses a value that would have to be rounded, the way Neo4j refuses it (issue #12398).
      // The refusal is the caller's mistake, so it is reported as a client error and not as an internal failure.
      throw new CommandSemanticException("Rounding necessary", e);
    }
  }

  /**
   * Rounds a value to the nearest whole number, with a tie going toward positive infinity: the answer round(value)
   * gives, and the one Neo4j gives for a value rounded to a whole number (issue #12398).
   */
  private static double roundToNearestInteger(final double value) {
    // From 2^52 up every double is a whole number already, and Math.round() saturates at the long range:
    // round(1.0e19) answered 9.223372036854776E18 (issue #11627)
    return Math.abs(value) >= 0x1p52 ? value : (double) Math.round(value);
  }

  /**
   * Resolves the third argument of {@code round()} to a rounding mode. Shared with the parse-time check in
   * {@code CypherSemanticValidator}, which applies it to a mode written as a literal so that the two paths accept
   * exactly the same set of names and word an unknown one identically.
   * <p>
   * A {@code null} argument means the mode was omitted, and yields HALF_UP. {@code execute} does not call this method
   * for a mode written as an explicit {@code null}: that propagates instead, per
   * {@link CypherFunctionHelper#isExplicitNull} (issue #5629), and an omitted mode does not reach it either, because
   * the default it selects depends on the precision (see {@link #execute}). The parse-time check in
   * {@code CypherSemanticValidator} likewise skips null literals, so the {@code null} branch below serves callers that
   * pass the omitted-argument sentinel directly.
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
      // The mode Neo4j accepts: it answers the value when no rounding is needed and refuses it otherwise
      // (issue #12398)
      case "UNNECESSARY" -> RoundingMode.UNNECESSARY;
      default -> throw new CommandSemanticException("round() unknown rounding mode: " + mode
          + ". Valid modes are UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN, HALF_EVEN and UNNECESSARY");
    };
  }
}
