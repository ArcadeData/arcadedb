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
package com.arcadedb.serializer.json;

/**
 * THE encoding of the three doubles JSON cannot represent - {@code NaN}, {@code +Infinity} and {@code -Infinity} -
 * as the marker strings ArcadeDB's own formats read back.
 * <p>
 * JSON has no literal for any of them, and {@link JSONArray#put(Number)} resolves that by rewriting them to
 * {@code 0} - which is a measurement of zero, indistinguishable from data. Every writer that must preserve the
 * distinction therefore substitutes a string, and every reader of that writer's output must substitute back with
 * the SAME three tokens: this class is where they live, so a change on one side cannot silently diverge from the
 * other (record properties through {@code JsonGraphSerializer}, TIMESERIES samples through the JSONL
 * exporter/importer).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class NonFiniteNumbers {

  public static final String NAN               = "NaN";
  public static final String POSITIVE_INFINITY = "PosInfinity";
  public static final String NEGATIVE_INFINITY = "NegInfinity";

  private NonFiniteNumbers() {
  }

  /**
   * Returns the marker string for a non-finite {@code Double}/{@code Float}, and {@code value} itself for anything
   * else - so a caller can pipe every value through it.
   */
  public static Object encode(final Object value) {
    if (value instanceof Double d && !Double.isFinite(d))
      return markerOf(d);
    if (value instanceof Float f && !Float.isFinite(f))
      return markerOf(f);
    return value;
  }

  /**
   * The inverse of {@link #encode(Object)} for a single token.
   *
   * @return the double the marker stands for, or {@code null} if {@code text} is not one of the three markers
   */
  public static Double decode(final String text) {
    return switch (text) {
      case NAN -> Double.NaN;
      case POSITIVE_INFINITY -> Double.POSITIVE_INFINITY;
      case NEGATIVE_INFINITY -> Double.NEGATIVE_INFINITY;
      case null, default -> null;
    };
  }

  private static String markerOf(final double value) {
    if (Double.isNaN(value))
      return NAN;
    return value > 0 ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
  }
}
