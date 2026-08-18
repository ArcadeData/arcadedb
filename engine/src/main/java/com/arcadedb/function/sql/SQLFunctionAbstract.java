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
package com.arcadedb.function.sql;

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.utility.NumberUtils;

/**
 * Abstract class to extend to build Custom SQL Functions.
 * <p>
 * Extends the unified function system via {@link SQLFunction} which implements
 * {@link com.arcadedb.function.RecordFunction}.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class SQLFunctionAbstract implements SQLFunction {
  protected final String name;

  public SQLFunctionAbstract(final String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * Returns the minimum number of arguments required.
   * Subclasses should override this to specify their requirements.
   *
   * @return minimum argument count (default: 0)
   */
  @Override
  public int getMinArgs() {
    return 0;
  }

  /**
   * Returns the maximum number of arguments allowed.
   * Subclasses should override this to specify their requirements.
   *
   * @return maximum argument count (default: Integer.MAX_VALUE)
   */
  @Override
  public int getMaxArgs() {
    return Integer.MAX_VALUE;
  }

  /**
   * Returns a description of the function for documentation.
   * Subclasses should override this to provide meaningful documentation.
   *
   * @return function description (default: the syntax)
   */
  @Override
  public String getDescription() {
    return getSyntax();
  }

  @Override
  public String toString() {
    return getSyntax();
  }

  @Override
  public SQLFunction config(final Object[] iConfiguredParameters) {
    return this;
  }

  /**
   * Validates a value a numeric function is about to accumulate: {@code null} passes through (nulls are skipped,
   * matching the documented aggregation behavior), a {@link Number} passes through unchanged, and anything else
   * (STRING, BOOLEAN, a collection, a record, ...) is a client-facing type error naming the function and the type
   * it got.
   * <p>
   * Issue #5799 introduced this for {@code sum()} so a non-numeric element stopped being silently dropped - which
   * left the accumulator unchanged and made an all-invalid input indistinguishable from an all-null one. Issue
   * #6390 pulled it up here because every sibling still did a raw {@code (Number)} cast and answered a
   * ClassCastException (HTTP 500) where {@code sum()} answered a typed error (HTTP 400).
   *
   * @param value the value to validate
   *
   * @return the value as a {@link Number}, or {@code null}
   *
   * @throws IllegalArgumentException if the value is neither null nor a number
   */
  protected Number requireNumericOrNull(final Object value) {
    if (value == null || value instanceof Number)
      return (Number) value;
    throw new IllegalArgumentException(
        getName() + "() requires numeric input, but received a value of type " + value.getClass().getSimpleName());
  }

  /**
   * Orders two values the way {@code max()} / {@code min()} need them ordered, answering a typed argument error when
   * they are not mutually comparable instead of the ClassCastException the raw {@code ((Comparable) a).compareTo(b)}
   * threw for something as ordinary as {@code max([1, 'a'])} (issue #6389). Numeric widening is the caller's job -
   * both functions run {@code Type.castComparableNumber} first, so this only ever sees values already brought to a
   * common numeric class, or values of genuinely different kinds.
   *
   * @param left  the value to place
   * @param right the value to place it against
   *
   * @return the sign of the comparison, as {@link Comparable#compareTo}
   *
   * @throws IllegalArgumentException if the two values cannot be compared with each other
   */
  @SuppressWarnings("unchecked")
  protected int compareValues(final Object left, final Object right) {
    try {
      return ((Comparable<Object>) left).compareTo(right);
    } catch (final ClassCastException e) {
      throw new IllegalArgumentException(
          getName() + "() cannot compare a value of type " + left.getClass().getSimpleName() + " with a value of type "
              + right.getClass().getSimpleName(), e);
    }
  }

  /**
   * The boolean analogue of {@link #requireNumericOrNull(Object)}: {@code null} passes through, a {@link Boolean}
   * passes through, anything else is a client-facing type error rather than a ClassCastException - or, worse, a
   * silently unchanged accumulator that returns a confident answer for input it never looked at (issue #6389).
   *
   * @param value the value to validate
   *
   * @return the value as a {@link Boolean}, or {@code null}
   *
   * @throws IllegalArgumentException if the value is neither null nor a boolean
   */
  protected Boolean requireBooleanOrNull(final Object value) {
    if (value == null || value instanceof Boolean)
      return (Boolean) value;
    throw new IllegalArgumentException(
        getName() + "() requires boolean input, but received a value of type " + value.getClass().getSimpleName());
  }

  /**
   * The strict counterpart of {@link #requireNumericOrNull(Object)} for a scalar CONFIGURATION argument - a window
   * size, a percentile, an offset - where {@code null} is not a value to skip but a missing setting.
   *
   * @param value        the argument value
   * @param argumentName the argument's name, for the error message
   *
   * @return the value as a {@link Number}
   *
   * @throws IllegalArgumentException if the value is null or not a number
   */
  protected Number requireNumeric(final Object value, final String argumentName) {
    if (value instanceof Number number)
      return number;
    throw new IllegalArgumentException(
        getName() + "() requires a numeric <" + argumentName + ">, but received " + (value == null ?
            "null" :
            "a value of type " + value.getClass().getSimpleName()));
  }

  /**
   * Reads a scalar CONFIGURATION argument as an int, saturating rather than wrapping: {@code Number.intValue()} on a
   * value outside the int range silently returns an unrelated number, which for an offset or a window size drives a
   * degenerate computation that still looks plausible. The {@link com.arcadedb.query.sql.method.AbstractSQLMethod}
   * side of the same batch takes the same care with character indexes.
   *
   * @param value        the argument value
   * @param argumentName the argument's name, for the error message
   *
   * @return the value as an int, saturated to the int range
   *
   * @throws IllegalArgumentException if the value is null or not a number
   */
  protected int requireIntArgument(final Object value, final String argumentName) {
    final Integer number = NumberUtils.saturateToIntOrNull(value);
    if (number != null)
      return number;

    throw new IllegalArgumentException(
        getName() + "() requires a numeric <" + argumentName + ">, but received " + NumberUtils.describeRejectedNumber(
            value));
  }

  /**
   * Converts various input types (float[], double[], Object[], List) to a float array.
   * Delegates to {@link com.arcadedb.index.vector.VectorUtils#toFloatArray(Object)}.
   *
   * @param vector The input vector (can be float[], double[], Object[], or List)
   *
   * @return float array representation
   *
   * @throws CommandSQLParsingException if input type is invalid or contains non-numeric elements
   */
  protected float[] toFloatArray(final Object vector) {
    try {
      return VectorUtils.toFloatArray(vector);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException(e.getMessage());
    }
  }

  /**
   * Null-tolerant variant of {@link #toFloatArray(Object)} that maps {@code null} collection elements to
   * {@link Float#NaN}. Used by the validity-check functions (vector.hasNaN / vector.hasInf) so a NULL produced
   * by an invalid SQL math op (e.g. sqrt(-1.0), coerced to NULL inside a collection) is detected as NaN rather
   * than crashing the conversion. Delegates to {@link com.arcadedb.index.vector.VectorUtils#toFloatArrayNaNForNull(Object)}.
   *
   * @param vector The input vector (can be float[], double[], Object[], or List)
   *
   * @return float array representation, with null elements replaced by {@link Float#NaN}
   *
   * @throws CommandSQLParsingException if input type is invalid or contains non-numeric, non-null elements
   */
  protected float[] toFloatArrayNaNForNull(final Object vector) {
    try {
      return VectorUtils.toFloatArrayNaNForNull(vector);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException(e.getMessage());
    }
  }
}
