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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.executor.MultiValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A membership test over an {@code IN} right-hand side that is constant for the whole command execution -
 * a literal list or a bound parameter - built once and then probed per row instead of being rebuilt and
 * linearly rescanned for every row.
 * <p>
 * The row-at-a-time evaluator is the only path a negated {@code IN} can take: a {@code NOT IN} has no
 * complement cursor in {@link com.arcadedb.query.sql.executor.FetchFromIndexStep}, so
 * {@link InCondition#isIndexAware} declines the index for it (#6796) and the whole type is scanned with the
 * condition as a residual filter. Without this class that filter costs {@code rows * listSize} - and it is
 * worse than it looks, because {@code rightMathExpression.execute()} rebuilds the entire N-item list from the
 * parse tree once per row before the linear scan even starts. Measured on 20,000 rows: 2,000 literal values
 * took ~1s, scaling linearly in both factors, which puts the reported shape (a 15,000-value {@code NOT IN}
 * over a 36M-row type) in the hours.
 * <p>
 * The fast path is deliberately narrow, because {@link com.arcadedb.query.sql.executor.QueryOperatorEquals}
 * equality is not hash-compatible in general: it coerces across types (a {@code String} equals a
 * {@code Number} whose text matches, a {@code String} equals a {@code RID} it spells) and unwraps
 * single-property {@code Result}s. So a hash set is built only for a list whose every element is a
 * {@code String}, or whose every element is a {@code Number} ({@code null} elements are allowed in either
 * case and recorded separately, since they make a miss UNKNOWN rather than FALSE). It is then consulted only
 * for a left value of that same kind. Anything else - a mixed list, a {@code RID}, a date, a left value of
 * the other kind - falls back to {@link InCondition#evaluateExpressionThreeValued}, which is the exact
 * pre-existing semantics; the fallback is the definition, the hash set is an accelerator for the two shapes
 * that cover essentially every real {@code IN} list.
 * <p>
 * Two deliberate, strictly-more-correct divergences from the linear path remain inside the fast path, both
 * beyond any value a query realistically carries:
 * <ul>
 * <li>numbers are keyed as exact {@link BigDecimal}, where the linear path widens both sides to a common type
 * first ({@code Type.castComparableNumber}) and so reports the {@code long} {@code 9007199254740993} as equal
 * to the {@code double} {@code 9007199254740992.0}; past 2^53 the exact answer is the right one.</li>
 * <li>strings are keyed by {@link String#equals}, where the linear path compares their UTF-8 encodings, which
 * differ only for two distinct unpaired surrogates - both encode to the same replacement byte.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class InListMembership {
  private static final int KIND_NONE   = 0;
  private static final int KIND_STRING = 1;
  private static final int KIND_NUMBER = 2;

  /** The right-hand value exactly as evaluated, and the authority whenever the fast path declines. */
  private final Object      rightValue;
  /** Normalized keys of every non-null element, or {@code null} when no fast path applies. */
  private final Set<Object> keys;
  private final int         kind;
  private final boolean     containsNull;

  private InListMembership(final Object rightValue, final Set<Object> keys, final int kind, final boolean containsNull) {
    this.rightValue = rightValue;
    this.keys = keys;
    this.kind = kind;
    this.containsNull = containsNull;
  }

  /**
   * Builds the probe for an already-evaluated right-hand value. Never returns {@code null}: a value no fast
   * path can index still produces a probe that delegates every test to the linear evaluator, so callers have
   * one shape to handle and the memo entry is written once either way.
   */
  public static InListMembership build(final Object rightValue) {
    if (!(rightValue instanceof Collection) && (rightValue == null || !rightValue.getClass().isArray()))
      // Only the two re-readable shapes a constant list ever takes. A scalar right-hand side degrades to an
      // equality test anyway; a ResultSet or a bare Iterable is a one-shot cursor that indexing here would
      // consume out from under the linear evaluator; a Map's membership semantics are the evaluator's to define.
      return new InListMembership(rightValue, null, KIND_NONE, false);

    int kind = KIND_NONE;
    boolean containsNull = false;
    final Set<Object> keys = new HashSet<>();

    for (final Object item : MultiValue.getMultiValueIterable(rightValue, false)) {
      if (item == null) {
        containsNull = true;
        continue;
      }

      final int itemKind = kindOf(item);
      if (itemKind == KIND_NONE || (kind != KIND_NONE && kind != itemKind))
        // Unsupported element type, or a list mixing strings and numbers: the coercions between the two
        // kinds are the ones a hash set cannot reproduce, so decline rather than get them subtly wrong.
        return new InListMembership(rightValue, null, KIND_NONE, false);

      final Object key = keyOf(item, itemKind);
      if (key == null)
        return new InListMembership(rightValue, null, KIND_NONE, false);

      kind = itemKind;
      keys.add(key);
    }

    if (kind == KIND_NONE)
      // Empty, or nulls only: cheap either way, and the linear evaluator already answers both exactly.
      return new InListMembership(rightValue, null, KIND_NONE, containsNull);

    return new InListMembership(rightValue, keys, kind, containsNull);
  }

  /**
   * SQL three-valued membership test of {@code left} against this right-hand side, with the same contract as
   * {@link InCondition#evaluateExpressionThreeValued}.
   */
  public Boolean evaluate(final Object left) {
    if (keys != null && left != null) {
      final int leftKind = kindOf(left);
      if (leftKind == kind) {
        final Object key = keyOf(left, leftKind);
        if (key != null) {
          if (keys.contains(key))
            return Boolean.TRUE;
          return containsNull ? null : Boolean.FALSE;
        }
      }
    }
    return InCondition.evaluateExpressionThreeValued(left, rightValue);
  }

  /** The right-hand value this probe was built from, so the caller can apply the "right is null is UNKNOWN" rule. */
  public Object getRightValue() {
    return rightValue;
  }

  /**
   * The kinds a hash key exists for. {@link BigDecimal} and {@link BigInteger} are deliberately excluded, and
   * so is any other {@link Number} implementation: the linear path compares two numbers by widening both to a
   * common type first, and {@code BigDecimal.equals} is scale-sensitive, so it reports {@code 5.5d} as NOT
   * equal to {@code new BigDecimal("5.50")} while any canonical key must call them the same number. That
   * relation is not even transitive, so no key can reproduce it - the only honest answer is to decline the
   * list entirely and let the linear evaluator keep its own semantics.
   */
  private static int kindOf(final Object value) {
    if (value instanceof String)
      return KIND_STRING;
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
        || value instanceof Float || value instanceof Double)
      return KIND_NUMBER;
    return KIND_NONE;
  }

  private static Object keyOf(final Object value, final int kind) {
    if (kind == KIND_STRING)
      return value;

    final Number number = (Number) value;
    if (number instanceof Double || number instanceof Float) {
      final double d = number.doubleValue();
      if (Double.isNaN(d) || Double.isInfinite(d))
        // Not representable as a BigDecimal at all; the linear path answers these from Double.equals.
        return null;
      return BigDecimal.valueOf(d).stripTrailingZeros();
    }
    // stripTrailingZeros() on this branch too, or the integral 100 and the double 100.0 - which the linear
    // path reports equal, having widened both to double - would land on BigDecimals differing only in scale.
    return BigDecimal.valueOf(number.longValue()).stripTrailingZeros();
  }
}
