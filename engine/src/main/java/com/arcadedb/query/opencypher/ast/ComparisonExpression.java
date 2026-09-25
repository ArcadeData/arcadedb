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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.schema.Type;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Comparison expression for WHERE clauses.
 * Supports: =, !=, <, >, <=, >=
 * Example: n.age > 30, n.name = 'Alice'
 */
public class ComparisonExpression implements BooleanExpression {
  public enum Operator {
    EQUALS("="),
    NOT_EQUALS("!="),
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN_OR_EQUAL(">=");

    private final String symbol;

    Operator(final String symbol) {
      this.symbol = symbol;
    }

    public String getSymbol() {
      return symbol;
    }

    public static Operator fromString(final String str) {
      return switch (str) {
        case "=" -> EQUALS;
        case "!=" -> NOT_EQUALS;
        case "<" -> LESS_THAN;
        case ">" -> GREATER_THAN;
        case "<=" -> LESS_THAN_OR_EQUAL;
        case ">=" -> GREATER_THAN_OR_EQUAL;
        default -> throw new IllegalArgumentException("Unknown operator: " + str);
      };
    }
  }

  private final Expression left;
  private final Operator operator;
  private final Expression right;

  // Single-slot memo so an invariant temporal operand (e.g. a bound $parameter re-evaluated on every
  // scanned row) is wrapped into a CypherTemporalValue once instead of allocating a fresh wrapper per
  // row. volatile + an immutable {raw, coerced} pair keeps concurrent evaluators from seeing a torn pair.
  private volatile Object[] temporalCoercionMemo;
  // Single-slot memo of the zone adoption below, {raw, zone, adjusted}: the zone-less operand is typically an invariant
  // parameter and the stored datetimes it is compared with typically share one zone, so this keeps that comparison
  // allocation-free per row, like the coercion memo above.
  private volatile Object[] zoneAdoptionMemo;

  public ComparisonExpression(final Expression left, final Operator operator, final Expression right) {
    this.left = left;
    this.operator = operator;
    this.right = right;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object ternary = evaluateTernary(result, context);
    return Boolean.TRUE.equals(ternary);
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object leftValue;
    final Object rightValue;

    if (left instanceof FunctionCallExpression || right instanceof FunctionCallExpression) {
      leftValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(left, result, context);
      rightValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(right, result, context);
    } else {
      leftValue = left.evaluate(result, context);
      rightValue = right.evaluate(result, context);
    }

    return compareValuesTernary(leftValue, rightValue);
  }

  public Object evaluateWithValues(final Object leftValue, final Object rightValue) {
    return compareValuesTernary(leftValue, rightValue);
  }

  /**
   * Returns a comparator that applies this operator's Cypher semantics to two already-evaluated values,
   * for callers that must agree with the operator but do not have operand expressions to hand (e.g. the
   * IN operator comparing the left operand against each list element, issue #5293). The result is usable
   * only through {@link #evaluateWithValues(Object, Object)}: it carries no operand expressions, so
   * {@link #evaluate(Result, CommandContext)}, {@link #evaluateTernary(Result, CommandContext)} and
   * {@link #getText()} do not apply to it. Reuse one instance per call site rather than allocating per
   * comparison, so the temporal coercion memo stays effective.
   */
  public static ComparisonExpression valueComparator(final Operator operator) {
    return new ComparisonExpression(null, operator, null);
  }

  private Object compareValuesTernary(final Object left, final Object right) {
    // In OpenCypher, any comparison involving null returns null
    if (left == null || right == null)
      return null;

    // id()/elementId() interop: id() now returns a Long-encoded RID (Neo4j-compatible, issue #4183).
    // Legacy queries still pass an RID string parameter (e.g. {@code WHERE id(n) = "#1:0"}). Coerce
    // the Long-encoded side to its RID-string form and compare as RIDs, so the legacy pattern keeps
    // working without forcing callers to migrate. Both equality and ordering use the encoded long so
    // ordering stays consistent with id() / RID natural order. Only the RID-string form is coerced -
    // numeric strings are ambiguous and treating them as ids would break the Cypher TCK invariant
    // that 5 = "5" returns false.
    if (left instanceof Number leftNum && right instanceof String rightStr && RID.is(rightStr)) {
      final long leftEncoded = leftNum.longValue();
      final long rightEncoded = IdFunction.encodeRidAsLong(new RID(rightStr));
      return numericCompare(leftEncoded, rightEncoded);
    }
    if (left instanceof String leftStr && right instanceof Number rightNum && RID.is(leftStr)) {
      final long leftEncoded = IdFunction.encodeRidAsLong(new RID(leftStr));
      final long rightEncoded = rightNum.longValue();
      return numericCompare(leftEncoded, rightEncoded);
    }

    // Graph entities compare by record identity even when different implementations represent them.
    // This is common when a Graph Analytical View vertex is compared with a regular OLTP vertex (#6010).
    if (left instanceof Identifiable leftIdentifiable
        && right instanceof Identifiable rightIdentifiable) {
      final boolean equal = leftIdentifiable.getIdentity().equals(rightIdentifiable.getIdentity());
      if (operator == Operator.EQUALS)
        return equal;
      if (operator == Operator.NOT_EQUALS)
        return !equal;
      return null;
    }

    // Temporal comparison. Coerce native java.time / java.util.Date operands into Cypher temporal
    // values first, so a native temporal parameter (e.g. a datetime sent over Bolt, which resolves to
    // a raw java.time value) compares against a stored temporal instead of silently not matching.
    // Hot path: coerceTemporal short-circuits on the common numeric/string/boolean operand with a
    // single instanceof pair, and memoizes an invariant temporal operand to avoid per-row allocation.
    Object leftTemporal = coerceTemporal(left);
    Object rightTemporal = coerceTemporal(right);
    // A java.util.Date or Instant carries an instant and no zone, and coerces to UTC only for want of one. Against a
    // zoned datetime it takes that operand's zone, so it equals every datetime at its instant rather than only the
    // UTC ones: datetimes at one instant in different zones are distinct values (issue #8300).
    // Only against a genuinely zoned operand: two zone-less ones are both UTC already.
    final boolean leftZoneless = left instanceof Date || left instanceof Instant;
    final boolean rightZoneless = right instanceof Date || right instanceof Instant;
    if (leftZoneless && !rightZoneless && rightTemporal instanceof CypherDateTime zoned)
      leftTemporal = adoptZone(left, (CypherDateTime) leftTemporal, zoned.getValue().getZone());
    else if (rightZoneless && !leftZoneless && leftTemporal instanceof CypherDateTime zoned)
      rightTemporal = adoptZone(right, (CypherDateTime) rightTemporal, zoned.getValue().getZone());
    if (leftTemporal instanceof CypherTemporalValue && rightTemporal instanceof CypherTemporalValue) {
      try {
        // A LocalDateTime and a zoned DateTime compare by instant (the LocalDateTime read as UTC): compareTo also
        // orders the two types apart at one instant, which ORDER BY needs and = must not see.
        final int cmp;
        if (leftTemporal instanceof CypherLocalDateTime local && rightTemporal instanceof CypherDateTime zoned)
          cmp = local.getValue().toInstant(ZoneOffset.UTC).compareTo(zoned.getValue().toInstant());
        else if (leftTemporal instanceof CypherDateTime zoned && rightTemporal instanceof CypherLocalDateTime local)
          cmp = zoned.getValue().toInstant().compareTo(local.getValue().toInstant(ZoneOffset.UTC));
        else
          cmp = ((CypherTemporalValue) leftTemporal).compareTo((CypherTemporalValue) rightTemporal);
        return switch (operator) {
          case EQUALS -> cmp == 0;
          case NOT_EQUALS -> cmp != 0;
          case LESS_THAN -> cmp < 0;
          case GREATER_THAN -> cmp > 0;
          case LESS_THAN_OR_EQUAL -> cmp <= 0;
          case GREATER_THAN_OR_EQUAL -> cmp >= 0;
        };
      } catch (final IllegalArgumentException e) {
        // Different temporal types: for equality, return false/true; for ordering, return null
        if (operator == Operator.EQUALS) return false;
        if (operator == Operator.NOT_EQUALS) return true;
        return null;
      }
    }

    // Numeric comparison
    if (left instanceof Number && right instanceof Number) {
      // Use long comparison when both are integer types to avoid precision loss
      if ((left instanceof Long || left instanceof Integer) &&
          (right instanceof Long || right instanceof Integer)) {
        final long leftNum = ((Number) left).longValue();
        final long rightNum = ((Number) right).longValue();
        return switch (operator) {
          case EQUALS -> leftNum == rightNum;
          case NOT_EQUALS -> leftNum != rightNum;
          case LESS_THAN -> leftNum < rightNum;
          case GREATER_THAN -> leftNum > rightNum;
          case LESS_THAN_OR_EQUAL -> leftNum <= rightNum;
          case GREATER_THAN_OR_EQUAL -> leftNum >= rightNum;
        };
      }
      // A Float reaches the comparison through its decimal form, as the SQL comparator does: the primitive widening
      // reproduces the single precision rounding error, so a FLOAT property holding 0.05 did not equal the literal
      // 0.05 - which Cypher reads as a 64-bit float. Neo4j has no 32-bit float to disagree with (issue #7609).
      final double leftNum = toComparableDouble((Number) left);
      final double rightNum = toComparableDouble((Number) right);
      return switch (operator) {
        case EQUALS -> leftNum == rightNum;
        case NOT_EQUALS -> leftNum != rightNum;
        case LESS_THAN -> leftNum < rightNum;
        case GREATER_THAN -> leftNum > rightNum;
        case LESS_THAN_OR_EQUAL -> leftNum <= rightNum;
        case GREATER_THAN_OR_EQUAL -> leftNum >= rightNum;
      };
    }

    // Boolean comparison (booleans only compare with booleans)
    if (left instanceof Boolean && right instanceof Boolean) {
      // false < true in Cypher
      final int cmp = Boolean.compare((Boolean) left, (Boolean) right);
      return switch (operator) {
        case EQUALS -> cmp == 0;
        case NOT_EQUALS -> cmp != 0;
        case LESS_THAN -> cmp < 0;
        case GREATER_THAN -> cmp > 0;
        case LESS_THAN_OR_EQUAL -> cmp <= 0;
        case GREATER_THAN_OR_EQUAL -> cmp >= 0;
      };
    }

    // String comparison (strings only compare with strings)
    if (left instanceof String && right instanceof String) {
      final int comparison = ((String) left).compareTo((String) right);
      return switch (operator) {
        case EQUALS -> comparison == 0;
        case NOT_EQUALS -> comparison != 0;
        case LESS_THAN -> comparison < 0;
        case GREATER_THAN -> comparison > 0;
        case LESS_THAN_OR_EQUAL -> comparison <= 0;
        case GREATER_THAN_OR_EQUAL -> comparison >= 0;
      };
    }

    // List comparison (element-wise with null propagation).
    // Coerce List/Collection/array (incl. primitive arrays from numeric-array parameters, issue #4284) to a List.
    final List<Object> leftList = MultiValue.getMultiValueAsList(left);
    final List<Object> rightList = MultiValue.getMultiValueAsList(right);
    if (leftList != null && rightList != null) {
      if (operator == Operator.EQUALS || operator == Operator.NOT_EQUALS) {
        if (leftList.size() != rightList.size())
          return operator == Operator.NOT_EQUALS;
        boolean hasNull = false;
        for (int i = 0; i < leftList.size(); i++) {
          final Object elemResult = new ComparisonExpression(
              new LiteralExpression(leftList.get(i), ""), Operator.EQUALS,
              new LiteralExpression(rightList.get(i), ""))
              .evaluateTernary(null, null);
          if (elemResult == null)
            hasNull = true;
          else if (!Boolean.TRUE.equals(elemResult))
            return operator == Operator.NOT_EQUALS;
        }
        if (hasNull)
          return null;
        return operator == Operator.EQUALS;
      }
      // List ordering: lexicographic comparison with null propagation
      final int minSize = Math.min(leftList.size(), rightList.size());
      for (int i = 0; i < minSize; i++) {
        final Object elemCmp = new ComparisonExpression(
            new LiteralExpression(leftList.get(i), ""), Operator.EQUALS,
            new LiteralExpression(rightList.get(i), ""))
            .evaluateTernary(null, null);
        if (elemCmp == null)
          return null; // null element makes ordering undefined
        if (Boolean.TRUE.equals(elemCmp))
          continue; // elements are equal, compare next
        // Elements differ: check less than
        final Object ltResult = new ComparisonExpression(
            new LiteralExpression(leftList.get(i), ""), Operator.LESS_THAN,
            new LiteralExpression(rightList.get(i), ""))
            .evaluateTernary(null, null);
        if (ltResult == null)
          return null;
        final boolean isLess = Boolean.TRUE.equals(ltResult);
        return switch (operator) {
          case LESS_THAN -> isLess;
          case GREATER_THAN -> !isLess;
          case LESS_THAN_OR_EQUAL -> isLess;
          case GREATER_THAN_OR_EQUAL -> !isLess;
          default -> null;
        };
      }
      // All compared elements are equal; compare by length
      final int sizeCmp = Integer.compare(leftList.size(), rightList.size());
      return switch (operator) {
        case LESS_THAN -> sizeCmp < 0;
        case GREATER_THAN -> sizeCmp > 0;
        case LESS_THAN_OR_EQUAL -> sizeCmp <= 0;
        case GREATER_THAN_OR_EQUAL -> sizeCmp >= 0;
        default -> null;
      };
    }

    // Map comparison with 3VL null propagation
    if (left instanceof Map && right instanceof Map) {
      final Map<?, ?> leftMap = (Map<?, ?>) left;
      final Map<?, ?> rightMap = (Map<?, ?>) right;
      if (operator == Operator.EQUALS || operator == Operator.NOT_EQUALS) {
        // Different key sets means definitely not equal
        if (!leftMap.keySet().equals(rightMap.keySet()))
          return operator == Operator.NOT_EQUALS;
        // Same key set: compare values with 3VL
        boolean hasNull = false;
        for (final Object key : leftMap.keySet()) {
          final Object lv = leftMap.get(key);
          final Object rv = rightMap.get(key);
          final Object cmp = new ComparisonExpression(
              new LiteralExpression(lv, ""), Operator.EQUALS,
              new LiteralExpression(rv, "")).evaluateTernary(null, null);
          if (cmp == null)
            hasNull = true;
          else if (!Boolean.TRUE.equals(cmp))
            return operator == Operator.NOT_EQUALS;
        }
        if (hasNull)
          return null;
        return operator == Operator.EQUALS;
      }
      return null;
    }

    // Same-type objects: use equals() for = and <> (handles nodes, relationships, etc.)
    if (left.getClass().equals(right.getClass())) {
      if (operator == Operator.EQUALS)
        return left.equals(right);
      if (operator == Operator.NOT_EQUALS)
        return !left.equals(right);
    }

    // For = and <>, different types are simply not equal (return false/true)
    if (operator == Operator.EQUALS)
      return false;
    if (operator == Operator.NOT_EQUALS)
      return true;

    // For ordering operators (<, >, <=, >=), incompatible types return null
    return null;
  }

  @Override
  public String getText() {
    return left.getText() + " " + operator.getSymbol() + " " + right.getText();
  }

  public Expression getLeft() {
    return left;
  }

  public Operator getOperator() {
    return operator;
  }

  public Expression getRight() {
    return right;
  }

  /**
   * Wrap a native {@code java.time} / {@code java.util.Date} operand into its Cypher temporal type,
   * returning any other value unchanged. Non-temporal operands (the common numeric/string/boolean case)
   * short-circuit on two instanceof checks; an invariant temporal operand re-evaluated across rows
   * (typically a bound parameter) is coerced once and served from a single-slot memo thereafter.
   */
  private Object coerceTemporal(final Object value) {
    if (!(value instanceof Temporal || value instanceof Date))
      return value;
    final Object[] memo = temporalCoercionMemo;
    if (memo != null && memo[0] == value && (long) memo[2] == dateMillis(value))
      return memo[1];
    final Object coerced = TemporalUtil.fromCoreJavaType(value);
    temporalCoercionMemo = new Object[] { value, coerced, dateMillis(value) };
    return coerced;
  }

  private CypherDateTime adoptZone(final Object raw, final CypherDateTime coerced, final ZoneId zone) {
    final Object[] memo = zoneAdoptionMemo;
    if (memo != null && memo[0] == raw && (long) memo[3] == dateMillis(raw) && memo[1].equals(zone))
      return (CypherDateTime) memo[2];
    final CypherDateTime adjusted = new CypherDateTime(coerced.getValue().withZoneSameInstant(zone));
    zoneAdoptionMemo = new Object[] { raw, zone, adjusted, dateMillis(raw) };
    return adjusted;
  }

  /**
   * The memos key an operand by identity, which is enough for the immutable java.time values but not for a
   * java.util.Date: the same instance can be moved with setTime() between two evaluations and would be answered from
   * the memo with the value it had before. Its millis are part of the key; any other operand keys as 0.
   */
  private static long dateMillis(final Object value) {
    return value instanceof Date date ? date.getTime() : 0L;
  }

  private Boolean numericCompare(final long leftNum, final long rightNum) {
    return switch (operator) {
      case EQUALS -> leftNum == rightNum;
      case NOT_EQUALS -> leftNum != rightNum;
      case LESS_THAN -> leftNum < rightNum;
      case GREATER_THAN -> leftNum > rightNum;
      case LESS_THAN_OR_EQUAL -> leftNum <= rightNum;
      case GREATER_THAN_OR_EQUAL -> leftNum >= rightNum;
    };
  }

  /**
   * Widens a number for comparison, reading a {@link Float} through its decimal form rather than its bits. See
   * {@link Type#widenFloat}: this evaluator is the authoritative answer for a Cypher predicate, so it has to agree
   * with what the SQL comparator answers over the same records.
   *
   * @param value the operand (never {@code null})
   *
   * @return the operand as a double
   */
  private static double toComparableDouble(final Number value) {
    return value instanceof Float float1 ? Type.widenFloat(float1) : value.doubleValue();
  }

}
