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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Standard SQL cast: {@code CAST(x AS type)} and {@code x::type}, both of which build this node (issue #13384).
 * <p>
 * It is strict, unlike the lenient {@code convert()} method of a record: a value that cannot be the target type (a text that is not
 * a number, a number outside the range of the type, NaN as an integer) raises {@link CommandExecutionException} instead of turning
 * into null or being truncated. {@code NULL} casts to {@code NULL}, as in PostgreSQL.
 * <p>
 * The type names are the common scalar ones, in their SQL spellings and the engine's: see {@link #resolve(String)}. A name outside
 * that list is refused when the statement is parsed.
 */
public class CastExpression extends MathExpression {
  /** The type names a cast accepts, upper case, and the engine type each one stands for. */
  private static final Map<String, Type> TYPES = Map.ofEntries(Map.entry("SMALLINT", Type.SHORT), Map.entry("INT2", Type.SHORT),
      Map.entry("SHORT", Type.SHORT), Map.entry("INTEGER", Type.INTEGER), Map.entry("INT", Type.INTEGER), Map.entry("INT4", Type.INTEGER),
      Map.entry("BIGINT", Type.LONG), Map.entry("INT8", Type.LONG), Map.entry("LONG", Type.LONG), Map.entry("REAL", Type.FLOAT),
      Map.entry("FLOAT4", Type.FLOAT), Map.entry("FLOAT", Type.FLOAT), Map.entry("DOUBLE", Type.DOUBLE),
      Map.entry("DOUBLE PRECISION", Type.DOUBLE), Map.entry("FLOAT8", Type.DOUBLE), Map.entry("NUMERIC", Type.DECIMAL),
      Map.entry("DECIMAL", Type.DECIMAL), Map.entry("TEXT", Type.STRING), Map.entry("VARCHAR", Type.STRING),
      Map.entry("CHAR", Type.STRING), Map.entry("STRING", Type.STRING), Map.entry("BOOLEAN", Type.BOOLEAN),
      Map.entry("BOOL", Type.BOOLEAN), Map.entry("DATE", Type.DATE), Map.entry("TIMESTAMP", Type.DATETIME),
      Map.entry("DATETIME", Type.DATETIME));

  public Expression operand;
  public Type       type;

  public CastExpression() {
  }

  public CastExpression(final Expression operand, final String typeName) {
    this.operand = operand;
    this.type = resolve(typeName);
  }

  /**
   * The engine type a SQL type name stands for.
   *
   * @throws CommandSQLParsingException for a name that is not one of the common scalar types a cast accepts
   */
  public static Type resolve(final String typeName) {
    final Type resolved = TYPES.get(typeName.trim().replaceAll("\\s+", " ").toUpperCase(Locale.ENGLISH));
    if (resolved == null)
      throw new CommandSQLParsingException(
          "Cannot cast to type '" + typeName + "': a cast takes one of " + String.join(", ", TYPES.keySet().stream().sorted().toList()));
    return resolved;
  }

  @Override
  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    return cast(operand.execute(currentRecord, context), type, context);
  }

  @Override
  public Object execute(final Result currentRecord, final CommandContext context) {
    return cast(operand.execute(currentRecord, context), type, context);
  }

  /**
   * Converts {@code value} to {@code target}, or raises when it cannot be that type.
   */
  public static Object cast(final Object value, final Type target, final CommandContext context) {
    if (value == null)
      return null;
    try {
      return switch (target) {
        case SHORT -> (short) wholeNumber(value, Short.MIN_VALUE, Short.MAX_VALUE);
        case INTEGER -> (int) wholeNumber(value, Integer.MIN_VALUE, Integer.MAX_VALUE);
        case LONG -> wholeNumber(value, Long.MIN_VALUE, Long.MAX_VALUE);
        case FLOAT -> floatOf(value);
        case DOUBLE -> doubleOf(value);
        case DECIMAL -> decimalOf(value);
        case STRING -> value.toString();
        case BOOLEAN -> booleanOf(value);
        case DATE, DATETIME -> temporal(value, target, context);
        default -> throw new IllegalArgumentException("unsupported");
      };
    } catch (final CommandExecutionException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw new CommandExecutionException("Cannot cast " + describe(value) + " to " + target.name(), e);
    }
  }

  private static String describe(final Object value) {
    return value instanceof CharSequence ? "'" + value + "'" : String.valueOf(value);
  }

  private static float floatOf(final Object value) {
    final float result = value instanceof Number number && !(value instanceof BigDecimal) ? number.floatValue()
        : decimalOf(value).floatValue();
    // a finite value too big for the type becomes an infinity in Java, where PostgreSQL raises; an infinity as such is a value
    if (Float.isInfinite(result) && !isInfinity(value))
      throw new CommandExecutionException("Cannot cast " + describe(value) + " to FLOAT: the value is out of range");
    return result;
  }

  private static double doubleOf(final Object value) {
    final double result = value instanceof Number number && !(value instanceof BigDecimal) ? number.doubleValue()
        : decimalOf(value).doubleValue();
    if (Double.isInfinite(result) && !isInfinity(value))
      throw new CommandExecutionException("Cannot cast " + describe(value) + " to DOUBLE: the value is out of range");
    return result;
  }

  private static boolean isInfinity(final Object value) {
    return value instanceof Double d && d.isInfinite() || value instanceof Float f && f.isInfinite();
  }

  /** A number or a text that holds a number, rounded half away from zero as PostgreSQL does, and checked against the range of the target. */
  private static long wholeNumber(final Object value, final long min, final long max) {
    if (value instanceof Boolean bool)
      return bool ? 1 : 0;
    final BigDecimal decimal = decimalOf(value);
    // The digits left of the decimal point, from the precision and the scale alone. A text such as 1e1000000000 is a short input
    // whose rescaling to a whole number would build a billion-digit integer, so it is refused here, before setScale (Long.MAX_VALUE
    // has 19 digits). A value below 0.1 rounds to 0 and is not rescaled for the same reason (1e-1000000000).
    final long digits = (long) decimal.precision() - decimal.scale();
    if (digits > 20)
      throw new CommandExecutionException("Cannot cast " + describe(value) + ": the value is out of range");
    final BigDecimal number = digits < 0 ? BigDecimal.ZERO : decimal.setScale(0, RoundingMode.HALF_UP);
    if (number.compareTo(BigDecimal.valueOf(min)) < 0 || number.compareTo(BigDecimal.valueOf(max)) > 0)
      throw new CommandExecutionException("Cannot cast " + describe(value) + ": the value is out of range");
    return number.longValueExact();
  }

  private static BigDecimal decimalOf(final Object value) {
    if (value instanceof BigDecimal decimal)
      return decimal;
    if (value instanceof Double || value instanceof Float) {
      final double d = ((Number) value).doubleValue();
      if (Double.isNaN(d) || Double.isInfinite(d))
        throw new CommandExecutionException("Cannot cast " + describe(value) + ": it is not a finite number");
      return new BigDecimal(value.toString());
    }
    if (value instanceof Number)
      return new BigDecimal(value.toString());
    if (value instanceof CharSequence text)
      return new BigDecimal(text.toString().trim());
    throw new CommandExecutionException("Cannot cast " + describe(value) + " (" + value.getClass().getSimpleName() + ") to a number");
  }

  private static Boolean booleanOf(final Object value) {
    if (value instanceof Boolean bool)
      return bool;
    if (value instanceof Number number)
      return number.doubleValue() != 0;
    final String text = value.toString().trim().toLowerCase(Locale.ENGLISH);
    return switch (text) {
      case "t", "true", "y", "yes", "on", "1" -> Boolean.TRUE;
      case "f", "false", "n", "no", "off", "0" -> Boolean.FALSE;
      default -> throw new CommandExecutionException("Cannot cast " + describe(value) + " to BOOLEAN");
    };
  }

  private static Object temporal(final Object value, final Type target, final CommandContext context) {
    final Object converted = Type.convert(context == null ? null : context.getDatabase(), value, target.getDefaultJavaType());
    if (converted == null)
      throw new CommandExecutionException("Cannot cast " + describe(value) + " to " + target.name());
    // both types are a java.util.Date and Type.convert hands one back unchanged, so a DATE drops its time of day here: the days
    // since the epoch, as a DATE property stores them
    if (target == Type.DATE && converted instanceof Date date)
      return new Date(Math.floorDiv(date.getTime(), DateUtils.MS_IN_A_DAY) * DateUtils.MS_IN_A_DAY);
    return converted;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("CAST(");
    if (operand != null)
      operand.toString(params, builder);
    builder.append(" AS ").append(type == null ? "" : type.name()).append(")");
  }

  @Override
  public boolean isExpand() {
    return operand.isExpand();
  }

  @Override
  public boolean isAggregate(final CommandContext context) {
    return operand.isAggregate(context);
  }

  @Override
  public boolean isCount() {
    return false;
  }

  @Override
  public boolean isEarlyCalculated(final CommandContext context) {
    return operand.isEarlyCalculated(context);
  }

  /** The operand is stored outside {@code childExpressions}, so the inherited walkers would not see it. */
  @Override
  public boolean isLiteral(final boolean allowInputParameters) {
    return operand.isLiteral(allowInputParameters);
  }

  @Override
  public boolean containsInputParameter() {
    return operand.containsInputParameter();
  }

  @Override
  public SimpleNode splitForAggregation(final AggregateProjectionSplit aggregateProj, final CommandContext context) {
    if (!isAggregate(context))
      return this;
    final CastExpression result = new CastExpression();
    result.operand = (Expression) operand.splitForAggregation(aggregateProj, context);
    result.type = type;
    return result;
  }

  @Override
  public CastExpression copy() {
    final CastExpression result = new CastExpression();
    result.operand = operand == null ? null : operand.copy();
    result.type = type;
    return result;
  }

  @Override
  public void extractSubQueries(final SubQueryCollector collector) {
    operand.extractSubQueries(collector);
  }

  @Override
  public void extractSubQueries(final Identifier letAlias, final SubQueryCollector collector) {
    operand.extractSubQueries(collector);
  }

  @Override
  public boolean refersToParent() {
    return operand.refersToParent();
  }

  @Override
  public List<String> getMatchPatternInvolvedAliases() {
    return operand.getMatchPatternInvolvedAliases();
  }

  @Override
  public void applyRemove(final ResultInternal result, final CommandContext context) {
    throw new CommandExecutionException("Cannot apply REMOVE " + this);
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    return new SimpleNode[] { operand };
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final CastExpression that = (CastExpression) o;
    return type == that.type && Objects.equals(operand, that.operand);
  }

  @Override
  public int hashCode() {
    return 31 * Objects.hashCode(operand) + Objects.hashCode(type);
  }
}
