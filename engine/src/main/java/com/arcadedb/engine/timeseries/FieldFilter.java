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
package com.arcadedb.engine.timeseries;

import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Range predicates on numeric FIELD columns, ANDed together, that a TimeSeries scan evaluates itself (issue #9612):
 * {@code uu > 90}, {@code ui BETWEEN 10 AND 20}, {@code uu = 5}.
 * <p>
 * A sealed block keeps the minimum and maximum of every numeric column, so a block whose range cannot satisfy a
 * condition is skipped without being read, a block whose whole range satisfies every condition is answered as if
 * there were no filter (from its statistics, when the query is an aggregation), and only the blocks in between are
 * decoded, and then only the filtered columns are tested, on the primitive arrays, before any row is built.
 * <p>
 * <b>The answer must be the one the SQL filter gives</b>, because the planner drops the predicates it hands to the
 * engine from an aggregation. That is why a condition is only built for the pairs whose SQL comparison is a plain
 * numeric one:
 * <ul>
 *   <li>a {@code DOUBLE} column against any {@code Integer}, {@code Long}, {@code Short}, {@code Byte} or {@code Double}
 *   operand, compared as doubles - which is what SQL widens that pair to;</li>
 *   <li>a {@code LONG}, {@code INTEGER}, {@code SHORT} or {@code BYTE} column against an integral operand, compared as
 *   longs, exactly.</li>
 * </ul>
 * Everything else - a {@code FLOAT} column (SQL compares a float with the double that narrows to it as equal), a
 * decimal operand against an integer column, a {@code BigDecimal}, a string, a date - is left to the SQL filter. A
 * missing measurement (the NaN a floating-point column stores for a null) never matches, as {@code null > x} does not.
 * <p>
 * Conditions use the column numbering {@link TagFilter} uses: the index among the columns that are not the timestamp.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class FieldFilter {

  /**
   * One range condition on one column. An integral condition keeps its bounds as INCLUSIVE longs ({@code ui > 5} is
   * {@code [6, Long.MAX_VALUE]}); a floating-point one keeps doubles with their own inclusiveness. An unbounded side is
   * the infinity (or the extreme long) of that side.
   */
  record Condition(int columnIndex, String columnName, boolean integral, long lowLong, long highLong, double low,
                   boolean lowInclusive, double high, boolean highInclusive) {

    boolean matches(final double value) {
      // a NaN fails both tests below, which is the "a missing measurement never matches" rule
      return (value > low || (lowInclusive && value == low)) && (value < high || (highInclusive && value == high));
    }

    boolean matches(final long value) {
      return value >= lowLong && value <= highLong;
    }

    boolean matchesNothing() {
      return integral ? lowLong > highLong : low > high || (low == high && !(lowInclusive && highInclusive));
    }
  }

  /**
   * What a block's statistics prove about a filter: no sample can match, every sample matches, or only a decode can
   * tell.
   */
  enum BlockMatch {
    NONE, ALL, SOME
  }

  private final List<Condition> conditions;

  private FieldFilter(final List<Condition> conditions) {
    this.conditions = conditions;
  }

  /**
   * Whether a comparison of {@code column} with {@code operand} can become a condition: a FIELD column of a supported
   * type stored with its native numeric codec, against an operand of a type the SQL comparison treats as a plain number
   * for that column. See the class description.
   */
  public static boolean supports(final ColumnDefinition column, final Object operand) {
    if (column == null || column.getRole() != ColumnDefinition.ColumnRole.FIELD || !(operand instanceof Number))
      return false;
    final boolean integralOperand = operand instanceof Integer || operand instanceof Long || operand instanceof Short || operand instanceof Byte;
    return switch (column.getDataType()) {
      case DOUBLE -> column.getCompressionHint() == TimeSeriesCodec.GORILLA_XOR && (integralOperand || operand instanceof Double d && !d.isNaN());
      case LONG, INTEGER, SHORT, BYTE -> column.getCompressionHint() == TimeSeriesCodec.SIMPLE8B && integralOperand;
      default -> false;
    };
  }

  /**
   * A filter of one condition, {@code low <(=) column <(=) high}, or {@code null} when {@link #supports} refuses either
   * operand. A {@code null} bound leaves that side open.
   *
   * @param nonTsColumnIndex the column's index among the columns that are not the timestamp
   */
  public static FieldFilter range(final int nonTsColumnIndex, final ColumnDefinition column, final Number low,
      final boolean lowInclusive, final Number high, final boolean highInclusive) {
    if ((low == null && high == null) || (low != null && !supports(column, low)) || (high != null && !supports(column, high)))
      return null;
    final List<Condition> conditions = new ArrayList<>(1);
    conditions.add(condition(nonTsColumnIndex, column, low, lowInclusive, high, highInclusive));
    return new FieldFilter(conditions);
  }

  private static Condition condition(final int nonTsColumnIndex, final ColumnDefinition column, final Number low,
      final boolean lowInclusive, final Number high, final boolean highInclusive) {
    if (column.getDataType() != Type.DOUBLE) {
      long lowLong = Long.MIN_VALUE;
      long highLong = Long.MAX_VALUE;
      boolean empty = false;
      if (low != null) {
        lowLong = low.longValue();
        if (!lowInclusive) {
          if (lowLong == Long.MAX_VALUE)
            empty = true;
          else
            lowLong++;
        }
      }
      if (high != null) {
        highLong = high.longValue();
        if (!highInclusive) {
          if (highLong == Long.MIN_VALUE)
            empty = true;
          else
            highLong--;
        }
      }
      if (empty) {
        lowLong = Long.MAX_VALUE;
        highLong = Long.MIN_VALUE;
      }
      return new Condition(nonTsColumnIndex, column.getName(), true, lowLong, highLong, lowLong, true, highLong, true);
    }
    return new Condition(nonTsColumnIndex, column.getName(), false, 0L, 0L,
        low != null ? low.doubleValue() : Double.NEGATIVE_INFINITY, low == null || lowInclusive,
        high != null ? high.doubleValue() : Double.POSITIVE_INFINITY, high == null || highInclusive);
  }

  /** This filter AND {@code other}. */
  public FieldFilter and(final FieldFilter other) {
    if (other == null)
      return this;
    final List<Condition> merged = new ArrayList<>(conditions.size() + other.conditions.size());
    merged.addAll(conditions);
    merged.addAll(other.conditions);
    return new FieldFilter(merged);
  }

  /**
   * Tests a row built from <em>every</em> column in schema order, the layout {@link TagFilter#matches(Object[])} reads:
   * {@code row[0]} is the timestamp and {@code row[1 + i]} the non-timestamp column {@code i}.
   */
  public boolean matches(final Object[] row) {
    for (final Condition condition : conditions) {
      final int position = condition.columnIndex + 1;
      if (position >= row.length || !(row[position] instanceof Number value))
        return false;
      if (condition.integral ? !condition.matches(value.longValue()) : !condition.matches(value.doubleValue()))
        return false;
    }
    return true;
  }

  /**
   * Refuses a filter built against another schema: each condition must name, by its non-timestamp index, a column of the
   * same name stored with the codec its kind of comparison decodes ({@code SIMPLE8B} for an integral one, {@code GORILLA_XOR}
   * otherwise). The storage layers decode the filtered columns by that codec, so a mismatch would read garbage.
   *
   * @throws IllegalArgumentException on the first condition that does not fit {@code columns}
   */
  public void validate(final List<ColumnDefinition> columns) {
    for (final Condition condition : conditions) {
      ColumnDefinition column = null;
      int nonTsIdx = 0;
      for (final ColumnDefinition candidate : columns) {
        if (candidate.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;
        if (nonTsIdx++ == condition.columnIndex) {
          column = candidate;
          break;
        }
      }
      final TimeSeriesCodec expected = condition.integral ? TimeSeriesCodec.SIMPLE8B : TimeSeriesCodec.GORILLA_XOR;
      if (column == null || !column.getName().equals(condition.columnName) || column.getRole() != ColumnDefinition.ColumnRole.FIELD
          || column.getCompressionHint() != expected)
        throw new IllegalArgumentException("Field filter condition on '" + condition.columnName + "' (column " + condition.columnIndex
            + ") does not match the TimeSeries schema");
    }
  }

  /**
   * Whether one condition can match no row on its own: an empty range such as {@code uu BETWEEN 30 AND 10} or
   * {@code ui > Long.MAX_VALUE}. Conditions are not merged, so {@code uu > 5 AND uu < 3} is two satisfiable conditions here;
   * every block still answers it as {@code NONE} from its statistics, and no row passes both.
   */
  public boolean matchesNothing() {
    for (final Condition condition : conditions)
      if (condition.matchesNothing())
        return true;
    return false;
  }

  public int getConditionCount() {
    return conditions.size();
  }

  List<Condition> getConditions() {
    return conditions;
  }

  /**
   * What a block's statistics prove about one condition. The statistics are doubles for every numeric column, so for
   * an integral column they are compared conservatively: a value is rounded to the nearest double, which is monotonic,
   * so a strict inequality between the rounded values still proves one between the values, and anything else answers
   * {@link BlockMatch#SOME}.
   *
   * @param min     the block's minimum of the column's real samples
   * @param max     the block's maximum
   * @param present how many of the block's samples are real (not the absent marker), {@code < 0} when unknown
   * @param samples how many samples the block holds
   */
  static BlockMatch blockMatch(final Condition condition, final double min, final double max, final long present, final int samples) {
    if (present == 0)
      return BlockMatch.NONE; // only absent markers, which never match
    if (condition.matchesNothing())
      return BlockMatch.NONE;
    if (Double.isNaN(min) || Double.isNaN(max) || present < 0)
      return BlockMatch.SOME;

    if (condition.integral) {
      final double low = condition.lowLong;
      final double high = condition.highLong;
      if ((condition.lowLong != Long.MIN_VALUE && max < low) || (condition.highLong != Long.MAX_VALUE && min > high))
        return BlockMatch.NONE;
      final boolean aboveLow = condition.lowLong == Long.MIN_VALUE || min > low;
      final boolean belowHigh = condition.highLong == Long.MAX_VALUE || max < high;
      return aboveLow && belowHigh && present == samples ? BlockMatch.ALL : BlockMatch.SOME;
    }

    if (max < condition.low || (max == condition.low && !condition.lowInclusive) || min > condition.high
        || (min == condition.high && !condition.highInclusive))
      return BlockMatch.NONE;
    return condition.matches(min) && condition.matches(max) && present == samples ? BlockMatch.ALL : BlockMatch.SOME;
  }

  /**
   * Keeps, out of the rows {@code [from, to)}, those whose values pass every condition, writing their indices into
   * {@code selected} in ascending order. {@code columns[c]} holds the decoded values of condition {@code c}'s column: a
   * {@code long[]} for an integral condition, a {@code double[]} otherwise.
   *
   * @return how many indices were written
   */
  static int select(final List<Condition> conditions, final Object[] columns, final int from, final int to, final int[] selected) {
    int count = 0;
    final Condition first = conditions.getFirst();
    if (first.integral) {
      final long[] values = (long[]) columns[0];
      for (int i = from; i < to; i++)
        if (first.matches(values[i]))
          selected[count++] = i;
    } else {
      final double[] values = (double[]) columns[0];
      for (int i = from; i < to; i++)
        if (first.matches(values[i]))
          selected[count++] = i;
    }
    for (int c = 1; c < conditions.size() && count > 0; c++) {
      final Condition condition = conditions.get(c);
      int kept = 0;
      if (condition.integral) {
        final long[] values = (long[]) columns[c];
        for (int s = 0; s < count; s++)
          if (condition.matches(values[selected[s]]))
            selected[kept++] = selected[s];
      } else {
        final double[] values = (double[]) columns[c];
        for (int s = 0; s < count; s++)
          if (condition.matches(values[selected[s]]))
            selected[kept++] = selected[s];
      }
      count = kept;
    }
    return count;
  }

  /**
   * Whether row {@code row} passes every condition, reading {@code columns} as {@link #select} does. For a caller that walks
   * the rows anyway and needs no index list.
   */
  static boolean matchesAt(final List<Condition> conditions, final Object[] columns, final int row) {
    for (int c = 0; c < conditions.size(); c++) {
      final Condition condition = conditions.get(c);
      if (condition.integral ? !condition.matches(((long[]) columns[c])[row]) : !condition.matches(((double[]) columns[c])[row]))
        return false;
    }
    return true;
  }

  /**
   * Renders the filter for an execution plan, e.g. {@code uu > 90.0 AND ui BETWEEN 1 AND 5}.
   */
  public String describe() {
    final StringBuilder sb = new StringBuilder();
    for (final Condition condition : conditions) {
      if (!sb.isEmpty())
        sb.append(" AND ");
      final String name = condition.columnName;
      if (condition.integral) {
        final boolean hasLow = condition.lowLong != Long.MIN_VALUE;
        final boolean hasHigh = condition.highLong != Long.MAX_VALUE;
        if (condition.lowLong == condition.highLong)
          sb.append(name).append(" = ").append(condition.lowLong);
        else if (hasLow && hasHigh)
          sb.append(name).append(" BETWEEN ").append(condition.lowLong).append(" AND ").append(condition.highLong);
        else if (hasLow)
          sb.append(name).append(" >= ").append(condition.lowLong);
        else
          sb.append(name).append(" <= ").append(condition.highLong);
        continue;
      }
      final boolean hasLow = condition.low != Double.NEGATIVE_INFINITY;
      final boolean hasHigh = condition.high != Double.POSITIVE_INFINITY;
      if (hasLow && condition.low == condition.high && condition.lowInclusive && condition.highInclusive)
        sb.append(name).append(" = ").append(condition.low);
      else {
        if (hasLow)
          sb.append(name).append(condition.lowInclusive ? " >= " : " > ").append(condition.low);
        if (hasLow && hasHigh)
          sb.append(" AND ");
        if (hasHigh)
          sb.append(name).append(condition.highInclusive ? " <= " : " < ").append(condition.high);
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return describe();
  }
}
