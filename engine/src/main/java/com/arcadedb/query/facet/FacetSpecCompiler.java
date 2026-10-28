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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FacetItem;
import com.arcadedb.query.sql.parser.FacetRangeItem;
import com.arcadedb.query.sql.parser.FacetsClause;
import com.arcadedb.query.sql.parser.InputParameter;
import com.arcadedb.query.sql.parser.Modifier;
import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Turns the {@link FacetsClause} of a SELECT into {@link FacetSpec}s: applies the defaults, resolves input parameters and checks the
 * values the grammar cannot (TOP above the configured maximum, empty ranges, unknown time zones, histogram intervals).
 */
public final class FacetSpecCompiler {
  private static final int  DEFAULT_TOP = 10;
  private static final long DAY_MS      = 86_400_000L;
  private static final int  MAX_BUCKETS  = 100_000;

  private FacetSpecCompiler() {
  }

  /**
   * @param clause the parsed FACETS clause
   * @param ctx    the context whose input parameters resolve the parameters used in bounds and INCLUDE lists
   *
   * @throws CommandSQLParsingException on an invalid value
   */
  public static List<FacetSpec> compile(final FacetsClause clause, final CommandContext ctx) {
    final List<FacetSpec> specs = new ArrayList<>(clause.items.size());
    for (final FacetItem item : clause.items)
      specs.add(compile(item, ctx));
    return specs;
  }

  /**
   * Checks the facets that depend on the schema of the faceted type: STATS needs a numeric or date property, HIERARCHY a COLUMNAR
   * index with a hierarchy in its METADATA.
   *
   * A property or index declared only on a subtype is enough: the executor resolves the declaring type per bucket.
   *
   * @param target the faceted type, null for a bucket or subquery target: only the type-free checks (made by {@link #compile}) apply,
   *               the executor makes these per declaring type
   *
   * @throws CommandSQLParsingException on a facet the type cannot answer
   */
  public static void validateAgainstSchema(final List<FacetSpec> specs, final DocumentType target) {
    if (target == null)
      return;
    for (final FacetSpec spec : specs)
      switch (spec) {
      case StatsFacetSpec stats -> {
        if (!anyTypeMatches(target, type -> {
          final Property declared = type.getPolymorphicPropertyIfExists(stats.property());
          return declared != null && isStatsType(declared.getType());
        }))
          throw new CommandSQLParsingException(
              "STATS requires a property of type BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DATE or DATETIME: " + stats.property());
      }
      case HierarchyFacetSpec hierarchy -> {
        if (!anyTypeMatches(target, type -> hasHierarchy(type, hierarchy.property())))
          throw new CommandSQLParsingException(
              "HIERARCHY requires a COLUMNAR index with METADATA hierarchy on " + target.getName() + "." + hierarchy.property());
      }
      default -> {
      }
      }
  }

  /** Whether the type or any of its subtypes (a query on a type reads its subtypes' buckets) satisfies the predicate. */
  private static boolean anyTypeMatches(final DocumentType type, final Predicate<DocumentType> predicate) {
    if (predicate.test(type))
      return true;
    for (final DocumentType sub : type.getSubTypes())
      if (anyTypeMatches(sub, predicate))
        return true;
    return false;
  }

  private static boolean isStatsType(final Type type) {
    return switch (type) {
      case BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DATE, DATETIME, DATETIME_SECOND -> true;
      default -> false;
    };
  }

  private static boolean hasHierarchy(final DocumentType target, final String property) {
    final TypeIndex index = target.getPolymorphicColumnarIndex(property);
    if (index != null)
      for (final IndexInternal sub : index.getSubIndexes())
        if (sub instanceof ColumnarIndex columnar) {
          final ColumnarIndexMetadata metadata = columnar.getColumnarMetadata();
          return metadata != null && metadata.getHierarchy() != null;
        }
    return false;
  }

  /**
   * Parses a histogram interval.
   *
   * @param calendar true for CALENDAR (case-sensitive units {@code m h d w M q y}, multiplier 1), false for FIXED (a number for a
   *                 numeric property, or {@code N(s|m|h|d|w)} as {@code ts.timeBucket} takes it)
   * @param text     the interval as written: a {@link String} or a {@link Number}
   */
  public static HistogramInterval parseInterval(final boolean calendar, final Object text) {
    if (calendar)
      return parseCalendarInterval(text);

    if (text instanceof Number number) {
      final double value = number.doubleValue();
      if (!(value > 0) || Double.isInfinite(value))
        throw new CommandSQLParsingException("FIXED histogram interval must be positive: " + text);
      return new HistogramInterval(false, null, 0L, value, text.toString());
    }

    final String string = String.valueOf(text).trim();
    if (!string.isEmpty()) {
      final char unit = string.charAt(string.length() - 1);
      // parseInterval lowercases its unit, so '1M' would silently mean one minute
      if (unit == 'M' || unit == 'q' || unit == 'y' || unit == 'Q' || unit == 'Y') {
        final String hint = unit == 'Q' || unit == 'Y' ? string.toLowerCase(Locale.ROOT) : string;
        throw new CommandSQLParsingException(
            "FIXED '" + string + "' is not a fixed interval: months, quarters and years vary in length, use CALENDAR '" + hint + "'");
      }
    }
    final long millis;
    try {
      millis = SQLFunctionTimeBucket.parseInterval(string);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException("Invalid FIXED histogram interval '" + string + "': " + e.getMessage());
    }
    if (millis <= 0)
      throw new CommandSQLParsingException("FIXED histogram interval must be positive: '" + string + "'");
    return new HistogramInterval(false, null, millis, 0d, string);
  }

  private static HistogramInterval parseCalendarInterval(final Object text) {
    if (!(text instanceof String string))
      throw new CommandSQLParsingException("CALENDAR histogram interval must be a string such as '1d' or '1M', found: " + text);

    int unitStart = 0;
    while (unitStart < string.length() && Character.isDigit(string.charAt(unitStart)))
      unitStart++;
    if (unitStart == 0 || unitStart != string.length() - 1)
      throw new CommandSQLParsingException("Invalid CALENDAR histogram interval '" + string + "', expected 1m, 1h, 1d, 1w, 1M, 1q or 1y");

    final TemporalUnit unit = switch (string.charAt(unitStart)) {
      case 'm' -> ChronoUnit.MINUTES;
      case 'h' -> ChronoUnit.HOURS;
      case 'd' -> ChronoUnit.DAYS;
      case 'w' -> ChronoUnit.WEEKS;
      case 'M' -> ChronoUnit.MONTHS;
      case 'q' -> IsoFields.QUARTER_YEARS;
      case 'y' -> ChronoUnit.YEARS;
      default -> throw new CommandSQLParsingException(
          "Unknown CALENDAR histogram unit in '" + string + "', expected 1m, 1h, 1d, 1w, 1M, 1q or 1y");
    };
    if (!string.substring(0, unitStart).equals("1"))
      throw new CommandSQLParsingException("CALENDAR intervals take a multiplier of 1: '" + string + "'");
    return new HistogramInterval(true, unit, 0L, 0d, string);
  }

  private static FacetSpec compile(final FacetItem item, final CommandContext ctx) {
    final String alias = item.getName();
    final String property = plainPath(item.expression);
    final Expression expression = property == null ? item.expression : null;

    return switch (item.kind) {
      case TERMS -> new TermsFacetSpec(alias, property, expression, item.filter, top(alias, item), minCount(item), order(item),
          include(item, ctx), item.missing, after(alias, item, ctx));
      case RANGES -> new RangeFacetSpec(alias, property, expression, item.filter, ranges(alias, item, ctx), item.defaultLabel);
      case HISTOGRAM -> histogram(alias, property, expression, item, ctx);
      case HIERARCHY -> new HierarchyFacetSpec(alias, property(alias, item, property), item.filter, top(alias, item), minCount(item));
      case DISTINCT -> new DistinctFacetSpec(alias, property(alias, item, property), item.filter, precision(alias, item));
      case STATS -> new StatsFacetSpec(alias, property(alias, item, property), item.filter);
    };
  }

  /** The dotted path when the expression is a chain of plain property names ({@code a} or {@code a.b.c}), else null. */
  private static String plainPath(final Expression expression) {
    if (!(expression.mathExpression instanceof BaseExpression base) || base.identifier == null || !base.identifier.isBaseIdentifier())
      return null;
    final StringBuilder path = new StringBuilder(base.identifier.getSuffix().identifier.getStringValue());
    for (Modifier m = base.getModifier(); m != null; m = m.next) {
      if (m.suffix == null || !m.suffix.isBaseIdentifier() || m.squareBrackets || m.arrayRange != null || m.condition != null
          || m.arraySingleValues != null || m.rightBinaryCondition != null || m.methodCall != null || m.nestedProjection != null)
        return null;
      path.append('.').append(m.suffix.identifier.getStringValue());
    }
    return path.toString();
  }

  /** The property of a HIERARCHY, DISTINCT or STATS facet: these kinds do not take an expression. */
  private static String property(final String alias, final FacetItem item, final String property) {
    if (property == null)
      throw new CommandSQLParsingException("FACETS " + alias + ": " + item.kind + " needs a property, not an expression");
    return property;
  }

  /** The cursor of AFTER, null when the facet has none. AFTER pages in ascending value order, so it excludes any other order. */
  private static Object after(final String alias, final FacetItem item, final CommandContext ctx) {
    if (item.after == null)
      return null;
    if (Boolean.TRUE.equals(item.orderByCount))
      throw new CommandSQLParsingException("AFTER pages in value order: remove ORDER BY COUNT");
    if (Boolean.FALSE.equals(item.orderByCount) && Boolean.FALSE.equals(item.ascending))
      throw new CommandSQLParsingException("AFTER pages in ascending value order: remove ORDER BY VALUE DESC");
    if (item.include != null)
      throw new CommandSQLParsingException("AFTER and INCLUDE cannot be combined");
    final Object cursor = item.after.execute((Result) null, ctx);
    if (cursor == null)
      throw new CommandSQLParsingException("FACETS " + alias + ": AFTER is null (its parameter is missing or bound to null), leave AFTER out for the first page");
    return cursor;
  }

  private static int top(final String alias, final FacetItem item) {
    if (item.top == null)
      return DEFAULT_TOP;
    final int top = item.top.getValue().intValue();
    final int max = GlobalConfiguration.COLUMNAR_TOP_MAX.getValueAsInteger();
    if (top > max)
      throw new CommandSQLParsingException(
          "FACETS " + alias + ": TOP " + top + " is above the maximum of " + max + " (setting " + GlobalConfiguration.COLUMNAR_TOP_MAX.getKey()
              + ")");
    if (top < 1)
      throw new CommandSQLParsingException("FACETS " + alias + ": TOP must be at least 1");
    return top;
  }

  private static long precision(final String alias, final FacetItem item) {
    if (item.precision == null)
      return Long.MAX_VALUE;
    if (item.precision < 1)
      throw new CommandSQLParsingException("FACETS " + alias + ": DISTINCT PRECISION must be at least 1");
    return item.precision;
  }

  private static int minCount(final FacetItem item) {
    return item.minCount == null ? 1 : item.minCount.getValue().intValue();
  }

  private static FacetOrder order(final FacetItem item) {
    if (item.orderByCount == null)
      return FacetOrder.COUNT_DESC;
    if (item.orderByCount)
      return Boolean.TRUE.equals(item.ascending) ? FacetOrder.COUNT_ASC : FacetOrder.COUNT_DESC;
    return Boolean.FALSE.equals(item.ascending) ? FacetOrder.VALUE_DESC : FacetOrder.VALUE_ASC;
  }

  private static List<Object> include(final FacetItem item, final CommandContext ctx) {
    if (item.include == null)
      return null;
    final List<Object> values = new ArrayList<>(item.include.size());
    for (final Expression e : item.include)
      values.add(e.execute((Result) null, ctx));
    return values;
  }

  private static List<RangeBound> ranges(final String alias, final FacetItem item, final CommandContext ctx) {
    final List<RangeBound> ranges = new ArrayList<>(item.ranges.size());
    final Set<String> keys = new HashSet<>();
    for (final FacetRangeItem r : item.ranges) {
      final Object from = resolve(alias, r.from, ctx);
      final Object to = resolve(alias, r.to, ctx);
      if (from != null && to != null && isEmpty(from, to))
        throw emptyRange(alias, from, to);
      final String key = r.label != null ? r.label : bound(from) + "-" + bound(to);
      if (!keys.add(key))
        throw new CommandSQLParsingException("FACETS " + alias + ": duplicate range key '" + key + "'");
      ranges.add(new RangeBound(from, to, key));
    }
    return ranges;
  }

  /** The refusal of a range that holds no value; the range collector raises it too, once the bounds are in the column's encoding. */
  static CommandSQLParsingException emptyRange(final String alias, final Object from, final Object to) {
    return new CommandSQLParsingException("FACETS " + alias + ": empty range [" + from + ", " + to + ")");
  }

  private static String bound(final Object bound) {
    return bound == null ? "*" : bound.toString();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static boolean isEmpty(final Object from, final Object to) {
    if (from instanceof Number a && to instanceof Number b)
      return isIntegral(a) && isIntegral(b) ? Long.compare(a.longValue(), b.longValue()) >= 0 : Double.compare(a.doubleValue(), b.doubleValue()) >= 0;
    if (from instanceof Comparable c && from.getClass() == to.getClass())
      return c.compareTo(to) >= 0;
    return false;
  }

  private static boolean isIntegral(final Number n) {
    return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
  }

  private static Object resolve(final String alias, final Object value, final CommandContext ctx) {
    if (!(value instanceof InputParameter parameter))
      return value;
    final Object resolved = parameter.getValue(ctx.getInputParameters());
    if (resolved == null)
      throw new CommandSQLParsingException("FACETS " + alias + ": a bound parameter is bound to null, use * for an unbounded end");
    return resolved;
  }

  private static FacetSpec histogram(final String alias, final String property, final Expression expression, final FacetItem item,
      final CommandContext ctx) {
    final boolean calendar = Boolean.TRUE.equals(item.calendar);
    final HistogramInterval interval = parseInterval(calendar, item.interval);
    validateZoneAndOffset(alias, calendar, interval, item.timezone, item.offset);
    final Object boundsFrom = resolve(alias, item.boundsFrom, ctx);
    final Object boundsTo = resolve(alias, item.boundsTo, ctx);
    checkBucketCount(alias, interval, boundsFrom, boundsTo);
    return new HistogramFacetSpec(alias, property, expression, item.filter, interval, item.timezone, item.offset, minCount(item),
        boundsFrom, boundsTo);
  }

  /** BOUNDS with a FIXED interval make the bucket count known before any data is read. */
  private static void checkBucketCount(final String alias, final HistogramInterval interval, final Object from, final Object to) {
    if (interval.calendar() || !(from instanceof Number lo) || !(to instanceof Number hi))
      return;
    final double step = interval.fixedMillis() > 0 ? interval.fixedMillis() : interval.fixedNumber();
    if ((hi.doubleValue() - lo.doubleValue()) / step > MAX_BUCKETS)
      throw new CommandSQLParsingException("FACETS " + alias + ": the histogram would have more than " + MAX_BUCKETS + " buckets");
  }

  private static void validateZoneAndOffset(final String alias, final boolean calendar, final HistogramInterval interval,
      final String timezone, final String offset) {
    if (!calendar && interval.fixedNumber() > 0) {
      // a numeric histogram: buckets start at OFFSET + k * step (a number), a zone means nothing to numbers
      if (timezone != null)
        throw new CommandSQLParsingException("FACETS " + alias + ": TIMEZONE applies only to time histograms (CALENDAR, or FIXED with a duration)");
      if (offset != null)
        try {
          final double value = Double.parseDouble(offset.trim());
          if (!Double.isFinite(value))
            throw new NumberFormatException();
        } catch (final NumberFormatException e) {
          throw new CommandSQLParsingException("FACETS " + alias + ": the OFFSET of a numeric histogram must be a number, found '" + offset + "'");
        }
      return;
    }
    try {
      if (!calendar && interval.fixedMillis() > 0) {
        // a FIXED time histogram follows the ts.timeBucket rules: one of timezone or offset, and a fixed-offset zone
        final Map<String, Object> options = new HashMap<>(2);
        if (timezone != null)
          options.put("timezone", timezone);
        if (offset != null)
          options.put("offset", offset);
        SQLFunctionTimeBucket.resolveOffset(options, interval.text(), interval.fixedMillis());
      } else {
        if (timezone != null)
          ZoneId.of(timezone.trim());
        if (offset != null)
          SQLFunctionTimeBucket.resolveOffset(Map.of("offset", offset), "1d", DAY_MS);
      }
    } catch (final DateTimeException | IllegalArgumentException e) {
      final String reason = e instanceof DateTimeException ? "unknown time zone '" + timezone + "'" : e.getMessage();
      throw new CommandSQLParsingException("FACETS " + alias + ": " + reason);
    }
  }
}
