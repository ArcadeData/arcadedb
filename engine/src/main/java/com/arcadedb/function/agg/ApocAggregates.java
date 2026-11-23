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
package com.arcadedb.function.agg;

import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The APOC aggregating functions {@code apoc.agg.first}, {@code last}, {@code nth}, {@code slice}, {@code median}, {@code product},
 * {@code percentiles}, {@code statistics}, {@code maxItems} and {@code minItems}, with APOC 5's semantics (issue #12102).
 * <p>
 * In APOC each of them folds the rows of a group into one value, like {@code avg()}. ArcadeDB's {@code agg.*} functions of the same
 * names are scalar functions over ONE list argument ({@code agg.median([1, 2, 3])}), and the {@code apoc.} prefix used to resolve to
 * them, so {@code UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.median(x)} answered four rows, each the median of one value. The
 * openCypher planner now sees an {@code apoc.agg.*} call as an aggregation and builds one of these per group through
 * {@code CypherFunctionFactory}; the {@code agg.*} names keep their list form.
 * <p>
 * Every function skips a null value, as APOC does. The ones that need the whole group (slice, median, percentiles, statistics) hold
 * it until the group ends, charged to the heap budget of the query like {@code collect()}.
 */
public final class ApocAggregates {
  /** The prefix of every name answered here, lower case. */
  public static final String PREFIX = "apoc.agg.";

  /** APOC's default percentiles of {@code apoc.agg.percentiles} and {@code apoc.agg.statistics}. */
  private static final List<Double> DEFAULT_PERCENTILES = List.of(0.5, 0.75, 0.9, 0.95, 0.99);

  private static final String NUMBER = "an INTEGER or a FLOAT";

  private static final int      MIN_CAPACITY = 4;
  private static final double[] NO_DOUBLES   = new double[0];
  private static final long[]   NO_LONGS     = new long[0];

  private static final Map<String, Supplier<StatelessFunction>> FACTORIES = Map.of(//
      PREFIX + "first", First::new, //
      PREFIX + "last", Last::new, //
      PREFIX + "nth", Nth::new, //
      PREFIX + "slice", Slice::new, //
      PREFIX + "median", Median::new, //
      PREFIX + "product", Product::new, //
      PREFIX + "percentiles", Percentiles::new, //
      PREFIX + "statistics", Statistics::new, //
      PREFIX + "maxitems", () -> new ExtremeItems("apoc.agg.maxItems", 1), //
      PREFIX + "minitems", () -> new ExtremeItems("apoc.agg.minItems", -1));

  private ApocAggregates() {
  }

  /** Whether {@code name} (any case) is one of the APOC aggregating functions. */
  public static boolean isAggregate(final String name) {
    return FACTORIES.containsKey(name.toLowerCase(Locale.ROOT));
  }

  /**
   * A new aggregator for the APOC aggregating function {@code name} (any case), one per group, or {@code null} when the name is not
   * one of them.
   */
  public static StatelessFunction create(final String name) {
    final Supplier<StatelessFunction> factory = FACTORIES.get(name.toLowerCase(Locale.ROOT));
    return factory != null ? factory.get() : null;
  }

  /** What every aggregator shares: the APOC name, the aggregation flag and the heap budget of the values a group holds. */
  private abstract static class ApocAggregate implements StatelessFunction, HeapBufferingFunction {
    private final String             name;
    private final int                minArgs;
    private final int                maxArgs;
    private       OperationHeapLimit limit;
    // TRUE WHEN NO STEP HANDED AN OPERATION: THE CHARGE IS THEN THIS FUNCTION'S TO GIVE BACK
    private       boolean            ownLimit;
    // HOW MANY VALUES ARE CHARGED: WHAT THE GROUP HOLDS, OR THE MOST IT HELD FOR AN AGGREGATOR THAT DROPS VALUES
    long                             held;

    ApocAggregate(final String name, final int minArgs, final int maxArgs) {
      this.name = name;
      this.minArgs = minArgs;
      this.maxArgs = maxArgs;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public int getMinArgs() {
      return minArgs;
    }

    @Override
    public int getMaxArgs() {
      return maxArgs;
    }

    @Override
    public boolean aggregateResults() {
      return true;
    }

    @Override
    public void setHeapLimit(final OperationHeapLimit owner) {
      limit = owner.child(name + "()");
      ownLimit = false;
    }

    /** Accounts one more value held until the group ends. */
    void hold(final Object value, final CommandContext context) {
      if (limit == null) {
        limit = OperationHeapLimit.of(context, name + "()");
        ownLimit = true;
      }
      limit.add(++held, value);
    }

    /** Gives back what {@link #hold} charged, when this function charged it itself: the result goes to the caller. */
    void releaseHeld() {
      if (ownLimit && limit != null)
        limit.release();
    }

    @Override
    public final Object execute(final Object[] args, final CommandContext context) {
      checkArity(args);
      accumulate(args, context);
      return null;
    }

    abstract void accumulate(Object[] args, CommandContext context);

    /** A whole number argument, refusing null as APOC does. */
    long wholeNumber(final Object value, final String argumentName) {
      if (value == null)
        throw new InvalidArgumentException(name + "() argument <" + argumentName + "> must not be null");
      return CypherFunctionHelper.requireWholeNumberArgument(value, name, argumentName);
    }

    /** A value that must be a number: a type error otherwise, as APOC answers "Can't coerce ... to Number". */
    Number number(final Object value) {
      if (value instanceof Number n)
        return n;
      throw CypherFunctionHelper.typeMismatch(name, NUMBER, value);
    }

    /** The percentiles of the first row, APOC's default when the call omits them. */
    static double[] percentiles(final Object[] args, final int index, final String name) {
      if (args.length <= index)
        return DEFAULT_PERCENTILES.stream().mapToDouble(Double::doubleValue).toArray();
      final List<Object> list = CypherFunctionHelper.asListOrNull(args[index]);
      if (list == null)
        throw new InvalidArgumentException(name + "() argument <percentiles> must be a LIST<FLOAT>");
      final double[] result = new double[list.size()];
      for (int i = 0; i < result.length; i++) {
        final Object p = list.get(i);
        if (!(p instanceof Number n))
          throw CypherFunctionHelper.typeMismatch(name, "a LIST<FLOAT> of percentiles as", p);
        result[i] = n.doubleValue();
      }
      return result;
    }
  }

  /** {@code apoc.agg.first(value)}: the first non-null value of the group. */
  private static final class First extends ApocAggregate {
    private Object first;

    First() {
      super("apoc.agg.first", 1, 1);
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      if (first == null)
        first = args[0];
    }

    @Override
    public Object getAggregatedResult() {
      return first;
    }
  }

  /** {@code apoc.agg.last(value)}: the last non-null value of the group. */
  private static final class Last extends ApocAggregate {
    private Object last;

    Last() {
      super("apoc.agg.last", 1, 1);
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      if (args[0] != null)
        last = args[0];
    }

    @Override
    public Object getAggregatedResult() {
      return last;
    }
  }

  /**
   * {@code apoc.agg.nth(value, offset)}: the non-null value at {@code offset} (0-based) in the group, the last one for -1, null for
   * an offset past the end or any other negative one.
   */
  private static final class Nth extends ApocAggregate {
    private Object value;
    private long   index;

    Nth() {
      super("apoc.agg.nth", 2, 2);
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      final long offset = wholeNumber(args[1], "offset");
      if (args[0] == null)
        return;
      if (offset == index || offset == -1)
        value = args[0];
      index++;
    }

    @Override
    public Object getAggregatedResult() {
      return value;
    }
  }

  /**
   * {@code apoc.agg.slice(value, from = 0, length = -1)}: the non-null values of the group from the {@code from}-th on, at most
   * {@code length} of them, all of them for -1. APOC names the third argument {@code to} but reads it as a length.
   */
  private static final class Slice extends ApocAggregate {
    private final List<Object> values = new ArrayList<>();
    private       long         index;

    Slice() {
      super("apoc.agg.slice", 1, 3);
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      final long from = args.length > 1 ? wholeNumber(args[1], "from") : 0L;
      final long length = args.length > 2 ? wholeNumber(args[2], "to") : -1L;
      if (args[0] == null)
        return;
      // THE END IS CLAMPED, NOT WRAPPED: A HUGE LENGTH OR OFFSET WOULD OTHERWISE OVERFLOW THE LONG AND SLICE NOTHING
      if (index >= from && (length == -1 || index < saturatedAdd(from, length))) {
        values.add(args[0]);
        hold(args[0], context);
      }
      index++;
    }

    @Override
    public Object getAggregatedResult() {
      releaseHeld();
      return new ArrayList<>(values);
    }
  }

  /** {@code apoc.agg.median(value)}: the median of the numbers of the group as a FLOAT; a value that is no number is skipped. */
  private static final class Median extends ApocAggregate {
    // ALLOCATED WHEN THE FIRST NUMBER ARRIVES: A GROUP THAT NEVER GETS ONE (HIGH-CARDINALITY GROUP BY) COSTS NO ARRAY
    private double[] values = NO_DOUBLES;
    private int      size;

    Median() {
      super("apoc.agg.median", 1, 1);
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      if (!(args[0] instanceof Number n))
        return;
      // CHARGED FIRST, SO THAT A GROUP OVER THE CAP FAILS BEFORE ITS ARRAY GROWS
      hold(n, context);
      if (size == values.length)
        values = Arrays.copyOf(values, Math.max(MIN_CAPACITY, size << 1));
      values[size++] = n.doubleValue();
    }

    @Override
    public Object getAggregatedResult() {
      releaseHeld();
      if (size == 0)
        return null;
      Arrays.sort(values, 0, size);
      final int middle = size / 2;
      return size % 2 == 1 ? values[middle] : (values[middle - 1] + values[middle]) / 2.0;
    }
  }

  /**
   * {@code apoc.agg.product(value)}: the product of the numbers of the group, an INTEGER while every factor is one (an overflow is
   * an error, as in APOC) and a FLOAT from the first FLOAT on. APOC answers 0.0 for a group without a number.
   */
  private static final class Product extends ApocAggregate {
    private long    longProduct   = 1L;
    private double  doubleProduct = 1.0;
    private boolean integral      = true;
    private boolean any;

    Product() {
      super("apoc.agg.product", 1, 1);
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      if (args[0] == null)
        return;
      final Number n = number(args[0]);
      any = true;
      if (integral && isIntegral(n)) {
        try {
          longProduct = Math.multiplyExact(longProduct, n.longValue());
        } catch (final ArithmeticException e) {
          throw new InvalidArgumentException(getName() + "() overflows the INTEGER range");
        }
        return;
      }
      if (integral) {
        doubleProduct = longProduct;
        integral = false;
      }
      doubleProduct *= n.doubleValue();
    }

    @Override
    public Object getAggregatedResult() {
      if (!any)
        return 0.0;
      return integral ? (Object) longProduct : (Object) doubleProduct;
    }
  }

  /**
   * The numbers of a group, held as longs while each one is an INTEGER and as doubles from the first FLOAT on, so that an INTEGER
   * percentile is answered as the INTEGER it is, as APOC does.
   */
  private abstract static class NumbersOfGroup extends ApocAggregate {
    // ALLOCATED WHEN THE FIRST NUMBER ARRIVES: A GROUP THAT NEVER GETS ONE (HIGH-CARDINALITY GROUP BY) COSTS NO ARRAY
    long[]   longs = NO_LONGS;
    double[] doubles;
    int      size;

    NumbersOfGroup(final String name, final int minArgs, final int maxArgs) {
      super(name, minArgs, maxArgs);
    }

    void add(final Number n, final CommandContext context) {
      // CHARGED FIRST, SO THAT A GROUP OVER THE CAP FAILS BEFORE ITS ARRAY GROWS
      hold(n, context);
      if (doubles == null && !isIntegral(n)) {
        doubles = new double[Math.max(MIN_CAPACITY, size << 1)];
        for (int i = 0; i < size; i++)
          doubles[i] = longs[i];
        longs = null;
      }
      if (doubles == null) {
        if (size == longs.length)
          longs = Arrays.copyOf(longs, Math.max(MIN_CAPACITY, size << 1));
        longs[size++] = n.longValue();
      } else {
        if (size == doubles.length)
          doubles = Arrays.copyOf(doubles, Math.max(MIN_CAPACITY, size << 1));
        doubles[size++] = n.doubleValue();
      }
    }

    void sort() {
      if (doubles == null)
        Arrays.sort(longs, 0, size);
      else
        Arrays.sort(doubles, 0, size);
    }

    double doubleAt(final int i) {
      return doubles == null ? longs[i] : doubles[i];
    }

    /**
     * The percentile of the sorted numbers by APOC's rank, HdrHistogram's {@code valueAtPercentile}: the value at the rank
     * {@code p * count} rounded half up, at least the first and at most the last ({@code [1, 2, 3]} answers 2 for 0.75). The clamp is
     * deliberate and what the histogram does: a percentile below 0 or NaN answers the first value, one above 1 the last. APOC reads
     * it from the histogram's buckets, whose FLOAT values are approximations ({@code 2.5000076} for 2.5); this answers the value
     * itself.
     */
    Object percentile(final double p) {
      final int rank = (int) Math.min(size, Math.max(1L, (long) (p * size + 0.5))) - 1;
      return doubles == null ? (Object) longs[rank] : (Object) doubles[rank];
    }
  }

  /** {@code apoc.agg.percentiles(value, percentiles = [0.5, 0.75, 0.9, 0.95, 0.99])}: the percentiles of the numbers of the group. */
  private static final class Percentiles extends NumbersOfGroup {
    private double[] percentiles;

    Percentiles() {
      super("apoc.agg.percentiles", 1, 2);
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      if (percentiles == null)
        percentiles = percentiles(args, 1, getName());
      if (args[0] != null)
        add(number(args[0]), context);
    }

    @Override
    public Object getAggregatedResult() {
      releaseHeld();
      final int count = percentiles != null ? percentiles.length : DEFAULT_PERCENTILES.size();
      final List<Object> result = new ArrayList<>(count);
      if (size == 0) {
        for (int i = 0; i < count; i++)
          result.add(null);
        return result;
      }
      sort();
      for (final double p : percentiles)
        result.add(percentile(p));
      return result;
    }
  }

  /**
   * {@code apoc.agg.statistics(value, percentiles = [0.5, 0.75, 0.9, 0.95, 0.99])}: APOC's map of {@code total}, {@code min},
   * {@code minNonZero}, {@code max}, {@code mean}, {@code stdev} (of the population) and one entry per percentile, keyed by the
   * percentile as text ({@code "0.5"}). As in APOC, {@code minNonZero} is {@code Long.MAX_VALUE} as a FLOAT when no number is
   * other than 0, and an empty group has no percentile entry. NaN is the greatest value, as in the percentiles: it is the {@code max}
   * of a group that holds one, and a NaN does not hide the smallest number as {@code min}.
   */
  private static final class Statistics extends NumbersOfGroup {
    private double[] percentiles;
    private Number   min;
    private Number   max;

    Statistics() {
      super("apoc.agg.statistics", 1, 2);
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      if (percentiles == null)
        percentiles = percentiles(args, 1, getName());
      if (args[0] == null)
        return;
      final Number n = number(args[0]);
      // THE ORDER THE PERCENTILES ARE SORTED BY (NaN GREATEST): A NaN MET FIRST MUST NOT PIN MIN AND MAX TO NaN
      if (min == null || lessThan(n.doubleValue(), min.doubleValue()))
        min = n;
      if (max == null || lessThan(max.doubleValue(), n.doubleValue()))
        max = n;
      add(n, context);
    }

    @Override
    public Object getAggregatedResult() {
      releaseHeld();
      final Map<String, Object> result = new LinkedHashMap<>();
      result.put("total", (long) size);
      result.put("min", min);
      result.put("max", max);

      double sum = 0.0;
      double minNonZero = Long.MAX_VALUE;
      for (int i = 0; i < size; i++) {
        final double v = doubleAt(i);
        sum += v;
        if (v != 0.0 && v < minNonZero)
          minNonZero = v;
      }
      final double mean = size == 0 ? 0.0 : sum / size;
      double squares = 0.0;
      for (int i = 0; i < size; i++) {
        final double d = doubleAt(i) - mean;
        squares += d * d;
      }
      result.put("minNonZero", minNonZero);
      result.put("mean", mean);
      result.put("stdev", size == 0 ? 0.0 : Math.sqrt(squares / size));

      if (size > 0) {
        sort();
        for (final double p : percentiles)
          result.put(Double.toString(p), percentile(p));
      }
      return result;
    }
  }

  /**
   * {@code apoc.agg.maxItems(item, value, groupLimit = -1)} and {@code apoc.agg.minItems(...)}: the map {@code {items, value}} of the
   * greatest (smallest) value of the group and the items that carry it, at most {@code groupLimit} of them (all for a negative
   * limit; a limit of 0 keeps the first). A row whose item or value is null is skipped.
   */
  private static final class ExtremeItems extends ApocAggregate {
    private final int          sign;
    private final List<Object> items = new ArrayList<>();
    private       Object       value;

    ExtremeItems(final String name, final int sign) {
      super(name, 2, 3);
      this.sign = sign;
    }

    @Override
    void accumulate(final Object[] args, final CommandContext context) {
      final long groupLimit = args.length > 2 ? wholeNumber(args[2], "groupLimit") : -1L;
      if (args[0] == null || args[1] == null)
        return;
      final int compared = value == null ? 1 : sign * CypherFunctionHelper.cypherCompare(args[1], value);
      if (compared > 0) {
        value = args[1];
        items.clear();
      } else if (compared < 0 || (groupLimit >= 0 && items.size() >= Math.max(1L, groupLimit)))
        return;
      items.add(args[0]);
      // a new extreme drops the items held so far, so only a list longer than any before costs more heap
      if (items.size() > held)
        hold(args[0], context);
    }

    @Override
    public Object getAggregatedResult() {
      releaseHeld();
      final Map<String, Object> result = new LinkedHashMap<>(2);
      result.put("items", new ArrayList<>(items));
      result.put("value", value);
      return result;
    }
  }

  private static boolean isIntegral(final Number n) {
    return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
  }

  /** {@code a < b} with NaN the greatest value, the order {@link Arrays#sort(double[])} gives the percentiles. */
  private static boolean lessThan(final double a, final double b) {
    return a < b || (Double.isNaN(b) && !Double.isNaN(a));
  }

  /** {@code a + b} clamped to the long range instead of wrapping around. */
  private static long saturatedAdd(final long a, final long b) {
    final long sum = a + b;
    // OVERFLOW: BOTH OPERANDS HAVE A SIGN THE SUM DOES NOT
    if (((a ^ sum) & (b ^ sum)) < 0)
      return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
    return sum;
  }
}
