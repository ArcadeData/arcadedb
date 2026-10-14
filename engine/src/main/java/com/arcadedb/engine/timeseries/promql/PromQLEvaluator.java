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
package com.arcadedb.engine.timeseries.promql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.AggregationMetrics;
import com.arcadedb.engine.timeseries.TimeSeriesNaN;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.TimeBoundRegex;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.ScalarResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.AggregationExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.FunctionCallExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.LabelMatcher;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.MatchOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.MatrixSelector;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.NumberLiteral;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.StringLiteral;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.UnaryExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.VectorSelector;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Tree-walking interpreter for PromQL AST.
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PromQLEvaluator {

  private static final long DEFAULT_LOOKBACK_MS  = 5 * 60_000; // 5 minutes
  private static final int  MAX_RECURSION_DEPTH  = 64;
  private static final long MAX_RANGE_STEPS      = 1_000_000;

  private final DatabaseInternal        database;
  private final long                    lookbackMs;
  // Lazily computed, then shared for the lifetime of this evaluator instance (issue #5886 follow-up): a fresh
  // PromQLEvaluator is created per top-level query (see SQLFunctionPromQL), never reused across executions the
  // way RegexExpression's AST nodes are cached by CypherStatementCache, so caching this as an instance field is
  // safe here (no stale-deadline-across-executions risk). Without it, evaluateRange()'s per-step loop - up to
  // MAX_RANGE_STEPS = 1,000,000 steps - would let each step's evaluateVectorSelector()/evaluateMatrixSelector()
  // compute its own fresh regexDeadline, reopening the N-times-timeout bypass one level up from the per-row
  // sharing those methods already do within a single step's scan.
  private Long                          regexDeadline;
  private static final int              MAX_REGEX_LENGTH   = 1024;
  private static final int              MAX_PATTERN_CACHE  = 1024;
  // Detects patterns that cause catastrophic backtracking (ReDoS):
  // 1. Nested quantifiers: (a+)+, (a*b)*, (a+){n,}
  // 2. Alternation groups with outer quantifier: (a|aa)+ — overlapping alternatives
  private static final Pattern          REDOS_CHECK        = Pattern.compile(
      "\\((?:[^()\\\\]|\\\\.)*[+*](?:[^()\\\\]|\\\\.)*\\)[+*{]"   // nested quantifier (including {n,})
      + "|\\((?:[^()\\\\]|\\\\.)*\\|(?:[^()\\\\]|\\\\.)*\\)[+*{]" // alternation with quantifier
  );
  private final Set<String>             warnedMultiFieldTypes = Collections.newSetFromMap(new ConcurrentHashMap<>());
  // LRU-evicting pattern cache: synchronized LinkedHashMap removes eldest entry when full
  private final Map<String, Pattern>    patternCache;

  {
    // LinkedHashMap with access-order = true gives LRU eviction (oldest access evicted first)
    patternCache = Collections.synchronizedMap(new LinkedHashMap<>(MAX_PATTERN_CACHE, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(final Map.Entry<String, Pattern> eldest) {
        return size() > MAX_PATTERN_CACHE;
      }
    });
  }

  public PromQLEvaluator(final DatabaseInternal database) {
    this(database, DEFAULT_LOOKBACK_MS);
  }

  public PromQLEvaluator(final DatabaseInternal database, final long lookbackMs) {
    this.database = database;
    this.lookbackMs = lookbackMs;
  }

  // The read counters this evaluator's selector scans fill in, or null for "do not count" (issue #7717).
  private AggregationMetrics readMetrics;

  private long regexDeadline() {
    if (regexDeadline == null)
      regexDeadline = TimeBoundRegex.newDeadline(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(database));
    return regexDeadline;
  }

  /**
   * Overrides the lazily-computed {@link #regexDeadline()} with one already resolved elsewhere (issue #5886,
   * 17th review pass). {@code SQLFunctionPromQL} constructs a fresh {@code PromQLEvaluator} on every {@code
   * promql()} call rather than reusing one across a query, so without this, each call would fall back to its
   * own {@code GlobalConfiguration}-derived budget instead of the {@code CommandContext}-cached deadline every
   * other per-row SQL function in this issue shares ({@code text.regexReplace()}, {@code .normalize()}).
   * Must be called before the first regex-bearing evaluation on this instance to have any effect.
   */
  public void setRegexDeadline(final long regexDeadline) {
    this.regexDeadline = regexDeadline;
  }

  /**
   * Counts what this evaluator's selector scans do into {@code metrics} (issue #7717), or stops counting when
   * given {@code null}, which is the default and what every caller outside the two PromQL HTTP endpoints uses.
   * <p>
   * A field rather than a parameter on {@code evaluateInstant}/{@code evaluateRange}, matching
   * {@link #setRegexDeadline(long)} right above: an evaluator is built per request, the metrics belong to that
   * request, and threading a counter through every arm of the expression evaluator would put it in a dozen
   * signatures that have nothing to say about it. Set it before evaluating.
   */
  public void setReadMetrics(final AggregationMetrics readMetrics) {
    this.readMetrics = readMetrics;
  }

  /**
   * Evaluate an instant query at a single timestamp.
   */
  public PromQLResult evaluateInstant(final PromQLExpr expr, final long evalTimeMs) {
    return evaluate(expr, evalTimeMs, evalTimeMs, evalTimeMs, 0, 0);
  }

  /**
   * Evaluate a range query, returning a matrix result with values at each step.
   */
  public PromQLResult evaluateRange(final PromQLExpr expr, final long startMs, final long endMs, final long stepMs) {
    if (endMs < startMs)
      throw new IllegalArgumentException("endMs (" + endMs + ") must be >= startMs (" + startMs + ")");
    if (stepMs <= 0)
      throw new IllegalArgumentException("stepMs must be positive, got: " + stepMs);

    // Math.subtractExact, not a plain subtraction: a span wider than Long.MAX_VALUE milliseconds (start
    // -9e18, end 9e18 - both accepted by the ordering test above) wrapped negative, so the step-count guard
    // below passed and the loop ran unbounded, wedging the calling thread for the life of the process
    // (issue #6807). An overflowing span is rejected as the bad request it is rather than silently wrapping.
    final long spanMs;
    try {
      spanMs = Math.subtractExact(endMs, startMs);
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException(
          "Range [" + startMs + "," + endMs + "] is wider than the representable time span. Reduce the time range");
    }

    // Tested BEFORE the +1, which would itself overflow to Long.MIN_VALUE for spanMs=Long.MAX_VALUE and
    // stepMs=1 and turn the rejection into a silently empty result. Same threshold as `span/step + 1 > MAX`.
    final long stepsAfterFirst = spanMs / stepMs;
    if (stepsAfterFirst >= MAX_RANGE_STEPS)
      throw new IllegalArgumentException(
          "Range query would produce more than the maximum of " + MAX_RANGE_STEPS + " steps (" + stepsAfterFirst
              + " intervals of " + stepMs + "ms). Increase stepMs or reduce the time range");

    final long maxSteps = stepsAfterFirst + 1;

    // A string literal evaluates to a NaN scalar, so the result-type test inside the loop cannot see it (issue #9922).
    // The literal is the only string-typed expression there is: the parser returns the inner expression of a
    // parenthesized one, so `("a")` arrives here bare, and no function, aggregation or operator yields a string
    if (expr instanceof StringLiteral)
      throw new IllegalArgumentException(invalidRangeQueryType("string"));

    // For range queries, evaluate at each step point and collect into MatrixResult
    final Map<String, List<double[]>> seriesMap = new LinkedHashMap<>();
    final Map<String, Map<String, String>> labelsMap = new LinkedHashMap<>();

    // Indexed by step rather than accumulated into `t`: `t += stepMs` could itself overflow past
    // Long.MAX_VALUE and wrap to a value still <= endMs, which is the same unbounded loop by another route.
    // `i * stepMs <= spanMs` holds for every iteration, so `startMs + i * stepMs` cannot overflow.
    for (long i = 0; i < maxSteps; i++) {
      final long t = startMs + i * stepMs;
      final PromQLResult result = evaluate(expr, t, startMs, endMs, stepMs, 0);
      if (result instanceof InstantVector iv) {
        for (final VectorSample sample : iv.samples()) {
          final String key = labelKey(sample.labels());
          // Stamped with the evaluation step `t`, not with the sample's own timestamp (issue #6938):
          // Prometheus guarantees one matrix point per step at exactly start + n*step, while the sample
          // timestamp is wherever the lookback window happened to resolve. A series sparser than the step
          // resolved to the same sample on several consecutive steps and repeated its timestamp, so any
          // consumer indexing the matrix by timestamp (Grafana included) saw duplicate keys and jitter
          // instead of a regular grid. The raw timestamp still drives staleness/lookback inside evaluate().
          seriesMap.computeIfAbsent(key, k -> new ArrayList<>()).add(new double[] { t, sample.value() });
          labelsMap.putIfAbsent(key, sample.labels());
        }
      } else if (result instanceof ScalarResult sr) {
        final String key = "{}";
        seriesMap.computeIfAbsent(key, k -> new ArrayList<>()).add(new double[] { t, sr.value() });
        labelsMap.putIfAbsent(key, Map.of());
      } else
        // a range vector (`m[5m]`) used to fall through both branches and answer an empty matrix (issue #9922). The type
        // of an expression does not depend on the step, so this fires on the first one, before any series is collected
        throw new IllegalArgumentException(invalidRangeQueryType(valueTypeName(expr, result)));
    }

    final List<MatrixSeries> series = new ArrayList<>();
    for (final Map.Entry<String, List<double[]>> entry : seriesMap.entrySet())
      series.add(new MatrixSeries(labelsMap.get(entry.getKey()), entry.getValue()));
    return new MatrixResult(series);
  }

  private PromQLResult evaluate(final PromQLExpr expr, final long evalTimeMs, final long queryStartMs, final long queryEndMs,
      final long stepMs, final int depth) {
    if (depth > MAX_RECURSION_DEPTH)
      throw new IllegalArgumentException("PromQL expression exceeds maximum nesting depth of " + MAX_RECURSION_DEPTH);
    final int next = depth + 1;
    return switch (expr) {
      case NumberLiteral nl -> new ScalarResult(nl.value(), evalTimeMs);
      case StringLiteral ignored -> new ScalarResult(Double.NaN, evalTimeMs);
      case VectorSelector vs -> evaluateVectorSelector(vs, evalTimeMs);
      case MatrixSelector ms -> evaluateMatrixSelector(ms, evalTimeMs);
      case AggregationExpr agg -> evaluateAggregation(agg, evalTimeMs, queryStartMs, queryEndMs, stepMs, next);
      case FunctionCallExpr fn -> evaluateFunction(fn, evalTimeMs, queryStartMs, queryEndMs, stepMs, next);
      case BinaryExpr bin -> evaluateBinary(bin, evalTimeMs, queryStartMs, queryEndMs, stepMs, next);
      case UnaryExpr un -> evaluateUnary(un, evalTimeMs, queryStartMs, queryEndMs, stepMs, next);
    };
  }

  private PromQLResult evaluateVectorSelector(final VectorSelector vs, final long evalTimeMs) {
    final String typeName = sanitizeTypeName(vs.metricName());
    if (!database.getSchema().existsType(typeName))
      return new InstantVector(List.of());

    final DocumentType docType = database.getSchema().getType(typeName);
    if (!(docType instanceof LocalTimeSeriesType tsType))
      return new InstantVector(List.of());

    // Gated accessor: PromQL reads the same samples a SELECT would, so a metric the caller is denied must fail
    // loudly here rather than be served through this side door. Checked before the engine-availability test so a
    // denied caller learns nothing about the type's storage state.
    final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
    if (engine == null)
      return new InstantVector(List.of());
    final List<ColumnDefinition> columns = tsType.getTsColumns();

    warnIfMultipleFields(columns, vs.metricName());

    if (excludesEverySeries(vs.matchers(), columns, regexDeadline()))
      return new InstantVector(List.of());

    final TagFilter tagFilter = buildTagFilter(vs.matchers(), columns);
    final RowMatchers rowMatchers = rowMatchers(vs.matchers(), columns);
    final long offset = vs.offsetMs();
    final long queryEnd = evalTimeMs - offset;
    final long queryStart = windowScanStart(queryEnd, lookbackMs);

    // Folded straight from the scan through forEachRow rather than collected via iterateQuery first (issue
    // #7696): iterateQuery's own javadoc says the sealed layer materialises every matching row of the range
    // before the caller sees the first one, and this selector's answer keeps ONE sample per label combination -
    // O(series), paid for by visiting O(samples in the window) either way. forEachRow pays the same visit cost
    // without ever holding the range in one list.
    //
    // Only IOException is caught here, not the broader Exception the iterateQuery call used to sit under: the
    // post-filter regex logic now runs INSIDE the visitor, on the same call stack as the scan, and
    // excludesEverySeries()'s contract - a malformed or ReDoS-shaped =~/!~ propagates rather than being read as
    // an empty result - has to hold for it exactly as it already does for excludesEverySeries() itself. A
    // TimeoutException/IllegalArgumentException from matchesRow() is a RuntimeException, not an
    // IOException, so it is unaffected by this catch and still reaches the caller.
    final Map<String, VectorSample> latestByLabels = new LinkedHashMap<>();
    try {
      // One shared regexDeadline() for the whole evaluator instance (issue #5886 follow-up) - see the comment
      // on regexDeadline() itself: evaluateRange() calls this method once per step (up to MAX_RANGE_STEPS), so
      // anything less than one shared deadline for the whole query would let a crafted pattern tie up the
      // thread for rowCount * stepCount * regexTimeout.
      engine.forEachRow(queryStart, queryEnd, null, tagFilter, readMetrics, row -> {
        if (!matchesRow(row, rowMatchers))
          return true;
        final Map<String, String> labels = extractLabels(row, columns, vs.metricName());
        final String key = labelKey(labels);
        final long ts = (long) row[0];
        final double value = extractValue(row, columns);
        // forEachRow visits shard by shard, NOT merged by timestamp, and a label combination's samples are
        // round-robin-scattered across every shard (TimeSeriesEngine#appendSamples), so they can reach this
        // visitor in any order across the whole scan - unlike iterateQuery's merge, which used to guarantee
        // the last row seen for a key was also the newest. "Latest" is decided on timestamp instead of on
        // scan order: keep whichever of the existing and the new sample is newer rather than always
        // overwriting.
        latestByLabels.merge(key, new VectorSample(labels, value, ts),
            (existing, candidate) -> candidate.timestampMs() >= existing.timestampMs() ? candidate : existing);
        return true;
      });
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error querying TimeSeries type '%s': %s", null, typeName, e.getMessage());
      return new InstantVector(List.of());
    }

    return new InstantVector(new ArrayList<>(latestByLabels.values()));
  }

  /**
   * First timestamp to scan for a selector window of {@code windowMs} ending at {@code windowEndMs} (issue #9801).
   * Prometheus selects the window LEFT-OPEN, {@code (end - window, end]}: a sample exactly one lookback old is stale
   * for an instant selector (in every Prometheus version), and {@code [5m]} excludes the sample at {@code t-5m} (the
   * 3.x semantics). {@code forEachRow} scans a CLOSED range, so the lower bound moves up by one tick of the stored
   * unit, which is the millisecond (see {@code TimeSeriesType#MILLISECOND_PRECISION}). Shared by both selectors so the
   * two cannot drift apart again.
   */
  static long windowScanStart(final long windowEndMs, final long windowMs) {
    return windowEndMs - windowMs + 1;
  }

  private PromQLResult evaluateMatrixSelector(final MatrixSelector ms, final long evalTimeMs) {
    final VectorSelector vs = ms.selector();
    final String typeName = sanitizeTypeName(vs.metricName());
    if (!database.getSchema().existsType(typeName))
      return new RangeVector(List.of());

    final DocumentType docType = database.getSchema().getType(typeName);
    if (!(docType instanceof LocalTimeSeriesType tsType))
      return new RangeVector(List.of());

    // Same per-type read check as the instant-vector selector above.
    final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
    if (engine == null)
      return new RangeVector(List.of());
    final List<ColumnDefinition> columns = tsType.getTsColumns();

    warnIfMultipleFields(columns, vs.metricName());

    if (excludesEverySeries(vs.matchers(), columns, regexDeadline()))
      return new RangeVector(List.of());

    final TagFilter tagFilter = buildTagFilter(vs.matchers(), columns);
    final RowMatchers rowMatchers = rowMatchers(vs.matchers(), columns);
    final long offset = vs.offsetMs();
    final long queryEnd = evalTimeMs - offset;
    final long windowStart = queryEnd - ms.rangeMs();
    final long queryStart = windowScanStart(queryEnd, ms.rangeMs());

    // Folded straight from the scan through forEachRow rather than collected via iterateQuery first (issue
    // #7696) - same rationale as evaluateVectorSelector() above, including the narrowed IOException catch so a
    // post-filter regex failure still propagates instead of reading as an empty result.
    final Map<String, List<double[]>> seriesByLabels = new LinkedHashMap<>();
    final Map<String, Map<String, String>> labelsMap = new LinkedHashMap<>();
    try {
      // One shared regexDeadline() for the whole evaluator instance - see evaluateVectorSelector().
      engine.forEachRow(queryStart, queryEnd, null, tagFilter, readMetrics, row -> {
        if (!matchesRow(row, rowMatchers))
          return true;
        final Map<String, String> labels = extractLabels(row, columns, vs.metricName());
        final String key = labelKey(labels);
        seriesByLabels.computeIfAbsent(key, k -> new ArrayList<>())
            .add(new double[] { (long) row[0], extractValue(row, columns) });
        labelsMap.putIfAbsent(key, labels);
        return true;
      });
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error querying TimeSeries type '%s': %s", null, typeName, e.getMessage());
      return new RangeVector(List.of());
    }

    final List<RangeSeries> result = new ArrayList<>();
    for (final Map.Entry<String, List<double[]>> entry : seriesByLabels.entrySet()) {
      final List<double[]> points = entry.getValue();
      // forEachRow visits shard by shard, NOT merged by timestamp, and a label combination's samples are
      // round-robin-scattered across every shard, so the points accumulated above are no longer in timestamp
      // order the way iterateQuery's merge used to guarantee. rate()/irate()/increase() (PromQLFunctions) read
      // the first, last and consecutive pairs of this list assuming ascending order, so it is restored here -
      // once per series, over just that series' own points, rather than once for the whole scan.
      points.sort(Comparator.comparingDouble(p -> p[0]));
      // The window's own boundaries, not the scan's: extrapolation measures from where the range begins
      result.add(new RangeSeries(labelsMap.get(entry.getKey()), points, windowStart, queryEnd));
    }
    return new RangeVector(result);
  }

  private PromQLResult evaluateAggregation(final AggregationExpr agg, final long evalTimeMs, final long queryStartMs,
      final long queryEndMs, final long stepMs, final int depth) {
    final PromQLResult inner = evaluate(agg.expr(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    // Prometheus aggregates instant vectors only: a range vector or a scalar used to answer an empty vector (issue #9883).
    // Checked before the parameter, in the same order as Prometheus' type checker.
    if (!(inner instanceof InstantVector iv) || agg.expr() instanceof StringLiteral)
      throw new IllegalArgumentException(
          "expected type instant vector in aggregation expression, got " + valueTypeName(agg.expr(), inner));
    // k does not depend on the group, and an invalid one is refused even when the vector is empty, like Prometheus
    final boolean ranking = agg.op() == PromQLExpr.AggOp.TOPK || agg.op() == PromQLExpr.AggOp.BOTTOMK;
    final int rankLimit = ranking ? rankingLimit(agg, evalTimeMs, queryStartMs, queryEndMs, stepMs, depth) : 0;

    // Group samples by labels
    final Map<String, List<VectorSample>> groups = new LinkedHashMap<>();
    for (final VectorSample sample : iv.samples()) {
      final Map<String, String> groupKey = computeGroupLabels(sample.labels(), agg.groupLabels(), agg.without());
      final String key = labelKey(groupKey);
      groups.computeIfAbsent(key, k -> new ArrayList<>()).add(sample);
    }

    final List<VectorSample> result = new ArrayList<>();
    for (final Map.Entry<String, List<VectorSample>> entry : groups.entrySet()) {
      final List<VectorSample> group = entry.getValue();
      final Map<String, String> groupLabels = computeGroupLabels(group.getFirst().labels(), agg.groupLabels(), agg.without());
      final long ts = group.getFirst().timestampMs();

      final double value = switch (agg.op()) {
        case SUM -> {
          double sum = 0;
          for (final VectorSample s : group) sum += s.value();
          yield sum;
        }
        case AVG -> {
          double sum = 0;
          for (final VectorSample s : group) sum += s.value();
          yield sum / group.size();
        }
        // Same NaN policy as every other MIN/MAX in the time-series stack (issue #7039): a group whose samples
        // are all NaN yields NaN, not the seed. Seeding +/-Infinity and relying on `<`/`>` to displace it returned
        // the sentinel as data, because a NaN sample never wins either comparison.
        case MIN -> {
          double min = TimeSeriesNaN.ABSENT;
          for (final VectorSample s : group) min = TimeSeriesNaN.min(min, s.value());
          yield min;
        }
        case MAX -> {
          double max = TimeSeriesNaN.ABSENT;
          for (final VectorSample s : group) max = TimeSeriesNaN.max(max, s.value());
          yield max;
        }
        case COUNT -> (double) group.size();
        case TOPK, BOTTOMK -> {
          // Prometheus ranks the absent marker below every real sample for BOTH (issue #8927): it only fills a slot
          // when there are not enough real samples. Double.compare would order NaN above +Infinity.
          final int k = rankLimit;
          final boolean descending = agg.op() == PromQLExpr.AggOp.TOPK;
          group.sort((a, b) -> {
            final boolean aAbsent = TimeSeriesNaN.isAbsent(a.value());
            final boolean bAbsent = TimeSeriesNaN.isAbsent(b.value());
            if (aAbsent || bAbsent)
              return aAbsent == bAbsent ? 0 : aAbsent ? 1 : -1;
            return descending ? Double.compare(b.value(), a.value()) : Double.compare(a.value(), b.value());
          });
          for (int i = 0; i < Math.min(k, group.size()); i++)
            result.add(group.get(i));
          yield Double.NaN; // topk/bottomk add samples directly
        }
      };

      if (!ranking)
        result.add(new VectorSample(groupLabels, value, ts));
    }

    return new InstantVector(result);
  }

  /**
   * Evaluates the {@code k} parameter of topk/bottomk as any scalar expression. Like Prometheus, a NaN is refused and
   * a value below 1 selects nothing; a value above the vector size selects everything.
   */
  private int rankingLimit(final AggregationExpr agg, final long evalTimeMs, final long queryStartMs, final long queryEndMs,
      final long stepMs, final int depth) {
    if (agg.param() == null)
      return 1;
    final PromQLResult param = evaluate(agg.param(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    if (!(param instanceof ScalarResult sr))
      throw new IllegalArgumentException("Parameter of " + agg.op().name().toLowerCase(Locale.ROOT) + " must be a scalar");
    final double k = sr.value();
    if (Double.isNaN(k))
      throw new IllegalArgumentException("Parameter value of " + agg.op().name().toLowerCase(Locale.ROOT) + " is NaN");
    return k < 1 ? 0 : k >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) k;
  }

  private PromQLResult evaluateFunction(final FunctionCallExpr fn, final long evalTimeMs, final long queryStartMs,
      final long queryEndMs, final long stepMs, final int depth) {
    final String name = fn.name().toLowerCase(Locale.ROOT);

    // Range-vector functions
    if (isRangeFunction(name)) {
      // every function isRangeFunction() lists takes exactly one argument; quantile_over_time or predict_linear would not
      checkArgumentCount(name, fn.args().size(), 1, 1);
      final PromQLResult argResult = evaluate(fn.args().getFirst(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
      if (!(argResult instanceof RangeVector rv))
        throw new IllegalArgumentException("expected type range vector in call to function \"" + name + "\", got "
            + valueTypeName(fn.args().getFirst(), argResult));

      final List<VectorSample> samples = new ArrayList<>();
      for (final RangeSeries series : rv.series()) {
        final double value = switch (name) {
          case "rate" -> PromQLFunctions.rate(series);
          case "irate" -> PromQLFunctions.irate(series);
          case "increase" -> PromQLFunctions.increase(series);
          case "sum_over_time" -> PromQLFunctions.sumOverTime(series);
          case "avg_over_time" -> PromQLFunctions.avgOverTime(series);
          case "min_over_time" -> PromQLFunctions.minOverTime(series);
          case "max_over_time" -> PromQLFunctions.maxOverTime(series);
          case "count_over_time" -> PromQLFunctions.countOverTime(series);
          default -> throw new IllegalArgumentException("Unknown range function: " + fn.name());
        };
        samples.add(new VectorSample(functionKeepsMetricName(name) ? series.labels() : withoutMetricName(series.labels()), value,
            evalTimeMs));
      }
      return new InstantVector(samples);
    }

    // Scalar functions
    if ("abs".equals(name) || "ceil".equals(name) || "floor".equals(name) || "round".equals(name))
      return evaluateScalarFunction(fn, evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);

    throw new IllegalArgumentException("Unknown function: " + fn.name());
  }

  private PromQLResult evaluateScalarFunction(final FunctionCallExpr fn, final long evalTimeMs, final long queryStartMs,
      final long queryEndMs, final long stepMs, final int depth) {
    final String name = fn.name().toLowerCase(Locale.ROOT);
    checkArgumentCount(name, fn.args().size(), 1, "round".equals(name) ? 2 : 1);
    final PromQLResult argResult = evaluate(fn.args().getFirst(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    // Like Prometheus, only an instant vector is accepted: a range vector used to come back unchanged and a string became
    // a NaN scalar (issue #9883), and a scalar answered a scalar (`abs(-1)` was 1, issue #9924). Checked on the evaluated
    // argument, because whether it is a scalar is not always syntactic (`abs(1 + 1)`, `abs((1 > bool 0))`).
    if (!(argResult instanceof InstantVector iv))
      throw new IllegalArgumentException("expected type instant vector in call to function \"" + name + "\", got "
          + valueTypeName(fn.args().getFirst(), argResult));
    final double param = extractSecondParam(fn, name, evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);

    final List<VectorSample> samples = new ArrayList<>();
    final boolean keepName = functionKeepsMetricName(name);
    for (final VectorSample sample : iv.samples())
      samples.add(new VectorSample(keepName ? sample.labels() : withoutMetricName(sample.labels()),
          applyScalarFn(name, sample.value(), param), sample.timestampMs()));
    return new InstantVector(samples);
  }

  private double extractSecondParam(final FunctionCallExpr fn, final String name, final long evalTimeMs, final long queryStartMs,
      final long queryEndMs, final long stepMs, final int depth) {
    if (fn.args().size() <= 1)
      return 1.0;
    final PromQLExpr paramExpr = fn.args().get(1);
    final PromQLResult paramResult = evaluate(paramExpr, evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    // a non-scalar precision used to be silently replaced with 1 (issue #9883)
    if (!(paramResult instanceof ScalarResult sr) || paramExpr instanceof StringLiteral)
      throw new IllegalArgumentException(
          "expected type scalar in call to function \"" + name + "\", got " + valueTypeName(paramExpr, paramResult));
    return sr.value();
  }

  /**
   * Refuses a call with too few or too many arguments, in Prometheus' wording. An extra argument used to be ignored and a
   * missing one raised a {@code NoSuchElementException} (issue #9883).
   */
  private static void checkArgumentCount(final String name, final int count, final int min, final int max) {
    if (min == max) {
      if (count != min)
        throw new IllegalArgumentException(
            "expected " + min + " argument(s) in call to \"" + name + "\", got " + count);
    } else if (count < min)
      throw new IllegalArgumentException("expected at least " + min + " argument(s) in call to \"" + name + "\", got " + count);
    else if (count > max)
      throw new IllegalArgumentException("expected at most " + max + " argument(s) in call to \"" + name + "\", got " + count);
  }

  /**
   * Prometheus' name for the type of an evaluated expression, as its type-check errors print it. A string literal
   * evaluates to a NaN scalar here, so its type is read from the expression.
   */
  private static String valueTypeName(final PromQLExpr expr, final PromQLResult result) {
    if (expr instanceof StringLiteral)
      return "string";
    return switch (result) {
      case ScalarResult ignored -> "scalar";
      case InstantVector ignored -> "instant vector";
      case RangeVector ignored -> "range vector";
      // only evaluateRange() builds one, from the results of evaluate(), so no argument evaluates to it
      case MatrixResult ignored -> "matrix";
    };
  }

  /**
   * Prometheus' refusal of a range query whose expression is neither a scalar nor an instant vector (issue #9922).
   */
  private static String invalidRangeQueryType(final String typeName) {
    return "invalid expression type \"" + typeName + "\" for range query, must be Scalar or instant Vector";
  }

  private double applyScalarFn(final String name, final double value, final double param) {
    return switch (name) {
      case "abs" -> PromQLFunctions.abs(value);
      case "ceil" -> PromQLFunctions.ceil(value);
      case "floor" -> PromQLFunctions.floor(value);
      case "round" -> PromQLFunctions.round(value, param);
      default -> value;
    };
  }

  private PromQLResult evaluateBinary(final BinaryExpr bin, final long evalTimeMs, final long queryStartMs,
      final long queryEndMs, final long stepMs, final int depth) {
    final PromQLResult leftResult = evaluate(bin.left(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    final PromQLResult rightResult = evaluate(bin.right(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);

    // Prometheus refuses any other operand type: a range vector (`m[5m] + 1`, `m[5m] and m`) fell through every branch
    // below to a scalar NaN, and a string literal evaluates to a NaN scalar (issue #9788). Checked before the set-operator
    // rule, in the same order as Prometheus' type checker.
    if (!isScalarOrInstantVector(bin.left(), leftResult) || !isScalarOrInstantVector(bin.right(), rightResult))
      throw new IllegalArgumentException(BINARY_OPERAND_TYPE);

    // Prometheus refuses a set operator with a scalar operand: `and`, `or` and `unless` match series by labels, and a
    // scalar has none. Without this check the scalar branches below ran them through applyBinaryOp(), which maps them to
    // NaN (issue #9743). Checked on the evaluated operands for the same reason as the comparison check below.
    if (bin.op().isSetOperator() && (leftResult instanceof ScalarResult || rightResult instanceof ScalarResult))
      throw new IllegalArgumentException("set operator \"" + bin.op().symbol() + "\" not allowed in binary scalar expression");

    // scalar-scalar
    if (leftResult instanceof ScalarResult ls && rightResult instanceof ScalarResult rs) {
      // Prometheus refuses a scalar-scalar comparison without `bool`: there is no sample to keep or drop, so only the
      // 0/1 answer is meaningful and it must be asked for (issue #9723). Checked on the evaluated operands, not in the
      // parser, because whether an operand is a scalar is not always syntactic (`(1 + 1)`, `(1 > bool 0)`).
      if (bin.op().isComparison() && !bin.returnBool())
        throw new IllegalArgumentException(SCALAR_COMPARISON_WITHOUT_BOOL);
      return new ScalarResult(applyBinaryOp(bin.op(), ls.value(), rs.value()), evalTimeMs);
    }

    // vector-scalar
    if (leftResult instanceof InstantVector iv && rightResult instanceof ScalarResult rs)
      return applyVectorScalar(iv, rs.value(), bin.op(), false, bin.returnBool());

    // scalar-vector
    if (leftResult instanceof ScalarResult ls && rightResult instanceof InstantVector iv)
      return applyVectorScalar(iv, ls.value(), bin.op(), true, bin.returnBool());

    // vector-vector
    if (leftResult instanceof InstantVector lv && rightResult instanceof InstantVector rv)
      return applyVectorVector(lv, rv, bin.op(), bin.returnBool());

    return new ScalarResult(Double.NaN, evalTimeMs);
  }

  private PromQLResult evaluateUnary(final UnaryExpr un, final long evalTimeMs, final long queryStartMs, final long queryEndMs,
      final long stepMs, final int depth) {
    final PromQLResult result = evaluate(un.expr(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    // a range vector used to come back unchanged, silently dropping the sign, and a string became a NaN scalar (#9788)
    if (!isScalarOrInstantVector(un.expr(), result))
      throw new IllegalArgumentException(UNARY_OPERAND_TYPE);
    if (un.op() == '-') {
      if (result instanceof ScalarResult sr)
        return new ScalarResult(-sr.value(), sr.timestampMs());
      if (result instanceof InstantVector iv) {
        final List<VectorSample> samples = new ArrayList<>();
        for (final VectorSample s : iv.samples())
          samples.add(new VectorSample(withoutMetricName(s.labels()), -s.value(), s.timestampMs()));
        return new InstantVector(samples);
      }
    }
    return result;
  }

  // --- Helper methods ---

  /**
   * The only operand types an operator accepts. A string literal is tested on the expression because it evaluates to a
   * {@link ScalarResult} (NaN); the parser returns the inner expression of a parenthesized one, so {@code ("a")} is seen too.
   */
  private static boolean isScalarOrInstantVector(final PromQLExpr expr, final PromQLResult result) {
    return !(expr instanceof StringLiteral) && (result instanceof ScalarResult || result instanceof InstantVector);
  }

  private Pattern compilePattern(final String regex) {
    if (regex.length() > MAX_REGEX_LENGTH)
      throw new IllegalArgumentException("Regex pattern exceeds maximum length of " + MAX_REGEX_LENGTH + " characters");
    // Reject patterns that cause catastrophic backtracking (ReDoS):
    // covers nested quantifiers (a+)+, (a*b)*, (a+){n,} and alternation groups (a|aa)+
    if (REDOS_CHECK.matcher(regex).find())
      throw new IllegalArgumentException(
          "Regex pattern is not allowed (ReDoS risk): " + regex);
    // LRU-evicting cache: the synchronized LinkedHashMap automatically removes the
    // eldest entry when size exceeds MAX_PATTERN_CACHE, avoiding thundering-herd
    // re-compilation that bulk-clear caused.
    synchronized (patternCache) {
      return patternCache.computeIfAbsent(regex, r -> {
        try {
          return Pattern.compile(r);
        } catch (final PatternSyntaxException e) {
          throw new IllegalArgumentException("Invalid regex pattern: " + r, e);
        }
      });
    }
  }

  /**
   * Whether the label matchers of a selector rule out every series of a type before any row is read, because a
   * matcher names a label the type does not declare (issue #6938). Public for the series endpoint, which has to
   * agree with {@code /query} about what a selector selects (issue #9386).
   */
  public boolean selectsNoSeries(final List<LabelMatcher> matchers, final List<ColumnDefinition> columns) {
    return excludesEverySeries(matchers, columns, regexDeadline());
  }

  /**
   * The part of a selector's label matchers a scan can apply itself: its non-empty equalities on declared columns, as a tag
   * filter, or {@code null} when it has none. Every other matcher is decided per row by {@link #matchesRow}, which skips what
   * this filter already applied, and {@link #selectsNoSeries} settles a matcher on an undeclared label for the whole type:
   * the three are one unit, public for the remote-read endpoint so it selects exactly what {@code /query} does (issue #9803).
   */
  public TagFilter tagFilter(final List<LabelMatcher> matchers, final List<ColumnDefinition> columns) {
    return buildTagFilter(matchers, columns);
  }

  /**
   * Binds a selector's label matchers to the rows of a scan {@link #tagFilter} narrowed, whose layout is the engine's
   * {@code [timestamp, non-ts-col-0, non-ts-col-1, ...]}: the matchers that filter did not apply ({@code !=}, {@code =~},
   * {@code !~}, {@code label=""}), each with its tag's position in the row and the values its text reads as in the tag's declared
   * type. Resolved once per scan, so the per-row work is a membership test and, for a pattern, a match (issue #9833).
   * <p>
   * The three - {@link #tagFilter}, this and {@link #selectsNoSeries} - are one unit, public for the remote-read endpoint so it
   * selects exactly what {@code /query} does (issue #9803).
   */
  public RowMatchers rowMatchers(final List<LabelMatcher> matchers, final List<ColumnDefinition> columns) {
    final List<ColumnDefinition> rowColumns = new ArrayList<>(columns.size());
    for (final ColumnDefinition col : columns)
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        rowColumns.add(col);
    return bindMatchers(matchers, rowColumns, true);
  }

  /**
   * Binds every label matcher of a selector to the rows of a scan that projects some columns and pushes no tag filter down,
   * as the series endpoint's tag projection does: {@code row[0]} is the timestamp and {@code row[i + 1]} the value of
   * {@code rowColumns.get(i)}. A matcher on a label the type does not declare is left to {@link #selectsNoSeries} (issue #9386).
   */
  public RowMatchers projectedRowMatchers(final List<LabelMatcher> matchers, final List<ColumnDefinition> rowColumns) {
    return bindMatchers(matchers, rowColumns, false);
  }

  /**
   * Whether a row passes the matchers bound by {@link #rowMatchers} or {@link #projectedRowMatchers}, under this evaluator's
   * shared regex deadline. A malformed or ReDoS-shaped pattern raises an {@link IllegalArgumentException} rather than reading
   * as "no match" (issue #9803).
   */
  public boolean matchesRow(final Object[] row, final RowMatchers rowMatchers) {
    final long regexDeadline = rowMatchers.matchers.length == 0 ? 0L : regexDeadline();
    for (int i = 0; i < rowMatchers.matchers.length; i++) {
      final int rowIdx = rowMatchers.rowIndexes[i];
      if (!matchesTag(rowMatchers.matchers[i], rowMatchers.valueForms[i], rowMatchers.patterns[i],
          rowIdx < row.length ? row[rowIdx] : null, regexDeadline))
        return false;
    }
    return true;
  }

  /**
   * A selector's label matchers bound to one scan's row layout by {@link #rowMatchers} or {@link #projectedRowMatchers}.
   * Immutable; valid for the columns it was bound to.
   */
  public static final class RowMatchers {
    private final LabelMatcher[] matchers;
    private final int[]          rowIndexes;
    private final Set<Object>[]  valueForms;
    private final Pattern[]      patterns;

    private RowMatchers(final LabelMatcher[] matchers, final int[] rowIndexes, final Set<Object>[] valueForms,
        final Pattern[] patterns) {
      this.matchers = matchers;
      this.rowIndexes = rowIndexes;
      this.valueForms = valueForms;
      this.patterns = patterns;
    }
  }

  /**
   * Whether every {@code __name__} matcher of a selector holds on {@code metricName}: how a selector without a metric name,
   * {@code {__name__=~"cp.*"}}, picks the metrics it reads (issue #9803). True when there is no such matcher.
   */
  public boolean matchesMetricName(final List<LabelMatcher> matchers, final String metricName) {
    for (final LabelMatcher m : matchers) {
      if (!METRIC_NAME_LABEL.equals(m.name()))
        continue;
      final boolean matches = switch (m.op()) {
        case EQ -> metricName.equals(m.value());
        case NEQ -> !metricName.equals(m.value());
        case RE -> TimeBoundRegex.matchesUntil(compilePattern(m.value()), metricName, regexDeadline());
        case NRE -> !TimeBoundRegex.matchesUntil(compilePattern(m.value()), metricName, regexDeadline());
      };
      if (!matches)
        return false;
    }
    return true;
  }

  private TagFilter buildTagFilter(final List<LabelMatcher> matchers, final List<ColumnDefinition> columns) {
    TagFilter filter = null;
    for (final LabelMatcher m : matchers) {
      // {label=""} selects the series that do NOT carry the label, which a tag equality filter cannot express (a missing
      // tag is null, an old row may hold ""): the row matchers decide it (issue #9363)
      if (m.op() != MatchOp.EQ || isEmptyEquality(m) || "__name__".equals(m.name()))
        continue;
      final int idx = findNonTsColumnIndex(m.name(), columns);
      if (idx < 0)
        continue;
      // A matcher value is always text and the stored tag is of the column's declared type: compared as text, a LONG or
      // BOOLEAN tag matched nothing. Coerced as every other tag push-down coerces it (issues #5475, #9803)
      final Object value = coerceTagValue(nonTsColumn(idx, columns), m.value());
      filter = filter == null ? TagFilter.eq(idx, value) : filter.and(idx, value);
    }
    return filter;
  }

  /**
   * Returns {@code true} when a matcher naming a column the type does not declare rules out every series of
   * that type (issue #6938).
   * <p>
   * A label the schema has no column for is absent from every series, so Prometheus' absent-label rules decide
   * the whole type in one go: {@code =} with a non-empty value and {@code !=""} select nothing, while
   * {@code !=} with a non-empty value, {@code =""} and any regex the empty string satisfies select everything.
   * The evaluator used to get both directions backwards - {@link #buildTagFilter} silently dropped an unknown
   * {@code =} matcher (so a typo like {@code up{jobb="api"}} widened the query to every series instead of
   * narrowing it to none) and the per-row matcher rejected every row for an unknown {@code !=}/{@code !~}.
   * Deciding it here, before the scan, also means the query never runs when the answer is already known.
   * <p>
   * Deliberately outside the {@code try/catch} that turns an {@code iterateQuery()} failure into an empty
   * result: the {@code IllegalArgumentException}/{@code TimeoutException} a bad or ReDoS-shaped {@code =~} can
   * raise from here propagates to the caller, exactly as the same pattern already does from
   * {@link #matchesRow}, which runs outside that {@code try} too. A rejected query is not an empty one.
   */
  private boolean excludesEverySeries(final List<LabelMatcher> matchers, final List<ColumnDefinition> columns,
      final long regexDeadline) {
    for (final LabelMatcher m : matchers) {
      if ("__name__".equals(m.name()))
        continue;
      if (findNonTsRowIndex(m.name(), columns) >= 0)
        continue;
      if (!matchesAbsentLabel(m, regexDeadline))
        return true;
    }
    return false;
  }

  /**
   * Applies a matcher to a label that no series carries, which Prometheus treats as the empty string.
   * <p>
   * Deliberate change of error behaviour for {@code =~}/{@code !~} on an unknown column: the pattern is now
   * compiled - and therefore validated by {@link #compilePattern}, syntax and ReDoS guard alike - where
   * the per-row matcher used to bail out on {@code rowIdx < 0} before ever reaching the switch, so a
   * malformed or ReDoS-shaped regex against a nonexistent label silently produced an empty vector instead of
   * the {@code IllegalArgumentException} the same pattern raises against a label that does exist. Prometheus
   * validates the regex regardless of whether any series carries the label, and a typo'd label name is exactly
   * the case where the query author most needs to be told rather than handed a plausible empty result.
   */
  private boolean matchesAbsentLabel(final LabelMatcher m, final long regexDeadline) {
    return switch (m.op()) {
      case EQ -> m.value().isEmpty();
      case NEQ -> !m.value().isEmpty();
      case RE -> TimeBoundRegex.matchesUntil(compilePattern(m.value()), "", regexDeadline);
      case NRE -> !TimeBoundRegex.matchesUntil(compilePattern(m.value()), "", regexDeadline);
    };
  }

  /**
   * Whether {@code m} is {@code label=""}: it selects the series that do not carry the label, so it cannot go through the
   * tag equality filter and is decided by {@link #matchesRow} instead (issue #9363). Kept in one place so the two
   * sides of that split cannot disagree.
   */
  private static boolean isEmptyEquality(final LabelMatcher m) {
    return m.op() == MatchOp.EQ && m.value().isEmpty();
  }

  @SuppressWarnings("unchecked")
  private RowMatchers bindMatchers(final List<LabelMatcher> matchers, final List<ColumnDefinition> rowColumns,
      final boolean equalitiesPushedDown) {
    final LabelMatcher[] bound = new LabelMatcher[matchers.size()];
    final int[] rowIndexes = new int[matchers.size()];
    final Set<Object>[] forms = new Set[matchers.size()];
    final Pattern[] patterns = new Pattern[matchers.size()];
    int count = 0;
    for (final LabelMatcher m : matchers) {
      // a non-empty EQ was already applied by the tag filter; an empty one is decided here
      if ((equalitiesPushedDown && m.op() == MatchOp.EQ && !isEmptyEquality(m)) || METRIC_NAME_LABEL.equals(m.name()))
        continue;
      int col = -1;
      for (int i = 0; i < rowColumns.size(); i++)
        if (rowColumns.get(i).getName().equals(m.name())) {
          col = i;
          break;
        }
      if (col < 0)
        // The column is absent from the schema, so the label is absent from every series. excludesEverySeries()
        // already settled such a matcher for the whole type before the scan started: reaching a row at all
        // means it selected everything, so it cannot reject this one (issue #6938).
        continue;
      bound[count] = m;
      rowIndexes[count] = col + 1;
      forms[count] = valueFormsOf(rowColumns.get(col), m);
      // compiled once per scan, before it starts: a malformed or ReDoS-shaped pattern is refused even when no row reaches it
      if (m.op() == MatchOp.RE || m.op() == MatchOp.NRE)
        patterns[count] = compilePattern(m.value());
      count++;
    }
    return new RowMatchers(Arrays.copyOf(bound, count), Arrays.copyOf(rowIndexes, count), Arrays.copyOf(forms, count),
        Arrays.copyOf(patterns, count));
  }

  /**
   * The stored values a matcher's text names in its tag's declared type, as the tag push-down reads them: {@code 007} names the
   * LONG 7, {@code 5} the DOUBLE 5.0, a date the epoch count a DATETIME tag holds (issues #5475, #9724, #9803, #9833). For a
   * pattern, the values the pattern names when read as a literal: its text as written, and, when every metacharacter in it is
   * escaped, the text it matches. Empty for {@code label=""}/{@code label!=""}, which test presence instead.
   */
  private Set<Object> valueFormsOf(final ColumnDefinition column, final LabelMatcher m) {
    final String value = m.value();
    if (value.isEmpty() && (m.op() == MatchOp.EQ || m.op() == MatchOp.NEQ))
      return Set.of();
    final Set<Object> forms = new HashSet<>();
    addEqualityForms(forms, column, value);
    if (m.op() == MatchOp.RE || m.op() == MatchOp.NRE) {
      final String literal = literalOfPattern(value);
      if (literal != null && !literal.equals(value))
        addEqualityForms(forms, column, literal);
    }
    return forms;
  }

  private void addEqualityForms(final Set<Object> forms, final ColumnDefinition column, final String text) {
    final Object typed = coerceTagValue(column, text);
    if (typed != null)
      forms.addAll(TagFilter.equalityForms(typed));
  }

  /**
   * The one text {@code pattern} matches when it is a plain literal - every character either ordinary or a backslash-escaped
   * punctuation character, as in {@code 5\.00} - or {@code null} when it is a real pattern.
   */
  static String literalOfPattern(final String pattern) {
    final StringBuilder sb = new StringBuilder(pattern.length());
    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      if (c == '\\') {
        if (++i >= pattern.length() || Character.isLetterOrDigit(pattern.charAt(i)))
          return null;
        sb.append(pattern.charAt(i));
      } else if (".[]{}()*+?^$|".indexOf(c) >= 0)
        return null;
      else
        sb.append(c);
    }
    return sb.toString();
  }

  /**
   * The one decision of whether a tag holds a matcher, for every operator (issue #9833). Equality is membership in
   * {@code valueForms}, the forms the tag push-down compares a row with ({@link TagFilter#equalityForms}), so {@code =} and
   * {@code !=} are complements whichever of the two decides a row. A pattern selects a tag it names as a literal, which is how
   * {@code =~"007"} agrees with {@code ="007"} on a LONG tag, and otherwise a tag whose label text - what the series is
   * reported under - it matches. An absent tag (null, or {@code ""} in a row an older build stored) is the empty string.
   * <p>
   * The literal reading is broader than Prometheus, whose label values are text only: there {@code =~"007"} never matches a
   * label {@code 7}. Here it does, deliberately, because {@code ="007"} selects that tag by its declared type and a literal
   * pattern has to agree with the equality it spells. A real pattern is still matched against the label text only, so
   * {@code =~"0.7"} does not select a LONG tag holding 7.
   */
  private static boolean matchesTag(final LabelMatcher m, final Set<Object> valueForms, final Pattern pattern, final Object stored,
      final long regexDeadline) {
    final boolean absent = isAbsent(stored);
    return switch (m.op()) {
      case EQ -> m.value().isEmpty() ? absent : !absent && valueForms.contains(stored);
      case NEQ -> m.value().isEmpty() ? !absent : absent || !valueForms.contains(stored);
      case RE -> !absent && valueForms.contains(stored) || TimeBoundRegex.matchesUntil(pattern, labelText(stored), regexDeadline);
      case NRE -> (absent || !valueForms.contains(stored)) && !TimeBoundRegex.matchesUntil(pattern, labelText(stored),
          regexDeadline);
    };
  }

  /** Whether a row carries no value for a tag: null, or {@code ""} in a row an older build stored (issue #9363). */
  private static boolean isAbsent(final Object stored) {
    return stored == null || stored instanceof String text && text.isEmpty();
  }

  /** The text a tag is reported under, which a pattern is matched against; {@code ""} for an absent tag. */
  private static String labelText(final Object stored) {
    return stored != null ? stored.toString() : "";
  }

  /**
   * Extracts label key/value pairs from a row.
   * The row format is always {@code [timestamp, non-ts-col-0, non-ts-col-1, ...]}, so we
   * iterate non-TIMESTAMP columns and map them to row positions 1, 2, 3… regardless of
   * where the TIMESTAMP column appears in the schema definition.
   */
  private Map<String, String> extractLabels(final Object[] row, final List<ColumnDefinition> columns, final String metricName) {
    final Map<String, String> labels = new LinkedHashMap<>();
    labels.put(METRIC_NAME_LABEL, metricName);
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      final int rowPos = 1 + nonTsIdx;
      nonTsIdx++;
      // An empty label value is an absent label in the Prometheus data model, so it is not reported (issue #9363). It
      // also covers rows an older build stored with "".
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG && rowPos < row.length && row[rowPos] != null) {
        final String value = row[rowPos].toString();
        if (!value.isEmpty())
          labels.put(col.getName(), value);
      }
    }
    return labels;
  }

  /**
   * Extracts the first FIELD column value from a row.
   * The row format is always {@code [timestamp, non-ts-col-0, non-ts-col-1, ...]}, so we
   * iterate non-TIMESTAMP columns and map them to row positions 1, 2, 3… regardless of
   * where the TIMESTAMP column appears in the schema definition.
   */
  private double extractValue(final Object[] row, final List<ColumnDefinition> columns) {
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      final int rowPos = 1 + nonTsIdx;
      nonTsIdx++;
      if (col.getRole() == ColumnDefinition.ColumnRole.FIELD)
        return rowPos < row.length && row[rowPos] instanceof Number ? ((Number) row[rowPos]).doubleValue() : Double.NaN;
    }
    return Double.NaN;
  }

  /**
   * Logs a warning if the given column list has more than one FIELD column,
   * since PromQL evaluation only uses the first one. Logged once per type name.
   */
  private void warnIfMultipleFields(final List<ColumnDefinition> columns, final String typeName) {
    if (warnedMultiFieldTypes.contains(typeName))
      return;
    int fieldCount = 0;
    String firstName = null;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.FIELD) {
        if (firstName == null)
          firstName = col.getName();
        fieldCount++;
      }
    }
    if (fieldCount > 1) {
      warnedMultiFieldTypes.add(typeName);
      LogManager.instance().log(this, Level.WARNING,
          """
          PromQL evaluation: type '%s' has %d FIELD columns but only the first ('%s') is used. \
          Use explicit column selection or split into separate types""",
          null, typeName, fieldCount, firstName);
    }
  }

  /**
   * The value an equality matcher compares a tag with: the matcher's text in the column's declared type, so a LONG or BOOLEAN tag
   * is compared like with like (issues #5475, #9803). Text the type cannot read stays text, which equals no stored value and so
   * selects no series: {@code coerceValue} already leaves a number that does not parse as it is, but reads any text that is not
   * {@code true} as {@code false}, which would select the series whose tag is false.
   */
  private Object coerceTagValue(final ColumnDefinition column, final String text) {
    if (column.getDataType() == Type.BOOLEAN && !"true".equalsIgnoreCase(text) && !"false".equalsIgnoreCase(text))
      return text;
    // read through the database, as the INSERT that wrote the tag read it, not in the JVM's default zone (issue #9833)
    return column.coerceValue(database, text);
  }

  private int findNonTsColumnIndex(final String name, final List<ColumnDefinition> columns) {
    int nonTsIdx = -1;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      nonTsIdx++;
      if (col.getName().equals(name))
        return nonTsIdx;
    }
    return -1;
  }

  /** The column at {@code nonTsIndex} among the non-timestamp columns, as {@link #findNonTsColumnIndex} counts them. */
  private static ColumnDefinition nonTsColumn(final int nonTsIndex, final List<ColumnDefinition> columns) {
    int nonTsIdx = -1;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (++nonTsIdx == nonTsIndex)
        return col;
    }
    throw new IllegalArgumentException("No non-timestamp column at index " + nonTsIndex);
  }

  /**
   * Returns the row index for a named column in the engine row format
   * {@code [timestamp, non-ts-col-0, non-ts-col-1, ...]}.
   * Returns -1 if the column is not found or is the TIMESTAMP column.
   */
  private int findNonTsRowIndex(final String name, final List<ColumnDefinition> columns) {
    int rowIdx = 1; // row[0] is always the timestamp
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (col.getName().equals(name))
        return rowIdx;
      rowIdx++;
    }
    return -1;
  }

  private Map<String, String> computeGroupLabels(final Map<String, String> labels, final List<String> groupLabels,
      final boolean without) {
    if (groupLabels.isEmpty() && !without)
      return Map.of();
    final Map<String, String> result = new LinkedHashMap<>();
    if (without) {
      // Prometheus' `without` always deletes the metric name too (issue #8926)
      final Set<String> exclude = new HashSet<>(groupLabels);
      exclude.add(METRIC_NAME_LABEL);
      for (final Map.Entry<String, String> entry : labels.entrySet())
        if (!exclude.contains(entry.getKey()))
          result.put(entry.getKey(), entry.getValue());
    } else {
      for (final String label : groupLabels)
        if (labels.containsKey(label))
          result.put(label, labels.get(label));
    }
    return result;
  }

  private String labelKey(final Map<String, String> labels) {
    return labelKey(labels, null);
  }

  /** Same as {@link #labelKey(Map)} but skipping the label named {@code excluded} without copying the map. */
  private String labelKey(final Map<String, String> labels, final String excluded) {
    if (labels.isEmpty())
      return "{}";
    final List<String> sorted = new ArrayList<>(labels.keySet());
    if (excluded != null)
      sorted.remove(excluded);
    Collections.sort(sorted);
    final StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < sorted.size(); i++) {
      if (i > 0)
        sb.append(',');
      // length-prefixed value: a value holding ',' or '=' cannot make two distinct label sets share a key
      final String value = labels.get(sorted.get(i));
      sb.append(sorted.get(i)).append('=').append(value.length()).append(':').append(value);
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Signature used to match two instant vectors: the label set with the metric name EXCLUDED, as Prometheus'
   * {@code signatureFunc} does for every operator without {@code on(...)} (issue #8926).
   */
  private String matchKey(final Map<String, String> labels) {
    return labelKey(labels, METRIC_NAME_LABEL);
  }

  /**
   * The result labels of a binary operator: Prometheus drops the metric name for an arithmetic operator and for a
   * comparison with {@code bool} (its {@code returnBool} adds a {@code DropMetricName} of its own, issue #9644), and
   * keeps it only for a filtering comparison.
   */
  private Map<String, String> resultLabels(final Map<String, String> labels, final BinaryOp op, final boolean returnBool) {
    return op.isComparison() && !returnBool ? labels : withoutMetricName(labels);
  }

  /**
   * The sample a comparison or arithmetic operator produces from {@code source} and the operator's raw {@code value},
   * or {@code null} when a filtering comparison drops it. A comparison with {@code bool} keeps every sample and answers
   * {@code 0}/{@code 1} (issue #9644); without it a comparison keeps the source value of the samples it lets through.
   */
  private VectorSample binarySample(final VectorSample source, final double value, final BinaryOp op, final boolean returnBool) {
    if (!op.isComparison() || returnBool)
      return new VectorSample(resultLabels(source.labels(), op, returnBool), value, source.timestampMs());
    return value != 0 ? source : null; // a filter passes the sample through unchanged, and VectorSample is immutable
  }

  /**
   * The labels of a series derived from another one, with the metric name deleted: Prometheus' {@code DropMetricName()},
   * applied by every operator and function that transforms the value (issue #9330). Returns {@code labels} itself when
   * it carries no name, so the common case allocates nothing.
   */
  private static Map<String, String> withoutMetricName(final Map<String, String> labels) {
    if (!labels.containsKey(METRIC_NAME_LABEL))
      return labels;
    final Map<String, String> withoutName = new LinkedHashMap<>(labels);
    withoutName.remove(METRIC_NAME_LABEL);
    return withoutName;
  }

  /**
   * The functions that keep the metric name, as in Prometheus: they reorder or relabel a series instead of deriving a
   * new value from it. Every other function drops it. Mirrors {@code DropMetricName()} in Prometheus' promql/functions.go.
   */
  private static boolean functionKeepsMetricName(final String name) {
    return switch (name) {
      case "sort", "sort_desc", "label_replace", "label_join", "last_over_time" -> true;
      default -> false;
    };
  }

  private InstantVector applyVectorScalar(final InstantVector iv, final double scalar, final BinaryOp op,
      final boolean scalarOnLeft, final boolean returnBool) {
    final List<VectorSample> result = new ArrayList<>();
    for (final VectorSample s : iv.samples()) {
      final double value = scalarOnLeft ? applyBinaryOp(op, scalar, s.value()) : applyBinaryOp(op, s.value(), scalar);
      final VectorSample out = binarySample(s, value, op, returnBool);
      if (out != null)
        result.add(out);
    }
    return new InstantVector(result);
  }

  private InstantVector applyVectorVector(final InstantVector left, final InstantVector right, final BinaryOp op,
      final boolean returnBool) {
    // Simple vector-vector matching by label identity
    final Map<String, VectorSample> rightMap = new HashMap<>();
    for (final VectorSample s : right.samples())
      rightMap.put(matchKey(s.labels()), s);

    final List<VectorSample> result = new ArrayList<>();
    for (final VectorSample ls : left.samples()) {
      final VectorSample rs = rightMap.get(matchKey(ls.labels()));
      if (rs != null) {
        if (op == BinaryOp.AND || op == BinaryOp.OR) {
          // For a matched label set both AND and OR are the left-hand sample by definition. OR used to fall
          // into the arithmetic branch below, where applyBinaryOp() maps the set operators to NaN (issue
          // #6938) - and since aggregation collapses labels to the empty set, the canonical
          // `sum(a) or sum(b)` fallback always matched and always produced that NaN.
          result.add(ls);
        } else if (op == BinaryOp.UNLESS) {
          // skip — matched, so excluded
        } else {
          final VectorSample out = binarySample(ls, applyBinaryOp(op, ls.value(), rs.value()), op, returnBool);
          if (out != null)
            result.add(out);
        }
      } else if (op == BinaryOp.OR || op == BinaryOp.UNLESS) {
        result.add(ls);
      }
    }
    // For OR, also add unmatched right-side samples
    if (op == BinaryOp.OR) {
      final Set<String> leftKeys = new HashSet<>();
      for (final VectorSample s : left.samples())
        leftKeys.add(matchKey(s.labels()));
      for (final VectorSample rs : right.samples())
        if (!leftKeys.contains(matchKey(rs.labels())))
          result.add(rs);
    }
    return new InstantVector(result);
  }

  private double applyBinaryOp(final BinaryOp op, final double left, final double right) {
    return switch (op) {
      case ADD -> left + right;
      case SUB -> left - right;
      case MUL -> left * right;
      case DIV -> left / right; // IEEE-754 like Prometheus: 1/0 = +Inf, only 0/0 is NaN (issue #8929)
      case MOD -> left % right; // Java and Go both answer NaN for a zero divisor
      case POW -> Math.pow(left, right);
      case EQ -> left == right ? 1.0 : 0.0;
      case NEQ -> left != right ? 1.0 : 0.0;
      case LT -> left < right ? 1.0 : 0.0;
      case GT -> left > right ? 1.0 : 0.0;
      case LTE -> left <= right ? 1.0 : 0.0;
      case GTE -> left >= right ? 1.0 : 0.0;
      case AND, OR, UNLESS -> Double.NaN; // handled at vector level
    };
  }

  // evaluateFunction() checks every function listed here for exactly one argument: a function taking more (quantile_over_time,
  // predict_linear) needs its own count there
  private boolean isRangeFunction(final String name) {
    return switch (name) {
      case "rate", "irate", "increase", "sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "count_over_time" ->
        true;
      default -> false;
    };
  }

  private static final String METRIC_NAME_LABEL = "__name__";
  private static final int    MAX_TYPE_NAME_LENGTH   = 256;
  private static final Pattern VALID_TYPE_NAME_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

  /** Prometheus' own wording, so a client matching on it sees the same error from either server. */
  private static final String SCALAR_COMPARISON_WITHOUT_BOOL = "comparisons between scalars must use BOOL modifier";
  private static final String BINARY_OPERAND_TYPE            = "binary expression must contain only scalar and instant vector types";
  private static final String UNARY_OPERAND_TYPE             = "unary expression only allowed on expressions of type scalar or instant vector";

  public static String sanitizeTypeName(final String name) {
    final String sanitized = name.replace('.', '_').replace('-', '_').replace(':', '_');
    if (!sanitized.equals(name))
      LogManager.instance().log(PromQLEvaluator.class, Level.WARNING,
          """
          Metric name '%s' was sanitized to '%s'. Distinct Prometheus names that differ only by '.', '-', ':' vs '_' \
          will map to the same ArcadeDB type and may return merged data""", null, name, sanitized);
    if (sanitized.length() > MAX_TYPE_NAME_LENGTH)
      throw new IllegalArgumentException("Metric name too long: " + sanitized.length() + " chars (max " + MAX_TYPE_NAME_LENGTH + ")");
    if (!VALID_TYPE_NAME_PATTERN.matcher(sanitized).matches())
      throw new IllegalArgumentException("Invalid metric name: '" + name + "'");
    return sanitized;
  }
}
