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
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
      }
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
    PromQLResult result;
    if (Objects.requireNonNull(expr) instanceof NumberLiteral nl) {
      result = new ScalarResult(nl.value(), evalTimeMs);
    } else if (expr instanceof StringLiteral) {
      result = new ScalarResult(Double.NaN, evalTimeMs);
    } else if (expr instanceof VectorSelector vs) {
      result = evaluateVectorSelector(vs, evalTimeMs);
    } else if (expr instanceof MatrixSelector ms) {
      result = evaluateMatrixSelector(ms, evalTimeMs);
    } else if (expr instanceof AggregationExpr agg) {
      result = evaluateAggregation(agg, evalTimeMs, queryStartMs, queryEndMs, stepMs, next);
    } else if (expr instanceof FunctionCallExpr fn) {
      result = evaluateFunction(fn, evalTimeMs, queryStartMs, queryEndMs, stepMs, next);
    } else if (expr instanceof BinaryExpr bin) {
      result = evaluateBinary(bin, evalTimeMs, queryStartMs, queryEndMs, stepMs, next);
    } else if (expr instanceof UnaryExpr un) {
      result = evaluateUnary(un, evalTimeMs, queryStartMs, queryEndMs, stepMs, next);
    } else {
      throw new IllegalArgumentException();
    }
    return result;
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
    final long offset = vs.offsetMs();
    final long queryEnd = evalTimeMs - offset;
    final long queryStart = queryEnd - lookbackMs;

    final Iterator<Object[]> rowIter;
    try {
      rowIter = engine.iterateQuery(queryStart, queryEnd, null, tagFilter);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error querying TimeSeries type '%s': %s", null, typeName, e.getMessage());
      return new InstantVector(List.of());
    }

    // Post-filter for NEQ/RE/NRE and group by label combination. One shared deadline for the whole evaluator
    // instance (issue #5886 follow-up) - regexDeadline() computes it once and reuses it for every row of every
    // step, not a fresh one per row or per step: matchesPostFilters() runs per row, and evaluateRange() calls
    // this method once per step (up to MAX_RANGE_STEPS), so anything less than one shared deadline for the
    // whole query would let a crafted =~/!~ pattern tie up the thread for rowCount * stepCount * regexTimeout.
    final Map<String, VectorSample> latestByLabels = new LinkedHashMap<>();
    while (rowIter.hasNext()) {
      final Object[] row = rowIter.next();
      if (!matchesPostFilters(row, vs.matchers(), columns, regexDeadline()))
        continue;
      final Map<String, String> labels = extractLabels(row, columns, vs.metricName());
      final String key = labelKey(labels);
      final long ts = (long) row[0];
      final double value = extractValue(row, columns);
      // Keep the latest sample for each label combination
      latestByLabels.put(key, new VectorSample(labels, value, ts));
    }

    return new InstantVector(new ArrayList<>(latestByLabels.values()));
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
    final long offset = vs.offsetMs();
    final long queryEnd = evalTimeMs - offset;
    final long queryStart = queryEnd - ms.rangeMs();

    final Iterator<Object[]> rowIter;
    try {
      rowIter = engine.iterateQuery(queryStart, queryEnd, null, tagFilter);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error querying TimeSeries type '%s': %s", null, typeName, e.getMessage());
      return new RangeVector(List.of());
    }

    // Group rows by label combination. One shared deadline for the whole evaluator instance - see
    // evaluateVectorSelector().
    final Map<String, List<double[]>> seriesByLabels = new LinkedHashMap<>();
    final Map<String, Map<String, String>> labelsMap = new LinkedHashMap<>();
    while (rowIter.hasNext()) {
      final Object[] row = rowIter.next();
      if (!matchesPostFilters(row, vs.matchers(), columns, regexDeadline()))
        continue;
      final Map<String, String> labels = extractLabels(row, columns, vs.metricName());
      final String key = labelKey(labels);
      seriesByLabels.computeIfAbsent(key, k -> new ArrayList<>()).add(new double[] { (long) row[0], extractValue(row, columns) });
      labelsMap.putIfAbsent(key, labels);
    }

    final List<RangeSeries> result = new ArrayList<>();
    for (final Map.Entry<String, List<double[]>> entry : seriesByLabels.entrySet())
      result.add(new RangeSeries(labelsMap.get(entry.getKey()), entry.getValue(), queryStart, queryEnd));
    return new RangeVector(result);
  }

  private PromQLResult evaluateAggregation(final AggregationExpr agg, final long evalTimeMs, final long queryStartMs,
      final long queryEndMs, final long stepMs, final int depth) {
    final PromQLResult inner = evaluate(agg.expr(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    if (!(inner instanceof InstantVector iv))
      return new InstantVector(List.of());

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
      final Map<String, String> groupLabels = computeGroupLabels(group.get(0).labels(), agg.groupLabels(), agg.without());
      final long ts = group.get(0).timestampMs();

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
        case MIN -> {
          double min = Double.POSITIVE_INFINITY;
          for (final VectorSample s : group) if (s.value() < min) min = s.value();
          yield min;
        }
        case MAX -> {
          double max = Double.NEGATIVE_INFINITY;
          for (final VectorSample s : group) if (s.value() > max) max = s.value();
          yield max;
        }
        case COUNT -> (double) group.size();
        case TOPK -> {
          final int k = agg.param() != null ? (int) ((NumberLiteral) agg.param()).value() : 1;
          group.sort(Comparator.comparingDouble(VectorSample::value).reversed());
          for (int i = 0; i < Math.min(k, group.size()); i++)
            result.add(group.get(i));
          yield Double.NaN; // topk adds samples directly
        }
        case BOTTOMK -> {
          final int k = agg.param() != null ? (int) ((NumberLiteral) agg.param()).value() : 1;
          group.sort(Comparator.comparingDouble(VectorSample::value));
          for (int i = 0; i < Math.min(k, group.size()); i++)
            result.add(group.get(i));
          yield Double.NaN; // bottomk adds samples directly
        }
      };

      if (agg.op() != PromQLExpr.AggOp.TOPK && agg.op() != PromQLExpr.AggOp.BOTTOMK)
        result.add(new VectorSample(groupLabels, value, ts));
    }

    return new InstantVector(result);
  }

  private PromQLResult evaluateFunction(final FunctionCallExpr fn, final long evalTimeMs, final long queryStartMs,
      final long queryEndMs, final long stepMs, final int depth) {
    final String name = fn.name().toLowerCase();

    // Range-vector functions
    if (isRangeFunction(name)) {
      if (fn.args().isEmpty())
        throw new IllegalArgumentException("Function '" + fn.name() + "' requires a range vector argument");
      final PromQLResult argResult = evaluate(fn.args().get(0), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
      if (!(argResult instanceof RangeVector rv))
        throw new IllegalArgumentException("Function '" + fn.name() + "' requires a range vector argument");

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
        samples.add(new VectorSample(series.labels(), value, evalTimeMs));
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
    final String name = fn.name().toLowerCase();
    final PromQLResult argResult = evaluate(fn.args().get(0), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    final double param = extractSecondParam(fn, evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);

    if (argResult instanceof ScalarResult sr)
      return new ScalarResult(applyScalarFn(name, sr.value(), param), evalTimeMs);

    if (argResult instanceof InstantVector iv) {
      final List<VectorSample> samples = new ArrayList<>();
      for (final VectorSample sample : iv.samples())
        samples.add(new VectorSample(sample.labels(), applyScalarFn(name, sample.value(), param), sample.timestampMs()));
      return new InstantVector(samples);
    }

    return argResult;
  }

  private double extractSecondParam(final FunctionCallExpr fn, final long evalTimeMs, final long queryStartMs,
      final long queryEndMs, final long stepMs, final int depth) {
    if (fn.args().size() <= 1)
      return 1.0;
    final PromQLResult paramResult = evaluate(fn.args().get(1), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    if (paramResult instanceof ScalarResult sr)
      return sr.value();
    return 1.0;
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

    // scalar-scalar
    if (leftResult instanceof ScalarResult ls && rightResult instanceof ScalarResult rs)
      return new ScalarResult(applyBinaryOp(bin.op(), ls.value(), rs.value()), evalTimeMs);

    // vector-scalar
    if (leftResult instanceof InstantVector iv && rightResult instanceof ScalarResult rs)
      return applyVectorScalar(iv, rs.value(), bin.op(), false);

    // scalar-vector
    if (leftResult instanceof ScalarResult ls && rightResult instanceof InstantVector iv)
      return applyVectorScalar(iv, ls.value(), bin.op(), true);

    // vector-vector
    if (leftResult instanceof InstantVector lv && rightResult instanceof InstantVector rv)
      return applyVectorVector(lv, rv, bin.op());

    return new ScalarResult(Double.NaN, evalTimeMs);
  }

  private PromQLResult evaluateUnary(final UnaryExpr un, final long evalTimeMs, final long queryStartMs, final long queryEndMs,
      final long stepMs, final int depth) {
    final PromQLResult result = evaluate(un.expr(), evalTimeMs, queryStartMs, queryEndMs, stepMs, depth);
    if (un.op() == '-') {
      if (result instanceof ScalarResult sr)
        return new ScalarResult(-sr.value(), sr.timestampMs());
      if (result instanceof InstantVector iv) {
        final List<VectorSample> samples = new ArrayList<>();
        for (final VectorSample s : iv.samples())
          samples.add(new VectorSample(s.labels(), -s.value(), s.timestampMs()));
        return new InstantVector(samples);
      }
    }
    return result;
  }

  // --- Helper methods ---

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

  private TagFilter buildTagFilter(final List<LabelMatcher> matchers, final List<ColumnDefinition> columns) {
    TagFilter filter = null;
    for (final LabelMatcher m : matchers) {
      if (m.op() != MatchOp.EQ || "__name__".equals(m.name()))
        continue;
      final int idx = findNonTsColumnIndex(m.name(), columns);
      if (idx < 0)
        continue;
      filter = filter == null ? TagFilter.eq(idx, m.value()) : filter.and(idx, m.value());
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
   * narrowing it to none) and {@link #matchesPostFilters} rejected every row for an unknown {@code !=}/{@code !~}.
   * Deciding it here, before the scan, also means the query never runs when the answer is already known.
   * <p>
   * Deliberately outside the {@code try/catch} that turns an {@code iterateQuery()} failure into an empty
   * result: the {@code IllegalArgumentException}/{@code TimeoutException} a bad or ReDoS-shaped {@code =~} can
   * raise from here propagates to the caller, exactly as the same pattern already does from
   * {@link #matchesPostFilters}, which runs outside that {@code try} too. A rejected query is not an empty one.
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
   * {@link #matchesPostFilters} used to bail out on {@code rowIdx < 0} before ever reaching the switch, so a
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

  private boolean matchesPostFilters(final Object[] row, final List<LabelMatcher> matchers,
      final List<ColumnDefinition> columns, final long regexDeadline) {
    for (final LabelMatcher m : matchers) {
      if (m.op() == MatchOp.EQ || "__name__".equals(m.name()))
        continue;
      final int rowIdx = findNonTsRowIndex(m.name(), columns);
      if (rowIdx < 0)
        // The column is absent from the schema, so the label is absent from every series. excludesEverySeries()
        // already settled such a matcher for the whole type before the scan started: reaching a row at all
        // means it selected everything, so it cannot reject this one (issue #6938).
        continue;
      final Object val = rowIdx < row.length ? row[rowIdx] : null;
      final String strVal = val != null ? val.toString() : "";
      switch (m.op()) {
        case NEQ:
          if (strVal.equals(m.value()))
            return false;
          break;
        case RE:
          if (!TimeBoundRegex.matchesUntil(compilePattern(m.value()), strVal, regexDeadline))
            return false;
          break;
        case NRE:
          if (TimeBoundRegex.matchesUntil(compilePattern(m.value()), strVal, regexDeadline))
            return false;
          break;
        default:
          break;
      }
    }
    return true;
  }

  /**
   * Extracts label key/value pairs from a row.
   * The row format is always {@code [timestamp, non-ts-col-0, non-ts-col-1, ...]}, so we
   * iterate non-TIMESTAMP columns and map them to row positions 1, 2, 3… regardless of
   * where the TIMESTAMP column appears in the schema definition.
   */
  private Map<String, String> extractLabels(final Object[] row, final List<ColumnDefinition> columns, final String metricName) {
    final Map<String, String> labels = new LinkedHashMap<>();
    labels.put("__name__", metricName);
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      final int rowPos = 1 + nonTsIdx;
      nonTsIdx++;
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG && rowPos < row.length && row[rowPos] != null)
        labels.put(col.getName(), row[rowPos].toString());
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
      final Set<String> exclude = new HashSet<>(groupLabels);
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
    if (labels.isEmpty())
      return "{}";
    final List<String> sorted = new ArrayList<>(labels.keySet());
    Collections.sort(sorted);
    final StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < sorted.size(); i++) {
      if (i > 0)
        sb.append(',');
      sb.append(sorted.get(i)).append('=').append(labels.get(sorted.get(i)));
    }
    sb.append('}');
    return sb.toString();
  }

  private InstantVector applyVectorScalar(final InstantVector iv, final double scalar, final BinaryOp op,
      final boolean scalarOnLeft) {
    final List<VectorSample> result = new ArrayList<>();
    for (final VectorSample s : iv.samples()) {
      final double value = scalarOnLeft ? applyBinaryOp(op, scalar, s.value()) : applyBinaryOp(op, s.value(), scalar);
      if (!isComparisonOp(op) || value != 0)
        result.add(new VectorSample(s.labels(), isComparisonOp(op) ? s.value() : value, s.timestampMs()));
    }
    return new InstantVector(result);
  }

  private InstantVector applyVectorVector(final InstantVector left, final InstantVector right, final BinaryOp op) {
    // Simple vector-vector matching by label identity
    final Map<String, VectorSample> rightMap = new HashMap<>();
    for (final VectorSample s : right.samples())
      rightMap.put(labelKey(s.labels()), s);

    final List<VectorSample> result = new ArrayList<>();
    for (final VectorSample ls : left.samples()) {
      final VectorSample rs = rightMap.get(labelKey(ls.labels()));
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
          final double value = applyBinaryOp(op, ls.value(), rs.value());
          if (!isComparisonOp(op) || value != 0)
            result.add(new VectorSample(ls.labels(), isComparisonOp(op) ? ls.value() : value, ls.timestampMs()));
        }
      } else if (op == BinaryOp.OR || op == BinaryOp.UNLESS) {
        result.add(ls);
      }
    }
    // For OR, also add unmatched right-side samples
    if (op == BinaryOp.OR) {
      final Set<String> leftKeys = new HashSet<>();
      for (final VectorSample s : left.samples())
        leftKeys.add(labelKey(s.labels()));
      for (final VectorSample rs : right.samples())
        if (!leftKeys.contains(labelKey(rs.labels())))
          result.add(rs);
    }
    return new InstantVector(result);
  }

  private double applyBinaryOp(final BinaryOp op, final double left, final double right) {
    return switch (op) {
      case ADD -> left + right;
      case SUB -> left - right;
      case MUL -> left * right;
      case DIV -> right == 0 ? Double.NaN : left / right;
      case MOD -> right == 0 ? Double.NaN : left % right;
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

  private boolean isComparisonOp(final BinaryOp op) {
    return op == BinaryOp.EQ || op == BinaryOp.NEQ || op == BinaryOp.LT || op == BinaryOp.GT || op == BinaryOp.LTE
        || op == BinaryOp.GTE;
  }

  private boolean isRangeFunction(final String name) {
    return switch (name) {
      case "rate", "irate", "increase", "sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "count_over_time" ->
        true;
      default -> false;
    };
  }

  private static final int    MAX_TYPE_NAME_LENGTH   = 256;
  private static final Pattern VALID_TYPE_NAME_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

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
