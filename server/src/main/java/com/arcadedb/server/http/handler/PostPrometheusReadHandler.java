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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.promql.PromQLEvaluator;
import com.arcadedb.engine.timeseries.promql.PromQLEvaluator.RowMatchers;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.RequestBodyTooLargeException;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.QueryResult;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP handler for Prometheus remote_read protocol.
 * Endpoint: POST /api/v1/ts/{database}/prom/read
 * <p>
 * Receives Snappy-compressed protobuf ReadRequest messages,
 * queries the TimeSeries engine, and returns Snappy-compressed protobuf ReadResponse.
 * <p>
 * Session-aware since issue #7681: {@link AbstractBinaryHttpHandler} was reparented onto
 * {@link DatabaseAbstractHandler} in that change, so a request carrying {@code arcadedb-session-id} reads
 * through that session's transaction, under its lock and on its principal and refreshing its idle timer, and
 * the base class subsumes the {@code checkAuthorizationOnDatabase} call this handler used to make by hand.
 * <p>
 * {@link #requiresTransaction()} is false: remote_read is a read, and an auto-commit wrapper would only add a
 * commit with nothing to commit. An unresolvable session id therefore degrades to a session-less read rather
 * than being refused, the same asymmetry with {@link PostPrometheusWriteHandler} that #7402 established
 * between the {@code /api/v1/ts} reads and write.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostPrometheusReadHandler extends AbstractBinaryHttpHandler {
  private static final String METRIC_NAME_LABEL = "__name__";

  public PostPrometheusReadHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  /**
   * Answered here rather than inherited from {@link AbstractObservabilityHandler}, which this handler cannot
   * extend because its binary body already spends its one superclass (issue #7859). The reason is that class's:
   * a remote-read never writes into the caller's transaction, so the 413 it raises when the answer would exceed
   * {@code arcadedb.server.maxResultRows} must not destroy a transaction the client opened with {@code /begin}
   * and still believes it owns.
   */
  @Override
  protected boolean participatesInSessionTransaction() {
    return false;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    // A request naming another protobuf message in its Content-Type is refused with the 415 the remote-write
    // specification gives that case, rather than decoded in a schema it is not in (issue #9624)
    final String protoMessage = PrometheusTypes.protoMessage(exchange.getRequestHeaders().getFirst(Headers.CONTENT_TYPE));
    if (protoMessage != null && !ReadRequest.PROTO_MESSAGE.equals(protoMessage))
      return new ExecutionResponse(415, new JSONObject().put("error", "Unsupported remote-read protobuf message '" + protoMessage
          + "': this endpoint implements " + ReadRequest.PROTO_MESSAGE).toString());

    // THIS request's bytes, off the exchange rather than off a field shared with every concurrent request; see
    // AbstractBinaryHttpHandler.RAW_BINARY_PAYLOAD and issue #7683.
    final byte[] rawBytes = rawBytes(exchange);
    if (rawBytes == null || rawBytes.length == 0)
      return new ExecutionResponse(400, "{ \"error\" : \"Request body is empty\"}");

    // Snappy decompress, under the decoded-body budget (issue #8084): the read side decodes a body exactly as the
    // write side does, so it carries the same exposure and takes the same bound. See PostPrometheusWriteHandler for
    // why the 413 is rethrown past the 400 below.
    final byte[] decompressed;
    try {
      decompressed = CompressedBodyDecoder.snappyUncompress(rawBytes,
          CompressedBodyDecoder.maxDecompressedSize(httpServer.getServer().getConfiguration()));
    } catch (final RequestBodyTooLargeException e) {
      throw e;
    } catch (final Exception e) {
      return new ExecutionResponse(400, "{ \"error\" : \"Invalid Snappy-compressed data\"}");
    }

    // A malformed body is the client's mistake, not a server fault: answered 400 like the invalid Snappy data above,
    // rather than letting the decoder's IllegalStateException reach the 500 arm of the error classification (issue #9624)
    final ReadRequest readRequest;
    try {
      readRequest = ReadRequest.decode(decompressed);
    } catch (final IllegalStateException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", "Malformed protobuf " + ReadRequest.PROTO_MESSAGE + ": " + e.getMessage()).toString());
    }
    final DatabaseInternal database = (DatabaseInternal) db;

    // The hard ceiling this endpoint never consulted (issue #7663). One budget for the WHOLE response: a
    // remote-read request carries one Query per selector and they all travel back in a single ReadResponse, so a
    // per-query ceiling would let a request with twenty selectors return twenty times the maximum.
    final TimeSeriesHandlerUtils.RowBudget budget = new TimeSeriesHandlerUtils.RowBudget(getMaxResultRows());

    final List<QueryResult> queryResults = new ArrayList<>();
    // One evaluator for the whole request: its regex deadline is shared by every query and every row, so a crafted pattern
    // cannot be charged its timeout once per selector (issue #9803, the bound #5886 gave the PromQL endpoints)
    final PromQLEvaluator matcherEvaluator = new PromQLEvaluator(database);

    try {
      for (final Query query : readRequest.getQueries())
        queryResults.add(new QueryResult(readSeries(database, query, matcherEvaluator, budget)));
    } catch (final IllegalArgumentException e) {
      // A malformed or ReDoS-shaped =~/!~ is a rejected request, not an empty answer: an empty answer is how a metric that
      // does not exist reads (issue #9803)
      return new ExecutionResponse(400, new JSONObject().put("error", "Invalid label matcher: " + e.getMessage()).toString());
    }

    // Encode response
    final ReadResponse readResponse = new ReadResponse(queryResults);
    final byte[] responseBytes = readResponse.encode();
    final byte[] compressed = Snappy.compress(responseBytes);

    // Send binary response
    exchange.getResponseHeaders().put(new HttpString("Content-Type"), "application/x-protobuf");
    exchange.setStatusCode(200);
    exchange.getResponseSender().send(ByteBuffer.wrap(compressed));

    return null; // response already sent
  }

  /**
   * The series one remote-read query selects, every one of its label matchers applied (issue #9803).
   * <p>
   * The matchers are decided by the engine's own PromQL matcher logic - the rules {@code /query}, {@code /query_range} and
   * {@code /series} apply - rather than by a loop of this endpoint's own: {@link PromQLEvaluator#selectsNoSeries} settles a
   * matcher on a label the type does not declare for the whole type, {@link PromQLEvaluator#tagFilter} pushes the non-empty
   * equalities down to the scan, and {@link PromQLEvaluator#matchesRow} decides the rest per row ({@code !=}, {@code =~},
   * {@code !~}, {@code label=""}) under the request's regex deadline. The loop this replaces honoured {@code =} only and
   * dropped every other matcher, so {@code {host!="bad"}} returned the very series it excludes, and Prometheus does not
   * filter a remote-read response again.
   * <p>
   * {@code __name__} selects the metrics: an equality names one, any other matcher is tested against every TimeSeries type
   * the caller may read, each of them reported under its own name. It used to be recognised as an equality only, and a
   * {@code {__name__=~"cp.*"}} answered no series at all - the same answer as a metric that does not exist.
   * <p>
   * The rows are visited shard by shard and put in timestamp order per series afterwards, as the evaluator's range selector
   * does. The budget is charged per row that passes the matchers, so the response is refused, never truncated, at the first
   * row past {@code arcadedb.server.maxResultRows}, and holds at most that many rows meanwhile (issue #7663).
   */
  private List<TimeSeries> readSeries(final DatabaseInternal database, final Query query, final PromQLEvaluator matcherEvaluator,
      final TimeSeriesHandlerUtils.RowBudget budget) throws IOException {
    final List<PromQLExpr.LabelMatcher> matchers = toEngineMatchers(query.getMatchers());
    final List<TimeSeries> seriesList = new ArrayList<>();

    for (final Map.Entry<String, LocalTimeSeriesType> metric : selectedMetrics(database, matchers, matcherEvaluator).entrySet()) {
      final String metricName = metric.getKey();
      final LocalTimeSeriesType tsType = metric.getValue();

      // Gated accessor (per-type ACL): a metric named by an equality is answered with a 403 when denied rather than with an
      // empty result - an empty result is how a NON-EXISTENT metric answers, and conflating the two would let the caller keep
      // probing a type it has no read right on. A metric selected by a pattern was already filtered on that right.
      final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
      if (engine == null)
        continue;
      final List<ColumnDefinition> columns = tsType.getTsColumns();

      if (matcherEvaluator.selectsNoSeries(matchers, columns))
        continue;

      final TagFilter tagFilter = matcherEvaluator.tagFilter(matchers, columns);
      final RowMatchers rowMatchers = matcherEvaluator.rowMatchers(matchers, columns);
      final Map<String, List<Object[]>> grouped = new LinkedHashMap<>();
      final boolean[] overBudget = { false };
      engine.forEachRow(query.getStartTimestampMs(), query.getEndTimestampMs(), null, tagFilter, null, row -> {
        if (!matcherEvaluator.matchesRow(row, rowMatchers))
          return true;
        // Refused rather than truncated: Prometheus has no way to represent a partial remote-read response, so a silently cut
        // one is indistinguishable from a gap in the data (issue #5719's rule, applied here)
        if (!budget.charge(1)) {
          overBudget[0] = true;
          return false;
        }
        grouped.computeIfAbsent(buildLabelKey(columns, row), k -> new ArrayList<>()).add(row);
        return true;
      });
      if (overBudget[0])
        throw resultSetTooLarge(budget.ceiling());

      final int valueRowIndex = findFieldRowIndex(columns, "value");
      final List<TimeSeries> metricSeries = new ArrayList<>(grouped.size());
      for (final List<Object[]> groupRows : grouped.values()) {
        // a series' samples are spread over every shard, and a remote-read series lists them in timestamp order
        groupRows.sort(Comparator.comparingLong(row -> (long) row[0]));
        metricSeries.add(new TimeSeries(labelsOf(metricName, columns, groupRows.getFirst()), samplesOf(groupRows, valueRowIndex)));
      }
      // Ordered by each series' first sample, the order the merged ascending read this replaces gave them
      metricSeries.sort(Comparator.comparingLong(series -> series.getSamples().getFirst().timestampMs()));
      seriesList.addAll(metricSeries);
    }
    return seriesList;
  }

  /**
   * The TimeSeries types a query's {@code __name__} matchers select, by the metric name their series are reported under, in
   * schema order. A non-empty equality names the metric, so it is the only candidate, and a name the schema does not hold
   * selects nothing - the answer for a metric that does not exist. Without one, every TimeSeries type the caller may read is
   * a candidate (series discovery: a type the caller cannot read is left out rather than refused, as {@code /series} does),
   * and every {@code __name__} matcher must hold on its name.
   */
  private static Map<String, LocalTimeSeriesType> selectedMetrics(final DatabaseInternal database,
      final List<PromQLExpr.LabelMatcher> matchers, final PromQLEvaluator matcherEvaluator) {
    final Map<String, LocalTimeSeriesType> selected = new LinkedHashMap<>();
    String namedMetric = null;
    for (final PromQLExpr.LabelMatcher matcher : matchers)
      if (METRIC_NAME_LABEL.equals(matcher.name()) && matcher.op() == PromQLExpr.MatchOp.EQ && !matcher.value().isEmpty()) {
        namedMetric = matcher.value();
        break;
      }

    if (namedMetric != null) {
      final String typeName = PostPrometheusWriteHandler.sanitizeTypeName(namedMetric);
      if (matcherEvaluator.matchesMetricName(matchers, namedMetric) && database.getSchema().existsType(typeName)
          && database.getSchema().getType(typeName) instanceof LocalTimeSeriesType tsType)
        selected.put(namedMetric, tsType);
      return selected;
    }

    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof LocalTimeSeriesType tsType && tsType.getEngine() != null
          && SecurityHelper.canAccessType(database, tsType, SecurityDatabaseUser.ACCESS.READ_RECORD)
          && matcherEvaluator.matchesMetricName(matchers, type.getName()))
        selected.put(type.getName(), tsType);
    return selected;
  }

  /**
   * The remote-read matchers in the engine's form. A label name is spelled as the column the remote write stored it under
   * ({@link PostPrometheusWriteHandler#sanitizeColumnName}); {@code __name__} is kept as it is, since it names the metric.
   */
  private static List<PromQLExpr.LabelMatcher> toEngineMatchers(final List<LabelMatcher> matchers) {
    final List<PromQLExpr.LabelMatcher> converted = new ArrayList<>(matchers.size());
    for (final LabelMatcher matcher : matchers) {
      final String name = METRIC_NAME_LABEL.equals(matcher.name()) ?
          matcher.name() :
          PostPrometheusWriteHandler.sanitizeColumnName(matcher.name());
      final PromQLExpr.MatchOp op = switch (matcher.type()) {
        case EQ -> PromQLExpr.MatchOp.EQ;
        case NEQ -> PromQLExpr.MatchOp.NEQ;
        case RE -> PromQLExpr.MatchOp.RE;
        case NRE -> PromQLExpr.MatchOp.NRE;
      };
      converted.add(new PromQLExpr.LabelMatcher(name, op, matcher.value() != null ? matcher.value() : ""));
    }
    return converted;
  }

  /**
   * The labels of a series. The row is { timestamp, non-TIMESTAMP columns in schema order... }, so a TAG column's value sits
   * at 1 + its ordinal AMONG THE NON-TIMESTAMP COLUMNS - not at its schema index, which is the same number only while the
   * TIMESTAMP column is declared first (issue #7899). An empty value is an ABSENT label in Prometheus (issue #7712): left
   * out here exactly as {@link #buildLabelKey} leaves it out of the key, or a series would be grouped on a label it is not
   * reported with (issue #9623).
   */
  private static List<Label> labelsOf(final String metricName, final List<ColumnDefinition> columns, final Object[] row) {
    final List<Label> labels = new ArrayList<>();
    labels.add(new Label(METRIC_NAME_LABEL, metricName));
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG) {
        final Object tagVal = 1 + nonTsIdx < row.length ? row[1 + nonTsIdx] : null;
        if (tagVal != null && PromQLResponseFormatter.isLabelValuePresent(tagVal.toString()))
          labels.add(new Label(col.getName(), tagVal.toString()));
      }
      nonTsIdx++;
    }
    return labels;
  }

  private static List<Sample> samplesOf(final List<Object[]> rows, final int valueRowIndex) {
    final List<Sample> samples = new ArrayList<>(rows.size());
    for (final Object[] row : rows) {
      double value = 0;
      if (valueRowIndex >= 0 && valueRowIndex < row.length && row[valueRowIndex] instanceof Number n)
        value = n.doubleValue();
      samples.add(new Sample(value, (long) row[0]));
    }
    return samples;
  }

  /**
   * The POSITION IN THE ROW of the named FIELD column, or -1 when the type declares none: the row the scan
   * hands back is {@code { timestamp, non-TIMESTAMP columns in schema order... }}, so it is one past the
   * column's ordinal among the non-timestamp columns and not its schema index (issue #7899). Read as a schema
   * index, a type declaring its TIMESTAMP column anywhere but first reported the neighbouring column's content
   * as the sample - and where that neighbour was the timestamp itself, a sample of about 1.7e12.
   */
  private static int findFieldRowIndex(final List<ColumnDefinition> columns, final String name) {
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (col.getRole() == ColumnDefinition.ColumnRole.FIELD && col.getName().equals(name))
        return 1 + nonTsIdx;
      nonTsIdx++;
    }
    return -1;
  }

  /**
   * Groups rows by their TAG values, indexed the way {@link #findFieldRowIndex} explains.
   * <p>
   * Each label is LENGTH-PREFIXED, name and value, as {@code GetPromQLSeriesHandler} and
   * {@code PromQLEvaluator.labelKey} do: a tag value arrives from a remote-write client and can hold any
   * character, so a separator-joined key let two distinct label sets ({@code host="a|b",region="c"} and
   * {@code host="a",region="b|c"}) spell one key and be answered as ONE series carrying both series' samples
   * under the first one's labels (issue #9623). An empty value is an absent label (issue #7712), so it takes no
   * part in the identity; carrying the NAME is what keeps that skip from merging {@code host="",region="x"}
   * with {@code host="x",region=""}.
   */
  private static String buildLabelKey(final List<ColumnDefinition> columns, final Object[] row) {
    final StringBuilder sb = new StringBuilder(64);
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG) {
        final Object val = 1 + nonTsIdx < row.length ? row[1 + nonTsIdx] : null;
        if (val != null) {
          final String value = val.toString();
          if (PromQLResponseFormatter.isLabelValuePresent(value)) {
            final String name = col.getName();
            sb.append(name.length()).append(':').append(name).append(value.length()).append(':').append(value);
          }
        }
      }
      nonTsIdx++;
    }
    return sb.toString();
  }
}
