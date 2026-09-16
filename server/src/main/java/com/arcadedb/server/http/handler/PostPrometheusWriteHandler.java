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
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import org.xerial.snappy.Snappy;

import java.util.Arrays;
import java.util.List;

/**
 * HTTP handler for Prometheus remote_write protocol.
 * Endpoint: POST /api/v1/ts/{database}/prom/write
 * <p>
 * Receives Snappy-compressed protobuf WriteRequest messages,
 * auto-creates TimeSeries types as needed, and inserts samples.
 * <p>
 * Session-aware since issue #7681: {@link AbstractBinaryHttpHandler} was reparented onto
 * {@link DatabaseAbstractHandler} in that change, so a request carrying {@code arcadedb-session-id} is
 * resolved against that session - running under its lock and principal, refreshing the idle clock instead of
 * letting a client that only ingests have its transaction reaped underneath it, and refused outright when the
 * id names a session this server no longer knows.
 * <p>
 * The two transaction answers are the pairing issue #7402 gave {@code POST /api/v1/ts/{database}/write}, for
 * the same reasons:
 * <ul>
 * <li>{@link #requiresTransaction()} is <b>false</b>. The body below opens its OWN {@code begin}/{@code commit}
 * around the auto-create and the appends, and has since before this handler knew what a session was; a second,
 * outer auto-commit wrapper around that one would have nothing of its own to commit. It is not what makes the
 * request atomic either - {@code TimeSeriesShard.appendSamples} commits per shard inside it, which is what the
 * NOTE on that block says.</li>
 * <li>{@link #rejectsUnresolvableSession()} is <b>true</b>. A read that names a session this server cannot
 * resolve can degrade to reading outside it; a write cannot, because the client believes it is writing
 * something it can still roll back.</li>
 * </ul>
 * When a session DOES resolve, the {@code database.begin()} below opens a transaction NESTED inside it. An
 * ArcadeDB nested transaction is an independent transaction rather than a savepoint, so the samples are
 * durable and global before the caller commits anything, and the session's own transaction is untouched by the
 * {@code commit()} that closes the nested one. Issue #7410 tracks that divergence from
 * {@code TimeSeriesEngine}'s javadoc.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostPrometheusWriteHandler extends AbstractBinaryHttpHandler {

  public PostPrometheusWriteHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  @Override
  protected boolean rejectsUnresolvableSession() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    exchange.getResponseHeaders().put(new HttpString("X-Prometheus-Remote-Write-Version"), "0.1.0");

    // THIS request's bytes, off the exchange rather than off a field shared with every concurrent request; see
    // AbstractBinaryHttpHandler.RAW_BINARY_PAYLOAD and issue #7683. Prometheus remote-write is exactly the
    // path where that matters: several shippers pointed at one endpoint is the normal deployment.
    final byte[] rawBytes = rawBytes(exchange);
    if (rawBytes == null || rawBytes.length == 0)
      return new ExecutionResponse(400, "{ \"error\" : \"Request body is empty\"}");

    // Snappy decompress
    final byte[] decompressed;
    try {
      decompressed = Snappy.uncompress(rawBytes);
    } catch (final Exception e) {
      return new ExecutionResponse(400, "{ \"error\" : \"Invalid Snappy-compressed data\"}");
    }

    // Decode protobuf WriteRequest
    final WriteRequest writeRequest = WriteRequest.decode(decompressed);
    if (writeRequest.getTimeSeries().isEmpty())
      return new ExecutionResponse(204, "");

    final DatabaseInternal database = (DatabaseInternal) db;

    // Per-type ACL preflight, before the first append and before getOrCreateType() can mutate the schema:
    // TimeSeriesShard.appendSamples commits its own shard transaction, so a denial discovered mid-loop would
    // return 403 with the earlier series already durable. Checked by NAME so a denied metric is refused without
    // its type being auto-created first; an unknown name has no entry in the permission map and stays allowed,
    // which is what keeps auto-create working for a brand-new metric.
    for (final TimeSeries ts : writeRequest.getTimeSeries()) {
      final String metricName = ts.getMetricName();
      if (metricName == null || metricName.isEmpty())
        continue;
      database.checkPermissionsOnType(sanitizeTypeName(metricName), SecurityDatabaseUser.ACCESS.CREATE_RECORD);
    }

    // NOTE: this transaction does NOT make the request atomic. TimeSeriesShard.appendSamples runs its own
    // begin/commit on getWrappedDatabaseInstance(), so every appendBatch below has already committed its
    // shard writes by the time it returns. If a later series throws, the rollback here cannot undo the
    // series already written and the caller sees an error with part of the payload persisted.
    //
    // Opened only when this request does not already have one, which since issue #7681 it can: a request
    // carrying arcadedb-session-id arrives with the session's transaction bound onto this thread. Beginning
    // here anyway would push a NESTED transaction onto it, and DatabaseContext caps nesting at 3
    // (maxNested) - a depth the auto-create path below needs, because opening a brand-new type's shards and
    // tag dictionary takes transactions of its own. A remote write that created a type inside a session
    // failed with "Exceeded number of 3 nested transactions" wrapped as a SchemaException naming the type,
    // which reads as a broken type rather than as one transaction level too many.
    //
    // It would buy nothing either: the block is explicitly not what makes the request atomic, and the
    // caller's own transaction is the one it asked to write inside. commit()/rollback() below are gated on
    // the same flag, because either one applied to a transaction this handler did not open would settle the
    // caller's session transaction behind its back.
    final boolean ownTransaction = !database.isTransactionActive();
    if (ownTransaction)
      database.begin();
    try {
      for (final TimeSeries ts : writeRequest.getTimeSeries()) {
        final String metricName = ts.getMetricName();
        if (metricName == null || metricName.isEmpty())
          continue;

        // Sanitize metric name: dots/hyphens → underscores
        final String typeName = sanitizeTypeName(metricName);

        // Auto-create type if needed
        final LocalTimeSeriesType tsType = getOrCreateType(database, typeName, ts.getLabels());
        // requireEngine(), not getEngine(): getOrCreateType() now returns an existing TimeSeries type whatever
        // state its storage is in, so this is where a missing engine is reported - naming the type and why the
        // engine never started, rather than letting a null reach an NPE (issue #6356) or, as before, a phantom
        // "already exists" from the auto-create branch (issue #6839).
        final TimeSeriesEngine engine = tsType.requireEngine(SecurityDatabaseUser.ACCESS.CREATE_RECORD);
        final List<ColumnDefinition> columns = tsType.getTsColumns();

        // Append this series' samples as ONE batch. All samples of a remote-write TimeSeries share the
        // same type and labels, so they can go in a single shard transaction. Appending one at a time
        // would cost a transaction per sample - and on a Raft HA leader, a replicated quorum round trip
        // per sample, serialized behind the per-shard append lock.
        final List<Sample> tsSamples = ts.getSamples();
        final int count = tsSamples.size();
        if (count == 0)
          continue;

        final long[] timestamps = new long[count];
        for (int s = 0; s < count; s++)
          timestamps[s] = tsSamples.get(s).timestampMs();

        // Fill the value grid column by column, matching its column-major layout. A TAG value comes from
        // the series labels, which are per-series and not per-sample, so it is resolved ONCE and broadcast
        // down the column: findLabelValue is a linear scan that re-sanitizes every label name it visits,
        // so resolving it per sample would repeat identical work for every sample of every tag column.
        final Object[][] columnValues = new Object[columns.size() - 1][count];

        int colIdx = 0;
        for (final ColumnDefinition col : columns) {
          if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
            continue;

          if (col.getRole() == ColumnDefinition.ColumnRole.TAG)
            Arrays.fill(columnValues[colIdx], findLabelValue(ts.getLabels(), col.getName()));
          else
            for (int s = 0; s < count; s++)
              columnValues[colIdx][s] = tsSamples.get(s).value(); // the "value" field

          colIdx++;
        }

        engine.appendBatch(timestamps, columnValues);
      }
      if (ownTransaction)
        database.commit();
    } catch (final Exception e) {
      if (ownTransaction)
        database.rollback();
      throw e;
    }

    return new ExecutionResponse(204, "");
  }

  private LocalTimeSeriesType getOrCreateType(final DatabaseInternal database, final String typeName,
      final List<Label> labels) {
    if (database.getSchema().existsType(typeName)) {
      final DocumentType docType = database.getSchema().getType(typeName);
      // The two halves are separate questions and were one condition, which is what made this the only TimeSeries
      // call site PR #6779 left reporting the wrong error (issue #6839). "Is it a TimeSeries type" decides whether
      // to auto-create; "does it have an engine" decides whether the write can proceed, and that is the caller's
      // requireEngine() to answer. Folding them together sent a registered-but-engine-unavailable type (#6356)
      // into the auto-create branch below, where create() throws SchemaException("Type 'X' already exists") - a
      // name collision that does not exist, instead of the reason the engine is missing and the file it names.
      if (docType instanceof LocalTimeSeriesType tsType)
        return tsType;
    }

    // Auto-create: timestamp + tags from labels + one DOUBLE field "value"
    final TimeSeriesTypeBuilder builder = new TimeSeriesTypeBuilder(database)
        .withName(typeName)
        .withTimestamp("timestamp");

    for (final Label label : labels) {
      if ("__name__".equals(label.name()))
        continue;
      builder.withTag(sanitizeColumnName(label.name()), Type.STRING);
    }

    builder.withField("value", Type.DOUBLE);
    // The builder was constructed with this embedded DatabaseInternal, so its create() is the local one and the
    // type it returns is a LocalTimeSeriesType. The cast is what the widened TimeSeriesType return type (issue
    // #7399) costs a caller that needs the engine, which this one does.
    return (LocalTimeSeriesType) builder.create();
  }

  private static String findLabelValue(final List<Label> labels, final String tagName) {
    for (final Label l : labels) {
      if (sanitizeColumnName(l.name()).equals(tagName))
        return l.value();
    }
    return null;
  }

  static String sanitizeTypeName(final String name) {
    return name.replace('.', '_').replace('-', '_').replace(':', '_');
  }

  static String sanitizeColumnName(final String name) {
    return name.replace('.', '_').replace('-', '_').replace(':', '_');
  }
}
