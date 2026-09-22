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
package com.arcadedb.integration.exporter.format;

import com.arcadedb.Constants;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.engine.timeseries.AggregationMetrics;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesWalkCoarsenedException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.exporter.ExportException;
import com.arcadedb.integration.exporter.ExporterContext;
import com.arcadedb.integration.exporter.ExporterSettings;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.serializer.json.NonFiniteNumbers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.zip.GZIPOutputStream;

public class JsonlExporterFormat extends AbstractExporterFormat {
  public static final  String             NAME       = "jsonl";
  private final static int                VERSION    = 1;
  /**
   * Samples per {@code "ts"} line. One line per sample would multiply the per-line envelope by the sample count
   * (a TimeSeries type is the one place where records are counted in millions); one line for the whole type would
   * make the reader hold it all in memory.
   */
  private final static int                TIMESERIES_CHUNK_SIZE = 1_000;
  private              OutputStreamWriter writer;
  protected final      JSONObject         sharedJson = new JSONObject();

  public JsonlExporterFormat(final DatabaseInternal database, final ExporterSettings settings, final ExporterContext context,
      final ConsoleLogger logger) {
    super(database, settings, context, logger);
  }

  @Override
  public void exportDatabase() throws Exception {
    final File file = new File(settings.file);
    // NO ensureParentDirectory HERE: 'file' is the UNRESOLVED settings.file, which for a 'file://' target names a
    // different (bogus) parent than the archive actually goes in. claimExportFile creates the real one, from the
    // resolved path, right before it takes the claim in it.
    refuseExistingTarget(file);

    if (database.isTransactionActive())
      database.getTransaction().rollback();

    logger.logLine(0, "Exporting database to '%s'...", settings.file);

    final File exportFile;
    if (settings.file.startsWith("file://"))
      exportFile = new File(settings.file.substring("file://".length()));
    else
      exportFile = new File(settings.file);

    final File lock = claimExportFile(exportFile);
    try (final OutputStreamWriter fileWriter = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(exportFile)),
        DatabaseFactory.getDefaultCharset())) {
      writer = fileWriter;

      writeJsonLine("info", new JSONObject().put("description", "ArcadeDB Database Export").put("exporterVersion", VERSION)//
          .put("dbVersion", Constants.getRawVersion()).put("dbBranch", Constants.getBranch())
          .put("dbBuild", Constants.getBuildNumber()).put("dbTimestamp", Constants.getTimestamp()));

      final long now = System.currentTimeMillis();
      writeJsonLine("db",
          new JSONObject().put("name", database.getName()).put("executedOn", dateFormat.format(Instant.ofEpochMilli(now)))
              .put("executedOnTimestamp", now));

      writeJsonLine("schema", ((LocalSchema) database.getSchema()).toJSON());

      final List<String> vertexTypes = new ArrayList<>();
      final List<String> edgeTypes = new ArrayList<>();
      final List<String> documentTypes = new ArrayList<>();
      final List<String> timeSeriesTypes = new ArrayList<>();

      for (final DocumentType type : database.getSchema().getTypes()) {
        final String typeName = type.getName();

        if (settings.includeTypes != null && !settings.includeTypes.contains(typeName))
          continue;
        if (settings.excludeTypes != null && settings.excludeTypes.contains(typeName))
          continue;

        // Checked before LocalVertexType/LocalEdgeType only for symmetry with them; a TimeSeries type is a
        // LocalDocumentType and would otherwise fall through to documentTypes, where iterateType() finds nothing
        // because the type owns no record bucket - its samples live in its own engine (issue #7032).
        if (type instanceof LocalTimeSeriesType)
          timeSeriesTypes.add(typeName);
        else if (type instanceof LocalVertexType)
          vertexTypes.add(typeName);
        else if (type instanceof LocalEdgeType)
          edgeTypes.add(typeName);
        else
          documentTypes.add(typeName);
      }

      final JSONObject recordJson = new JSONObject();

      // Issue #6455: the importer feeds the exported JSON straight back through MutableDocument.fromMap(), so
      // DATE/DATETIME_MICROS/DATETIME_NANOS must be encoded the way that schema-typed write-back path decodes
      // them, not as the epoch-millis number the default (HTTP graph-mode) encoding uses.
      // Issue #7032: a vertex line used to carry the RID of every one of its edges, in both directions, and no
      // import path has ever read them - edges are rebuilt from the "e" lines. On a graph of average degree d that
      // was 2d RIDs per vertex of pure overhead, in the one thing an export cannot promise to honour on the way
      // back in. Edges keep their endpoints; only the vertex-side duplicate goes.
      final JsonGraphSerializer graphSerializer = JsonGraphSerializer.createJsonGraphSerializer()
          .setSharedJson(recordJson)
          .setIncludeMetadata(false)
          .setIncludeVertexEdgeMetadata(false)
          .setPrecisionAwareTemporals(true);

      exportVertices(vertexTypes, graphSerializer);
      exportDocuments(documentTypes, graphSerializer);
      exportEdges(edgeTypes, graphSerializer);
      exportLightweightEdges(vertexTypes, graphSerializer);
      exportTimeSeries(timeSeriesTypes);
    } finally {
      releaseExportFile(lock);
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  private void exportVertices(final List<String> vertexTypes, final JsonGraphSerializer graphSerializer) throws IOException {
    for (final String type : vertexTypes) {
      for (final Iterator<Record> cursor = database.iterateType(type, false); cursor.hasNext(); ) {
        Vertex record = null;
        try {
          record = cursor.next().asVertex(true);

          if (settings.includeRecords != null && !settings.includeRecords.contains(record.getIdentity().toString()))
            continue;

          writeJsonLine("v", graphSerializer.serializeGraphElement(record));
          context.vertices.incrementAndGet();
        } catch (Exception e) {
          context.skippedRecords.incrementAndGet();
          LogManager.instance()
              .log(this, Level.SEVERE, "Error on exporting vertex %s", e, record != null ? record.getIdentity() : null);
        }
      }
    }
  }

  private void exportEdges(final List<String> edgeTypes, final JsonGraphSerializer graphSerializer) throws IOException {
    for (final String type : edgeTypes) {
      for (final Iterator<Record> cursor = database.iterateType(type, false); cursor.hasNext(); ) {
        Edge record = null;
        try {
          record = cursor.next().asEdge(true);

          if (settings.includeRecords != null && !settings.includeRecords.contains(record.getIdentity().toString()))
            continue;

          writeJsonLine("e", graphSerializer.serializeGraphElement(record));
          context.edges.incrementAndGet();
        } catch (Exception e) {
          context.skippedRecords.incrementAndGet();
          LogManager.instance()
              .log(this, Level.SEVERE, "Error on exporting vertex %s", e, record != null ? record.getIdentity() : null);
        }
      }
    }
  }

  /**
   * Exports the edges of every LIGHTWEIGHT edge type.
   * <p>
   * {@link #exportEdges} cannot see them: it iterates the edge type's own buckets, and a lightweight edge has no
   * record, so those buckets are empty and every such edge was silently dropped from the export. They live inside
   * the vertices instead, so they are collected by walking each vertex's outgoing list once. Only the OUT direction
   * is walked - the IN entry of a bidirectional edge is the same edge seen from the other end, and the importer
   * recreates both sides from a single {@code "e"} line.
   */
  private void exportLightweightEdges(final List<String> vertexTypes, final JsonGraphSerializer graphSerializer)
      throws IOException {
    for (final String type : vertexTypes) {
      for (final Iterator<Record> cursor = database.iterateType(type, false); cursor.hasNext(); ) {
        Vertex vertex = null;
        final Iterable<Edge> outEdges;
        try {
          vertex = cursor.next().asVertex(true);

          if (settings.includeRecords != null && !settings.includeRecords.contains(vertex.getIdentity().toString()))
            continue;

          outEdges = vertex.getEdges(Vertex.DIRECTION.OUT);
        } catch (Exception e) {
          context.skippedRecords.incrementAndGet();
          LogManager.instance().log(this, Level.SEVERE, "Error on exporting lightweight edges of vertex %s", e,
              vertex != null ? vertex.getIdentity() : null);
          continue;
        }

        // Issue #6795 (follow-up on #6471): each edge gets its OWN try/catch, so a failure on one edge is
        // counted on its own and does not silently drop the rest of this vertex's edges.
        for (final Edge edge : outEdges) {
          if (!(edge instanceof LightEdge))
            continue;
          if (settings.excludeTypes != null && settings.excludeTypes.contains(edge.getTypeName()))
            continue;
          if (settings.includeTypes != null && !settings.includeTypes.contains(edge.getTypeName()))
            continue;

          try {
            writeJsonLine("e", graphSerializer.serializeGraphElement(edge));
            context.edges.incrementAndGet();
          } catch (Exception e) {
            context.skippedRecords.incrementAndGet();
            LogManager.instance().log(this, Level.SEVERE, "Error on exporting lightweight edge %s of vertex %s", e,
                edge.getIdentity(), vertex.getIdentity());
          }
        }
      }
    }
  }

  private void exportDocuments(final List<String> documentTypes, final JsonGraphSerializer graphSerializer) throws IOException {
    for (final String type : documentTypes) {
      for (final Iterator<Record> cursor = database.iterateType(type, false); cursor.hasNext(); ) {
        Document record = null;
        try {
          record = cursor.next().asDocument(true);

          if (settings.includeRecords != null && !settings.includeRecords.contains(record.getIdentity().toString()))
            continue;

          writeJsonLine("d", graphSerializer.serializeGraphElement(record));
          context.documents.incrementAndGet();
        } catch (Exception e) {
          context.skippedRecords.incrementAndGet();
          LogManager.instance()
              .log(this, Level.SEVERE, "Error on exporting vertex %s", e, record != null ? record.getIdentity() : null);
        }
      }
    }
  }

  /**
   * Exports the samples of every TIMESERIES type, in chunks, as {@code "ts"} lines.
   * <p>
   * The schema line already carries the type's definition; without this the definition came back on import with no
   * data behind it, which is not a round trip. The samples do not go through {@link JsonGraphSerializer}: a
   * TimeSeries row is a fixed column tuple with no RID and no type of its own, so it is written as the raw value
   * array the engine reads and writes.
   * <p>
   * <b>That array is an ENGINE ROW</b>: position 0 is the timestamp and positions 1..n are the NON-TIMESTAMP
   * columns in schema order. This paragraph used to call it "schema-column order, timestamp first", two
   * descriptions of the same array only while the TIMESTAMP column is declared FIRST - and
   * {@code JsonlImporterFormat} read it the second way, so a type declaring the timestamp anywhere else, which
   * issue #7702 made spellable, did not survive the round trip (issue #7899). The engine layout is what is
   * emitted, here and before the correction, so nothing about the FILE changes and no format-version bump is
   * involved; what changed is that the reader and this sentence now agree with it.
   * <p>
   * Walked through {@link TimeSeriesEngine#forEachRow} rather than {@code iterateQuery} (issue #7697):
   * {@code iterateQuery}'s own javadoc says the sealed layer materialises every matching row before it returns an
   * iterator over them, so an export - which asks for {@code Long.MIN_VALUE} to {@code Long.MAX_VALUE}, the widest
   * range there is - held the whole series in heap before the chunk size ever bounded anything. {@code forEachRow}
   * folds each row into the chunk as it is produced, bounded by one block, so residency is independent of how many
   * samples the type holds. The chunk flush is unchanged; it just runs from inside the visitor now. The one thing
   * this trades away is the global timestamp order {@code iterateQuery}'s shard merge gave: {@code forEachRow}
   * visits shard by shard, so samples across shards are no longer interleaved by timestamp in the export. Nothing
   * downstream needs that order - {@code JsonlImporterFormat} hands each chunk straight to
   * {@code TimeSeriesEngine#appendBatch}, which carries its own per-row timestamps and does not require them
   * sorted - so the round trip is unaffected.
   *
   * @param timeSeriesTypes names of the TIMESERIES types selected for export
   */
  private void exportTimeSeries(final List<String> timeSeriesTypes) throws IOException {
    for (final String typeName : timeSeriesTypes) {
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(typeName);
      // The gated accessor, not getEngine(): a TimeSeries type owns no bucket for the per-file read check to
      // apply to, so this per-type check is the only thing standing between a denied user and the samples.
      final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
      if (engine == null) {
        LogManager.instance().log(this, Level.SEVERE,
            "TimeSeries engine for type '%s' is not available, its samples are NOT part of this export", null, typeName);
        context.skippedRecords.incrementAndGet();
        continue;
      }

      final List<ColumnDefinition> columns = tsType.getTsColumns();

      // The scan reads the shards' mutable pages, which are only reachable through a transaction; the record
      // exports above go through iterateType(), which opens an implicit one of its own.
      final boolean ownTransaction = !database.isTransactionActive();
      if (ownTransaction)
        database.begin();
      try {
        // A single-element holder, not a local variable reassigned in the loop: the visitor lambda can only close
        // over an effectively-final reference, and the chunk itself is replaced (not mutated) on every flush.
        final JSONArray[] chunkHolder = { new JSONArray() };
        // An AggregationMetrics, where this used to pass null (issue #8166). The engine counts a sealed block a
        // retention truncate removed from under the walk, and until now the only reader of that count anywhere
        // was the PromQL/HTTP metrics surface - so an export that lost blocks mid-walk wrote a SHORT file with no
        // exception, no log line and no count, which issue #8043 called the worst available outcome. A block a
        // DOWNSAMPLE replaced does not reach here at all any more: the engine raises for it, because its rows
        // were coarsened rather than removed and no mixed-resolution answer is a consistent one.
        final AggregationMetrics metrics = new AggregationMetrics();
        // Set by the coarsening arm below rather than jumping out of the loop, so the flush and the report that
        // follow the scan run for a refused walk exactly as they do for a whole one.
        boolean coarsened = false;
        try {
          engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, metrics, row -> {
            final JSONArray sample = new JSONArray();
            // Copied position for position: the row IS the wire order (see the engine-row note above), so there
            // is nothing to permute here - the bound is the row's own length, guarded by the column count
            // because an unprojected row carries exactly one value per column.
            for (int i = 0; i < columns.size() && i < row.length; i++)
              // Only the non-finite doubles need work, and they need it badly: JSONArray.put(Number) rewrites NaN
              // and +/-Infinity to 0, so writing them straight would turn "no measurement" into a measurement of
              // zero. NonFiniteNumbers is the same encoding record properties already travel by, and
              // JsonlImporterFormat decodes them back against the column's declared type.
              sample.put(NonFiniteNumbers.encode(row[i]));
            chunkHolder[0].put(sample);
            context.timeSeriesSamples.incrementAndGet();

            if (chunkHolder[0].length() >= TIMESERIES_CHUNK_SIZE) {
              try {
                writeJsonLine("ts", new JSONObject().put("t", typeName).put("s", chunkHolder[0]));
              } catch (final IOException e) {
                // TimeSeriesRowVisitor#visit declares no checked exception; unwrapped just below the scan.
                throw new UncheckedIOException(e);
              }
              chunkHolder[0] = new JSONArray();
            }
            return true;
          });
        } catch (final UncheckedIOException e) {
          throw e.getCause();
        } catch (final TimeSeriesWalkCoarsenedException e) {
          // Fails THIS type, not the whole export (issue #8166, review of PR #8197). A downsample is a scheduled
          // maintenance event and an export of a large database is long, so the two overlap; aborting everything
          // would throw away the vertices, edges, documents and other TIMESERIES types already written and make
          // the operator re-run the lot to learn about one series. Counted as a skipped record, which is the
          // mechanism issue #6471 established for a part of the export that could not be written: every other
          // type is still exported, and Exporter turns a non-zero count into a failed outcome at the end, so the
          // run is loudly incomplete rather than silently short - which is the whole point.
          //
          // And counted a SECOND time, under its own name. The rows already written for this type stay in the
          // archive - they are real, just fewer than the type holds - which is not the shape a skipped record
          // has, since that one produced no output at all. A consumer reading skippedRecords as "nothing was
          // written for this" would be wrong here, so partialTimeSeriesTypes says which types those are.
          //
          // It does NOT skip the flush and the report below, and that is the whole point of setting a flag here
          // instead of the `continue` this used to be (review of PR #8197). Both of those exist to say what
          // happened to the rows this walk DID read, and stepping over them threw that away twice: the rows
          // buffered since the last chunk boundary - up to TIMESERIES_CHUNK_SIZE - 1 of them, already counted in
          // context.timeSeriesSamples inside the visitor - never reached the file, so the count and the archive
          // disagreed; and a vanished-block tally this same walk had already accumulated, which the store-side
          // fix makes possible in one walk, went unreported. Silently short numbers, in the middle of the change
          // that exists to abolish them.
          coarsened = true;
          context.skippedRecords.incrementAndGet();
          context.partialTimeSeriesTypes.incrementAndGet();
          LogManager.instance().log(this, Level.SEVERE,
              "TIMESERIES type '%s' was downsampled while this export was reading it, so the samples written for "
                  + "it are PARTIAL and at a finer resolution than the store now holds; re-run the export for a "
                  + "whole answer. %s", null, typeName, e.getMessage());
        }

        // Always, for a whole walk and a refused one alike: every row the visitor counted is a row in the file.
        if (chunkHolder[0].length() > 0)
          writeJsonLine("ts", new JSONObject().put("t", typeName).put("s", chunkHolder[0]));

        // WARNING rather than a failure: retention dropping old blocks while a long export runs is legitimate and
        // expected, and the rows are genuinely gone rather than somewhere else, so the export is not wrong - it is
        // merely not the snapshot the operator may think it is. What it must not be is SILENT.
        // Reported even when the walk was refused above: a walk can step over blocks retention removed and only
        // later meet one a downsample replaced, which is precisely the "both maintenance passes in one walk" case
        // the store-side discrimination was built for. The two counts answer different questions and an operator
        // reading the summary needs both.
        if (metrics.getVanishedBlocks() > 0) {
          context.vanishedTimeSeriesBlocks.addAndGet(metrics.getVanishedBlocks());
          LogManager.instance().log(this, Level.WARNING,
              "%d sealed block(s) of TIMESERIES type '%s' were removed by retention while this export was reading "
                  + "them%s", null, metrics.getVanishedBlocks(), typeName,
              coarsened ? ", before the downsample that cut the read short" : "; their samples are NOT part of this export");
        }
      } finally {
        // Rolled back, never committed, on the success path too: the scan above only reads, so there is nothing
        // to publish, and a rollback releases the read view without asking the page manager to flush anything.
        if (ownTransaction && database.isTransactionActive())
          database.rollback();
      }
    }
  }

  protected void writeJsonLine(final String type, final JSONObject json) throws IOException {
    writer.write(sharedJson.put("t", type).put("c", json).toString() + "\n");
    sharedJson.clear();
  }
}
