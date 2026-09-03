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
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
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
    if (file.exists() && !settings.overwriteFile)
      throw new ExportException("The export file '%s' already exist and '-o' setting is false".formatted(settings.file));

    if (file.getParentFile() != null && !file.getParentFile().exists()) {
      if (!file.getParentFile().mkdirs())
        throw new ExportException("The export file '%s' cannot be created".formatted(settings.file));
    }

    if (database.isTransactionActive())
      database.getTransaction().rollback();

    logger.logLine(0, "Exporting database to '%s'...", settings.file);

    final File exportFile;
    if (settings.file.startsWith("file://"))
      exportFile = new File(settings.file.substring("file://".length()));
    else
      exportFile = new File(settings.file);

    if (!exportFile.getParentFile().exists())
      exportFile.getParentFile().mkdirs();

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
   * array the engine reads and writes, in schema-column order, timestamp first.
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
        JSONArray chunk = new JSONArray();
        for (final Iterator<Object[]> rows = engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
            rows.hasNext(); ) {
          final Object[] row = rows.next();
          final JSONArray sample = new JSONArray();
          for (int i = 0; i < columns.size() && i < row.length; i++)
              // Only the non-finite doubles need work, and they need it badly: JSONArray.put(Number) rewrites NaN
            // and +/-Infinity to 0, so writing them straight would turn "no measurement" into a measurement of
            // zero. NonFiniteNumbers is the same encoding record properties already travel by, and
            // JsonlImporterFormat decodes them back against the column's declared type.
            sample.put(NonFiniteNumbers.encode(row[i]));
          chunk.put(sample);
          context.timeSeriesSamples.incrementAndGet();

          if (chunk.length() >= TIMESERIES_CHUNK_SIZE) {
            writeJsonLine("ts", new JSONObject().put("t", typeName).put("s", chunk));
            chunk = new JSONArray();
          }
        }

        if (chunk.length() > 0)
          writeJsonLine("ts", new JSONObject().put("t", typeName).put("s", chunk));
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
