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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.exporter.ExportException;
import com.arcadedb.integration.exporter.ExporterContext;
import com.arcadedb.integration.exporter.ExporterSettings;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.zip.*;

public class JsonlExporterFormat extends AbstractExporterFormat {
  public static final  String             NAME       = "jsonl";
  protected final      JSONObject         sharedJson = new JSONObject();
  private              OutputStreamWriter writer;
  private final static int                VERSION    = 1;

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
      writeJsonLine("db", new JSONObject().put("name", database.getName()).put("executedOn", dateFormat.format(now))
          .put("executedOnTimestamp", now));

      writeJsonLine("schema", ((LocalSchema) database.getSchema()).toJSON());

      final List<String> vertexTypes = new ArrayList<>();
      final List<String> edgeTypes = new ArrayList<>();
      final List<String> documentTypes = new ArrayList<>();

      for (final DocumentType type : database.getSchema().getTypes()) {
        final String typeName = type.getName();

        if (settings.includeTypes != null && !settings.includeTypes.contains(typeName))
          continue;
        if (settings.excludeTypes != null && settings.excludeTypes.contains(typeName))
          continue;

        if (type instanceof LocalVertexType)
          vertexTypes.add(typeName);
        else if (type instanceof LocalEdgeType)
          edgeTypes.add(typeName);
        else
          documentTypes.add(typeName);
      }

      final JSONObject recordJson = new JSONObject();

      final JsonGraphSerializer graphSerializer = JsonGraphSerializer.createJsonGraphSerializer()
          .setSharedJson(recordJson)
          .setIncludeMetadata(false)
          .setExpandVertexEdges(true);

      exportVertices(vertexTypes, graphSerializer);
      exportDocuments(documentTypes, graphSerializer);
      exportEdges(edgeTypes, graphSerializer);
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
          LogManager.instance()
              .log(this, Level.SEVERE, "Error on exporting vertex %s", e, record != null ? record.getIdentity() : null);
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
          LogManager.instance()
              .log(this, Level.SEVERE, "Error on exporting vertex %s", e, record != null ? record.getIdentity() : null);
        }
      }
    }
  }

  protected void writeJsonLine(final String type, final JSONObject json) throws IOException {
    writer.write(sharedJson.put("t", type).put("c", json).toString() + "\n");
    sharedJson.clear();
  }
}
