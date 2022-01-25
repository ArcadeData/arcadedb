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
import com.arcadedb.database.Record;
import com.arcadedb.integration.exporter.ExportException;
import com.arcadedb.integration.exporter.ExporterContext;
import com.arcadedb.integration.exporter.ExporterSettings;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.JsonGraphSerializer;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class JsonlExporterFormat extends AbstractExporterFormat {
  public static final  String             NAME       = "jsonl";
  protected final      JSONObject         sharedJson = new JSONObject();
  private              OutputStreamWriter writer;
  private final static int                VERSION    = 1;

  public JsonlExporterFormat(final DatabaseInternal database, final ExporterSettings settings, final ExporterContext context, final ConsoleLogger logger) {
    super(database, settings, context, logger);
  }

  @Override
  public void exportDatabase() throws Exception {
    final File file = new File(settings.file);
    if (file.exists() && !settings.overwriteFile)
      throw new ExportException(String.format("The export file '%s' already exist and '-o' setting is false", settings.file));

    if (file.getParentFile() != null && !file.getParentFile().exists()) {
      if (!file.getParentFile().mkdirs())
        throw new ExportException(String.format("The export file '%s' cannot be created", settings.file));
    }

    if (database.isTransactionActive())
      throw new ExportException("Transaction in progress found");

    logger.logLine(0, "Exporting database to '%s'...", settings.file);

    final File exportFile;
    if (settings.file.startsWith("file://"))
      exportFile = new File(settings.file.substring("file://".length()));
    else
      exportFile = new File(settings.file);

    if (!exportFile.getParentFile().exists())
      exportFile.getParentFile().mkdirs();

    try (OutputStreamWriter fileWriter = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(exportFile)), DatabaseFactory.getDefaultCharset())) {
      writer = fileWriter;

      writeJsonLine("info", new JSONObject().put("description", "ArcadeDB Database Export").put("exporterVersion", VERSION)//
          .put("dbVersion", Constants.getRawVersion()).put("dbBranch", Constants.getBranch()).put("dbBuild", Constants.getBuildNumber())
          .put("dbTimestamp", Constants.getTimestamp()));

      final long now = System.currentTimeMillis();
      writeJsonLine("db", new JSONObject().put("name", database.getName()).put("executedOn", dateFormat.format(now)).put("executedOnTimestamp", now));

      writeJsonLine("schema", ((EmbeddedSchema) database.getSchema()).toJSON());

      final List<String> vertexTypes = new ArrayList<>();
      final List<String> edgeTypes = new ArrayList<>();
      final List<String> documentTypes = new ArrayList<>();

      for (DocumentType type : database.getSchema().getTypes()) {
        if (type instanceof VertexType)
          vertexTypes.add(type.getName());
        else if (type instanceof EdgeType)
          edgeTypes.add(type.getName());
        else
          documentTypes.add(type.getName());
      }

      final JSONObject recordJson = new JSONObject();

      final JsonGraphSerializer graphSerializer = new JsonGraphSerializer().setSharedJson(recordJson).setExpandVertexEdges(true);

      exportVertices(vertexTypes, graphSerializer);
      exportEdges(edgeTypes, graphSerializer);
      exportDocuments(documentTypes, graphSerializer);
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  private void exportVertices(List<String> vertexTypes, JsonGraphSerializer graphSerializer) throws IOException {
    for (String type : vertexTypes) {
      for (Iterator<Record> cursor = database.iterateType(type, false); cursor.hasNext(); ) {
        writeJsonLine("v", graphSerializer.serializeGraphElement(cursor.next().asVertex(true)));
        context.vertices.incrementAndGet();
      }
    }
  }

  private void exportEdges(List<String> edgeTypes, JsonGraphSerializer graphSerializer) throws IOException {
    for (String type : edgeTypes) {
      for (Iterator<Record> cursor = database.iterateType(type, false); cursor.hasNext(); ) {
        writeJsonLine("e", graphSerializer.serializeGraphElement(cursor.next().asEdge(true)));
        context.edges.incrementAndGet();
      }
    }
  }

  private void exportDocuments(List<String> documentTypes, JsonGraphSerializer graphSerializer) throws IOException {
    for (String type : documentTypes) {
      for (Iterator<Record> cursor = database.iterateType(type, false); cursor.hasNext(); ) {
        writeJsonLine("d", graphSerializer.serializeGraphElement(cursor.next().asDocument(true)));
        context.documents.incrementAndGet();
      }
    }
  }

  protected void writeJsonLine(final String type, final JSONObject json) throws IOException {
    writer.write(sharedJson.put("t", type).put("c", json).toString() + "\n");
    sharedJson.clear();
  }
}
