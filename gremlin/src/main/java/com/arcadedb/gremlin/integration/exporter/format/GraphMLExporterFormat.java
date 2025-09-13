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
package com.arcadedb.gremlin.integration.exporter.format;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.integration.exporter.ExportException;
import com.arcadedb.integration.exporter.ExporterContext;
import com.arcadedb.integration.exporter.ExporterSettings;
import com.arcadedb.integration.exporter.format.AbstractExporterFormat;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.*;
import java.util.zip.*;

public class GraphMLExporterFormat extends AbstractExporterFormat {
  public static final String     NAME       = "graphml";
  protected final     JSONObject sharedJson = new JSONObject();

  public GraphMLExporterFormat(final DatabaseInternal database, final ExporterSettings settings, final ExporterContext context, final ConsoleLogger logger) {
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
      throw new ExportException("Transaction in progress found");

    logger.logLine(0, "Exporting database to '%s'...", settings.file);

    final File exportFile;
    if (settings.file.startsWith("file://"))
      exportFile = new File(settings.file.substring("file://".length()));
    else
      exportFile = new File(settings.file);

    if (!exportFile.getParentFile().exists())
      exportFile.getParentFile().mkdirs();

    final ArcadeGraph graph = ArcadeGraph.open(database);
    try (final FileOutputStream fos = new FileOutputStream(exportFile)) {
      try (final GZIPOutputStream out = new GZIPOutputStream(fos)) {
        graph.io(IoCore.graphml()).writer().create().writeGraph(out, graph);
      }
    }
  }

  @Override
  public String getName() {
    return NAME;
  }
}
