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
import com.arcadedb.integration.exporter.ExportException;
import com.arcadedb.integration.exporter.ExporterContext;
import com.arcadedb.integration.exporter.ExporterSettings;
import com.arcadedb.integration.exporter.format.AbstractExporterFormat;
import com.arcadedb.integration.importer.ConsoleLogger;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.GZIPOutputStream;

public class GraphSONExporterFormat extends AbstractExporterFormat {
  public static final String     NAME       = "graphson";
  protected final     JSONObject sharedJson = new JSONObject();

  public GraphSONExporterFormat(final DatabaseInternal database, final ExporterSettings settings, final ExporterContext context, final ConsoleLogger logger) {
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

    try (ArcadeGraph graph = ArcadeGraph.open(database)) {
      try (FileOutputStream fos = new FileOutputStream(exportFile)) {
        try (GZIPOutputStream out = new GZIPOutputStream(fos)) {
          graph.io(IoCore.graphson()).writer().create().writeGraph(out, graph);
        }
      }
    }
  }

  @Override
  public String getName() {
    return NAME;
  }
}
