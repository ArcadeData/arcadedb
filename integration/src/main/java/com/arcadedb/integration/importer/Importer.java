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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;

import java.io.*;
import java.util.*;

public class Importer extends AbstractImporter {
  public Importer(final String[] args) {
    super(args);
  }

  public Importer(final Database database, final String url) {
    super((DatabaseInternal) database);
    settings.url = url;
  }

  public static void main(final String[] args) {
    new Importer(args).load();
    System.exit(0);
  }

  public Map<String, Object> load() {
    source = null;

    try {
      final int cfgValue = settings.getIntValue("maxValueSampling", 100);
      final AnalyzedSchema analyzedSchema = new AnalyzedSchema(cfgValue);

      openDatabase();

      startImporting();

      loadFromSource(settings.url, AnalyzedEntity.EntityType.DATABASE, analyzedSchema);
      loadFromSource(settings.documents, AnalyzedEntity.EntityType.DOCUMENT, analyzedSchema);
      loadFromSource(settings.vertices, AnalyzedEntity.EntityType.VERTEX, analyzedSchema);
      loadFromSource(settings.edges, AnalyzedEntity.EntityType.EDGE, analyzedSchema);

      if (settings.probeOnly)
        return null;

      if (database.isTransactionActive())
        database.commit();

    } catch (final Exception e) {
      if (settings.probeOnly)
        throw new IllegalArgumentException(e);
      else
        throw new ImportException("Error on parsing source '" + source + "'", e);
    } finally {
      stopImporting();
      if (database != null) {
        closeDatabase();
      }
      closeInputFile();
    }

    return context.toMap();
  }

  protected void loadFromSource(final String url, AnalyzedEntity.EntityType entityType, final AnalyzedSchema analyzedSchema)
      throws IOException {
    if (url == null)
      // SKIP IT
      return;

    final SourceDiscovery sourceDiscovery = new SourceDiscovery(url);

    if (settings.probeOnly) {
      sourceDiscovery.getSource();
      return;
    }

    final SourceSchema sourceSchema = sourceDiscovery.getSchema(settings, entityType, analyzedSchema, logger);
    if (sourceSchema == null) {
      //LogManager.instance().log(this, Level.WARNING, "XML importing aborted because unable to determine the schema");
      return;
    }

    updateDatabaseSchema(sourceSchema.getSchema());

    source = sourceDiscovery.getSource();
    parser = new Parser(source, 0);
    parser.reset();

    format = sourceSchema.getContentImporter();

    format.load(sourceSchema, entityType, parser, database, context, settings);
  }

  protected void closeDatabase() {
    if (!databaseCreatedDuringImporting)
      return;

    if (database != null) {
      if (database.isTransactionActive())
        database.commit();
      database.close();
    }
  }
}
