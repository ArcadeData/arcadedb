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
package com.arcadedb.integration.importer.format;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.univocity.parsers.common.AbstractParser;

import java.io.*;

public class RDFImporterFormat extends CSVImporterFormat {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"', '<', '>' };

  @Override
  public void load(final SourceSchema sourceSchema, AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    long skipEntries = settings.edgesSkipEntries != null ? settings.edgesSkipEntries : 0;
    if (settings.edgesSkipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      skipEntries = 1l;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(), DatabaseFactory.getDefaultCharset())) {
      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          // SKIP IT
          continue;

        final String v1Id = getStringContent(row[0], STRING_CONTENT_SKIP);
        final String edgeLabel = getStringContent(row[1], STRING_CONTENT_SKIP);
        final String v2Id = getStringContent(row[2], STRING_CONTENT_SKIP);

        // CREATE AN EDGE
        database.newEdgeByKeys(settings.vertexTypeName, new String[] { settings.typeIdProperty }, new Object[] { v1Id }, settings.vertexTypeName,
            new String[] { settings.typeIdProperty }, new Object[] { v2Id }, true, settings.edgeTypeName, true, "label", edgeLabel);

        context.createdEdges.incrementAndGet();
        context.parsed.incrementAndGet();

        if (context.parsed.get() % settings.commitEvery == 0) {
          database.commit();
          database.begin();
        }
      }

      database.commit();

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV", e);
    }
  }

  @Override
  public String getFormat() {
    return "RDF";
  }
}
