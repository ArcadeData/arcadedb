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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.integration.importer.SourceSchema;

import java.io.*;

public interface FormatImporter {
  void load(SourceSchema sourceSchema, AnalyzedEntity.ENTITY_TYPE entityType, Parser parser, DatabaseInternal database, ImporterContext context,
      ImporterSettings settings) throws IOException;

  SourceSchema analyze(AnalyzedEntity.ENTITY_TYPE entityType, Parser parser, ImporterSettings settings, AnalyzedSchema analyzedSchema) throws IOException;

  String getFormat();

  default void printProgress(final ImporterSettings settings, final ImporterContext context, final Source source, final Parser parser,
      final ConsoleLogger logger) {
    if (settings.verboseLevel < 2)
      return;

    try {
      long deltaInSecs = (System.currentTimeMillis() - context.lastLapOn) / 1000;
      if (deltaInSecs == 0)
        deltaInSecs = 1;

      if (source == null || source.compressed || source.totalSize < 0) {
        logger.logLine(2,//
            "- Parsed %,d (%,d/sec) %,d documents (%,d/sec) %,d vertices (%,d/sec) %,d edges (%,d/sec %,d skipped) %,d linked edges (%,d/sec %,d%%) updated documents %,d (%,d%%)",
//
            context.parsed.get(), ((context.parsed.get() - context.lastParsed) / deltaInSecs), context.createdDocuments.get(),
            (context.createdDocuments.get() - context.lastDocuments) / deltaInSecs, context.createdVertices.get(),
            (context.createdVertices.get() - context.lastVertices) / deltaInSecs, context.createdEdges.get(),
            (context.createdEdges.get() - context.lastEdges) / deltaInSecs, context.skippedEdges.get(), context.linkedEdges.get(),
            (context.linkedEdges.get() - context.lastLinkedEdges) / deltaInSecs,
            context.createdEdges.get() > 0 ? (int) (context.linkedEdges.get() * 100 / context.createdEdges.get()) : 0,//
            context.updatedDocuments.get(),
            context.documentsWithLinksToUpdate.get() > 0 ? (int) (context.updatedDocuments.get() * 100 / context.documentsWithLinksToUpdate.get()) : 0);
      } else {
        final int progressPerc = (int) (parser.getPosition() * 100 / source.totalSize);
        logger.logLine(2,//
            "Parsed %,d (%,d/sec %,d%%) %,d records (%,d/sec) %,d vertices (%,d/sec) %,d edges (%,d/sec %,d skipped) %,d linked edges (%,d/sec %,d%%) updated documents %,d (%,d%%)",
            context.parsed.get(), ((context.parsed.get() - context.lastParsed) / deltaInSecs), progressPerc, context.createdDocuments.get(),
            (context.createdDocuments.get() - context.lastDocuments) / deltaInSecs, context.createdVertices.get(),
            (context.createdVertices.get() - context.lastVertices) / deltaInSecs, context.createdEdges.get(),
            (context.createdEdges.get() - context.lastEdges) / deltaInSecs, context.skippedEdges.get(), context.linkedEdges.get(),
            (context.linkedEdges.get() - context.lastLinkedEdges) / deltaInSecs,
            context.createdEdges.get() > 0 ? (int) (context.linkedEdges.get() * 100 / context.createdEdges.get()) : 0,//
            context.updatedDocuments.get(),
            context.documentsWithLinksToUpdate.get() > 0 ? (int) (context.updatedDocuments.get() * 100 / context.documentsWithLinksToUpdate.get()) : 0);

      }
      context.lastLapOn = System.currentTimeMillis();
      context.lastParsed = context.parsed.get();

      context.lastDocuments = context.createdDocuments.get();
      context.lastVertices = context.createdVertices.get();
      context.lastEdges = context.createdEdges.get();
      context.lastLinkedEdges = context.linkedEdges.get();

    } catch (final Exception e) {
      logger.errorLine("Error on print statistics: " + e.getMessage());
    }
  }
}
