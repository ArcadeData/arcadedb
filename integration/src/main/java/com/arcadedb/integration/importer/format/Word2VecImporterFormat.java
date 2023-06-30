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
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.integration.importer.vector.TextEmbeddingsImporter;

import java.io.*;

/**
 * Imports Word2Vec text embedding format.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Word2VecImporterFormat extends AbstractImporterFormat {
  private TextEmbeddingsImporter importer;

  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {

    context.parsed.set(0);

    try {
      settings.documentsSkipEntries = 1L; // SKIP 1ST LINE
      importer = new TextEmbeddingsImporter(database, parser.getSource().inputStream, settings).setContext(context);
      importer.run();

    } catch (final Exception e) {
      throw new ImportException("Error on importing Word2Vec datasource", e);
    }
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) throws IOException {
    parser.reset();

    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public void printProgress(ImporterSettings settings, ImporterContext context, Source source, Parser parser, ConsoleLogger logger) {
    if (importer != null)
      importer.printProgress();
  }

  @Override
  public String getFormat() {
    return "Word2Vec";
  }
}
