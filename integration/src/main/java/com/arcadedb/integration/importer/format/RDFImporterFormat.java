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
import com.arcadedb.log.LogManager;
import com.univocity.parsers.common.AbstractParser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;

public class RDFImporterFormat extends CSVImporterFormat {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"', '<', '>' };

  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.EntityType entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
    final AbstractParser csvParser = createCSVParser(settings);

    long skipEntries = settings.edgesSkipEntries != null ? settings.edgesSkipEntries : 0;
    if (settings.edgesSkipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      skipEntries = 1l;

    // Whether the transaction this method is about to use belongs to the import, as opposed to predating it.
    // Not the same as "this call pushed it": in the CLI pipeline AbstractImporter.openDatabase() ends with a
    // begin() that is deliberately left open for the whole import, so the begin() below finds one active and
    // reuses it. Rolling that one back on failure is still right - the import aborts with it either way, and
    // the alternative is AbstractImporter.closeDatabase() committing a half-finished import. What must never
    // be rolled back is a transaction that predates the import, which is exactly what
    // ImporterContext#callerTransactionActiveOnEntry records.
    final boolean ownsTransaction = !context.callerTransactionActiveOnEntry;

    // Whether a transaction this call owns is still the current one. Cleared right before every commit -
    // LocalDatabase#commit() pops the transaction in a finally, so a commit that throws still leaves it off the
    // stack - which keeps the rollback below from popping the caller's instead (issue #7272).
    boolean txOpen = false;

    // Edges an intermediate commit already made durable. context.createdEdges counts every edge created,
    // the ones still inside the transaction a failure rolls back included.
    long committedEdges = context.createdEdges.get();

    // Whether the loop ran to its own trailing commit. Distinct from txOpen: a commit() that throws pops the
    // transaction in its own finally, so txOpen is already false there, yet the batch it failed to make
    // durable still has to come back off the counter.
    boolean completed = false;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(), DatabaseFactory.getDefaultCharset())) {
      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();
      txOpen = ownsTransaction;

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
        database.newEdgeByKeys(settings.vertexTypeName,
            new String[] { settings.typeIdProperty },
            new Object[] { v1Id },
            settings.vertexTypeName,
            new String[] { settings.typeIdProperty },
            new Object[] { v2Id }, true,
            settings.edgeTypeName,
            true,
            "label",
            edgeLabel);

        context.createdEdges.incrementAndGet();
        context.parsed.incrementAndGet();

        // Gated on ownsTransaction the same way JsonlImporterFormat.load() gates its own periodic commit: a
        // transaction that predates this import is never ours to commit piecemeal, only to accumulate into and
        // hand back to whoever owns it (issue #6561). That guard is also what makes the txOpen below
        // unconditional - reached only when the begin() above pushed a transaction this call exclusively owns.
        if (ownsTransaction && context.parsed.get() % settings.commitEvery == 0) {
          txOpen = false;
          database.commit();
          committedEdges = context.createdEdges.get();
          database.begin();
          txOpen = true;
        }
      }

      txOpen = false;
      database.commit();
      completed = true;

    } catch (final IOException e) {
      throw new ImportException("Error on importing CSV", e);
    } finally {
      // In a finally rather than in a catch: a malformed row (csvParser.parseNext() itself throwing) or a
      // newEdgeByKeys() failure escapes uncaught otherwise, leaving the transaction opened above on the stack,
      // and an Error - an OutOfMemoryError is the one a large import can realistically raise - would too.
      if (txOpen && database.isTransactionActive()) {
        try {
          database.rollback();
        } catch (final Exception rollbackFailure) {
          // Swallowed: a throw here would replace the failure the caller can actually act on with one about the
          // cleanup, and would skip the counter correction below.
          LogManager.instance().log(this, Level.SEVERE,
              "Could not roll back after the RDF import failed: the transaction it opened may still be on the stack",
              rollbackFailure);
        }

      }

      // Outside the txOpen branch above, because a commit() that threw has already popped its own transaction
      // and would otherwise leave the batch it failed to write counted as if it had survived. Gated on
      // ownsTransaction for the same reason the rollback is: the edges accumulated into a caller's own
      // transaction are the caller's to commit or discard, so their fate is not this method's to report on.
      if (!completed && ownsTransaction) {
        // What the report calls "created" has to be what survived: leaving the counter at the number of edges
        // read would credit the import with the ones the rollback just took away.
        final long readEdges = context.createdEdges.get();
        context.createdEdges.set(committedEdges);

        if (committedEdges > 0)
          LogManager.instance().log(this, Level.WARNING,
              "RDF import failed after %,d edges: the import is PARTIAL - %,d of them an earlier batch commit made "
                  + "durable and they stay on the disk, the other %,d were rolled back", null, readEdges, committedEdges,
              readEdges - committedEdges);
      }
    }
  }

  @Override
  public String getFormat() {
    return "RDF";
  }
}
