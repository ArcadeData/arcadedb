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
import com.arcadedb.integration.importer.AnalyzedProperty;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.log.LogManager;
import com.univocity.parsers.common.AbstractParser;

import java.io.IOException;
import java.io.Reader;
import java.util.logging.Level;

public class RDFImporterFormat extends CSVImporterFormat {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"', '<', '>' };

  /**
   * The one property {@link #load} puts on the edges it creates: the statement's predicate. Named here because
   * {@link #analyze} declares it and the loop below writes it, and the two must agree.
   */
  private static final String LABEL_PROPERTY = "label";

  /**
   * No delimiter, so the inherited {@code createCSVParser}/{@code analyze} fall back to the generic
   * {@code delimiter} option and then to a comma.
   * <p>
   * Nothing in production builds the format this way: {@link com.arcadedb.integration.importer.SourceDiscovery}
   * is the only place one is constructed, and it always hands over the separator it took from between the first
   * statement's terms through {@link #RDFImporterFormat(String)}. This form exists for a caller that drives
   * {@code load()} directly with settings already carrying the delimiter - the format's own tests - and for parity
   * with {@link CSVImporterFormat}'s own pair. A production caller reaching for it is a caller that has a detected
   * separator to pass and is dropping it (issue #7315).
   */
  public RDFImporterFormat() {
    super();
  }

  /**
   * @param delimiter the delimiter this source is parsed with - the user's own when they set one, else the character
   *                  content sniffing found repeating between the {@code <...>} terms, which is the very thing that
   *                  identified the source as RDF. It is carried on the format rather than written into
   *                  {@code settings.options}, which one import shares across its entities (issue #6946), and dropping
   *                  it made the canonical space-delimited N-Triples form unimportable: the inherited fallback is a
   *                  comma, so the whole line arrived as a single column (issue #7315).
   */
  public RDFImporterFormat(final String delimiter) {
    super(delimiter);
  }

  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.EntityType entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
    final AbstractParser csvParser = createCSVParser(settings);

    // THE OPTION THAT GOVERNS THIS ROUTE, NOT ALWAYS -edgesSkipEntries. AN RDF SOURCE REACHES load() ON ANY OF THE
    // FOUR ROUTES Importer.load() DISPATCHES - -url, -documents, -vertices AND -edges - AND THIS READ
    // settings.edgesSkipEntries WHICHEVER ONE IT HAD ARRIVED ON, SO ON THE -vertices AND -documents ROUTES
    // -verticesSkipEntries WAS SILENTLY INERT WHILE -edgesSkipEntries, THE OPTION A USER THERE HAS NO REASON TO
    // REACH FOR, WAS THE ONE THAT WORKED (ISSUE #7487). THE SOURCE STILL BECOMES EDGES WHATEVER THE ROUTE: THE
    // ENTITY TYPE PICKS THE OPTION, NOT WHAT IS BUILT FROM THE ROWS.
    // defaultHeaderSkipEntries() ANSWERS 0 HERE (SEE THE OVERRIDE BELOW): AN RDF SOURCE HAS NO HEADER ROW, SO NOTHING
    // IS SKIPPED UNLESS THE CALLER ASKED FOR IT (ISSUE #7345).
    // A CALLER THAT DRIVES load() DIRECTLY MAY SUPPLY NO ENTITY TYPE - THE FORMAT'S OWN TESTS DO, AND THE CODE THIS
    // REPLACES COULD NOT TELL, SINCE IT IGNORED THE PARAMETER. EDGE IS WHAT IT BEHAVED AS, AND STAYS SO.
    final AnalyzedEntity.EntityType route = entityType != null ? entityType : AnalyzedEntity.EntityType.EDGE;

    final long skipEntries = skipEntries(route, settings);

    // Rows this loop dropped because skipEntries said to, reported apart from the failures: createdEdges being short
    // of parsedRecords used to mean any of "a header the caller asked to skip", "an unresolved reference" or "a row
    // -onRowError skip dropped", with nothing in the report to tell them apart (issue #7488).
    long skipped = 0;

    // Whether the transaction this method is about to use belongs to the import, as opposed to predating it.
    // Not the same as "this call pushed it": in the CLI pipeline AbstractImporter.openDatabase() ends with a
    // begin() that is deliberately left open for the whole import, so the begin() below finds one active and
    // reuses it. Rolling that one back on failure is still right - the import aborts with it either way, and
    // the alternative is AbstractImporter.closeDatabase() committing a half-finished import. What must never
    // be rolled back is a transaction that predates the import, which is exactly what
    // ImporterContext#callerTransactionActiveOnEntry records, and which ImporterContext#importOwnsTransaction()
    // answers for every row loop in one place rather than five times by hand (issue #7328). Read here, before the
    // begin() below, because after that begin() a transaction is always active.
    final boolean ownsTransaction = context.importOwnsTransaction(database);

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

    // Edges created since the last commit, and the only thing the commit boundary below is measured against. It used
    // to be measured against context.parsed, which is neither: that counter also advances for the header rows this
    // loop skips, and it was incremented twice per row, so under the default one-line header skip the value reaching
    // the modulo was 2N+1 for the Nth data row - always odd, which an even -commitEvery (5000 is the default) never
    // matches, so the commit never ran at all. A count of the rows this loop has created since the last boundary is
    // what -commitEvery actually asks for, and is the same shape CSVImporterFormat.loadEdges() uses (issue #7288).
    int txCount = 0;

    try (final Reader inputFileReader = sourceReader(parser)) {
      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();
      txOpen = ownsTransaction;

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        // CHECKED BEFORE THE SKIP-ROW 'continue' BELOW, THE SAME WAY CSVImporterFormat's ROW LOOPS ARE: A SKIPPED
        // ROW NEVER REACHES THE POST-PROCESSING CHECK AT THE BOTTOM OF THIS LOOP, SO parsingLimitBytes WOULD
        // OTHERWISE GO ON READING PAST ITS BUDGET FOR AS LONG AS ROWS KEEP BEING SKIPPED.
        if (settings.parsingLimitBytes > 0 && parser.getPosition() > settings.parsingLimitBytes)
          break;

        if (skipEntries > 0 && line < skipEntries) {
          // SKIP IT
          ++skipped;
          continue;
        }

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
            LABEL_PROPERTY,
            edgeLabel);

        context.createdEdges.incrementAndGet();

        // Gated on ownsTransaction the same way JsonlImporterFormat.load() gates its own periodic commit: a
        // transaction that predates this import is never ours to commit piecemeal, only to accumulate into and
        // hand back to whoever owns it (issue #6561). That guard is also what makes the txOpen below
        // unconditional - reached only when the begin() above pushed a transaction this call exclusively owns.
        // txCount is incremented inside the guard rather than beside the counter above so that it cannot run away
        // on the caller-owned path, where nothing would ever reset it.
        if (ownsTransaction && ++txCount >= settings.commitEvery) {
          txOpen = false;
          database.commit();
          committedEdges = context.createdEdges.get();
          database.begin();
          txOpen = true;
          txCount = 0;
        }

        // SAME CAP AND SAME '>=' AS XMLImporterFormat.load() (ISSUE #7341): context.parsed IS INCREMENTED ONCE PER
        // ROW, SO STOPPING ONCE IT REACHES THE LIMIT IMPORTS EXACTLY -parsingLimitEntries ROWS, NOT ONE MORE (#7482).
        // KEPT AS A POST-PROCESSING CHECK, UNLIKE parsingLimitBytes ABOVE: THE ROW THAT TRIPS THIS CAP IS MEANT TO
        // STILL LAND, THE SAME WAY XML's DOES.
        if (settings.parsingLimitEntries > 0 && context.parsed.get() >= settings.parsingLimitEntries)
          break;
      }

      txOpen = false;
      // Same ownsTransaction gate as the periodic commit above and as CSVImporterFormat.loadDocuments()'s own
      // trailing commit: a transaction that predates the import stays the caller's to commit or discard, and this
      // one used to commit it as a side effect of the import succeeding. The edges are left staged in it instead
      // (issue #7288).
      if (ownsTransaction)
        database.commit();
      completed = true;

    } catch (final IOException e) {
      throw new ImportException("Error on importing CSV", e);
    } finally {
      reportSkippedEntries(route, context, skipped);

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

  /**
   * The type an RDF source maps to, registered WITHOUT reading a statement: every statement of every RDF source has
   * the same three terms, and {@link #load} turns each into one edge of {@code settings.edgeTypeName} carrying the
   * predicate as {@value #LABEL_PROPERTY}. There is nothing in the source to discover.
   * <p>
   * Inheriting {@link CSVImporterFormat#analyze} instead meant the source's FIRST statement was consumed as the
   * column names, which is the same "the first line is a header" convention {@link #defaultHeaderSkipEntries()}
   * corrects on the load side and is just as wrong here, in two ways that {@code -edgesSkipEntries} cannot reach:
   * a one-statement source registered no type at all, so the import died with "Type ... was not found"; and a
   * longer one registered the EDGE type with a property per term of the first statement - {@code <http://a/s1>},
   * {@code <http://a/rel>}, {@code <http://a/o1>} and {@code .} - none of which any edge ever carries
   * (issue #7345, raised in review of #7497).
   * <p>
   * The property is declared with a {@code null} sample deliberately: {@link AnalyzedProperty#endParsing()} infers
   * a numeric type only from a sample it has actually seen, so no sample leaves it the {@code STRING} a predicate
   * IRI is, while an empty-string sample would have made it a {@code LONG}.
   */
  @Override
  public SourceSchema analyze(final AnalyzedEntity.EntityType entityType, final Parser parser,
      final ImporterSettings settings, final AnalyzedSchema analyzedSchema) {
    analyzedSchema.getOrCreateEntity(settings.edgeTypeName, AnalyzedEntity.EntityType.EDGE)
        .getOrCreateProperty(LABEL_PROPERTY, null);
    analyzedSchema.endParsing();

    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  /**
   * Zero: N-Triples, N-Quads and Turtle have no header row at all - every line of the source is a statement.
   * <p>
   * Inheriting {@link CSVImporterFormat}'s default of one meant the first triple of every RDF file was dropped as a
   * header, silently: nothing in the report told "N rows, one was a header" from "N rows, one was malformed", since
   * {@code parsedRecords} counted all N and {@code createdEdges} said N-1. The convention is right for CSV and wrong
   * here for the very reason the format was selected - content sniffing recognised the first line BECAUSE it is a
   * triple (issue #7345).
   */
  @Override
  protected long defaultHeaderSkipEntries() {
    return 0L;
  }

  @Override
  public String getFormat() {
    return "RDF";
  }
}
