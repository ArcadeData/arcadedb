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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedProperty;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.common.CommonParserSettings;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * On {@code -onRowError skip}, {@code loadDocuments}/{@code loadVertices} reuse whatever transaction is already
 * active rather than nesting a new one - see {@link ImporterSettings#isSkipOnRowError()} for the full
 * transaction-ownership contract this and {@code JSONImporterFormat} both satisfy, by different means.
 */
public class CSVImporterFormat extends AbstractImporterFormat {
  /**
   * The delimiter resolved for the entity this format was created for - the user's own, else the one content sniffing
   * found - or null when the format was created without one, in which case the generic {@code delimiter} option and
   * then a comma apply. Carried here rather than read back from {@code settings.options}: that map is shared by every
   * entity of one import, so a delimiter written there for the documents file stood in for the vertices file's own
   * (issue #6946).
   */
  private final String delimiter;

  /**
   * How many short rows this format instance has already reported, so only the first logs at WARNING (see
   * {@link #reportShortRow}). Per instance, which is per source: {@code SourceDiscovery} builds a fresh format for
   * each of an import's documents/vertices/edges files.
   */
  private long shortRowsReported = 0;

  public CSVImporterFormat() {
    this(null);
  }

  public CSVImporterFormat(final String delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * The delimiter in force for this call, refusing the one value neither pass below can express.
   * <p>
   * The single resolution point both {@link #analyze} and {@link #createCSVParser} read, and the only place the
   * value is checked: {@code -delimiter ""} used to reach {@code analyze()}'s {@code delimiter.charAt(0)} and come
   * back as a {@code StringIndexOutOfBoundsException} out of the middle of the analysis, naming neither the option
   * nor the value (issue #7867).
   */
  private String delimiterFor(final ImporterSettings settings) {
    final String resolved = delimiter != null ? delimiter : settings.getValue("delimiter", ",");
    if (resolved != null && resolved.isEmpty())
      throw new IllegalArgumentException("The CSV delimiter is set to an empty value: a field separator is at least "
          + "one character, such as \",\" or \";\". Set it with -delimiter (or WITH delimiter = '...')");
    return resolved;
  }

  /**
   * Configures {@code parserSettings} with the delimiter in force, the ONE way, so the schema analysis and the row
   * load cannot split the same file into different columns.
   * <p>
   * {@link #analyze} used to truncate the value to {@code delimiter.charAt(0)} while {@link #createCSVParser} handed
   * univocity the whole {@code String}, so a separator longer than one character - {@code ";;"} - was analysed with
   * one column split and loaded with another: the inferred property types belonged to different columns than the
   * values that landed in them, silently (issue #7867).
   * <p>
   * {@code detectFormatAutomatically} is kept for a single-character separator, where it is what discovers the
   * quote and quote-escape characters, and is skipped for a longer one: it takes {@code char...} and has no
   * multi-character form, so offering it a truncated candidate is the very truncation this method exists to remove.
   */
  private static void applyDelimiter(final CsvParserSettings parserSettings, final String delimiter,
      final boolean detectFormat) {
    if (delimiter == null)
      return;
    if (detectFormat && delimiter.length() == 1)
      parserSettings.detectFormatAutomatically(delimiter.charAt(0));
    parserSettings.getFormat().setDelimiter(delimiter);
  }

  /**
   * The separator a supplied {@code -documentsHeader} / {@code -verticesHeader} / {@code -edgesHeader} is split on:
   * the delimiter in force, with the two tab spellings resolved to a real tab.
   * <p>
   * The two spellings are the convention {@link #analyze} and {@link #createCSVParser} already branch on to pick
   * the TSV parser: a tab reaches the importer either as a real tab ({@code "\t"}, from a caller that built the
   * settings in Java) or as the two characters a shell hands over unescaped ({@code "\\t"}, from {@code -delimiter
   * \t}). The TSV branches never reach this method - their parser has no delimiter to split a header on - so the
   * translation has to happen here.
   * <p>
   * It was a hardcoded comma, which is the same disagreement as the one above on the header row: a header supplied
   * for a {@code ';'}-delimited source came back as ONE field named {@code "id;name"}, and the analysis then threw
   * {@code IndexOutOfBoundsException} out of {@code fieldNames.get(i)} on the second column (issue #7867). Nothing
   * that worked changes: for the comma this is the comma.
   */
  private static String headerSeparator(final String delimiter) {
    return "\\t".equals(delimiter) ? "\t" : delimiter;
  }

  private static final Object[] NO_PARAMS = new Object[] {};
  public static final  int      _32MB     = 32 * 1024 * 1024;

  @Override
  public void load(final SourceSchema sourceSchema,
      final AnalyzedEntity.EntityType entityType, final Parser parser,
      final DatabaseInternal database,
      final ImporterContext context,
      final ImporterSettings settings) throws ImportException {

    switch (entityType) {
    case DOCUMENT, DATABASE -> loadDocuments(sourceSchema, entityType, parser, database, context, settings);
    case VERTEX -> loadVertices(sourceSchema, parser, database, context, settings);
    case EDGE -> loadEdges(sourceSchema, parser, database, context, settings);
    }
  }

  private void loadDocuments(final SourceSchema sourceSchema, final AnalyzedEntity.EntityType entityType, final Parser parser,
      final Database database, final ImporterContext context, final ImporterSettings settings) throws ImportException {
    final AbstractParser<?> csvParser = createCSVParser(settings);

    LogManager.instance().log(this, Level.INFO, "Started importing documents from CSV source");

    final long beginTime = System.currentTimeMillis();
    // Per-phase summary log below reports only this method's own delta, not the running total.
    final long errorsBefore = context.errors.get();

    // A syntax-level parseNext() failure (outside the per-row try/catch) still aborts even in "skip" mode: it leaves
    // the parser's own position tracking compromised, so only row-content failures are skippable.
    final boolean skipOnError = settings.isSkipOnRowError();

    // "skip" mode commits/rolls back per row, so it must own the transaction outright (see
    // ImporterSettings#isSkipOnRowError()); callerTransactionActiveOnEntry is the signal for that, not a live
    // isTransactionActive() check (see its Javadoc).
    if (skipOnError && context.callerTransactionActiveOnEntry)
      throw ImporterSettings.newExclusiveTransactionRequiredException();

    final long skipEntries = skipEntries(entityType, settings);
    long skipped = 0;

    // Captured before the try below so both are also visible in the catch blocks.
    final TransactionOwnership ownership = computeTransactionOwnership(database, context);
    final boolean transactionActiveOnEntry = ownership.transactionActiveOnEntry();
    final boolean ownsTransaction = ownership.ownsTransaction();

    try (final Reader inputFileReader = sourceReader(parser)) {
      csvParser.beginParsing(inputFileReader);

      // Unlike loadVertices(), called unconditionally regardless of skipOnError: loadDocuments() needs an active
      // transaction to save() into in both modes (it never goes through database.async()).
      beginRowTransaction(database, transactionActiveOnEntry, ownsTransaction);

      final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.documentTypeName);
      checkAnalysisFoundUsableRows(entity, entityType, settings);

      // The null check covers BOTH branches below, where it used to guard only the second: a source the analysis
      // derived no entity from - a header-only file, or one whose every row was refused - reached
      // entity.getProperties() in the include-list branch and threw NullPointerException, while the same source with
      // the default -documentPropertiesInclude '*' imported fine. loadVertices()/loadEdges() answer this with an
      // early return naming the type; documents cannot, because an empty property list is a legitimate outcome here
      // (issue #7782).
      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (entity != null) {
        if (!"*".equalsIgnoreCase(settings.documentPropertiesInclude)) {
          final String[] includes = settings.documentPropertiesInclude.split(",");

          final Set<String> propertiesSet = new HashSet<>(Arrays.asList(includes));

          for (final AnalyzedProperty p : entity.getProperties()) {
            if (propertiesSet.contains(p.getName())) {
              properties.add(p);
            }
          }
        } else {
          // INCLUDE ALL THE PROPERTIES
          properties.addAll(entity.getProperties());
        }
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following document properties: %s", null, properties);

      // -1 WHEN THE ANALYSIS DERIVED NO ENTITY, WHICH DISABLES THE ARITY GATE BELOW FOR THIS SOURCE. SAFE ONLY
      // BECAUSE THE SAME CONDITION LEAVES 'properties' EMPTY (SEE ABOVE), SO NOTHING INDEXES INTO row[] AND THERE IS
      // NO RAGGEDNESS TO CATCH - THE TWO READ AS INDEPENDENT CONDITIONS BUT ARE NOT. A FUTURE CHANGE THAT POPULATES
      // 'properties' FROM ANYTHING OTHER THAN 'entity' HAS TO GIVE THIS ONE A HEADER WIDTH TOO (issue #7782).
      final int headerColumns = headerColumnsOf(entity);

      // In "abort" mode, rows accumulate here instead of directly in context.createdDocuments, merged in below only
      // once the whole file has parsed without a mid-loop failure. On failure this is deliberate even when
      // ownsTransaction is false: rows saved before the failing one are left staged, uncommitted, in the caller's
      // own still-open transaction (see csvDocumentImportAbortsWithoutDiscardingCallersPendingWorkInExternallyManagedTransaction)
      // - whether they ultimately become durable is the caller's own commit/rollback decision, not this import's to
      // report on. "skip" mode doesn't use this: each row is counted directly, gated on its own commit() succeeding.
      long documentsCreatedThisFile = 0;

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        // CHECKED BEFORE THE SKIP-ROW 'continue' BELOW, NOT ONLY AFTER A ROW IS ACTUALLY PROCESSED: A SKIPPED
        // ROW (e.g. A LARGE -documentsSkipEntries HEADER BLOCK) NEVER REACHES THE POST-PROCESSING CHECK AT THE
        // BOTTOM OF THIS LOOP, SO parsingLimitBytes WOULD OTHERWISE GO ON READING PAST ITS BUDGET FOR AS LONG AS
        // ROWS KEEP BEING SKIPPED. parsingLimitEntries STAYS A POST-PROCESSING CHECK ON PURPOSE (SEE BELOW).
        if (settings.parsingLimitBytes > 0 && parser.getPosition() > settings.parsingLimitBytes)
          break;

        if (skipEntries > 0 && line < skipEntries) {
          // SKIP IT
          ++skipped;
          continue;
        }

        try {
          // THE SAME ARITY GATE THE ANALYSIS PASS APPLIES, INSIDE THE PER-ROW try SO -onRowError GOVERNS IT HERE TOO
          checkRowIsNotLongerThanHeader(line, row.length, headerColumns);
          reportShortRow(line, row.length, headerColumns, context);

          final MutableDocument document = database.newDocument(settings.documentTypeName);

          for (final AnalyzedProperty prop : properties) {
            // A ROW SHORTER THAN THE HEADER DOES NOT SET THE TRAILING PROPERTIES - IT USED TO THROW
            // ArrayIndexOutOfBoundsException HERE INSTEAD (ISSUE #7782)
            if (prop.getIndex() >= row.length)
              continue;

            final String value = row[prop.getIndex()];
            if (value != null && !value.isEmpty())
              document.set(prop.getName(), value);
          }

          document.save();

          if (skipOnError) {
            // Count only after commit() succeeds: a duplicate-key violation is only detected at commit time, so
            // incrementing right after save() would overcount a row that commit() then rolls back.
            database.commit();
            context.createdDocuments.incrementAndGet();
            database.begin();
          } else
            ++documentsCreatedThisFile;
        } catch (final RuntimeException e) {
          rollbackIfOwned(database, ownsTransaction);

          if (!skipOnError)
            throw e;

          logSkippedRow("document", line, e);
          context.errors.incrementAndGet();
          database.begin();
        }

        // SAME CAP AND SAME '>=' AS XMLImporterFormat.load() (ISSUE #7341): context.parsed IS INCREMENTED ONCE PER
        // ROW, SO STOPPING ONCE IT REACHES THE LIMIT IMPORTS EXACTLY -parsingLimitEntries ROWS, NOT ONE MORE (#7482).
        // KEPT AS A POST-PROCESSING CHECK, UNLIKE parsingLimitBytes ABOVE: THE ROW THAT TRIPS THIS CAP IS MEANT TO
        // STILL LAND, THE SAME WAY XML's DOES.
        if (settings.parsingLimitEntries > 0 && context.parsed.get() >= settings.parsingLimitEntries)
          break;
      }

      // Same ownsTransaction gate as the rollback paths below: don't commit the caller's unrelated pending work as
      // a side effect of this import succeeding.
      if (ownsTransaction)
        database.commit();

      context.createdDocuments.addAndGet(documentsCreatedThisFile);

    } catch (final IOException e) {
      rollbackIfOwned(database, ownsTransaction);
      throw new ImportException("Error on importing CSV", e);
    } catch (final RuntimeException e) {
      // A source-level failure (parseNext()) escaped the loop and never went through the per-row catch above.
      rollbackIfOwned(database, ownsTransaction);
      throw e;
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      LogManager.instance()
          .log(this, Level.INFO, "Importing of documents from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdDocuments.get() / elapsedInSecs : context.createdDocuments.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total documents: %d", null, context.createdDocuments.get());
      LogManager.instance().log(this, Level.INFO, "- Failed rows....: %d", null, context.errors.get() - errorsBefore);
      reportSkippedEntries(entityType, context, skipped);

      stopParsingQuietly(csvParser);
    }
  }

  /**
   * {@code AbstractParser#stopParsing()} can itself throw (e.g. "Stream closed" if the underlying reader was already
   * closed by the time this runs). Cleanup in a {@code finally} block must never mask the exception already
   * propagating, so this is swallowed (logged at FINE) rather than let to escape.
   */
  private void stopParsingQuietly(final AbstractParser<?> parser) {
    try {
      parser.stopParsing();
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.FINE, "Error stopping the CSV/TSV parser during cleanup", e);
    }
  }

  /**
   * Refuses a row that carries MORE columns than the header declares, which is a row error like any other and
   * therefore obeys {@code -onRowError}.
   * <p>
   * It did not. The analysis pass indexed {@code fieldNames} by the row's own length and threw
   * {@code IndexOutOfBoundsException} out of {@code fieldNames.get(i)}, from a loop with no per-row handling at all -
   * {@code -onRowError skip} is implemented in the LOAD pass and never reached this one, so the documented way to
   * tolerate a bad row could not help and the whole import aborted on the first oversized row whatever the policy
   * said (issue #7782). Raised from one place used by both passes, so the two cannot disagree about the same row.
   * <p>
   * The extra values are genuinely unimportable - there is no field name to store them under - so the choice is
   * between refusing the row and dropping data silently, and the message has to carry what the raw
   * {@code IndexOutOfBoundsException} did not: the line, and both column counts.
   */
  /**
   * Whether {@code columns} exceeds a known header width. The single expression behind both
   * {@link #checkRowIsNotLongerThanHeader} (which throws) and {@code loadEdges()} (which skips and counts, because
   * edge rows do not honour {@code -onRowError}), so the two outcomes cannot end up disagreeing about which rows
   * they apply to - which is the same guarantee, one level down, that sharing the check between the analysis and
   * the load passes buys (issue #7782).
   */
  private static boolean isLongerThanHeader(final int columns, final int headerColumns) {
    return headerColumns > 0 && columns > headerColumns;
  }

  private static void checkRowIsNotLongerThanHeader(final long line, final int columns, final int headerColumns) {
    if (isLongerThanHeader(columns, headerColumns))
      throw new ImportException(
          "Row at line " + line + " has " + columns + " column(s) while the header has " + headerColumns
              + ": the extra value(s) have no field name to be stored under (use -onRowError skip to skip such rows"
              + " and continue)");
  }

  /**
   * Records a row that carries FEWER columns than the header declares. Not an error: the missing trailing columns are
   * absent values, and a property past the row's end is simply not set - which is what the callers of this method do,
   * instead of letting {@code row[prop.getIndex()]} throw {@code ArrayIndexOutOfBoundsException} the way they used to
   * (issue #7782).
   * <p>
   * One caller does more than that, which is why the message this logs names only the arity: when the column a vertex
   * row is missing is the {@code typeIdProperty} itself, {@code loadVertices()}' own guard drops the whole row a few
   * lines further down and says so separately. This still counts it - the row IS ragged, and leaving it out of the
   * total was the gap that guard's {@code continue} used to open - but it must not claim the row was imported.
   * <p>
   * Deliberately NOT symmetric with {@link #checkRowIsNotLongerThanHeader}: a short row was already importable
   * whenever the analysis had not created a property for the trailing column (a header column no row fills creates
   * none), so refusing it would break imports that work today, while tolerating one can only turn an abort into a
   * completed import. It is still counted and named, so "my file has a ragged row" reaches the operator in the
   * import result ({@code warnings}) rather than only in a log nobody reads.
   */
  private void reportShortRow(final long line, final int columns, final int headerColumns, final ImporterContext context) {
    if (headerColumns <= 0 || columns >= headerColumns)
      return;

    context.warnings.incrementAndGet();

    // WARNING for the first one only: a systematically ragged file would otherwise log a line per row. Throttled on
    // a counter of this FORMAT INSTANCE, which SourceDiscovery creates one of per source, rather than on
    // context.warnings - that one is import-wide, so an import loading a vertices file and then an edges file would
    // have logged one visible WARNING for the whole run and left the second source's first short row at FINE. Same
    // scope as the analysis-side throttle, which is a local in analyze() (issue #7782).
    // The message states the ARITY and stops there, because what happens to the row differs by call site and this
    // method is called from all three: documents and edges import it from the columns it does supply, and so does a
    // vertex row - unless the column it is missing is the typeIdProperty, in which case loadVertices()' own guard
    // drops the row entirely a few lines further down and logs that separately. Saying "the missing trailing
    // column(s) are left unset" here asserted the first outcome for every row, including the ones nothing was left
    // unset ON because no record was created at all (issue #7782).
    LogManager.instance().log(this, shortRowsReported++ == 0 ? Level.WARNING : Level.FINE,
        "Row at line %d has %d column(s) while the header has %d", null, line, columns, headerColumns);
  }

  /**
   * Refuses a source the analysis could derive nothing from, before anything downstream blames the wrong thing.
   * <p>
   * An entity exists only once the analysis has read a DATA row ({@code getOrCreateEntity} is called from that branch
   * alone), and every accepted row contributes at least one property - so an entity with no property at all means
   * every row it saw was refused for its shape. That became possible only with the ragged-row gate (issue #7782);
   * before it, such a row aborted the analysis outright.
   * <p>
   * Worth its own message because each of the three load paths misreports it otherwise, and all three point away from
   * the source: {@code loadEdges()} throws "Specify -edgeFromField &lt;from-field-name&gt;" at an operator who
   * specified it correctly, {@code loadVertices()} throws "Property Id 'T.p' is null" about a property the header
   * does declare, and {@code loadDocuments()} says nothing at all and writes one empty document per row. Misdirection
   * of exactly the kind #7782, #7781 and #7771 are all about.
   */
  private static void checkAnalysisFoundUsableRows(final AnalyzedEntity entity, final AnalyzedEntity.EntityType entityType,
      final ImporterSettings settings) {
    if (entity == null || !entity.getProperties().isEmpty())
      return;

    throw new ImportException("No usable row found in the " + entityType.name().toLowerCase(Locale.ENGLISH)
        + " source: every row the analysis read was refused because its column count did not match the header's (see the"
        + " WARNING lines above), so no property could be derived from it" + analysisWindowHint(settings));
  }

  /**
   * The part of the refusal above that names {@code -analysisLimitEntries}/{@code -analysisLimitBytes}, when one of
   * them is set.
   * <p>
   * Those two bound the ANALYSIS and not the load, so "every row the analysis read" can mean "every row in the
   * sampled head of the file" while the rest of it is perfectly well formed - and then the refusal is right about
   * what it saw and useless about what to do next, because the operator's fix is to widen the window rather than to
   * go looking for ragged rows that may not be there. Only appended when a limit is actually configured: with no
   * limit the analysis read the whole source and the window is not the story.
   */
  private static String analysisWindowHint(final ImporterSettings settings) {
    if (settings == null || (settings.analysisLimitEntries <= 0 && settings.analysisLimitBytes <= 0))
      return "";

    return ". Note the analysis only sampled the head of the source ("
        + (settings.analysisLimitEntries > 0 ? "-analysisLimitEntries " + settings.analysisLimitEntries : "")
        + (settings.analysisLimitEntries > 0 && settings.analysisLimitBytes > 0 ? ", " : "")
        + (settings.analysisLimitBytes > 0 ? "-analysisLimitBytes " + settings.analysisLimitBytes : "")
        + "), so raising that limit may be the fix if the rest of the source is well formed";
  }

  /** How many columns the analysis measured this source's rows against, or -1 when it recorded no header. */
  private static int headerColumnsOf(final AnalyzedEntity entity) {
    return entity != null ? entity.getHeaderColumns() : -1;
  }

  /**
   * Logs a skipped row at WARNING (message only) and FINE (full stack trace), used by both {@code loadDocuments} and
   * {@code loadVertices} when {@code -onRowError skip} discards a row.
   */
  private void logSkippedRow(final String what, final long line, final RuntimeException e) {
    LogManager.instance()
        .log(this, Level.WARNING, "Error on importing %s at line %d, skipping it (reason: %s)", null, what, line, e.getMessage());
    LogManager.instance().log(this, Level.FINE, "Full error on importing %s at line %d", e, what, line);
  }

  /**
   * A vertex can fail asynchronously (on the {@code database.async()} worker thread) around the same time as a
   * synchronous or source-level failure on the calling thread. Attaches the async failure to {@code target} as a
   * suppressed exception so it isn't lost, unless {@code target} already carries it as its own cause (which would
   * otherwise duplicate the same throwable as both {@code Caused by} and {@code Suppressed}).
   */
  private void attachConcurrentAsyncError(final Throwable target, final AtomicReference<Throwable> firstAsyncError) {
    final Throwable asyncError = firstAsyncError.get();
    // Throwable doesn't override equals(), so this is really an identity check (== would flag under ErrorProne's
    // Throwable-reference-equality lint) - not a "same message" comparison.
    if (asyncError != null && !asyncError.equals(target.getCause()) && !asyncError.equals(target))
      target.addSuppressed(asyncError);
  }

  /**
   * Only roll back if we own this transaction - an externally-managed database's pre-existing transaction is left
   * for the caller to reconcile instead of discarding their unrelated pending work.
   */
  private void rollbackIfOwned(final Database database, final boolean ownsTransaction) {
    if (ownsTransaction && database.isTransactionActive())
      database.rollback();
  }

  /**
   * Begins the transaction the per-row loop below will commit/roll back, reusing one that's already active - except
   * when this method owns the transaction (see {@link ImporterContext#callerTransactionActiveOnEntry}) and one is
   * nonetheless already active, in which case it's committed first, then a fresh one begun. That's the routine path
   * for a self-managed database ({@code AbstractImporter#openDatabase()} always leaves its own ambient transaction
   * active before this method runs) and also covers the rarer case of {@code updateDatabaseSchema()}'s lazy type
   * creation leaving a transaction open on an externally-managed database: committing it here first means a row-1
   * failure in "skip" mode can't undo the type/property it just created
   * (see {@code Issue5968ImporterSkipOnRowErrorTest#csvVertexImportSkipModeSurvivesFirstRowFailureWhenSchemaAutoCreatedViaEmbeddingConstructor}).
   */
  private void beginRowTransaction(final Database database, final boolean transactionActiveOnEntry, final boolean ownsTransaction) {
    if (transactionActiveOnEntry) {
      if (ownsTransaction) {
        LogManager.instance()
            .log(this, Level.FINE, "Committing a transaction already active on entry before starting the per-row loop");
        database.commit();
        database.begin();
      }
    } else
      database.begin();
  }

  /**
   * {@code transactionActiveOnEntry}: the live transaction state, used by {@link #beginRowTransaction} to decide
   * whether to begin, reuse, or replace it. {@code ownsTransaction}: see
   * {@link ImporterContext#importOwnsTransaction}.
   */
  private record TransactionOwnership(boolean transactionActiveOnEntry, boolean ownsTransaction) {
  }

  private TransactionOwnership computeTransactionOwnership(final Database database, final ImporterContext context) {
    // importOwnsTransaction() rather than the raw flag: a caller transaction recorded on entry that is no longer
    // live leaves nothing for beginRowTransaction() to reuse, so the transaction it pushes instead is the import's
    // own and has to be gated as such, or it stays on the stack with nothing allowed to resolve it (issue #7328).
    return new TransactionOwnership(database.isTransactionActive(), context.importOwnsTransaction(database));
  }

  private void loadVertices(final SourceSchema sourceSchema, final Parser parser, final Database database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {

    // Checked first, before any schema side effect below (typeIdProperty/unique index auto-creation) - see
    // loadDocuments() for why "skip" mode must own the transaction outright.
    if (settings.isSkipOnRowError() && context.callerTransactionActiveOnEntry)
      throw ImporterSettings.newExclusiveTransactionRequiredException();

    final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.vertexTypeName);
    if (entity == null) {
      LogManager.instance().log(this, Level.INFO, "Vertex type '%s' not defined", null, settings.vertexTypeName);
      return;
    }

    checkAnalysisFoundUsableRows(entity, AnalyzedEntity.EntityType.VERTEX, settings);

    int idIndex = -1;
    if (settings.typeIdProperty != null) {
      final AnalyzedProperty id = entity.getProperty(settings.typeIdProperty);

      if (id == null) {
        LogManager.instance()
            .log(this, Level.INFO, "Property Id '%s.%s' is null. Importing is aborted", null, settings.vertexTypeName,
                settings.typeIdProperty);
        throw new IllegalArgumentException(
            "Property Id '" + settings.vertexTypeName + "." + settings.typeIdProperty + "' is null. Importing is aborted");
      }

      idIndex = id.getIndex();

      // Ensure the typeIdProperty has a unique index for edge resolution
      if (!database.getSchema().getType(settings.vertexTypeName).existsProperty(settings.typeIdProperty))
        database.transaction(
            () -> database.getSchema().getType(settings.vertexTypeName).createProperty(settings.typeIdProperty, Type.STRING));
      if (database.getSchema().getType(settings.vertexTypeName).getIndexesByProperties(settings.typeIdProperty).isEmpty())
        database.transaction(
            () -> database.getSchema().getType(settings.vertexTypeName).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, settings.typeIdProperty));
    }

    final AbstractParser<?> csvParser = createCSVParser(settings);

    LogManager.instance().log(this, Level.INFO, "Started importing vertices from CSV source");

    final long beginTime = System.currentTimeMillis();
    final long errorsBefore = context.errors.get();

    // "skip" mode saves vertices synchronously instead of via database.async() - see
    // ImporterSettings#isSkipOnRowError() for why (an async batch rollback on a persist-time failure would take down
    // every other vertex queued in the same uncommitted batch, not just the failing one).
    final boolean skipOnError = settings.isSkipOnRowError();

    // -commitEvery/-parallel only affect the database.async() path, which "skip" mode doesn't use for vertices at
    // all - silently ignoring an explicitly-set value would otherwise cost someone a confusing perf-debugging
    // session, so call it out once if either was set.
    if (skipOnError && (settings.options.containsKey("commitEvery") || settings.options.containsKey("parallel")))
      LogManager.instance().log(this, Level.INFO,
          "-onRowError skip saves vertices synchronously, one at a time: -commitEvery/-parallel have no effect while it's enabled");

    final AtomicReference<Throwable> firstAsyncError = new AtomicReference<>();
    // database.async().onError() replaces the previous handler rather than stacking, with no getter to save/restore
    // whatever was registered before this call - handlerActive makes this handler inert once loadVertices() returns
    // (finally block below), so an externally-managed Database's later, unrelated async() failures don't get routed
    // into this call's stale context/firstAsyncError.
    final AtomicBoolean handlerActive = new AtomicBoolean(true);
    if (!skipOnError)
      database.async().onError(exception -> {
        if (!handlerActive.get())
          return;
        LogManager.instance().log(this, Level.SEVERE, "Error on inserting vertices", exception);
        context.errors.incrementAndGet();
        firstAsyncError.compareAndSet(null, exception);
      });

    final long skipEntries = skipEntries(AnalyzedEntity.EntityType.VERTEX, settings);
    long skipped = 0;

    final TransactionOwnership ownership = computeTransactionOwnership(database, context);
    final boolean transactionActiveOnEntry = ownership.transactionActiveOnEntry();
    final boolean ownsTransaction = ownership.ownsTransaction();

    try (final Reader inputFileReader = sourceReader(parser)) {
      csvParser.beginParsing(inputFileReader);

      // Unlike loadDocuments(), gated on skipOnError: in "abort" mode vertices persist via database.async() instead
      // of this foreground transaction, so there is no per-row commit/rollback cycle here to protect.
      if (skipOnError)
        beginRowTransaction(database, transactionActiveOnEntry, ownsTransaction);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.vertexPropertiesInclude.isEmpty() && !"*".equalsIgnoreCase(settings.vertexPropertiesInclude)) {
        final String[] includes = settings.vertexPropertiesInclude.split(",");
        final Set<String> propertiesSet = new HashSet<>(Arrays.asList(includes));
        for (final AnalyzedProperty p : entity.getProperties())
          if (propertiesSet.contains(p.getName()))
            properties.add(p);
      } else {
        properties.addAll(entity.getProperties());
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following vertex properties: %s", null, properties);

      final int headerColumns = headerColumnsOf(entity);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        // SAME REASONING AS loadDocuments() ABOVE: CHECKED BEFORE EITHER 'continue' BELOW, SO A LONG RUN OF SKIPPED
        // OR ID-LESS ROWS CANNOT KEEP READING PAST THE BYTE BUDGET.
        if (settings.parsingLimitBytes > 0 && parser.getPosition() > settings.parsingLimitBytes)
          break;

        if (skipEntries > 0 && line < skipEntries) {
          ++skipped;
          continue;
        }

        // BEFORE THE MISSING-ID GUARD BELOW, NOT INSIDE THE try: WHEN typeIdProperty IS A TRAILING HEADER COLUMN A
        // SHORT ROW TAKES THAT GUARD'S 'continue' AND WOULD NEVER REACH THE REPORT, SO THE ONE RAGGED-ROW NUMBER THE
        // OPERATOR IS SHOWN WOULD MISS EXACTLY THE ROWS THE GUARD SKIPPED (issue #7782)
        reportShortRow(line, row.length, headerColumns, context);

        if (idIndex >= 0 && idIndex >= row.length) {
          LogManager.instance()
              .log(this, Level.INFO, "Property Id is configured on property %d but cannot be found on current record. Skip it",
                  null, idIndex);
          continue;
        }

        try {
          // SEE loadDocuments(): ONE ARITY GATE, APPLIED INSIDE THE PER-ROW try SO -onRowError GOVERNS IT. THE SHORT
          // DIRECTION IS REPORTED ABOVE INSTEAD - IT IS NOT A ROW ERROR AND HAS A GUARD OF ITS OWN TO GET PAST.
          checkRowIsNotLongerThanHeader(line, row.length, headerColumns);

          final MutableVertex v = database.newVertex(settings.vertexTypeName);
          if (idIndex >= 0)
            v.set(settings.typeIdProperty, row[idIndex]);
          for (int p = 0; p < properties.size(); ++p) {
            final AnalyzedProperty prop = properties.get(p);
            if (prop.getIndex() >= row.length)
              continue;

            final String value = row[prop.getIndex()];
            if (value != null && !value.isEmpty())
              v.set(prop.getName(), value);
          }

          if (skipOnError) {
            // Each vertex commits in its own transaction; count only after commit() succeeds (see loadDocuments()).
            v.save();
            database.commit();
            context.createdVertices.incrementAndGet();
            database.begin();
          } else
            database.async().createRecord(v, doc -> context.createdVertices.incrementAndGet());
        } catch (final RuntimeException e) {
          rollbackIfOwned(database, ownsTransaction);

          if (!skipOnError)
            throw e;

          logSkippedRow("vertex", line, e);
          context.errors.incrementAndGet();
          database.begin();
        }

        // SAME CAP AND SAME '>=' AS XMLImporterFormat.load() (ISSUE #7341): context.parsed IS INCREMENTED ONCE PER
        // ROW, SO STOPPING ONCE IT REACHES THE LIMIT IMPORTS EXACTLY -parsingLimitEntries ROWS, NOT ONE MORE (#7482).
        // KEPT AS A POST-PROCESSING CHECK, UNLIKE parsingLimitBytes ABOVE: THE ROW THAT TRIPS THIS CAP IS MEANT TO
        // STILL LAND, THE SAME WAY XML's DOES.
        if (settings.parsingLimitEntries > 0 && context.parsed.get() >= settings.parsingLimitEntries)
          break;
      }

      if (skipOnError) {
        if (ownsTransaction)
          database.commit();
      } else {
        database.async().waitCompletion();

        // A vertex can also fail at persist time on the async worker thread (mandatory property, unique index, ...),
        // outside the per-row try/catch above: in "abort" mode that must still fail the import.
        if (firstAsyncError.get() != null)
          throw new ImportException("Error on inserting vertices", firstAsyncError.get());
      }

    } catch (final IOException e) {
      // In "abort" mode, drain any vertices from earlier rows already queued via database.async() before this
      // failure propagates.
      if (!skipOnError)
        database.async().waitCompletion();
      rollbackIfOwned(database, ownsTransaction);
      final ImportException importException = new ImportException("Error on importing CSV", e);
      if (!skipOnError)
        attachConcurrentAsyncError(importException, firstAsyncError);
      throw importException;
    } catch (final RuntimeException e) {
      // A synchronous per-row failure rethrows straight out of the loop, skipping the waitCompletion() check that
      // normally runs after it - drain here first so earlier rows' async writes aren't left in flight uncounted.
      if (!skipOnError) {
        database.async().waitCompletion();
        attachConcurrentAsyncError(e, firstAsyncError);
      }
      rollbackIfOwned(database, ownsTransaction);
      throw e;
    } finally {
      // Every path above that can reach here already called database.async().waitCompletion(), so this import's own
      // vertices have all already been through the handler (or never will be) by this point - deactivating it now
      // can't miss one of this call's own errors, only stop it from reacting to the caller's later, unrelated work.
      handlerActive.set(false);

      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      LogManager.instance()
          .log(this, Level.INFO, "Importing of vertices from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdVertices.get() / elapsedInSecs : context.createdVertices.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total vertices.: %d", null, context.createdVertices.get());
      LogManager.instance().log(this, Level.INFO, "- Failed rows....: %d", null, context.errors.get() - errorsBefore);
      reportSkippedEntries(AnalyzedEntity.EntityType.VERTEX, context, skipped);

      stopParsingQuietly(csvParser);
    }
  }

  private void loadEdges(final SourceSchema sourceSchema, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
    final AbstractParser csvParser = createCSVParser(settings);

    final long beginTime = System.currentTimeMillis();

    final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.edgeTypeName);
    if (entity == null) {
      LogManager.instance().log(this, Level.INFO, "Edge type '%s' not defined", null, settings.edgeTypeName);
      return;
    }

    checkAnalysisFoundUsableRows(entity, AnalyzedEntity.EntityType.EDGE, settings);

    final AnalyzedProperty from = entity.getProperty(settings.edgeFromField);
    if (from == null)
      throw new IllegalArgumentException("Specify -edgeFromField <from-field-name>");

    final AnalyzedProperty to = entity.getProperty(settings.edgeToField);
    if (to == null)
      throw new IllegalArgumentException("Specify -edgeToField <from-field-name>");

    long expectedEdges = settings.expectedEdges;
    // GATED ON A MEASURED AVERAGE, NOT JUST ON A MISSING -expectedEdges: getAverageRowLength() ANSWERS 0 WHEN THE
    // ANALYSIS MEASURED NO ROW, WHICH A FILE WHOSE ROWS WERE ALL REFUSED FOR THEIR SHAPE PRODUCES (ISSUE #7782), AND
    // THIS IS INTEGER DIVISION - totalSize IS A long, SO A ZERO DIVISOR THROWS ArithmeticException RATHER THAN
    // YIELDING AN INFINITY THE GUARD BELOW COULD CATCH. LEAVING expectedEdges AT 0 HANDS THE ANSWER TO THAT SAME
    // GUARD, WHICH IS ALREADY THE "NO IDEA HOW BIG THIS SOURCE IS" BRANCH (ISSUE #7782).
    final int averageRowLength = entity.getAverageRowLength();
    if (expectedEdges <= 0 && averageRowLength > 0)
      expectedEdges = (int) (sourceSchema.getSource().totalSize / averageRowLength);

    if (expectedEdges <= 0 || expectedEdges > _32MB)
      // USE CHUNKS OF 16MB EACH
      expectedEdges = _32MB;

    long expectedVertices = settings.expectedVertices;
    if (expectedVertices <= 0)
      expectedVertices = expectedEdges / 2;

    LogManager.instance()
        .log(this, Level.INFO, "Started importing edges from CSV source (expectedVertices=%d expectedEdges=%d)", null,
            expectedVertices, expectedEdges);

    // Edges already skip-and-log unconditionally regardless of -onRowError (see below), so this is a no-op - worth a
    // one-time notice, mirroring JSONImporterFormat.load()'s equivalent notice for a single top-level JSON object.
    if (settings.isSkipOnRowError())
      LogManager.instance().log(this, Level.INFO,
          "-onRowError skip has no additional effect on edges: an unresolved from/to reference is already skipped and logged unconditionally");

    database.async().onError(exception -> LogManager.instance().log(this, Level.SEVERE, "Error on inserting edges", exception));

    final long skipEntries = skipEntries(AnalyzedEntity.EntityType.EDGE, settings);
    long skipped = 0;

    try (final Reader inputFileReader = sourceReader(parser)) {
      csvParser.beginParsing(inputFileReader);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.edgePropertiesInclude.isEmpty() && !"*".equalsIgnoreCase(settings.edgePropertiesInclude)) {
        final String[] includes = settings.edgePropertiesInclude.split(",");

        final Set<String> propertiesSet = new HashSet<>(Arrays.asList(includes));

        for (final AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else {
        // INCLUDE ALL THE PROPERTIES
        properties.addAll(entity.getProperties());
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following edge properties: %s", null, properties);

      final int headerColumns = headerColumnsOf(entity);

      String[] row;
      // No ownsTransaction/callerTransactionActiveOnEntry guard needed here, unlike loadDocuments()/loadVertices():
      // database.begin() nests rather than reusing an already-active transaction (see LocalDatabase#begin()), so a
      // caller's own pre-existing transaction is never touched by this method's own commits below.
      database.begin();
      // Whether the transaction just opened (or the one begun after a periodic commit below) is still the current
      // one. Cleared right before every commit - which pops it in a finally even if it throws - so a rollback below
      // can never pop a transaction this method has already committed away, let alone the caller's own (issue #7272).
      boolean txOpen = true;
      // Edges an intermediate commit already made durable. context.createdEdges counts every edge
      // createEdgeFromRow() creates, the ones still inside the transaction a failure rolls back included.
      long committedEdges = context.createdEdges.get();
      // Whether the loop ran to its own trailing commit. Distinct from txOpen: a commit() that throws pops the
      // transaction in its own finally, so txOpen is already false there, yet the batch it failed to make
      // durable still has to come back off the counter.
      boolean completed = false;
      int txCount = 0;
      try {
        for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
          context.parsed.incrementAndGet();

          // SAME REASONING AS loadDocuments()/loadVertices() ABOVE: CHECKED BEFORE THE SKIP-ROW 'continue', SO A
          // LONG RUN OF SKIPPED ROWS CANNOT KEEP READING PAST THE BYTE BUDGET.
          if (settings.parsingLimitBytes > 0 && parser.getPosition() > settings.parsingLimitBytes)
            break;

          if (skipEntries > 0 && line < skipEntries) {
            ++skipped;
            continue;
          }

          // THE SAME ARITY GATE loadDocuments()/loadVertices() APPLY, AND FOR THE SAME REASON: THE ANALYSIS REFUSES
          // AN OVERSIZED ROW, SO MAKING AN EDGE OUT OF ITS FIRST COLUMNS HERE WOULD BE THE TWO PASSES DISAGREEING
          // ABOUT ONE ROW - THE DIVERGENCE #7487 EXISTS TO PREVENT. REACHABLE WHENEVER THE ROW IS PAST THE
          // ANALYSIS WINDOW (-analysisLimitEntries / -analysisLimitBytes BOUND THE ANALYSIS, NOT THE LOAD) AND
          // UNDER -onRowError skip, WHERE THE ANALYSIS SKIPPED IT RATHER THAN THROWING.
          //
          // SKIPPED AND COUNTED RATHER THAN THROWN, WHICHEVER THE POLICY: EDGE ROWS DO NOT HONOUR -onRowError AT
          // ALL (SEE THE catch BELOW AND THE ONE-TIME NOTICE ABOVE), AND MAKING RAGGEDNESS THE ONE EXCEPTION WOULD
          // CONTRADICT THE NOTICE THIS METHOD PRINTS. COUNTED IN errors AND NOT IN skippedEdges, WHICH MEANS
          // "from/to DID NOT RESOLVE" AND IS REPORTED UNDER THAT NAME (#7488).
          //
          // AN else RATHER THAN A continue, SO THIS ROW STILL REACHES THE -parsingLimitEntries CHECK AT THE BOTTOM OF
          // THE LOOP. A continue HERE WOULD LET A RUN OF OVERSIZED ROWS PARSE PAST THAT CAP INDEFINITELY, AND IT
          // WOULD ALSO PUT EDGES OUT OF STEP WITH loadDocuments()/loadVertices(), WHERE A ROW THAT FAILED FALLS
          // THROUGH TO THE SAME CHECK RATHER THAN JUMPING OVER IT (issue #7782).
          if (isLongerThanHeader(row.length, headerColumns)) {
            LogManager.instance().log(this, Level.WARNING,
                "Error on importing edge at line %d, skipping it (reason: it has %d column(s) while the header has %d)", null,
                line, row.length, headerColumns);
            context.errors.incrementAndGet();
          } else {
            // AND THE SHORT DIRECTION, COUNTED THE SAME WAY loadDocuments()/loadVertices() COUNT IT:
            // createEdgeFromRow() BELOW IMPORTS SUCH A ROW FROM THE COLUMNS IT DOES SUPPLY, SO WITHOUT THIS THE ONE
            // RAGGED-ROW NUMBER THE OPERATOR IS SHOWN WOULD COUNT DOCUMENTS AND VERTICES BUT NOT EDGES (ISSUE #7782).
            reportShortRow(line, row.length, headerColumns, context);

            try {
              createEdgeFromRow(database, row, properties, from, to, context, settings);
              txCount++;
            } catch (final Exception e) {
              // Unlike loadDocuments/loadVertices, edge rows are always skipped-and-logged regardless of -onRowError:
              // a "bad" edge row here is typically just an unresolved from/to vertex reference, expected during graph
              // imports rather than a data-corruption case.
              LogManager.instance().log(this, Level.SEVERE, "Error on parsing line %d", e, line);
            }

            // INSIDE THE else, SO A REFUSED ROW DOES NOT REACH IT. NO BEHAVIOUR CHANGE - txCount IS ONLY INCREMENTED
            // BY AN ATTEMPTED ROW, SO A REFUSED ONE COULD NEVER HAVE TRIPPED THE CADENCE ANYWAY - BUT IT SAVES THE
            // NEXT READER DERIVING THAT: THE COMMIT CADENCE COUNTS ATTEMPTED ROWS, NOT PARSED ONES.
            //
            // Deliberately outside the per-row catch above: a commit failure is not a row error. Caught there it
            // would be logged under a "parsing line N" message, and the loop would carry on with no transaction
            // active - LocalDatabase#commit() pops in its own finally and the begin() below never runs - turning
            // one failure into one more for every remaining row. Left to escape, it reaches the finally below,
            // which corrects the counter and lets the real cause propagate.
            if (txCount >= settings.commitEvery) {
              txOpen = false;
              database.commit();
              committedEdges = context.createdEdges.get();
              database.begin();
              txOpen = true;
              txCount = 0;
            }
          }

          // SAME CAP AND SAME '>=' AS XMLImporterFormat.load() (ISSUE #7341): context.parsed IS INCREMENTED ONCE PER
          // ROW, SO STOPPING ONCE IT REACHES THE LIMIT IMPORTS EXACTLY -parsingLimitEntries ROWS, NOT ONE MORE (#7482).
          // KEPT AS A POST-PROCESSING CHECK, UNLIKE parsingLimitBytes ABOVE: THE ROW THAT TRIPS THIS CAP IS MEANT TO
          // STILL LAND, THE SAME WAY XML's DOES.
          if (settings.parsingLimitEntries > 0 && context.parsed.get() >= settings.parsingLimitEntries)
            break;
        }
        txOpen = false;
        database.commit();
        completed = true;
      } finally {
        // A row-content failure is already caught and logged above without escaping; what reaches here is a
        // source-level failure - typically csvParser.parseNext() itself throwing on a malformed row - that the loop
        // never had a chance to catch.
        if (txOpen && database.isTransactionActive()) {
          try {
            database.rollback();
          } catch (final Exception rollbackFailure) {
            // Swallowed: a throw here would replace the failure the caller can actually act on with one about the
            // cleanup, and would skip the counter correction below.
            LogManager.instance().log(this, Level.SEVERE,
                "Could not roll back after the edge import failed: the transaction it opened may still be on the stack",
                rollbackFailure);
          }
        }

        // Outside the txOpen branch above, because a commit() that threw has already popped its own transaction
        // and would otherwise leave the batch it failed to write counted as if it had survived.
        if (!completed) {
          // What the report calls "created" has to be what survived: leaving the counter at the number of edges
          // read would credit the import with the ones the rollback just took away.
          final long readEdges = context.createdEdges.get();
          context.createdEdges.set(committedEdges);

          if (committedEdges > 0)
            LogManager.instance().log(this, Level.WARNING,
                "Edge import failed after %,d edges: the import is PARTIAL - %,d of them an earlier batch commit made "
                    + "durable and they stay on the disk, the other %,d were rolled back", null, readEdges,
                committedEdges, readEdges - committedEdges);
        }
      }

    } catch (final IOException e) {
      throw new ImportException("Error on importing CSV", e);
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      try {
        Thread.sleep(300);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      LogManager.instance()
          .log(this, Level.INFO, "Importing of edges from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdEdges.get() / elapsedInSecs : context.createdEdges.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines......: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total edges.......: %d", null, context.createdEdges.get());
      LogManager.instance().log(this, Level.INFO, "- Total linked Edges: %d", null, context.linkedEdges.get());
      LogManager.instance().log(this, Level.INFO, "- Unresolved edges..: %d", null, context.skippedEdges.get());
      reportSkippedEntries(AnalyzedEntity.EntityType.EDGE, context, skipped);

      stopParsingQuietly(csvParser);
    }
  }

  public void createEdgeFromRow(final Database database, final String[] row, final List<AnalyzedProperty> properties,
      final AnalyzedProperty from, final AnalyzedProperty to, final ImporterContext context, final ImporterSettings settings) {

    if (from.getIndex() >= row.length || to.getIndex() >= row.length) {
      context.skippedEdges.incrementAndGet();
      return;
    }

    final String fromValue = row[from.getIndex()];
    final String toValue = row[to.getIndex()];

    if (fromValue == null || toValue == null) {
      context.skippedEdges.incrementAndGet();
      return;
    }

    // Parse vertex keys based on typeIdType setting (fixes GitHub issue #1552)
    final Object sourceVertexKey = parseVertexKey(fromValue, settings.typeIdType);
    final Object destinationVertexKey = parseVertexKey(toValue, settings.typeIdType);

    final Object[] params;
    if (row.length > 2) {
      // ONLY THE PROPERTIES THIS ROW ACTUALLY SUPPLIES A COLUMN FOR: A ROW SHORTER THAN THE HEADER THREW
      // ArrayIndexOutOfBoundsException OUT OF row[property.getIndex()] BELOW, WHICH THE EDGE LOOP'S skip-and-log
      // CANNOT TURN INTO A COUNTED SKIP BECAUSE IT NEVER SAW IT AS AN UNRESOLVED REFERENCE (ISSUE #7782)
      int supplied = 0;
      for (int i = 0; i < properties.size(); ++i)
        if (properties.get(i).getIndex() < row.length)
          ++supplied;

      params = supplied > 0 ? new Object[supplied * 2] : NO_PARAMS;
      for (int i = 0, p = 0; i < properties.size(); ++i) {
        final AnalyzedProperty property = properties.get(i);
        if (property.getIndex() >= row.length)
          continue;

        params[p * 2] = property.getName();
        params[p * 2 + 1] = row[property.getIndex()];
        ++p;
      }
    } else {
      params = NO_PARAMS;
    }

    // Look up source and destination vertices by key across all vertex types that have the typeIdProperty indexed
    final Vertex srcVertex = findVertexByKey(database, settings.typeIdProperty, sourceVertexKey);
    if (srcVertex == null) {
      context.skippedEdges.incrementAndGet();
      return;
    }
    final Vertex dstVertex = findVertexByKey(database, settings.typeIdProperty, destinationVertexKey);
    if (dstVertex == null) {
      context.skippedEdges.incrementAndGet();
      return;
    }
    srcVertex.newEdge(settings.edgeTypeName, dstVertex.getIdentity(), settings.edgeBidirectional, params);
    context.createdEdges.incrementAndGet();
  }

  /**
   * Parses a vertex key string based on the configured type.
   * This supports any ID type (String, Long, Integer, etc.) based on typeIdType setting.
   * Added to fix GitHub issue #1552.
   */
  /**
   * Searches all vertex types for a vertex matching the given key property value.
   * Needed because edges can connect different vertex types.
   */
  private Vertex findVertexByKey(final Database database, final String keyProperty, final Object keyValue) {
    for (final DocumentType type : database.getSchema().getTypes()) {
      if (!(type instanceof VertexType))
        continue;
      if (!type.existsProperty(keyProperty))
        continue;
      if (type.getIndexesByProperties(keyProperty).isEmpty())
        continue;
      final IndexCursor cursor = lookupRecord(database, type.getName(), keyProperty, keyValue);
      if (cursor.hasNext())
        return cursor.next().asVertex();
    }
    return null;
  }

  private Object parseVertexKey(final String value, final String typeIdType) {
    if (value == null)
      return null;

    return switch (typeIdType.toUpperCase(Locale.ENGLISH)) {
      case "LONG" -> Long.parseLong(value);
      case "INTEGER", "INT" -> Integer.parseInt(value);
      case "SHORT" -> Short.parseShort(value);
      case "DOUBLE" -> Double.parseDouble(value);
      case "FLOAT" -> Float.parseFloat(value);
      default -> value; // String is the default
    };
  }

  /**
   * The source's character stream. The leading comment block ({@code #} and {@code //} lines) is already gone from
   * it: {@link Parser} drops it, once, for every format at once.
   * <p>
   * This used to strip the block here, which fixed the two delimited-text formats and left XML, JSON and JSONL
   * receiving the very comment lines content sniffing had skipped to recognise them - so a {@code #}-commented
   * N-Triples file imported and a {@code #}-commented XML file died on {@code ImportException} (issue #7490).
   */
  protected static Reader sourceReader(final Parser parser) {
    return new InputStreamReader(parser.getInputStream(), DatabaseFactory.getDefaultCharset());
  }

  /**
   * How many leading rows to skip for {@code entityType}, from the one option that governs that route:
   * {@code -verticesSkipEntries}, {@code -edgesSkipEntries} or {@code -documentsSkipEntries}, falling back to
   * {@link #defaultHeaderSkipEntries()}.
   * <p>
   * ONE function, because the answer used to be spelled out at each of the sites that needed it and they did not
   * all spell it the same way. {@code RDFImporterFormat.load()} read {@code -edgesSkipEntries} whichever entity the
   * source had arrived as, so on the {@code -vertices} and {@code -documents} routes {@code -verticesSkipEntries}
   * was silently inert while {@code -edgesSkipEntries} - the option a user on that route has no reason to reach
   * for - was the one that worked (issue #7487). {@link #analyze} and the delimited-text row loops disagreed about
   * a {@code -documentsHeader} source: the analysis skipped its first row as a header even though the caller had
   * supplied the header and the load imported that row.
   * <p>
   * {@code DATABASE} - the {@code -url} route with neither {@code -vertexType} nor {@code -edgeType} set - is the
   * documents route, which is where {@link #load} sends it.
   */
  protected long skipEntries(final AnalyzedEntity.EntityType entityType, final ImporterSettings settings) {
    return switch (entityType) {
      case VERTEX -> skipEntries(settings.verticesSkipEntries, settings.verticesHeader);
      case EDGE -> skipEntries(settings.edgesSkipEntries, settings.edgesHeader);
      // DATABASE IS THE DOCUMENTS ROUTE, WHICH IS WHERE load() SENDS IT
      case DOCUMENT, DATABASE -> skipEntries(settings.documentsSkipEntries, settings.documentsHeader);
    };
  }

  /**
   * The rule all three routes read, from that route's own pair of options.
   * <p>
   * The explicit option wins whenever it is set, including when it is set to something a supplied header would
   * otherwise have overridden - a caller who writes {@code -verticesSkipEntries 1} alongside {@code -verticesHeader}
   * has asked for a row to go and gets it.
   * <p>
   * Otherwise a SUPPLIED HEADER MEANS THE FILE HAS NO HEADER LINE, so there is nothing to skip. Only
   * {@code -documentsHeader} used to say that: {@code -vertices file.csv -verticesHeader id,name} skipped the file's
   * first row as a header even though the caller had just supplied one, and the first DATA row was silently dropped.
   * The workaround was an explicit {@code -verticesSkipEntries 0}, which is also why it stayed invisible - whoever
   * noticed the missing row added the zero and moved on (issue #7499).
   *
   * @param skipEntries the route's {@code -*SkipEntries} option, or null when unset
   * @param header      the route's {@code -*Header} option, or null when unset
   */
  private long skipEntries(final Long skipEntries, final String header) {
    if (skipEntries != null)
      return skipEntries;
    return header == null ? defaultHeaderSkipEntries() : 0L;
  }

  /**
   * The name of the option {@link #skipEntries} read, for the notice that names it. The silent case - rows missing
   * from the report because a setting said to drop them - is the one that costs an afternoon (issue #7488).
   * <p>
   * It fires on the DEFAULT header skip too, which is the common case and therefore most of the lines this adds.
   * That is deliberate: #7345 was a default skip, not an explicit one, and it ate the first triple of every RDF
   * source in silence. The notice joins a per-phase summary block that already logs its parsed, created and failed
   * counts unconditionally, so it is one more line in a block the import was printing anyway - which is why it is
   * kept short.
   */
  protected static String skipEntriesOption(final AnalyzedEntity.EntityType entityType) {
    return switch (entityType) {
      case VERTEX -> "-verticesSkipEntries";
      case EDGE -> "-edgesSkipEntries";
      case DOCUMENT, DATABASE -> "-documentsSkipEntries";
    };
  }

  /**
   * Records the rows a phase skipped on purpose and, when there were any, says so once with the setting responsible.
   * <p>
   * They are counted APART from {@link ImporterContext#errors}, which covers the failure half: a report of
   * {@code parsedRecords=4, createdEdges=3} used to read the same whether the missing row was a header the caller
   * asked to skip, an edge whose endpoints did not resolve or a row {@code -onRowError skip} dropped, and the usual
   * guess - "my file has a bad row" - is wrong for the first (issue #7488).
   *
   * @param skipped how many rows the loop actually skipped, which is the smaller of the setting and the number of
   *                rows the source turned out to have
   */
  protected void reportSkippedEntries(final AnalyzedEntity.EntityType entityType, final ImporterContext context,
      final long skipped) {
    if (skipped <= 0)
      return;

    context.skippedRecords.addAndGet(skipped);
    LogManager.instance().log(this, Level.INFO,
        "- Skipped rows.....: %d (dropped by %s, counted as skippedRecords and not as errors)", null, skipped,
        skipEntriesOption(entityType));
  }

  /**
   * How many leading rows a source of this format is assumed to spend on a header when the caller set no explicit
   * {@code -documentsSkipEntries} / {@code -verticesSkipEntries} / {@code -edgesSkipEntries}.
   * <p>
   * One for delimited text, where a header line is the convention. Overridden to zero by
   * {@link RDFImporterFormat}: N-Triples, N-Quads and Turtle have no header row - every line is a statement - and
   * the format is selected by sniffing the first line AS a statement, so the one line the importer is certain
   * carries data was the one it threw away, silently, on every RDF import (issue #7345).
   * <p>
   * The default only: an explicit {@code -edgesSkipEntries 1} still skips one, for the users who have been passing
   * nothing and relying on the skip.
   */
  protected long defaultHeaderSkipEntries() {
    return 1L;
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.EntityType entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) throws IOException {
    parser.reset();

    final String delimiter = delimiterFor(settings);

    final CsvParserSettings csvParserSettings;
    final TsvParserSettings tsvParserSettings;
    final AbstractParser csvParser;
    final CommonParserSettings parserSettings;

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      parserSettings = tsvParserSettings = new TsvParserSettings();
    } else {
      parserSettings = csvParserSettings = new CsvParserSettings();
      // Detection off for a source with no delimiter at all too, which applyDelimiter() below leaves untouched.
      csvParserSettings.setDelimiterDetectionEnabled(false);
      applyDelimiter(csvParserSettings, delimiter, true);
    }

    parserSettings.setReadInputOnSeparateThread(false);

    final int maxProperties = settings.getIntValue("maxProperties", 0);
    if (maxProperties > 0)
      parserSettings.setMaxColumns(maxProperties);

    final int maxPropertySize = settings.getIntValue("maxPropertySize", 0);
    if (maxPropertySize != 0) {
      parserSettings.setAutoConfigurationEnabled(false);
      parserSettings.setMaxCharsPerColumn(maxPropertySize);
    }

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      csvParser = new TsvParser((TsvParserSettings) parserSettings);
    } else {
      csvParser = new CsvParser((CsvParserSettings) parserSettings);
    }

    final List<String> fieldNames = new ArrayList<>();

    // How many oversized rows this analysis has already skipped, so only the first one logs at WARNING (see below).
    long raggedRowsSkippedInAnalysis = 0;

    final String entityName = entityType == AnalyzedEntity.EntityType.VERTEX ?
        settings.vertexTypeName :
        entityType == AnalyzedEntity.EntityType.EDGE ? settings.edgeTypeName : settings.documentTypeName;

    // ONE FUNCTION WITH THE ROW LOOPS, SO THE ANALYSIS AND THE LOAD CANNOT DISAGREE ABOUT THE SAME FILE (ISSUE #7487)
    final long skipEntries = skipEntries(entityType, settings);
    final String header = switch (entityType) {
      case VERTEX -> settings.verticesHeader;
      case EDGE -> settings.edgesHeader;
      // DATABASE IS THE DOCUMENTS ROUTE, WHICH IS WHERE load() SENDS IT
      case DOCUMENT, DATABASE -> settings.documentsHeader;
    };

    if (header != null) {
      if (delimiter == null)
        fieldNames.add(header);
      else {
        final String[] headerColumns = header.split(Pattern.quote(headerSeparator(delimiter)));
        fieldNames.addAll(Arrays.asList(headerColumns));
      }
      LogManager.instance().log(this, Level.INFO, "Parsing with custom header: %s", null, fieldNames);
    }

    try (final Reader inputFileReader = sourceReader(parser)) {
      csvParser.beginParsing(inputFileReader);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        if (skipEntries > 0 && line < skipEntries && !fieldNames.isEmpty())
          continue;

        if (settings.analysisLimitBytes > 0 && csvParser.getContext().currentChar() > settings.analysisLimitBytes)
          break;

        if (settings.analysisLimitEntries > 0 && line > settings.analysisLimitEntries)
          break;

        if (line == 0 && header == null) {
          // READ THE HEADER FROM FILE
          fieldNames.addAll(Arrays.asList(row));
          LogManager.instance().log(this, Level.INFO, "Reading header from 1st line in data file: %s", null, Arrays.toString(row));
        } else {
          // DATA LINE
          final AnalyzedEntity entity = analyzedSchema.getOrCreateEntity(entityName, entityType);

          // RECORDED BEFORE THE ARITY CHECK BELOW, SO THE LOAD PASS MEASURES ROWS AGAINST THE SAME NUMBER EVEN WHEN
          // EVERY ROW IN THE FILE IS RAGGED AND NONE OF THEM CONTRIBUTES A PROPERTY (ISSUE #7782)
          entity.setHeaderColumns(fieldNames.size());

          if (isLongerThanHeader(row.length, fieldNames.size())) {
            // THROWS UNLESS THE POLICY SAYS TO SKIP - THE SAME REFUSAL, WORDED THE SAME WAY, THAT THE LOAD PASS
            // RAISES FOR THIS ROW
            if (!settings.isSkipOnRowError())
              checkRowIsNotLongerThanHeader(line, row.length, fieldNames.size());

            // WARNING FOR THE FIRST ONE ONLY, THEN FINE - THE SAME THROTTLE reportShortRow() USES, SO A
            // SYSTEMATICALLY RAGGED FILE DOES NOT LOG A LINE PER ROW IN ONE DIRECTION WHILE THE MIRROR CASE IS
            // THROTTLED IN THE OTHER (ISSUE #7782)
            LogManager.instance().log(this, raggedRowsSkippedInAnalysis++ == 0 ? Level.WARNING : Level.FINE,
                "Error on analyzing row at line %d, skipping it (reason: it has %d column(s) while the header has %d)", null,
                line, row.length, fieldNames.size());
            continue;
          }

          entity.setRowSize(row);
          for (int i = 0; i < row.length; ++i) {
            entity.getOrCreateProperty(fieldNames.get(i), row[i]);
          }
        }
      }

    } catch (final EOFException e) {
      // REACHED THE LIMIT
    } catch (final IOException e) {
      throw new ImportException("Error on importing CSV", e);
    }

    // END OF PARSING. THIS DETERMINES THE TYPE
    analyzedSchema.endParsing();

    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public String getFormat() {
    return "CSV";
  }

  protected AbstractParser createCSVParser(final ImporterSettings settings) {
    final String delimiter = delimiterFor(settings);

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      final TsvParserSettings tsvParserSettings = new TsvParserSettings();
      tsvParserSettings.setMaxColumns(settings.getIntValue("maxProperties", tsvParserSettings.getMaxColumns()));
      tsvParserSettings.setMaxCharsPerColumn(settings.getIntValue("maxPropertySize", tsvParserSettings.getMaxCharsPerColumn()));
      return new TsvParser(tsvParserSettings);
    } else {
      final CsvParserSettings csvParserSettings = new CsvParserSettings();
      // The same helper analyze() uses, so the two passes cannot split one file into different columns (#7867).
      // No format auto-detection here: this pass never had it, and turning it on would change which quote character
      // a source already importing today is read with.
      applyDelimiter(csvParserSettings, delimiter, false);
      csvParserSettings.setMaxColumns(settings.getIntValue("maxProperties", csvParserSettings.getMaxColumns()));
      csvParserSettings.setMaxCharsPerColumn(settings.getIntValue("maxPropertySize", csvParserSettings.getMaxCharsPerColumn()));
      return new CsvParser(csvParserSettings);
    }

  }
}
