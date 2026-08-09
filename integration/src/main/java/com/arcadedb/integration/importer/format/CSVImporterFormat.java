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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * On {@code -onRowError skip}, {@code loadDocuments}/{@code loadVertices} reuse whatever transaction is already
 * active rather than nesting a new one - see {@link ImporterSettings#isSkipOnRowError()} for the full
 * transaction-ownership contract this and {@code JSONImporterFormat} both satisfy, by different means.
 */
public class CSVImporterFormat extends AbstractImporterFormat {
  private static final Object[] NO_PARAMS = new Object[] {};
  public static final  int      _32MB     = 32 * 1024 * 1024;

  @Override
  public void load(final SourceSchema sourceSchema,
      final AnalyzedEntity.EntityType entityType, final Parser parser,
      final DatabaseInternal database,
      final ImporterContext context,
      final ImporterSettings settings) throws ImportException {

    context.parsed.set(0);

    switch (entityType) {
    case DOCUMENT, DATABASE -> loadDocuments(sourceSchema, parser, database, context, settings);
    case VERTEX -> loadVertices(sourceSchema, parser, database, context, settings);
    case EDGE -> loadEdges(sourceSchema, parser, database, context, settings);
    }
  }

  private void loadDocuments(final SourceSchema sourceSchema, final Parser parser, final Database database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
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

    long skipEntries = settings.documentsSkipEntries != null ? settings.documentsSkipEntries : 0;
    if (settings.documentsHeader == null && settings.documentsSkipEntries == null)
      // by default skip the first line as header
      skipEntries = 1l;

    // Captured before the try below so both are also visible in the catch blocks.
    final TransactionOwnership ownership = computeTransactionOwnership(database, context);
    final boolean transactionActiveOnEntry = ownership.transactionActiveOnEntry();
    final boolean ownsTransaction = ownership.ownsTransaction();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
      csvParser.beginParsing(inputFileReader);

      // Unlike loadVertices(), called unconditionally regardless of skipOnError: loadDocuments() needs an active
      // transaction to save() into in both modes (it never goes through database.async()).
      beginRowTransaction(database, transactionActiveOnEntry, ownsTransaction);

      final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.documentTypeName);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!"*".equalsIgnoreCase(settings.documentPropertiesInclude)) {
        final String[] includes = settings.documentPropertiesInclude.split(",");

        final Set<String> propertiesSet = new HashSet<>(Arrays.asList(includes));

        for (final AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else if (entity != null) {
        // INCLUDE ALL THE PROPERTIES
        properties.addAll(entity.getProperties());
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following document properties: %s", null, properties);

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

        if (skipEntries > 0 && line < skipEntries)
          // SKIP IT
          continue;

        try {
          final MutableDocument document = database.newDocument(settings.documentTypeName);

          for (final AnalyzedProperty prop : properties) {
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
      LogManager.instance().log(this, Level.INFO, "- Skipped rows...: %d", null, context.errors.get() - errorsBefore);

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
   * {@link ImporterContext#callerTransactionActiveOnEntry}.
   */
  private record TransactionOwnership(boolean transactionActiveOnEntry, boolean ownsTransaction) {
  }

  private TransactionOwnership computeTransactionOwnership(final Database database, final ImporterContext context) {
    return new TransactionOwnership(database.isTransactionActive(), !context.callerTransactionActiveOnEntry);
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

    long skipEntries = settings.verticesSkipEntries != null ? settings.verticesSkipEntries : 0;
    if (settings.verticesSkipEntries == null)
      skipEntries = 1L;

    final TransactionOwnership ownership = computeTransactionOwnership(database, context);
    final boolean transactionActiveOnEntry = ownership.transactionActiveOnEntry();
    final boolean ownsTransaction = ownership.ownsTransaction();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
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

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          continue;

        if (idIndex >= 0 && idIndex >= row.length) {
          LogManager.instance()
              .log(this, Level.INFO, "Property Id is configured on property %d but cannot be found on current record. Skip it",
                  null, idIndex);
          continue;
        }

        try {
          final MutableVertex v = database.newVertex(settings.vertexTypeName);
          if (idIndex >= 0)
            v.set(settings.typeIdProperty, row[idIndex]);
          for (int p = 0; p < properties.size(); ++p) {
            final AnalyzedProperty prop = properties.get(p);
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
      LogManager.instance().log(this, Level.INFO, "- Skipped rows...: %d", null, context.errors.get() - errorsBefore);

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

    final AnalyzedProperty from = entity.getProperty(settings.edgeFromField);
    if (from == null)
      throw new IllegalArgumentException("Specify -edgeFromField <from-field-name>");

    final AnalyzedProperty to = entity.getProperty(settings.edgeToField);
    if (to == null)
      throw new IllegalArgumentException("Specify -edgeToField <from-field-name>");

    long expectedEdges = settings.expectedEdges;
    if (expectedEdges <= 0)
      expectedEdges = (int) (sourceSchema.getSource().totalSize / entity.getAverageRowLength());

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

    long skipEntries = settings.edgesSkipEntries != null ? settings.edgesSkipEntries : 0;
    if (settings.edgesSkipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      skipEntries = 1l;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
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

      String[] row;
      // No ownsTransaction/callerTransactionActiveOnEntry guard needed here, unlike loadDocuments()/loadVertices():
      // database.begin() nests rather than reusing an already-active transaction (see LocalDatabase#begin()), so a
      // caller's own pre-existing transaction is never touched by this method's own commits below.
      database.begin();
      int txCount = 0;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          continue;

        try {
          createEdgeFromRow(database, row, properties, from, to, context, settings);
          txCount++;
          if (txCount >= settings.commitEvery) {
            database.commit();
            database.begin();
            txCount = 0;
          }
        } catch (final Exception e) {
          // Unlike loadDocuments/loadVertices, edge rows are always skipped-and-logged regardless of -onRowError: a
          // "bad" edge row here is typically just an unresolved from/to vertex reference, expected during graph
          // imports rather than a data-corruption case.
          LogManager.instance().log(this, Level.SEVERE, "Error on parsing line %d", e, line);
        }
      }
      database.commit();

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
      LogManager.instance().log(this, Level.INFO, "- Skipped edges.....: %d", null, context.skippedEdges.get());

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
      params = new Object[properties.size() * 2];
      for (int i = 0; i < properties.size(); ++i) {
        final AnalyzedProperty property = properties.get(i);
        params[i * 2] = property.getName();
        params[i * 2 + 1] = row[property.getIndex()];
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

  @Override
  public SourceSchema analyze(final AnalyzedEntity.EntityType entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) throws IOException {
    parser.reset();

    final String delimiter = settings.getValue("delimiter", ",");

    final CsvParserSettings csvParserSettings;
    final TsvParserSettings tsvParserSettings;
    final AbstractParser csvParser;
    final CommonParserSettings parserSettings;

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      parserSettings = tsvParserSettings = new TsvParserSettings();
    } else {
      parserSettings = csvParserSettings = new CsvParserSettings();
      csvParserSettings.setDelimiterDetectionEnabled(false);
      if (delimiter != null) {
        csvParserSettings.detectFormatAutomatically(delimiter.charAt(0));
        csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
      }
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

    final String entityName = entityType == AnalyzedEntity.EntityType.VERTEX ?
        settings.vertexTypeName :
        entityType == AnalyzedEntity.EntityType.EDGE ? settings.edgeTypeName : settings.documentTypeName;

    long skipEntries = 0;
    final String header;

    switch (entityType) {
    case VERTEX:
      header = settings.verticesHeader;
      skipEntries = settings.verticesSkipEntries != null ? settings.verticesSkipEntries : 0;
      if (settings.verticesSkipEntries == null)
        // BY DEFAULT SKIP THE FIRST LINE AS HEADER
        skipEntries = 1l;
      break;

    case EDGE:
      header = settings.edgesHeader;
      skipEntries = settings.edgesSkipEntries != null ? settings.edgesSkipEntries : 0;
      if (settings.edgesSkipEntries == null)
        // BY DEFAULT SKIP THE FIRST LINE AS HEADER
        skipEntries = 1l;
      break;

    case DOCUMENT:
      header = settings.documentsHeader;
      skipEntries = settings.documentsSkipEntries != null ? settings.documentsSkipEntries : 0;
      if (settings.documentsSkipEntries == null)
        // BY DEFAULT SKIP THE FIRST LINE AS HEADER
        skipEntries = 1l;
      break;

    default:
      header = null;
    }

    if (header != null) {
      if (delimiter == null)
        fieldNames.add(header);
      else {
        final String[] headerColumns = header.split(",");
        fieldNames.addAll(Arrays.asList(headerColumns));
      }
      LogManager.instance().log(this, Level.INFO, "Parsing with custom header: %s", null, fieldNames);
    }

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
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
    String delimiter = ",";
    if (settings.options.containsKey("delimiter"))
      delimiter = settings.getValue("delimiter", ",");

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      final TsvParserSettings tsvParserSettings = new TsvParserSettings();
      tsvParserSettings.setMaxColumns(settings.getIntValue("maxProperties", tsvParserSettings.getMaxColumns()));
      tsvParserSettings.setMaxCharsPerColumn(settings.getIntValue("maxPropertySize", tsvParserSettings.getMaxCharsPerColumn()));
      return new TsvParser(tsvParserSettings);
    } else {
      final CsvParserSettings csvParserSettings = new CsvParserSettings();
      csvParserSettings.getFormat().setDelimiter(delimiter);
      csvParserSettings.setMaxColumns(settings.getIntValue("maxProperties", csvParserSettings.getMaxColumns()));
      csvParserSettings.setMaxCharsPerColumn(settings.getIntValue("maxPropertySize", csvParserSettings.getMaxCharsPerColumn()));
      return new CsvParser(csvParserSettings);
    }

  }
}
