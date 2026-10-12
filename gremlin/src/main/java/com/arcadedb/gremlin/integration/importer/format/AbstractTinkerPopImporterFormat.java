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
package com.arcadedb.gremlin.integration.importer.format;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.format.CSVImporterFormat;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

/**
 * A format that hands the whole source to one of TinkerPop's own graph readers ({@code GraphMLReader}, the standard
 * {@code GraphSONReader} branch).
 * <p>
 * <b>Who resolves the transaction.</b> TinkerPop's readers commit {@code graph.tx()} every batch and at the end whenever
 * the graph declares transactions supported, and an {@link ArcadeGraph} over a database with an active transaction JOINS
 * it rather than nesting under it. Handed the caller's transaction, the reader therefore committed it: the caller's own
 * pending records were published along with the import, and their {@code rollback()} found nothing left to take back
 * (issue #9783). So when the caller owns a live transaction ({@link ImporterContext#importOwnsTransaction}) the reader
 * gets {@link ArcadeGraph#openInCallerTransaction}, which declares transactions unsupported and makes the reader skip
 * every commit: the import is part of the caller's unit of work, which is what {@code Importer(Database, String)}
 * promises and what #7771 settled for the non-RID GraphSON branch. When the import owns its transaction nothing changes,
 * the reader commits on its own schedule; and if it fails part way, the batch it left open is the import's to roll back,
 * not a transaction to leave on a thread whose caller never began one. The batches it committed before the failure stay
 * committed: a standalone import that fails is partial, as every importer format that commits in batches is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class AbstractTinkerPopImporterFormat extends CSVImporterFormat {

  /**
   * Reads {@code source} into the database with the reader {@code readerFactory} builds, and adds what it created to the
   * import statistics.
   */
  protected void readGraph(final InputStream source, final DatabaseInternal database, final ImporterContext context,
      final Function<ArcadeGraph, GraphReader> readerFactory, final String formatName) {
    // ASKED ONCE, BEFORE THE READER WRITES ANYTHING: AFTER THAT A TRANSACTION IS ALWAYS ACTIVE
    final boolean ownsTransaction = context.importOwnsTransaction(database);
    final boolean transactionActiveBefore = database.isTransactionActive();
    final ArcadeGraph graph = ownsTransaction ? ArcadeGraph.open(database) : ArcadeGraph.openInCallerTransaction(database);

    // TinkerPop's readers report no per-record progress, so the statistics this format owes the caller
    // (context.parsed/createdVertices/createdEdges - see ImporterContext#toMap()) are taken as a before/after delta across
    // the whole schema instead of counted as the reader writes (issue #8054).
    final long verticesBefore = countRecordsOfKind(database, VertexType.class);
    final long edgesBefore = countRecordsOfKind(database, EdgeType.class);

    try {
      readerFactory.apply(graph).readGraph(source, graph);
    } catch (final IOException e) {
      final ImportException failure = new ImportException("Error on importing " + formatName, e);
      rollbackOwnBatch(database, ownsTransaction, transactionActiveBefore, failure);
      throw failure;
    } catch (final RuntimeException | Error e) {
      rollbackOwnBatch(database, ownsTransaction, transactionActiveBefore, e);
      throw e;
    } finally {
      // In a finally, not only after a successful read: the reader can fail part way with a RuntimeException (a
      // malformed record, a schema conflict) rather than the IOException above, and inside a caller-owned transaction
      // what it already wrote stays there until the caller resolves it either way - so the count must still reach the
      // caller. After a rollback of the import's own batch the delta is what its earlier batches committed.
      final long createdVertices = countRecordsOfKind(database, VertexType.class) - verticesBefore;
      final long createdEdges = countRecordsOfKind(database, EdgeType.class) - edgesBefore;

      context.createdVertices.addAndGet(createdVertices);
      context.createdEdges.addAndGet(createdEdges);
      context.parsed.addAndGet(createdVertices + createdEdges);
    }
  }

  /**
   * Owned and failed: the reader's last batch is still open on this thread. Rolled back only when the import began it - a
   * transaction already active before the read is not the import's to end. A rollback that fails too is attached to the
   * import's own failure instead of replacing it.
   */
  private static void rollbackOwnBatch(final DatabaseInternal database, final boolean ownsTransaction,
      final boolean transactionActiveBefore, final Throwable failure) {
    if (!ownsTransaction || transactionActiveBefore || !database.isTransactionActive())
      return;
    try {
      database.rollback();
    } catch (final RuntimeException rollbackFailure) {
      failure.addSuppressed(rollbackFailure);
    }
  }
}
