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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.logging.Level;

/**
 * {@code COMPACT INDEX <name>|*} statement (issue #5144).
 * <p>
 * Exposes {@link IndexInternal#compact()} to SQL (and therefore HTTP via {@code POST /command}), so client-server users can force
 * a foreground segment merge after a bulk load without falling back to the far more expensive {@code REBUILD INDEX} (which
 * re-indexes every record) or waiting for background compaction. Mirrors the shape of {@link RebuildIndexStatement}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CompactIndexStatement extends DDLStatement {
  public boolean    all = false;
  public Identifier name;

  public CompactIndexStatement() {
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    // Compaction is a schema-maintenance operation that mutates on-disk index files, gated by UPDATE_SCHEMA like
    // REBUILD INDEX / DROP INDEX. No-op with no bound user (embedded, schema load, HA replication apply).
    final DatabaseInternal database = context.getDatabase();
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    final ResultInternal result = new ResultInternal(database);
    result.setProperty("operation", "compact index");

    final List<String> compactedIndexes = new ArrayList<>();
    boolean compacted = false;
    String indexName = null;
    // PUBLISH LIVE PROGRESS (issue #5376): one step per index, pollable via the progress HTTP endpoint,
    // the console and Studio. Always retired in the finally.
    final OperationProgress progress = OperationProgressRegistry.instance().register(database.getName(), "compact index");
    try {
      final List<Index> targetIndexes = new ArrayList<>();
      if (all) {
        // Skip the logical TypeIndex wrappers: their bucket sub-indexes are already visited as standalone
        // automatic indexes, so compacting the wrapper too would merge the same files twice.
        for (final Index idx : database.getSchema().getIndexes())
          if (idx.isAutomatic() && !(idx instanceof TypeIndex))
            targetIndexes.add(idx);
      } else
        targetIndexes.add(database.getSchema().getIndexByName(name.getValue()));

      int stepIndex = 0;
      for (final Index idx : targetIndexes) {
        indexName = idx.getName();
        ++stepIndex;
        progress.onProgress("Compacting index '" + idx.getName() + "'", stepIndex, targetIndexes.size(), 0, -1);
        if (compactOutsideCallerTransaction(database, (IndexInternal) idx))
          compacted = true;
        compactedIndexes.add(idx.getName());
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on compacting index '%s': %s", e,
          indexName != null ? indexName : (name != null ? name.getValue() : "*"), e.getMessage());
      throw new IndexException(
          "Error on compacting index '" + (indexName != null ? indexName : (name != null ? name.getValue() : "*")) + "' (error="
              + e.getMessage() + ")", e);
    } finally {
      OperationProgressRegistry.instance().unregister(progress);
    }

    result.setProperty("indexes", compactedIndexes);
    result.setProperty("compacted", compacted);

    final InternalResultSet rs = new InternalResultSet();
    rs.add(result);
    return rs;
  }

  /**
   * Runs {@link #compactIndex} with no transaction on the thread, which is how a background compaction runs (issue #11580).
   * <p>
   * The swap that ends an LSM-tree compaction ({@code LSMTreeIndex.splitIndex}) refuses to run while a transaction is active,
   * and {@code POST /api/v1/command} runs every statement inside its auto-commit transaction, so the statement failed on the
   * default HTTP path, inside a {@code sqlscript} and inside any {@code database.transaction(...)}. Compaction is not part of
   * the caller's transaction anyway: it rewrites committed index pages, cannot be rolled back with it, and runs
   * concurrently with other threads' transactions every time it is triggered in the background. So with a transaction open,
   * the compaction runs on a thread of its own and this one waits for it, leaving the caller's transaction and everything in
   * it (pending changes, locks, read snapshot) untouched: its commit finds the index migrated exactly as it would after a
   * background compaction. Committing or rolling back the caller's transaction here instead would end work that is not this
   * statement's to end. Without a transaction it runs inline, as it always has.
   * <p>
   * An interrupted wait gives up waiting, not the compaction: like a background one, it runs to its end.
   */
  private static boolean compactOutsideCallerTransaction(final DatabaseInternal database, final IndexInternal idx)
      throws Exception {
    if (!database.isTransactionActive())
      return compactIndex(idx);

    final FutureTask<Boolean> compaction = new FutureTask<>(() -> {
      DatabaseContext.INSTANCE.init(database);
      try {
        return compactIndex(idx);
      } finally {
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      }
    });
    new Thread(compaction, "ArcadeDB-CompactIndex-" + database.getName()).start();

    try {
      return compaction.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof final Exception exception)
        throw exception;
      if (cause instanceof final Error error)
        throw error;
      throw e;
    }
  }

  /**
   * Compacts a single index (a {@link TypeIndex} wrapper or a concrete bucket index). Returns true if a merge actually ran.
   * <p>
   * LSM-tree family indexes gate {@code compact()} on a prior {@code scheduleCompaction()} state transition (AVAILABLE -&gt;
   * COMPACTION_SCHEDULED); the sparse-vector index compacts its engine segments directly and needs no scheduling (and scheduling
   * its unused underlying LSM would leave that status stuck). Branch on the index type to invoke each correctly.
   */
  private static boolean compactIndex(final IndexInternal idx) throws Exception {
    if (idx == null)
      throw new CommandExecutionException("Index name is null");

    if (!idx.isAutomatic())
      throw new CommandExecutionException(
          "Cannot compact index '" + idx.getName() + "' because it's manual and there aren't indications of what to index");

    if (!idx.isValid())
      throw new CommandExecutionException("Cannot compact index '" + idx.getName() + "' because it's not valid");

    if (idx.getType() == Schema.INDEX_TYPE.LSM_SPARSE_VECTOR)
      // The sparse-vector engine merges its own segment files; compact() is not gated on scheduleCompaction().
      return idx.compact();

    // LSM-tree family: compact() only runs when the index is in the COMPACTION_SCHEDULED state.
    if (!idx.scheduleCompaction())
      return false;
    return idx.compact();
  }

  @Override
  public void toString(final java.util.Map<String, Object> params, final StringBuilder builder) {
    builder.append("COMPACT INDEX ");
    if (all)
      builder.append("*");
    else
      name.toString(params, builder);
  }

  @Override
  public CompactIndexStatement copy() {
    final CompactIndexStatement result = new CompactIndexStatement();
    result.all = all;
    result.name = name == null ? null : name.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final CompactIndexStatement that = (CompactIndexStatement) o;
    if (all != that.all)
      return false;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    int result = all ? 1 : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
