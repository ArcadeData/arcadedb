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

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

/**
 * REBUILD TYPE typeName [POLYMORPHIC] [WITH batchSize = N] - re-serialises records to apply schema layout changes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RebuildTypeStatement extends DDLStatement {
  private static final int DEFAULT_BATCH_SIZE = 10_000;

  public Identifier                  typeName;
  public boolean                     polymorphic = false;
  public final Map<Expression, Expression> settings = new HashMap<>();

  public RebuildTypeStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database db = context.getDatabase();
    final Schema schema = db.getSchema();
    final DocumentType type = schema.getType(typeName.getStringValue());
    if (type == null)
      throw new CommandExecutionException("Type not found: " + typeName.getStringValue());

    int batchSize = DEFAULT_BATCH_SIZE;
    for (final Map.Entry<Expression, Expression> e : settings.entrySet()) {
      final String key = e.getKey().toString();
      if (key.equalsIgnoreCase("batchSize"))
        batchSize = Integer.parseInt(e.getValue().value.toString());
      else
        throw new CommandSQLParsingException(
            "Unrecognized setting '" + key + "' in REBUILD TYPE statement (supported: batchSize)");
    }
    final int finalBatchSize = batchSize;

    final long[] count = { 0L };
    // Records committed in earlier batches and therefore NOT rolled back by a later failure: needed so we can
    // tell the caller exactly how many rows are already in the new layout when the rebuild aborts mid-stream.
    final long[] committedBefore = { 0L };
    final boolean implicitTx = !db.isTransactionActive();
    if (implicitTx)
      db.begin();

    try {
      db.scanType(typeName.getStringValue(), polymorphic, rec -> {
        final MutableDocument m = (MutableDocument) rec.modify();
        m.markDirty();
        m.save();
        count[0]++;
        // Batch only when we own the transaction. Committing inside a caller-supplied TX would leak the user's
        // writes prematurely and leave the trailing batch uncommitted.
        if (implicitTx && count[0] % finalBatchSize == 0) {
          db.commit();
          committedBefore[0] = count[0];
          db.begin();
        }
        return true;
      });

      if (implicitTx)
        db.commit();
    } catch (Exception e) {
      if (implicitTx && db.isTransactionActive())
        db.rollback();
      // REBUILD TYPE is NOT atomic across batch boundaries: every db.commit() above already persisted those
      // records in the new layout. After rollback, only the in-flight batch is reverted; the type is left
      // half-migrated. Surface the boundary clearly so the operator knows to re-run REBUILD TYPE to finish.
      final long migratedAndKept = committedBefore[0];
      final long rolledBack = count[0] - committedBefore[0];
      throw new CommandExecutionException(
          "Error on rebuilding type '" + typeName.getStringValue() + "' after " + count[0] + " records ("
              + migratedAndKept + " committed in earlier batches and remain in the new layout, " + rolledBack
              + " rolled back from the in-flight batch). REBUILD TYPE is NOT atomic across batches; re-run the"
              + " command to migrate the remaining records once the underlying issue is fixed.", e);
    }

    // If the rebuild was triggered to revert a property from EXTERNAL to inline (i.e. the type no longer has
    // any EXTERNAL property), the previously-paired external buckets are now empty (orphan-cleanup in
    // serializeProperties deleted every external blob during the re-save). Drop them so they don't accumulate
    // across toggle cycles, and persist the cleared mapping.
    // Reclaim paired external buckets only if REBUILD owned the transaction. With a caller-supplied tx the
    // queued record updates haven't flushed yet (LocalDatabase.updateRecord defers serialization to commit),
    // so the orphan cleanup hasn't run and the buckets are still non-empty. The caller can re-run REBUILD
    // outside their tx to trigger reclaim, or invoke it explicitly.
    if (implicitTx && type instanceof com.arcadedb.schema.LocalDocumentType ldt && !ldt.hasExternalProperties())
      ldt.reclaimEmptyExternalBuckets();

    final ResultInternal result = new ResultInternal(db);
    result.setProperty("operation", "rebuild type");
    result.setProperty("typeName", typeName.getStringValue());
    result.setProperty("polymorphic", polymorphic);
    result.setProperty("recordsRebuilt", count[0]);
    final InternalResultSet rs = new InternalResultSet();
    rs.add(result);
    return rs;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("REBUILD TYPE ");
    typeName.toString(params, builder);
    if (polymorphic)
      builder.append(" POLYMORPHIC");
    if (!settings.isEmpty()) {
      builder.append(" WITH ");
      boolean first = true;
      for (final Map.Entry<Expression, Expression> e : settings.entrySet()) {
        if (!first)
          builder.append(", ");
        e.getKey().toString(params, builder);
        builder.append(" = ");
        e.getValue().toString(params, builder);
        first = false;
      }
    }
  }

  @Override
  public RebuildTypeStatement copy() {
    final RebuildTypeStatement result = new RebuildTypeStatement(-1);
    result.typeName = typeName == null ? null : typeName.copy();
    result.polymorphic = polymorphic;
    for (final Map.Entry<Expression, Expression> e : settings.entrySet())
      result.settings.put(e.getKey().copy(), e.getValue().copy());
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final RebuildTypeStatement that = (RebuildTypeStatement) o;
    return polymorphic == that.polymorphic
        && Objects.equals(typeName, that.typeName)
        && Objects.equals(settings, that.settings);
  }

  @Override
  public int hashCode() {
    int result = typeName != null ? typeName.hashCode() : 0;
    result = 31 * result + (polymorphic ? 1 : 0);
    result = 31 * result + settings.hashCode();
    return result;
  }
}
