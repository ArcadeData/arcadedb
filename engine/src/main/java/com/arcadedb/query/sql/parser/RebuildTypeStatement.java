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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

/**
 * REBUILD TYPE typeName [POLYMORPHIC]
 *
 * Re-serialises every record of the named type (and optionally its subtypes) so that storage-layout schema changes
 * are applied to records on disk. The primary use case is relocating values after toggling a property's EXTERNAL
 * flag: after `ALTER PROPERTY T.p EXTERNAL true`, existing records still carry the value inline; running
 * `REBUILD TYPE T` moves those values to the paired external bucket. The reverse case (EXTERNAL true -> false) is
 * also handled, with orphan external records cleaned up automatically by the serializer.
 *
 * Commits in batches to keep memory bounded on large types.
 */
public class RebuildTypeStatement extends DDLStatement {
  private static final int BATCH_SIZE = 10_000;

  public Identifier typeName;
  public boolean    polymorphic = false;

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

    final long[] count = { 0L };
    final boolean implicitTx = !db.isTransactionActive();
    if (implicitTx)
      db.begin();

    try {
      db.scanType(typeName.getStringValue(), polymorphic, rec -> {
        // Re-save forces re-serialization which routes property values according to the current schema (e.g. moves
        // values to/from the external bucket per the current EXTERNAL flag) and triggers orphan cleanup in the
        // serializer for any external pointers that no longer apply.
        final MutableDocument m = (MutableDocument) rec.modify();
        m.markDirty();
        m.save();
        count[0]++;
        if (count[0] % BATCH_SIZE == 0) {
          db.commit();
          db.begin();
        }
        return true;
      });

      if (implicitTx)
        db.commit();
    } catch (Exception e) {
      if (implicitTx && db.isTransactionActive())
        db.rollback();
      throw new CommandExecutionException("Error on rebuilding type '" + typeName.getStringValue() + "'", e);
    }

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
  }

  @Override
  public RebuildTypeStatement copy() {
    final RebuildTypeStatement result = new RebuildTypeStatement(-1);
    result.typeName = typeName == null ? null : typeName.copy();
    result.polymorphic = polymorphic;
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final RebuildTypeStatement that = (RebuildTypeStatement) o;
    return polymorphic == that.polymorphic && Objects.equals(typeName, that.typeName);
  }

  @Override
  public int hashCode() {
    int result = typeName != null ? typeName.hashCode() : 0;
    result = 31 * result + (polymorphic ? 1 : 0);
    return result;
  }
}
