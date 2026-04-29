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
