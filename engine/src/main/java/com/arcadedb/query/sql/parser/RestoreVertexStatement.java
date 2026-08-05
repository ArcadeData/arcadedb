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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@code RESTORE VERTEX <type> RID <rid> [SET ... | CONTENT ...]}
 * <p>
 * Emergency repair: recreates a deleted VERTEX record at the exact RID it used to hold, then rebuilds its OUT/IN
 * adjacency from every surviving edge that still names it (see {@link com.arcadedb.graph.GraphEngine#reconnectEdgesFromSurvivors}) -
 * unconditionally, not gated by {@code CHECK DATABASE}'s corruption heuristics, which never flag a freshly created,
 * legitimately edge-less vertex as needing a rebuild.
 * <p>
 * Restores graph STRUCTURE only: the vertex's original property values are not recoverable from its edges. SET or
 * CONTENT lets the caller supply them if known from another source (a backup, an application log, ...).
 */
public class RestoreVertexStatement extends SimpleExecStatement {
  public Identifier targetType;
  public Rid        targetRid;
  public InsertBody insertBody;

  public RestoreVertexStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeSimple(final CommandContext context) {
    final DatabaseInternal database = context.getDatabase();
    final String typeName = targetType.getStringValue();

    final DocumentType type = database.getSchema().getType(typeName);
    if (!(type instanceof VertexType))
      throw new CommandSQLParsingException("Type '" + typeName + "' is not a vertex type");

    final RID rid = targetRid.toRecordId((com.arcadedb.query.sql.executor.Result) null, context);

    if (!type.getBucketIds(true).contains(rid.getBucketId()))
      throw new CommandSQLParsingException(
          "Type '" + typeName + "' does not own bucket " + rid.getBucketId() + ", cannot restore " + rid + " as this type");

    final MutableVertex shell = database.newVertex(typeName);
    RestoreStatementSupport.applyBody(shell, insertBody, context);

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    final RID restoredRid = bucket.restoreRecordAtPosition(rid.getPosition(), shell);

    final Set<RID> asSet = Set.of(restoredRid);
    final long[] counts = database.getGraphEngine().reconnectEdgesFromSurvivors(asSet, asSet);

    final ResultInternal result = new ResultInternal(database);
    result.setProperty("operation", "restore vertex");
    result.setProperty("record", restoredRid.toString());
    result.setProperty("reconnectedOutEdges", counts[0]);
    result.setProperty("reconnectedInEdges", counts[1]);

    final InternalResultSet rs = new InternalResultSet();
    rs.add(result);
    return rs;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("RESTORE VERTEX ");
    targetType.toString(params, builder);
    builder.append(" RID ");
    targetRid.toString(params, builder);
    if (insertBody != null) {
      builder.append(" ");
      insertBody.toString(params, builder);
    }
  }

  @Override
  public RestoreVertexStatement copy() {
    final RestoreVertexStatement result = new RestoreVertexStatement(-1);
    result.targetType = targetType == null ? null : targetType.copy();
    result.targetRid = targetRid == null ? null : targetRid.copy();
    result.insertBody = insertBody == null ? null : insertBody.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final RestoreVertexStatement that = (RestoreVertexStatement) o;
    return Objects.equals(targetType, that.targetType) && Objects.equals(targetRid, that.targetRid) && Objects.equals(insertBody,
        that.insertBody);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetType, targetRid, insertBody);
  }
}
