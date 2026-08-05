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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.Map;
import java.util.Objects;

/**
 * {@code RESTORE EDGE <type> RID <rid> FROM <rid> TO <rid> [SET ... | CONTENT ...]}
 * <p>
 * Emergency repair: recreates a deleted EDGE record at the exact RID it used to hold, with the given OUT/IN
 * endpoints. Unlike RESTORE VERTEX, no adjacency reconnection follows: if the two endpoint vertices' adjacency
 * lists still reference this RID - the usual case this repairs, since a raw record delete never touches the
 * neighbours - restoring the edge record itself is enough. Refuses if either endpoint vertex does not exist: an
 * edge to a missing vertex is not what this statement is for (see RESTORE VERTEX first in that case).
 */
public class RestoreEdgeStatement extends SimpleExecStatement {
  public Identifier targetType;
  public Rid        targetRid;
  public Rid        fromRid;
  public Rid        toRid;
  public InsertBody insertBody;

  public RestoreEdgeStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeSimple(final CommandContext context) {
    final DatabaseInternal database = context.getDatabase();
    final String typeName = targetType.getStringValue();

    final DocumentType type = database.getSchema().getType(typeName);
    if (!(type instanceof EdgeType edgeType))
      throw new CommandSQLParsingException("Type '" + typeName + "' is not an edge type");

    final RID rid = targetRid.toRecordId((com.arcadedb.query.sql.executor.Result) null, context);

    if (!type.getBucketIds(true).contains(rid.getBucketId()))
      throw new CommandSQLParsingException(
          "Type '" + typeName + "' does not own bucket " + rid.getBucketId() + ", cannot restore " + rid + " as this type");

    final RID out = fromRid.toRecordId((com.arcadedb.query.sql.executor.Result) null, context);
    final RID in = toRid.toRecordId((com.arcadedb.query.sql.executor.Result) null, context);

    // existsRecord, not lookupByRID: the latter's loadContent=false mode builds a lazy proxy without verifying the
    // record is actually there, so it never raises RecordNotFoundException for a made-up RID.
    if (!recordExists(database, out))
      throw new CommandSQLParsingException("Cannot restore edge " + rid + ": FROM vertex " + out + " does not exist");
    if (!recordExists(database, in))
      throw new CommandSQLParsingException("Cannot restore edge " + rid + ": TO vertex " + in + " does not exist");

    final MutableEdge shell = new MutableEdge(database, edgeType, out, in);
    RestoreStatementSupport.applyBody(shell, insertBody, context);

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    final RID restoredRid = bucket.restoreRecordAtPosition(rid.getPosition(), shell);

    final ResultInternal result = new ResultInternal(database);
    result.setProperty("operation", "restore edge");
    result.setProperty("record", restoredRid.toString());
    result.setProperty("out", out.toString());
    result.setProperty("in", in.toString());

    final InternalResultSet rs = new InternalResultSet();
    rs.add(result);
    return rs;
  }

  private static boolean recordExists(final DatabaseInternal database, final RID rid) {
    final Bucket bucket = database.getSchema().getBucketByIdIfExists(rid.getBucketId());
    return bucket != null && bucket.existsRecord(rid);
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("RESTORE EDGE ");
    targetType.toString(params, builder);
    builder.append(" RID ");
    targetRid.toString(params, builder);
    builder.append(" FROM ");
    fromRid.toString(params, builder);
    builder.append(" TO ");
    toRid.toString(params, builder);
    if (insertBody != null) {
      builder.append(" ");
      insertBody.toString(params, builder);
    }
  }

  @Override
  public RestoreEdgeStatement copy() {
    final RestoreEdgeStatement result = new RestoreEdgeStatement(-1);
    result.targetType = targetType == null ? null : targetType.copy();
    result.targetRid = targetRid == null ? null : targetRid.copy();
    result.fromRid = fromRid == null ? null : fromRid.copy();
    result.toRid = toRid == null ? null : toRid.copy();
    result.insertBody = insertBody == null ? null : insertBody.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final RestoreEdgeStatement that = (RestoreEdgeStatement) o;
    return Objects.equals(targetType, that.targetType) && Objects.equals(targetRid, that.targetRid) && Objects.equals(fromRid,
        that.fromRid) && Objects.equals(toRid, that.toRid) && Objects.equals(insertBody, that.insertBody);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetType, targetRid, fromRid, toRid, insertBody);
  }
}
