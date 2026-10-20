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
 * A LIGHTWEIGHT edge has no record: one of a bidirectional type is rebuilt from its copy in the far endpoint's list, and
 * counted apart in {@code reconnectedLightweightOutEdges}/{@code reconnectedLightweightInEdges} (#10407). An outgoing one
 * of a unidirectional type was stored in the vertex's own list only and cannot be rebuilt:
 * {@code unrecoverableLightweightEdgeTypes} names the types in which the vertex may have had such edges. Neither can a
 * lightweight self-loop: both its copies were in the vertex's own lists. No count can be given for either, since no
 * surviving structure records them.
 * <p>
 * COST: finding those copies reads the edge lists of every vertex of the database, on top of the scan of every edge
 * record the rebuild always did, whenever the schema holds a bidirectional edge type. It cannot be narrowed to the
 * types declared LIGHTWEIGHT: the deprecated {@code newLightEdge()} stores lightweight entries under regular types too.
 * Fine for an emergency repair, not for a routine operation.
 * <p>
 * The whole rebuild is written in the CALLER's transaction, deliberately, however many edges the vertex had: the restore
 * is one atomic repair the caller can roll back, and committing part of it from inside the statement would break that
 * (as #6220 established for {@code TRUNCATE}). A hub with millions of edges is therefore one large transaction.
 * <p>
 * Restores graph STRUCTURE only: the vertex's original property values are not recoverable from its edges. SET or
 * CONTENT lets the caller supply them if known from another source (a backup, an application log, ...).
 * <p>
 * The type's schema applies in full (#6127): declared default values are set, the vertex is validated, and the
 * create events fire, exactly as for an INSERT. On a type with MANDATORY properties a bare structure-only restore is
 * therefore refused until SET/CONTENT supplies them - the placeholder value has to be the operator's explicit
 * choice, because a vertex written past its own constraints could not be updated afterwards either.
 * <p>
 * The create events fire on the bare vertex, BEFORE the adjacency rebuild below - deliberately, and this is the
 * INSERT-parity behaviour rather than a gap in it: a vertex a plain INSERT creates has no edges either, and firing
 * the event later would move it out of the read lock that {@code restoreRecord} holds for exactly the whole
 * create sequence, and would make the vertex arm's event ordering differ from the document and edge arms. The
 * consequence to know about is that an afterCreate trigger deriving something from adjacency (a degree counter,
 * say) sees zero here and is never notified of the reconnection that follows, because reconnecting adjacency
 * writes no records and so raises no event of its own. Such a trigger has to be driven from the statement's
 * {@code reconnectedOutEdges}/{@code reconnectedInEdges} result instead.
 */
public class RestoreVertexStatement extends SimpleExecStatement {
  public Identifier targetType;
  public Rid        targetRid;
  public InsertBody insertBody;

  public RestoreVertexStatement() {
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
    final RID restoredRid = database.restoreRecord(shell, bucket, rid.getPosition());

    final Set<RID> asSet = Set.of(restoredRid);
    final long[] counts = database.getGraphEngine().reconnectEdgesFromSurvivors(asSet, asSet);

    final ResultInternal result = new ResultInternal(database);
    result.setProperty("operation", "restore vertex");
    result.setProperty("record", restoredRid.toString());
    result.setProperty("reconnectedOutEdges", counts[0]);
    result.setProperty("reconnectedInEdges", counts[1]);
    // #10407: THE LIGHTWEIGHT PART OF THE TWO COUNTS ABOVE, REBUILT FROM THE COPIES IN THE FAR ENDPOINTS' LISTS, AND THE TYPES
    // WHOSE OUTGOING LIGHTWEIGHT EDGES HAD NO SUCH COPY (UNIDIRECTIONAL): ANY THE VERTEX HAD ARE LOST
    result.setProperty("reconnectedLightweightOutEdges", counts[2]);
    result.setProperty("reconnectedLightweightInEdges", counts[3]);
    result.setProperty("unrecoverableLightweightEdgeTypes", database.getGraphEngine().lightEdgeTypesWithoutFarCopy());

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
    final RestoreVertexStatement result = new RestoreVertexStatement();
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
