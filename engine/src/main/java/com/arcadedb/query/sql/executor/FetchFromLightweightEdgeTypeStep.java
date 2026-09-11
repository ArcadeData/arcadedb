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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.MultiIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;

/**
 * Scans an edge type that stores some or all of its edges LIGHTWEIGHT, i.e. as a pair of pointers inside the two
 * vertices rather than as a record of its own.
 * <p>
 * A lightweight edge allocates nothing in the edge type's bucket, so the ordinary bucket scan
 * {@link FetchFromTypeExecutionStep} performs answers a query over such a type with zero rows - not because the
 * graph holds no edges, but because the place it looks is structurally empty (issue #7477). The edges are reachable
 * only from the vertices that carry them, which is what this step walks: every vertex the caller may read, then the
 * lightweight entries of its outgoing edge list. Walking OUT alone is complete and yields each edge exactly once,
 * whichever way the type is declared: the outgoing entry is always written, and the incoming one is the optional
 * half.
 * <p>
 * The type's own records are emitted first, before the walk. They are normally none - a type can only be declared
 * LIGHTWEIGHT while it holds no edge record - but a non-lightweight supertype scanned together with a lightweight
 * subtype has them, and so does a type whose LIGHTWEIGHT declaration was lifted later. The walk skips whatever the
 * bucket scan already returned by testing the storage shape of each entry rather than the declaration of its type,
 * which is the rule every other read path applies: an edge-list entry whose position is negative is a lightweight
 * edge, whatever its type says today.
 * <p>
 * Cost is O(V + E) against the O(E) of a bucket scan, and there is no index over a lightweight edge to replace it
 * with - a type with no records has no properties to index. Nor is there any way to seek into the walk, so a
 * {@code LIMIT} is satisfied by walking vertices until enough entries have been found rather than by addressing
 * them: on a graph whose edges hang off a small corner of a large vertex set, even a small {@code LIMIT} can cost
 * most of the vertex scan. {@code EXPLAIN} names the step so that cost is visible rather than a surprise.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromLightweightEdgeTypeStep extends AbstractExecutionStep {
  private final String           edgeTypeName;
  private       Iterator<Record> typeRecords;
  private       Iterator<Record> vertices;
  private       Iterator<Edge>   currentVertexEdges = Collections.emptyIterator();
  private       Edge             nextEdge;
  private       Record           nextRecord;
  private       boolean          inited             = false;
  private       long             vertexSetSize      = 0;

  public FetchFromLightweightEdgeTypeStep(final String edgeTypeName, final CommandContext context) {
    super(context);
    this.edgeTypeName = edgeTypeName;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    init(context);

    return new ResultSet() {
      private int served = 0;

      @Override
      public boolean hasNext() {
        if (served >= nRecords)
          return false;
        return fetchNext();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final ResultInternal result;
          if (nextRecord != null) {
            result = new ResultInternal(nextRecord);
            nextRecord = null;
          } else {
            result = new ResultInternal(nextEdge);
            nextEdge = null;
          }
          ++served;
          context.setVariable("current", result);
          return result;
        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
        }
      }
    };
  }

  /**
   * Resolves the vertices to walk once, when the first row is asked for.
   * <p>
   * A vertex type the caller cannot read is left out instead of failing the query: an edge is only visible through
   * the vertices that hold it, so the ones behind a denied vertex type are not the caller's to see, and refusing the
   * whole statement would make the edge type unqueryable for that user over an unrelated type's ACL. The edge type
   * itself is checked up front, because the bucket scan this step replaces used to be where that check happened.
   */
  private void init(final CommandContext context) {
    if (inited)
      return;
    inited = true;

    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final Schema schema = database.getSchema();

    // Every type the walk can emit, not only the one the caller named: getEdges(OUT, name) resolves the entries it
    // accepts polymorphically, so a subtype's lightweight edges come out of a scan of its supertype. The record
    // scan below happens to check the same set today - iterateType() opens a BucketIterator per bucket of the
    // hierarchy, and each of those checks - but that is incidental, and it would go away the moment this step
    // learned to skip the (usually empty) record scan on a purely lightweight type. Stated here so the coverage
    // belongs to the walk rather than to a side effect of something else (PR #7478 review).
    checkAccessOnTypeAndSubTypes(database, schema.getType(edgeTypeName));

    typeRecords = database.iterateType(edgeTypeName, true);

    final MultiIterator<Record> vertexIterator = new MultiIterator<>();
    for (final DocumentType type : schema.getTypes()) {
      if (type.getType() != Vertex.RECORD_TYPE)
        continue;
      // Own buckets only: a subtype contributes its own through its own entry in this loop, so a polymorphic
      // walk here would visit every inherited bucket once per level of the hierarchy. Checked one bucket at a
      // time rather than once per type: a type-level check answers yes as soon as ONE of its buckets is readable,
      // and opening the rest would then fail the whole statement inside BucketIterator - the very thing leaving a
      // denied type out is meant to avoid.
      for (final Bucket bucket : type.getBuckets(false))
        if (SecurityHelper.canAccessFile(database, bucket.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD)) {
          vertexIterator.addIterator(bucket.iterator());
          vertexSetSize += sizeOf(database, bucket.getFileId());
        }
    }
    vertices = vertexIterator;

    warnIfTheVertexSetIsLarge(database);
  }

  /**
   * The walk is the whole cost of a query on this type, and unlike the scan it replaces there is nothing to point
   * an operator at: no index can exist over a type with no records, and the vertex types are not narrowed by the
   * edge type, so an unrelated one adds its buckets to every walk. A {@code count(*)} in particular drops from the
   * O(1) cached counter to this, which is a complexity class an operator would otherwise discover on a large graph
   * by watching a query they believe is free. Throttled through {@link CommandWarnings} and sized on the same
   * threshold as the type-scan warning in {@link FetchFromTypeExecutionStep}, so the two speak about "large" in the
   * same terms (PR #7478 review).
   */
  private void warnIfTheVertexSetIsLarge(final DatabaseInternal database) {
    if (vertexSetSize <= FetchFromTypeExecutionStep.LARGE_TYPE_BYTES)
      return;

    final int counter = CommandWarnings.occurrencesWhenDue(edgeTypeName + ".lightweightWalk");
    if (counter > 0)
      LogManager.instance().log(this, Level.WARNING,
          "Query on LIGHTWEIGHT edge type '%s' in database '%s' walked the whole vertex set (%s) %d times, because a "
              + "lightweight edge is stored inside its two vertices and has no record to scan or index. Anchor the "
              + "query on the vertices instead (e.g. out('%s') from the vertex type that holds them) where you can",
          edgeTypeName, database.getName(), FileUtils.getSizeAsString(vertexSetSize), counter, edgeTypeName);
  }

  /** Size on disk of a bucket's file, or 0 when it cannot be read - this only feeds a warning threshold. */
  private static long sizeOf(final DatabaseInternal database, final int fileId) {
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) database.getFileManager().getFile(fileId);
      return file != null ? file.getSize() : 0;
    } catch (final IOException e) {
      return 0;
    }
  }

  /** Refuses unless the caller may read {@code type} and every type inheriting from it. */
  private static void checkAccessOnTypeAndSubTypes(final DatabaseInternal database, final DocumentType type) {
    if (type == null)
      return;

    SecurityHelper.checkAccessOnType(database, type, SecurityDatabaseUser.ACCESS.READ_RECORD);
    for (final DocumentType subType : type.getSubTypes())
      checkAccessOnTypeAndSubTypes(database, subType);
  }

  /**
   * How many edges of {@code edgeTypeName} the walk would return, without building a {@link Result} for any of
   * them. Shares {@link #fetchNext} with the scan itself, so a count and a scan of the same type cannot disagree -
   * which is the whole point of routing {@link CountFromTypeStep} here rather than letting it read a record count
   * that is 0 by construction (issue #7477).
   */
  static long countEdgesOf(final CommandContext context, final String edgeTypeName) {
    final FetchFromLightweightEdgeTypeStep walk = new FetchFromLightweightEdgeTypeStep(edgeTypeName, context);
    walk.init(context);

    long count = 0;
    while (walk.fetchNext()) {
      walk.nextRecord = null;
      walk.nextEdge = null;
      ++count;
    }
    return count;
  }

  /** Advances to the next row to serve, leaving it in {@link #nextRecord} or {@link #nextEdge}. */
  private boolean fetchNext() {
    if (nextRecord != null || nextEdge != null)
      return true;

    if (typeRecords.hasNext()) {
      nextRecord = typeRecords.next();
      return true;
    }

    while (true) {
      while (currentVertexEdges.hasNext()) {
        final Edge edge = currentVertexEdges.next();
        // Only the entries with no record of their own: the ones that do were already served from the bucket scan
        // above, and serving them again here would double every edge of a mixed type.
        if (edge.getIdentity() != null && edge.getIdentity().getPosition() < 0) {
          nextEdge = edge;
          return true;
        }
      }

      if (!vertices.hasNext())
        return false;

      final Record record = vertices.next();
      if (record instanceof Vertex vertex)
        currentVertexEdges = vertex.getEdges(Vertex.DIRECTION.OUT, edgeTypeName).iterator();
    }
  }

  @Override
  public void reset() {
    inited = false;
    typeRecords = null;
    vertices = null;
    vertexSetSize = 0;
    currentVertexEdges = Collections.emptyIterator();
    nextEdge = null;
    nextRecord = null;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append(ExecutionStepInternal.getIndent(depth, indent));
    builder.append("+ FETCH LIGHTWEIGHT EDGES OF TYPE ").append(edgeTypeName).append(" (scan of the vertices)");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
