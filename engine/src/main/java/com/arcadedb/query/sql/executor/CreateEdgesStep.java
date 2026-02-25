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
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.EdgeType;

import java.util.*;

/**
 * Created by luigidellaquila on 28/11/16.
 */
public class CreateEdgesStep extends AbstractExecutionStep {

  private final Identifier  targetClass;
  private final Identifier  targetBucket;
  private final String      uniqueIndexName;
  private final Identifier  fromAlias;
  private final Identifier  toAlias;
  private final boolean     ifNotExists;
  private final boolean     unidirectional;
  // operation stuff
  private       Iterator    fromIter;
  private       Iterator    toIterator;
  private       Vertex      currentFrom;
  private       Vertex      currentTo;
  private       MutableEdge edgeToUpdate; // for upsert
  private       boolean     finished = false;
  private final List        toList   = new ArrayList<>();
  private       Index       uniqueIndex;

  private boolean initiated = false;

  public CreateEdgesStep(
      final Identifier targetClass,
      final Identifier targetBucketName,
      final String uniqueIndex,
      final Identifier fromAlias,
      final Identifier toAlias,
      final boolean unidirectional,
      final boolean ifNotExists,
      final CommandContext context) {
    super(context);
    this.targetClass = targetClass;
    this.targetBucket = targetBucketName;
    this.uniqueIndexName = uniqueIndex;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
    this.unidirectional = unidirectional;
    this.ifNotExists = ifNotExists;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    init();
    return new CreateRecordResultSet() {
      private int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && currentFrom != null && !toList.isEmpty()
            && (toIterator.hasNext() || fromIter.hasNext()));
      }

      @Override
      public Result next() {
        return next(null);
      }

      @Override
      public Result next(final Object[] properties) {
        if (currentTo == null) {
          loadNextFromTo();
          if (edgeToUpdate != null && !ifNotExists) {
            currentTo = null;
            currentBatch++;
            return new UpdatableResult(edgeToUpdate);
          }
        }

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {

          if (finished || currentBatch >= nRecords)
            throw new NoSuchElementException();

          if (currentTo == null)
            throw new CommandExecutionException("Invalid TO vertex for edge");

          if (ifNotExists) {
            if (context.getDatabase().getGraphEngine()
                .isVertexConnectedTo((VertexInternal) currentFrom, currentTo, Vertex.DIRECTION.OUT, targetClass.getStringValue())) {

              for (Edge existingEdge : context.getDatabase().getGraphEngine()
                  .getEdges((VertexInternal) currentFrom, Vertex.DIRECTION.OUT, targetClass.getStringValue())) {

                if (existingEdge.getIn().equals(currentTo)) {
                  currentTo = null;
                  currentBatch++;
                  return new UpdatableResult(existingEdge.modify());
                }
              }
            }
          }

          final String target = targetBucket != null ? "bucket:" + targetBucket.getStringValue() : targetClass.getStringValue();

          if (unidirectional && ((EdgeType) context.getDatabase().getSchema().getType(target)).isBidirectional())
            throw new CommandExecutionException("Cannot create unidirectional edge on a bidirectional edge type");

          final MutableEdge edge;
          if (edgeToUpdate != null) {
            edge = edgeToUpdate;
          } else {
            // Create unsaved edge - it will be saved later by SaveElementStep
            // This allows UpdateContentStep and ApplyDefaultsStep to run before save
            final EdgeType edgeType = (EdgeType) context.getDatabase().getSchema().getType(targetClass.getStringValue());
            edge = new MutableEdge(context.getDatabase(), edgeType, currentFrom.getIdentity(), currentTo.getIdentity());

            // Set properties if provided
            if (properties != null && properties.length > 0) {
              edge.set(properties);
            }

            // Store vertices for later connection after save
            context.setVariable("$__ARCADEDB_EDGE_FROM_" + System.identityHashCode(edge), currentFrom);
            context.setVariable("$__ARCADEDB_EDGE_TO_" + System.identityHashCode(edge), currentTo);
            context.setVariable("$__ARCADEDB_EDGE_TYPE_" + System.identityHashCode(edge), edgeType);
          }

          final UpdatableResult result = new UpdatableResult(edge);
          currentTo = null;
          currentBatch++;
          return result;
        } finally {
          if (context.isProfiling()) {
            cost += (System.nanoTime() - begin);
          }
        }
      }
    };
  }

  private void init() {
    synchronized (this) {
      if (this.initiated)
        return;

      initiated = true;
    }
    Object fromValues = context.getVariable(fromAlias.getStringValue());
    if (fromValues instanceof Iterable iterable && !(fromValues instanceof Identifiable))
      fromValues = iterable.iterator();
    else if (!(fromValues instanceof Iterator))
      fromValues = Set.of(fromValues).iterator();

    if (fromValues instanceof InternalResultSet set)
      fromValues = set.copy();

    Object toValues = context.getVariable(toAlias.getStringValue());
    if (toValues instanceof Iterable iterable && !(toValues instanceof Identifiable))
      toValues = iterable.iterator();
    else if (!(toValues instanceof Iterator))
      toValues = Set.of(toValues).iterator();

    if (toValues instanceof InternalResultSet set)
      toValues = set.copy();

    fromIter = (Iterator) fromValues;
    if (fromIter instanceof ResultSet set) {
      try {
        set.reset();
      } catch (final Exception ignore) {
      }
    }

    final Iterator toIter = (Iterator) toValues;

    if (toIter instanceof ResultSet set) {
      try {
        set.reset();
      } catch (final Exception ignore) {
      }
    }

    while (toIter != null && toIter.hasNext())
      toList.add(toIter.next());

    toIterator = toList.iterator();

    currentFrom = fromIter != null && fromIter.hasNext() ? asVertex(fromIter.next()) : null;

    if (uniqueIndexName != null) {
      final DatabaseInternal database = context.getDatabase();
      uniqueIndex = database.getSchema().getIndexByName(uniqueIndexName);
      if (uniqueIndex == null)
        throw new CommandExecutionException("Index not found for upsert: " + uniqueIndexName);
    }
  }

  protected void loadNextFromTo() {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      edgeToUpdate = null;
      this.currentTo = null;
      if (!toIterator.hasNext()) {
        toIterator = toList.iterator();
        if (!fromIter.hasNext()) {
          finished = true;
          return;
        }
        currentFrom = asVertex(fromIter.next());
      }
      if (toIterator.hasNext() || (toList.size() > 0 && fromIter.hasNext())) {
        if (currentFrom == null) {
          if (!fromIter.hasNext()) {
            finished = true;
            return;
          }
        }

        final Object obj = toIterator.next();

        currentTo = asVertex(obj);
        if (currentTo == null) {
          throw new CommandExecutionException("Invalid TO vertex for edge");
        }

        if (isUpsert()) {
          final Edge existingEdge = getExistingEdge(currentFrom, currentTo);
          if (existingEdge != null) {
            edgeToUpdate = existingEdge.modify();
          }
        }

      } else {
        this.currentTo = null;
      }
    } finally {
      if (context.isProfiling()) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private Edge getExistingEdge(final Vertex currentFrom, final Vertex currentTo) {
    final RID[] key = new RID[] { currentFrom.getIdentity(), currentTo.getIdentity() };

    final IndexCursor cursor = uniqueIndex.get(key);
    if (cursor.hasNext())
      return cursor.next().asEdge();

    return null;
  }

  private boolean isUpsert() {
    return uniqueIndex != null;
  }

  private Vertex asVertex(Object currentFrom) {
    if (currentFrom instanceof RID iD)
      currentFrom = iD.getRecord();

    if (currentFrom instanceof Result result) {
      // First try to get the vertex directly
      final var vertexOpt = result.getVertex();
      if (vertexOpt.isPresent()) {
        currentFrom = vertexOpt.get();
      } else {
        // If no vertex, try to extract @rid property (for projection results) - issue #3315
        final Object rid = result.getProperty("@rid");
        if (rid != null)
          currentFrom = rid;
        else
          throw new CommandExecutionException("Invalid vertex for edge creation: " + result);
      }
    }

    if (currentFrom instanceof Vertex vertex)
      return vertex;
    else if (currentFrom instanceof Document document)
      return document.asVertex();
    else if (RID.is(currentFrom))
      return new RID(getContext().getDatabase(), currentFrom.toString()).asVertex();

    throw new CommandExecutionException(
        "Invalid vertex for edge creation: " + (currentFrom == null ? "null" : currentFrom.toString()));
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result +=
        spaces + "       CREATE EDGE " + targetClass + " FROM x TO y " + (unidirectional ? "UNIDIRECTIONAL" : "BIDIRECTIONAL");
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";

    if (targetBucket != null)
      result += "\n" + spaces + "       (target cluster " + targetBucket + ")";

    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new CreateEdgesStep(
        targetClass == null ? null : targetClass.copy(),
        targetBucket == null ? null : targetBucket.copy(),
        uniqueIndexName,
        fromAlias == null ? null : fromAlias.copy(),
        toAlias == null ? null : toAlias.copy(),
        unidirectional,
        ifNotExists,
        context);
  }
}
