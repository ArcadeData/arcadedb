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
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.parser.Identifier;

import java.util.*;

/**
 * Created by luigidellaquila on 28/11/16.
 */
public class CreateEdgesStep extends AbstractExecutionStep {

  private final Identifier targetClass;
  private final Identifier targetCluster;
  private final String     uniqueIndexName;
  private final Identifier fromAlias;
  private final Identifier toAlias;
  private final boolean    ifNotExists;
  private final Number     wait;
  private final Number     retry;

  // operation stuff
  private Iterator    fromIter;
  private Iterator    toIterator;
  private Vertex      currentFrom;
  private Vertex      currentTo;
  private MutableEdge edgeToUpdate; // for upsert
  private boolean     finished = false;
  private final List        toList   = new ArrayList<>();
  private Index       uniqueIndex;

  private boolean inited = false;

  private long cost = 0;

  public CreateEdgesStep(Identifier targetClass, Identifier targetClusterName, String uniqueIndex, Identifier fromAlias, Identifier toAlias,
      final boolean ifNotExists, Number wait, Number retry, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.targetCluster = targetClusterName;
    this.uniqueIndexName = uniqueIndex;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
    this.ifNotExists = ifNotExists;
    this.wait = wait;
    this.retry = retry;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();
    return new ResultSet() {
      private int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && (toIterator.hasNext() || (!toList.isEmpty() && fromIter.hasNext())));
      }

      @Override
      public Result next() {
        if (currentTo == null) {
          loadNextFromTo();
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          if (finished || currentBatch >= nRecords) {
            throw new IllegalStateException();
          }
          if (currentTo == null) {
            throw new CommandExecutionException("Invalid TO vertex for edge");
          }

          if (ifNotExists) {
            if (ctx.getDatabase().getGraphEngine()
                .isVertexConnectedTo((VertexInternal) currentFrom, currentTo, Vertex.DIRECTION.OUT, targetClass.getStringValue()))
              // SKIP CREATING EDGE
              return null;
          }

          final MutableEdge edge = edgeToUpdate != null ? edgeToUpdate : currentFrom.newEdge(targetClass.getStringValue(), currentTo, true);

          final UpdatableResult result = new UpdatableResult(edge);
          result.setElement(edge);
          currentTo = null;
          currentBatch++;
          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void init() {
    synchronized (this) {
      if (this.inited) {
        return;
      }
      inited = true;
    }
    Object fromValues = ctx.getVariable(fromAlias.getStringValue());
    if (fromValues instanceof Iterable && !(fromValues instanceof Identifiable)) {
      fromValues = ((Iterable) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }
    if (fromValues instanceof InternalResultSet) {
      fromValues = ((InternalResultSet) fromValues).copy();
    }

    Object toValues = ctx.getVariable(toAlias.getStringValue());
    if (toValues instanceof Iterable && !(toValues instanceof Identifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }
    if (toValues instanceof InternalResultSet) {
      toValues = ((InternalResultSet) toValues).copy();
    }

    fromIter = (Iterator) fromValues;
    if (fromIter instanceof ResultSet) {
      try {
        ((ResultSet) fromIter).reset();
      } catch (Exception ignore) {
      }
    }

    Iterator toIter = (Iterator) toValues;

    if (toIter instanceof ResultSet) {
      try {
        ((ResultSet) toIter).reset();
      } catch (Exception ignore) {
      }
    }
    while (toIter != null && toIter.hasNext()) {
      toList.add(toIter.next());
    }

    toIterator = toList.iterator();

    currentFrom = fromIter != null && fromIter.hasNext() ? asVertex(fromIter.next()) : null;

    if (uniqueIndexName != null) {
      final DatabaseInternal database = ctx.getDatabase();
      uniqueIndex = database.getSchema().getIndexByName(uniqueIndexName);
      if (uniqueIndex == null) {
        throw new CommandExecutionException("Index not found for upsert: " + uniqueIndexName);
      }
    }
  }

  protected void loadNextFromTo() {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      edgeToUpdate = null;
      this.currentTo = null;
      if (!toIterator.hasNext()) {
        toIterator = toList.iterator();
        if (!fromIter.hasNext()) {
          finished = true;
          return;
        }
        currentFrom = fromIter.hasNext() ? asVertex(fromIter.next()) : null;
      }
      if (toIterator.hasNext() || (toList.size() > 0 && fromIter.hasNext())) {
        if (currentFrom == null) {
          if (!fromIter.hasNext()) {
            finished = true;
            return;
          }
        }

        Object obj = toIterator.next();

        currentTo = asVertex(obj);
        if (currentTo == null) {
          throw new CommandExecutionException("Invalid TO vertex for edge");
        }

        if (isUpsert()) {
          Edge existingEdge = getExistingEdge(currentFrom, currentTo);
          if (existingEdge != null) {
            edgeToUpdate = existingEdge.modify();
          }
        }
        return;

      } else {
        this.currentTo = null;
        return;
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private Edge getExistingEdge(Vertex currentFrom, Vertex currentTo) {
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
    if (currentFrom instanceof RID) {
      currentFrom = ((RID) currentFrom).getRecord();
    }
    if (currentFrom instanceof Result) {
      Object from = currentFrom;
      currentFrom = ((Result) currentFrom).getVertex().orElseThrow(() -> new CommandExecutionException("Invalid vertex for edge creation: " + from.toString()));
    }
    if (currentFrom instanceof Vertex) {
      return (Vertex) currentFrom;
    }
    if (currentFrom instanceof Document) {
      Object from = currentFrom;
      return ((Document) currentFrom).asVertex();
    }
    throw new CommandExecutionException("Invalid vertex for edge creation: " + (currentFrom == null ? "null" : currentFrom.toString()));
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result += spaces + "       CREATE EDGE " + targetClass + " FROM x TO y";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (targetCluster != null) {
      result += "\n" + spaces + "       (target cluster " + targetCluster + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new CreateEdgesStep(targetClass == null ? null : targetClass.copy(), targetCluster == null ? null : targetCluster.copy(), uniqueIndexName,
        fromAlias == null ? null : fromAlias.copy(), toAlias == null ? null : toAlias.copy(), ifNotExists, wait, retry, ctx, profilingEnabled);
  }
}
