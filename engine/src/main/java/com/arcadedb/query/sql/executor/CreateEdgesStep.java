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
 */
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.parser.Batch;
import com.arcadedb.query.sql.parser.Identifier;

import java.util.*;

/**
 * Created by luigidellaquila on 28/11/16.
 */
public class CreateEdgesStep extends AbstractExecutionStep {
  private final Identifier targetClass;
  private final Identifier targetCluster;
  private final Identifier fromAlias;
  private final Identifier toAlias;
  private final boolean    ifNotExists;

  //operation stuff
  Iterator fromIter;
  Iterator toIterator;
  Vertex   currentFrom;
  final   List    toList = new ArrayList<>();
  private boolean inited = false;
  private long    cost   = 0;

  public CreateEdgesStep(final Identifier targetClass, final Identifier targetClusterName, final Identifier fromAlias, final Identifier toAlias,
      final boolean ifNotExists, final Number wait, final Number retry, final Batch batch, final CommandContext ctx, final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.targetCluster = targetClusterName;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
    this.ifNotExists = ifNotExists;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();
    return new ResultSet() {
      int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && (toIterator.hasNext() || (!toList.isEmpty() && fromIter.hasNext())));
      }

      @Override
      public Result next() {
        if (!toIterator.hasNext()) {
          toIterator = toList.iterator();
          if (!fromIter.hasNext()) {
            throw new IllegalStateException();
          }
          currentFrom = fromIter.hasNext() ? asVertex(fromIter.next()) : null;
        }
        if (currentBatch < nRecords && (toIterator.hasNext() || (!toList.isEmpty() && fromIter.hasNext()))) {

          if (currentFrom == null) {
            throw new CommandExecutionException("Invalid FROM vertex for edge");
          }

          Object obj = toIterator.next();
          long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
            Vertex currentTo = asVertex(obj);
            if (currentTo == null) {
              throw new CommandExecutionException("Invalid TO vertex for edge");
            }

            if (ifNotExists) {
              if (ctx.getDatabase().getGraphEngine()
                  .isVertexConnectedTo((VertexInternal) currentFrom, currentTo, Vertex.DIRECTION.OUT, targetClass.getStringValue()))
                // SKIP CREATING EDGE
                return null;
            }

            final MutableEdge edge = currentFrom.newEdge(targetClass.getStringValue(), currentTo, true);

            UpdatableResult result = new UpdatableResult(edge);
            result.setElement(edge);
            currentBatch++;
            return result;
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
        } else {
          throw new IllegalStateException();
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
    if (fromValues instanceof Iterable) {
      fromValues = ((Iterable) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }

    Object toValues = ctx.getVariable(toAlias.getStringValue());
    if (toValues instanceof Iterable) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    fromIter = (Iterator) fromValues;
    if (fromIter instanceof ResultSet) {
      try {
        ((ResultSet) fromIter).reset();
      } catch (Exception ignore) {
      }
    }

    Iterator toIter = (Iterator) toValues;

    while (toIter != null && toIter.hasNext()) {
      toList.add(toIter.next());
    }

    toIterator = toList.iterator();
    if (toIter instanceof ResultSet) {
      try {
        ((ResultSet) toIter).reset();
      } catch (Exception ignore) {
      }
    }

    currentFrom = fromIter != null && fromIter.hasNext() ? asVertex(fromIter.next()) : null;

  }

  private Vertex asVertex(Object currentFrom) {
    if (currentFrom instanceof RID) {
      currentFrom = ((RID) currentFrom).getRecord();
    }
    if (currentFrom instanceof Result && ((Result) currentFrom).isVertex()) {
      return (Vertex) ((Result) currentFrom).getElement().get();
    }
    if (currentFrom instanceof Vertex) {
      return (Vertex) currentFrom;
    }
    return null;
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
      result += "\n" + spaces + "       (target bucket " + targetCluster + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
