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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.parser.Identifier;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 21/02/17.
 */
public class FetchEdgesToVerticesStep extends AbstractExecutionStep {
  private final String     toAlias;
  private final Identifier targetBucket;
  private final Identifier targetType;

  private boolean        inited = false;
  private Iterator       toIter;
  private Edge           nextEdge;
  private Iterator<Edge> currentToEdgesIter;

  public FetchEdgesToVerticesStep(String toAlias, Identifier targetType, Identifier targetBucket, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.toAlias = toAlias;
    this.targetType = targetType;
    this.targetBucket = targetBucket;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();

    return new ResultSet() {
      int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && nextEdge != null);
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        Edge edge = nextEdge;
        fetchNextEdge();
        ResultInternal result = new ResultInternal();
        result.setElement(edge);
        currentBatch++;
        return result;
      }

      @Override
      public void close() {
        if (toIter instanceof ResultSet) {
          ((ResultSet) toIter).close();
        }
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

    Object toValues;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof Identifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    this.toIter = (Iterator) toValues;

    fetchNextEdge();
  }

  private void fetchNextEdge() {
    this.nextEdge = null;
    while (true) {
      while (this.currentToEdgesIter == null || !this.currentToEdgesIter.hasNext()) {
        if (this.toIter == null) {
          return;
        }
        if (this.toIter.hasNext()) {
          Object from = toIter.next();
          if (from instanceof Result) {
            from = ((Result) from).toElement();
          }
          if (from instanceof Identifiable && !(from instanceof Record)) {
            from = ((Identifiable) from).getRecord();
          }
          if (from instanceof Vertex) {
            currentToEdgesIter = ((Vertex) from).getEdges(Vertex.DIRECTION.IN).iterator();
          } else {
            throw new CommandExecutionException("Invalid vertex: " + from);
          }
        } else {
          return;
        }
      }
      Edge edge = this.currentToEdgesIter.next();
      if (matchesClass(edge) && matchesCluster(edge)) {
        this.nextEdge = edge;
        return;
      }
    }
  }

  private boolean matchesCluster(Edge edge) {
    if (targetBucket == null) {
      return true;
    }
    int bucketId = edge.getIdentity().getBucketId();
    String bucketName = ctx.getDatabase().getSchema().getBucketById(bucketId).getName();
    return bucketName.equals(targetBucket.getStringValue());
  }

  private boolean matchesClass(Edge edge) {
    if (targetType == null) {
      return true;
    }
    return edge.getTypeName().equals(targetType.getStringValue());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + toAlias + "\n";
    result += spaces + "       FETCH EDGES TO x";
    if (targetType != null) {
      result += "\n" + spaces + "       (target type " + targetType + ")";
    }
    if (targetBucket != null) {
      result += "\n" + spaces + "       (target bucket " + targetBucket + ")";
    }
    return result;
  }
}
