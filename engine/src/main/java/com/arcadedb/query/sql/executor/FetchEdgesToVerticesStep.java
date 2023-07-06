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

import java.util.*;

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

  public FetchEdgesToVerticesStep(final String toAlias, final Identifier targetType, final Identifier targetBucket, final CommandContext context,
      final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.toAlias = toAlias;
    this.targetType = targetType;
    this.targetBucket = targetBucket;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    init();

    return new ResultSet() {
      int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && nextEdge != null);
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        final Edge edge = nextEdge;
        fetchNextEdge();
        final ResultInternal result = new ResultInternal(edge);
        currentBatch++;
        return result;
      }

      @Override
      public void close() {
        if (toIter instanceof ResultSet) {
          ((ResultSet) toIter).close();
        }
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

    toValues = context.getVariable(toAlias);
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
      final Edge edge = this.currentToEdgesIter.next();
      if (matchesType(edge) && matchesBucket(edge)) {
        this.nextEdge = edge;
        return;
      }
    }
  }

  private boolean matchesBucket(final Edge edge) {
    if (targetBucket == null)
      return true;

    final int bucketId = edge.getIdentity().getBucketId();
    final String bucketName = context.getDatabase().getSchema().getBucketById(bucketId).getName();
    return bucketName.equals(targetBucket.getStringValue());
  }

  private boolean matchesType(final Edge edge) {
    if (targetType == null)
      return true;

    return edge.getTypeName().equals(targetType.getStringValue());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
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
