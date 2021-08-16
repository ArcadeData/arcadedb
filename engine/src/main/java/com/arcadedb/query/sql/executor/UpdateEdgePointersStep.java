/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;

import java.util.Map;
import java.util.Optional;

/**
 * after an update of an edge, this step updates edge pointers on vertices to make the graph consistent again
 */
public class UpdateEdgePointersStep extends AbstractExecutionStep {

  public UpdateEdgePointersStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    ResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        Result result = upstream.next();
        if (result instanceof ResultInternal) {
          handleUpdateEdge(result.getElement().get());
        }
        return result;
      }

      private void updateIn(Result item) {

      }

      private void updateOut(Result item) {

      }

      @Override
      public void close() {
        upstream.close();
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE EDGE POINTERS");
    return result.toString();
  }

  /**
   * handles vertex consistency after an UPDATE EDGE
   *
   * @param record the edge record
   */
  private void handleUpdateEdge(Document record) {
    Object currentOut = record.get("out");
    Object currentIn = record.get("in");

    throw new UnsupportedOperationException();
//    Object prevOut = record.getOriginalValue("out");
//    Object prevIn = record.getOriginalValue("in");
//
//    validateOutInForEdge(record, currentOut, currentIn);
//
//    changeVertexEdgePointer(record, (PIdentifiable) prevIn, (PIdentifiable) currentIn, "in");
//    changeVertexEdgePointer(record, (PIdentifiable) prevOut, (PIdentifiable) currentOut, "out");
  }

  /**
   * updates old and new vertices connected to an edge after out/in update on the edge itself
   *
   * @param edge          the edge
   * @param prevVertex    the previously connected vertex
   * @param currentVertex the currently connected vertex
   * @param direction     the direction ("out" or "in")
   */
  private void changeVertexEdgePointer(Edge edge, Identifiable prevVertex, Identifiable currentVertex, String direction) {
//    if (prevVertex != null && !prevVertex.equals(currentVertex)) {
//      String edgeClassName = edge.getClassName();
//      if (edgeClassName.equalsIgnoreCase("E")) {
//        edgeClassName = "";
//      }
//      String vertexFieldName = direction + "_" + edgeClassName;
//      ODocument prevOutDoc = ((PIdentifiable) prevVertex).getRecord();
//      ORidBag prevBag = prevOutDoc.field(vertexFieldName);
//      if (prevBag != null) {
//        prevBag.remove(edge);
//        prevOutDoc.save();
//      }
//
//      ODocument currentVertexDoc = ((PIdentifiable) currentVertex).getRecord();
//      ORidBag currentBag = currentVertexDoc.field(vertexFieldName);
//      if (currentBag == null) {
//        currentBag = new ORidBag();
//        currentVertexDoc.field(vertexFieldName, currentBag);
//      }
//      currentBag.add(edge);
//    }
  }

  private void validateOutInForEdge(Record record, Object currentOut, Object currentIn) {
//    if (!isRecordInstanceOf(currentOut, "V")) {
//      throw new PCommandExecutionException("Error updating edge: 'out' is not a vertex - " + currentOut + "");
//    }
//    if (!isRecordInstanceOf(currentIn, "V")) {
//      throw new PCommandExecutionException("Error updating edge: 'in' is not a vertex - " + currentIn + "");
//    }
  }

  /**
   * checks if an object is an PIdentifiable and an instance of a particular (schema) class
   *
   * @param iRecord     The record object
   * @param orientClass The schema class
   *
   * @return
   */
  private boolean isRecordInstanceOf(Object iRecord, String orientClass) {
    if (iRecord == null) {
      return false;
    }
    if (!(iRecord instanceof Identifiable)) {
      return false;
    }
    final Document record = ((Identifiable) iRecord).asDocument();
    if (iRecord == null) {
      return false;
    }
    return (record.getTypeName().equals(orientClass));
  }
}
