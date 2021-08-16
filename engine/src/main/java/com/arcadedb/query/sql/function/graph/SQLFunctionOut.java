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

package com.arcadedb.query.sql.function.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Vertex;

import java.util.Collections;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public class SQLFunctionOut extends SQLFunctionMoveFiltered {
  public static final String NAME = "out";

  public SQLFunctionOut() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(final Database graph, final Identifiable iRecord, final String[] iLabels) {
    return v2v(graph, iRecord, Vertex.DIRECTION.OUT, iLabels);
  }

  protected Object move(final Database graph, final Identifiable iRecord, final String[] iLabels,
      Iterable<Identifiable> iPossibleResults) {
    if (iPossibleResults == null) {
      return v2v(graph, iRecord, Vertex.DIRECTION.OUT, iLabels);
    }

    if (!iPossibleResults.iterator().hasNext()) {
      return Collections.emptyList();
    }

    return v2v(graph, iRecord, Vertex.DIRECTION.OUT, iLabels);
  }

  private Object fetchFromIndex(Database graph, Identifiable iFrom, Iterable<Identifiable> iTo, String[] iEdgeTypes) {
    throw new UnsupportedOperationException();
//    String edgeClassName = null;
//    if (iEdgeTypes == null) {
//      edgeClassName = "E";
//    } else if (iEdgeTypes.length == 1) {
//      edgeClassName = iEdgeTypes[0];
//    } else {
//      return null;
//    }
//    OClass edgeClass = graph.getMetadata().getSchema().getClass(edgeClassName);
//    if (edgeClass == null) {
//      return null;
//    }
//    Set<OIndex<?>> indexes = edgeClass.getInvolvedIndexes("out", "in");
//    if (indexes == null || indexes.size() == 0) {
//      return null;
//    }
//    OIndex index = indexes.iterator().next();
//
//    PMultiIterator<PVertex> result = new PMultiIterator<PVertex>();
//    for (PIdentifiable to : iTo) {
//      OCompositeKey key = new OCompositeKey(iFrom, to);
//      Object indexResult = index.get(key);
//      if (indexResult instanceof PIdentifiable) {
//        indexResult = Collections.singleton(indexResult);
//      }
//      Set<PIdentifiable> identities = new HashSet<PIdentifiable>();
//      for (PIdentifiable edge : ((Iterable<OEdge>) indexResult)) {
//        identities.add((PIdentifiable) ((ODocument) edge.getRecord()).rawField("in"));
//      }
//      result.add(identities);
//    }
//
//    return result;
  }

}
