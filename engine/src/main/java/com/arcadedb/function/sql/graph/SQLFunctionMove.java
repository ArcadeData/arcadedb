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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.CSRVertexIterable;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.function.sql.SQLFunctionConfigurableAbstract;
import com.arcadedb.utility.FileUtils;

import java.util.ArrayList;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class SQLFunctionMove extends SQLFunctionConfigurableAbstract {

  protected SQLFunctionMove(final String iName) {
    super(iName);
  }

  protected abstract Object move(final Database db, final Identifiable iRecord, final String[] iLabels,
      final CommandContext context);

  public String getSyntax() {
    return "Syntax error: " + name + "([<labels>])";
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
      final Object[] iParameters, final CommandContext context) {

    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null)
      labels = MultiValue.array(iParameters, String.class, FileUtils::getStringContent);
    else
      labels = null;

    return SQLQueryEngine.foreachRecord(iArgument -> move(context.getDatabase(), iArgument, labels, context), self, context);
  }

  protected Object v2v(final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels, final CommandContext context) {
    if (iRecord != null) {
      final Document rec = (Document) iRecord.getRecord();
      if (rec instanceof Vertex vertex) {
        final Database database = vertex.getDatabase();
        // A VIEW ONLY ACCELERATES: ITS REVERSE INDEX HOLDS THE INCOMING SIDE OF A UNIDIRECTIONAL TYPE, WHICH A FUNCTION
        // CALLED ON ITS OWN MUST NOT ANSWER, OR in()/both() WOULD RETURN DIFFERENT ROWS WITH AND WITHOUT A VIEW AND
        // DISAGREE WITH inE() AND THE VERTEX API (ISSUE #8939). A PATTERN WALK ANSWERS IT, VIEW OR NOT (ISSUE #8625)
        final boolean storedSideOnly = iDirection != Vertex.DIRECTION.OUT && !IncomingEdgeLookup.isWalkingPattern()
            && IncomingEdgeLookup.isAnyUnidirectional(database.getSchema(), iLabels != null ? iLabels : new String[0]);
        final GraphTraversalProvider provider =
            storedSideOnly ? null : GraphTraversalProviderRegistry.findProvider(database, iLabels);
        if (provider != null) {
          final int nodeId = provider.getNodeId(vertex.getIdentity());
          if (nodeId >= 0) {
            final int[] neighborIds = provider.getNeighborIds(nodeId, iDirection, iLabels);
            markCSRAccelerated(context);
            return new CSRVertexIterable(provider, neighborIds);
          }
        }
        // A unidirectional type stores no incoming side: a pattern walk (SQL MATCH) asks for it anyway, so answer it
        // there (issue #8625). Called on its own the function reads what the vertex stores, as the vertex API does
        if (IncomingEdgeLookup.isWalkingPattern() && IncomingEdgeLookup.isNeeded(context, database, iDirection, iLabels))
          return (Iterable<Vertex>) () -> IncomingEdgeLookup.getVertices(context, vertex, iDirection, iLabels);
        return vertex.getVertices(iDirection, iLabels);
      }
    }
    return null;
  }

  protected static void markCSRAccelerated(final CommandContext context) {
    if (context != null)
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
  }

  // Note: v2e always uses the OLTP path (Vertex.getEdges) because CSR stores only neighbor node IDs,
  // not edge RIDs. Unlike v2v, there is no CSR acceleration possible for edge-returning functions.
  protected Object v2e(final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels, final CommandContext context) {
    if (iRecord == null)
      return null;
    final Document rec = (Document) iRecord.getRecord();
    if (rec instanceof Vertex vertex) {
      if (IncomingEdgeLookup.isWalkingPattern()
          && IncomingEdgeLookup.isNeeded(context, vertex.getDatabase(), iDirection, iLabels))
        return (Iterable<Edge>) () -> IncomingEdgeLookup.getEdges(context, vertex, iDirection, iLabels);
      return vertex.getEdges(iDirection, iLabels);
    }
    return null;
  }

  protected Object e2v(final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels) {
    if (iRecord == null)
      return null;
    final Document rec = (Document) iRecord.getRecord();
    if (rec instanceof Edge edge) {
      // Tolerate a ghost edge (dangling segment pointer whose backing edge record is gone): it has no
      // resolvable endpoints, so return an empty/null result instead of throwing RecordNotFoundException.
      try {
        if (iDirection == Vertex.DIRECTION.BOTH) {
          var results = new ArrayList<Vertex>();
          results.add(edge.getOutVertex());
          results.add(edge.getInVertex());
          return results;
        }
        return edge.getVertex(iDirection);
      } catch (final RecordNotFoundException e) {
        // Ghost edge: backing record missing; no endpoint to resolve.
        GhostEdgeReporter.reportSkipped(e);
        return iDirection == Vertex.DIRECTION.BOTH ? new ArrayList<>() : null;
      }
    }

    return null;
  }
}
