/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionConfigurableAbstract;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Hi-level function that return the label for both edges and vertices. The label could be bound to
 * the class name.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionLabel extends OSQLFunctionConfigurableAbstract {
  public static final String NAME = "label";

  public OSQLFunctionLabel() {
    super(NAME, 0, 0);
  }

  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParameters,
      final OCommandContext iContext) {

    return OGraphCommandExecutorSQLFactory.runWithAnyGraph(
        new OGraphCommandExecutorSQLFactory.GraphCallBack<Object>() {
          @Override
          public Object call(final OrientBaseGraph graph) {
            if (iCurrentResult != null) {
              return OSQLEngine.foreachRecord(
                  new OCallable<Object, OIdentifiable>() {
                    @Override
                    public Object call(final OIdentifiable iArgument) {
                      return getLabel(graph, iArgument);
                    }
                  },
                  iCurrentResult,
                  iContext);
            } else return getLabel(graph, iCurrentRecord);
          }
        });
  }

  private Object getLabel(final OrientBaseGraph graph, final OIdentifiable iCurrentRecord) {
    final ODocument rec = iCurrentRecord.getRecord();

    OClass klass = ODocumentInternal.getImmutableSchemaClass(rec);
    if (klass == null && ODatabaseRecordThreadLocal.instance().getIfDefined() != null) {
      ODatabaseRecordThreadLocal.instance().getIfDefined().getMetadata().reload();
      klass = rec.getSchemaClass();
    }
    if (klass.isVertexType()) {
      // VERTEX
      final OrientVertex vertex = graph.getVertex(iCurrentRecord);
      return vertex.getLabel();

    } else if (klass.isEdgeType()) {
      // EDGE
      final OrientEdge edge = graph.getEdge(iCurrentRecord);
      return edge.getLabel();

    } else
      throw new OCommandExecutionException(
          "Invalid record: is neither a vertex nor an edge. Found class: " + klass);
  }

  public String getSyntax() {
    return "Syntax error: label()";
  }
}
