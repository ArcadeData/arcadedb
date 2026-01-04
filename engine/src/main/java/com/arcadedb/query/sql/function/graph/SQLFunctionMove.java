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
package com.arcadedb.query.sql.function.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.function.SQLFunctionConfigurableAbstract;
import com.arcadedb.utility.FileUtils;

import java.util.*;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class SQLFunctionMove extends SQLFunctionConfigurableAbstract {
  protected SQLFunctionMove(final String iName) {
    super(iName);
  }

  protected abstract Object move(final Database db, final Identifiable iRecord, final String[] iLabels);

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

    return SQLQueryEngine.foreachRecord(iArgument -> move(context.getDatabase(), iArgument, labels), self, context);
  }

  protected Object v2v(final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels) {
    if (iRecord != null) {
      final Document rec = (Document) iRecord.getRecord();
      if (rec instanceof Vertex vertex)
        return vertex.getVertices(iDirection, iLabels);
    }
    return null;
  }

  protected Object v2e(final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels) {
    final Document rec = (Document) iRecord.getRecord();
    if (rec instanceof Vertex vertex)
      return vertex.getEdges(iDirection, iLabels);
    return null;

  }

  protected Object e2v(final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels) {
    final Document rec = (Document) iRecord.getRecord();
    if (rec instanceof Edge edge) {
      if (iDirection == Vertex.DIRECTION.BOTH) {
        var results = new ArrayList<Vertex>();
        results.add(edge.getOutVertex());
        results.add(edge.getInVertex());
        return results;
      }
      return edge.getVertex(iDirection);
    }

    return null;
  }
}
