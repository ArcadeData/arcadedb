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

package com.arcadedb.serializer;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class JsonSerializer {

  public enum GRAPH_MODE {EXCLUDE, COUNT, FULL}

  private GRAPH_MODE graphMode = GRAPH_MODE.COUNT;

  public JSONObject serializeRecord(final Document document) {
    final JSONObject object = new JSONObject();

    object.put("@rid", document.getIdentity().toString());
    object.put("@type", document.getTypeName());

    for (String p : document.getPropertyNames()) {
      Object value = document.get(p);

      if (value instanceof Document)
        value = serializeRecord((Document) value);
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof Document)
            o = serializeRecord((Document) o);
          list.add(o);
        }
        value = list;
      }
      object.put(p, value);
    }

    if (graphMode != GRAPH_MODE.EXCLUDE) {
      if (document instanceof Vertex) {
        final Vertex vertex = ((Vertex) document);

        if (graphMode == GRAPH_MODE.COUNT) {
          object.put("@out", vertex.countEdges(Vertex.DIRECTION.OUT, null));
          object.put("@in", vertex.countEdges(Vertex.DIRECTION.IN, null));

        } else {
          final JSONArray outEdges = new JSONArray();
          for (Edge e : vertex.getEdges(Vertex.DIRECTION.OUT))
            outEdges.put(e.getIdentity().toString());
          object.put("@out", outEdges);

          final JSONArray inEdges = new JSONArray();
          for (Edge e : vertex.getEdges(Vertex.DIRECTION.IN))
            inEdges.put(e.getIdentity().toString());
          object.put("@in", inEdges);
        }

      } else if (document instanceof Edge) {
        if (graphMode == GRAPH_MODE.FULL) {
          final Edge edge = ((Edge) document);
          object.put("@in", edge.getIn());
          object.put("@out", edge.getOut());
        }
      }
    }

    return object;
  }

  public JSONObject serializeResult(final Result record) {
    final JSONObject object = new JSONObject();

    if (record.isElement()) {
      final Document document = record.toElement();
      object.put("@rid", document.getIdentity().toString());
      object.put("@type", document.getTypeName());

      if (graphMode != GRAPH_MODE.EXCLUDE) {
        if (document instanceof Vertex) {
          final Vertex vertex = ((Vertex) document);

          if (graphMode == GRAPH_MODE.COUNT) {
            object.put("@out", vertex.countEdges(Vertex.DIRECTION.OUT, null));
            object.put("@in", vertex.countEdges(Vertex.DIRECTION.IN, null));

          } else {
            final JSONArray outEdges = new JSONArray();
            for (Edge e : vertex.getEdges(Vertex.DIRECTION.OUT))
              outEdges.put(e.getIdentity().toString());
            object.put("@out", outEdges);

            final JSONArray inEdges = new JSONArray();
            for (Edge e : vertex.getEdges(Vertex.DIRECTION.IN))
              inEdges.put(e.getIdentity().toString());
            object.put("@in", inEdges);
          }

        } else if (document instanceof Edge) {
          if (graphMode == GRAPH_MODE.FULL) {
            final Edge edge = ((Edge) document);
            object.put("@in", edge.getIn());
            object.put("@out", edge.getOut());
          }
        }
      }
    }

    for (String p : record.getPropertyNames()) {
      Object value = record.getProperty(p);

      if (value instanceof Document)
        value = serializeRecord((Document) value);
      else if (value instanceof Result)
        value = serializeResult((Result) value);
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof Document)
            o = serializeRecord((Document) o);
          else if (o instanceof Result)
            o = serializeResult((Result) o);
          list.add(o);
        }
        value = list;
      }
      object.put(p, value);
    }

    return object;
  }

  public GRAPH_MODE getGraphMode() {
    return graphMode;
  }

  public JsonSerializer setGraphMode(final GRAPH_MODE graphMode) {
    this.graphMode = graphMode;
    return this;
  }
}
