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
  private boolean useCollectionSize  = false;
  private boolean includeVertexEdges = true;
  private boolean useVertexEdgeSize  = true;

  public JSONObject serializeDocument(final Document document) {
    final JSONObject object = new JSONObject();

    object.put("@rid", document.getIdentity().toString());
    object.put("@type", document.getTypeName());

    for (String p : document.getPropertyNames()) {
      Object value = document.get(p);

      if (value instanceof Document)
        value = serializeDocument((Document) value);
      else if (value instanceof Collection) {
        if (useCollectionSize) {
          value = ((Collection) value).size();
        } else {
          final List<Object> list = new ArrayList<>();
          for (Object o : (Collection) value) {
            if (o instanceof Document)
              o = serializeDocument((Document) o);
            list.add(o);
          }
          value = list;
        }
      }
      object.put(p, value);
    }

    setMetadata(document, object);

    return object;
  }

  public JSONObject serializeResult(final Result result) {
    final JSONObject object = new JSONObject();

    if (result.isElement()) {
      final Document document = result.toElement();
      object.put("@rid", document.getIdentity().toString());
      object.put("@type", document.getTypeName());

      setMetadata(document, object);
    }

    for (String p : result.getPropertyNames()) {
      Object value = result.getProperty(p);

      if (value instanceof Document)
        value = serializeDocument((Document) value);
      else if (value instanceof Result)
        value = serializeResult((Result) value);
      else if (value instanceof Collection) {
        if (useCollectionSize) {
          value = ((Collection) value).size();
        } else {
          final List<Object> list = new ArrayList<>();
          for (Object o : (Collection) value) {
            if (o instanceof Document)
              o = serializeDocument((Document) o);
            list.add(o);
          }
          value = list;
        }
      }
      object.put(p, value);
    }

    return object;
  }

  public boolean isUseCollectionSize() {
    return useCollectionSize;
  }

  public JsonSerializer setUseCollectionSize(final boolean useCollectionSize) {
    this.useCollectionSize = useCollectionSize;
    return this;
  }

  public boolean isIncludeVertexEdges() {
    return includeVertexEdges;
  }

  public JsonSerializer setIncludeVertexEdges(final boolean includeVertexEdges) {
    this.includeVertexEdges = includeVertexEdges;
    return this;
  }

  private void setMetadata(final Document document, final JSONObject object) {
    if (document instanceof Vertex) {
      if (includeVertexEdges) {
        final Vertex vertex = ((Vertex) document);

        object.put("@cat", "v");
        if (useVertexEdgeSize) {
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
      }
    } else if (document instanceof Edge) {
      final Edge edge = ((Edge) document);
      object.put("@cat", "e");
      object.put("@in", edge.getIn());
      object.put("@out", edge.getOut());
    } else
      object.put("@cat", "d");

  }
}
