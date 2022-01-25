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
package com.arcadedb.serializer;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class JsonGraphSerializer extends JsonSerializer {

  private boolean    expandVertexEdges = false;
  private JSONObject sharedJson        = null;

  public JSONObject serializeGraphElement(final Document document) {
    if (sharedJson != null)
      return serializeGraphElement(document, sharedJson);
    return serializeGraphElement(document, new JSONObject());
  }

  public JSONObject serializeGraphElement(final Document document, final JSONObject object) {
    final JSONObject properties;

    if (object.has("p")) {
      // REUSE PROPERTY OBJECT
      properties = object.getJSONObject("p");
      properties.clear();
    } else
      properties = new JSONObject();

    object.clear();
    object.put("p", properties);

    object.put("r", document.getIdentity().toString());
    object.put("t", document.getTypeName());

    for (String p : document.getPropertyNames()) {
      Object value = document.get(p);

      if (value instanceof Document)
        value = serializeGraphElement((Document) value, new JSONObject());
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof Document)
            o = serializeGraphElement((Document) o, new JSONObject());
          list.add(o);
        }
        value = list;
      }
      properties.put(p, value);
    }

    setMetadata(document, object);

    return object;
  }

  public boolean isExpandVertexEdges() {
    return expandVertexEdges;
  }

  public JsonGraphSerializer setExpandVertexEdges(final boolean expandVertexEdges) {
    this.expandVertexEdges = expandVertexEdges;
    return this;
  }

  private void setMetadata(final Document document, final JSONObject object) {
    if (document instanceof Vertex) {
      final Vertex vertex = ((Vertex) document);

      if (expandVertexEdges) {
        final JSONArray outEdges = new JSONArray();
        for (Edge e : vertex.getEdges(Vertex.DIRECTION.OUT))
          outEdges.put(e.getIdentity().toString());
        object.put("o", outEdges);

        final JSONArray inEdges = new JSONArray();
        for (Edge e : vertex.getEdges(Vertex.DIRECTION.IN))
          inEdges.put(e.getIdentity().toString());
        object.put("i", inEdges);
      } else {
        object.put("i", vertex.countEdges(Vertex.DIRECTION.IN, null));
        object.put("o", vertex.countEdges(Vertex.DIRECTION.OUT, null));
      }

    } else if (document instanceof Edge) {
      final Edge edge = ((Edge) document);
      object.put("i", edge.getIn());
      object.put("o", edge.getOut());
    }
  }

  public JsonGraphSerializer setSharedJson(final JSONObject json) {
    sharedJson = json;
    return this;
  }
}
