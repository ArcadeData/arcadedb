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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    final RID rid = document.getIdentity();
    if (rid != null)
      object.put("r", rid.toString());
    object.put("t", document.getTypeName());

    for (final Map.Entry<String, Object> prop : document.toMap(false).entrySet()) {
      Object value = prop.getValue();

      if (value != null) {
        if (value instanceof Document document1)
          value = serializeGraphElement(document1, new JSONObject());
        else if (value instanceof Collection collection) {
          final List<Object> list = new ArrayList<>();
          for (Object o : collection) {
            if (o instanceof Document document1)
              o = serializeGraphElement(document1, new JSONObject());
            list.add(o);
          }
          value = list;
        } else if (value.equals(Double.NaN) || value.equals(Float.NaN))
          // JSON DOES NOT SUPPORT NaN
          value = "NaN";
        else if (value.equals(Double.POSITIVE_INFINITY) || value.equals(Float.POSITIVE_INFINITY))
          // JSON DOES NOT SUPPORT INFINITY
          value = "PosInfinity";
        else if (value.equals(Double.NEGATIVE_INFINITY) || value.equals(Float.NEGATIVE_INFINITY))
          // JSON DOES NOT SUPPORT INFINITY
          value = "NegInfinity";
      }
      properties.put(prop.getKey(), value);
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
    if (document instanceof Vertex vertex1) {
      final Vertex vertex = vertex1;

      if (expandVertexEdges) {
        final JSONArray outEdges = new JSONArray();
        for (final Edge e : vertex.getEdges(Vertex.DIRECTION.OUT))
          outEdges.put(e.getIdentity().toString());
        object.put("o", outEdges);

        final JSONArray inEdges = new JSONArray();
        for (final Edge e : vertex.getEdges(Vertex.DIRECTION.IN))
          inEdges.put(e.getIdentity().toString());
        object.put("i", inEdges);
      } else {
        object.put("i", vertex.countEdges(Vertex.DIRECTION.IN, null));
        object.put("o", vertex.countEdges(Vertex.DIRECTION.OUT, null));
      }

    } else if (document instanceof Edge edge1) {
      final Edge edge = edge1;
      object.put("i", edge.getIn().toString());
      object.put("o", edge.getOut().toString());
    }
  }

  public JsonGraphSerializer setSharedJson(final JSONObject json) {
    sharedJson = json;
    return this;
  }
}
