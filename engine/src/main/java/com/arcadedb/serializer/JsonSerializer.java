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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.lang.reflect.*;
import java.util.*;

public class JsonSerializer {
  private boolean useCollectionSize         = false;
  private boolean includeVertexEdges        = true;
  private boolean useVertexEdgeSize         = true;
  private boolean useCollectionSizeForEdges = true;

  public JSONObject serializeDocument(final Document document) {
    final Database database = document.getDatabase();
    final JSONObject object = new JSONObject().setDateFormat(database.getSchema().getDateTimeFormat());

    if (document.getIdentity() != null)
      object.put("@rid", document.getIdentity().toString());
    object.put("@type", document.getTypeName());

    final Map<String, Object> documentAsMap = document.toMap();
    for (final Map.Entry<String, Object> documentEntry : documentAsMap.entrySet()) {
      final String p = documentEntry.getKey();
      Object value = documentEntry.getValue();

      if (value == null)
        value = JSONObject.NULL;
      else if (value instanceof Document)
        value = serializeDocument((Document) value);
      else if (value instanceof Collection) {
        serializeCollection(database, (Collection<?>) value);
      } else if (value instanceof Map)
        value = serializeMap(database, (Map<Object, Object>) value);

      value = convertNonNumbers(value);

      object.put(p, value);
    }

    setMetadata(document, object);

    return object;
  }

  public JSONObject serializeResult(final Database database, final Result result) {
    final JSONObject object = new JSONObject().setDateFormat(database.getSchema().getDateTimeFormat());

    if (result.isElement()) {
      final Document document = result.toElement();
      if (document.getIdentity() != null)
        object.put("@rid", document.getIdentity().toString());
      object.put("@type", document.getTypeName());

      setMetadata(document, object);
    }

    for (final String p : result.getPropertyNames()) {
      Object value = result.getProperty(p);

      if (value == null)
        value = JSONObject.NULL;
      else if (value instanceof Document)
        value = serializeDocument((Document) value);
      else if (value instanceof Result)
        value = serializeResult(database, (Result) value);
      else if (value instanceof Collection)
        value = serializeCollection(database, (Collection<?>) value);
      else if (value instanceof Map)
        value = serializeMap(database, (Map<Object, Object>) value);
      else if (value.getClass().isArray())
        value = serializeCollection(database, List.of((Object[]) value));

      value = convertNonNumbers(value);

      object.put(p, value);
    }

    return object;
  }

  private Object serializeCollection(final Database database, final Collection<?> value) {
    Object result = value;
    if (!value.isEmpty()) {
      if (useCollectionSizeForEdges && value.iterator().next() instanceof Edge)
        result = value.size();
      else if (useCollectionSize) {
        result = value.size();
      } else {
        final JSONArray list = new JSONArray();
        for (Object o : value) {
          if (o instanceof Document)
            o = serializeDocument((Document) o);
          else if (o instanceof Result)
            o = serializeResult(database, (Result) o);
          else if (o instanceof ResultSet)
            o = serializeResultSet(database, (ResultSet) o);
          else if (o instanceof Collection)
            o = serializeCollection(database, (Collection<?>) o);
          else if (o instanceof Map)
            o = serializeMap(database, (Map<Object, Object>) o);

          list.put(o);
        }
        result = list;
      }
    }
    return result;
  }

  private Object serializeResultSet(final Database database, final ResultSet resultSet) {
    final JSONArray array = new JSONArray();
    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      array.put(serializeResult(database, row));
    }
    return array;
  }

  private Object serializeMap(final Database database, final Map<Object, Object> value) {
    final Object result;
    if (useCollectionSize) {
      result = value.size();
    } else {
      final JSONObject map = new JSONObject().setDateFormat(database.getSchema().getDateTimeFormat());
      for (final Map.Entry<Object, Object> entry : value.entrySet()) {
        Object o = entry.getValue();
        if (o instanceof Document)
          o = serializeDocument((Document) o);
        else if (o instanceof ResultSet)
          o = serializeResultSet(database, (ResultSet) o);
        else if (o instanceof Result)
          o = serializeResult(database, (Result) o);
        else if (o instanceof Collection)
          o = serializeCollection(database, (Collection<?>) o);
        else if (o instanceof Map)
          o = serializeMap(database, (Map<Object, Object>) o);
        map.put(entry.getKey().toString(), o);
      }
      result = map;
    }
    return result;
  }

  public boolean isUseVertexEdgeSize() {
    return useVertexEdgeSize;
  }

  public JsonSerializer setUseVertexEdgeSize(final boolean useVertexEdgeSize) {
    this.useVertexEdgeSize = useVertexEdgeSize;
    return this;
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

  public boolean isUseCollectionSizeForEdges() {
    return useCollectionSizeForEdges;
  }

  public JsonSerializer setUseCollectionSizeForEdges(final boolean useCollectionSizeForEdges) {
    this.useCollectionSizeForEdges = useCollectionSizeForEdges;
    return this;
  }

  private void setMetadata(final Document document, final JSONObject object) {
    if (document instanceof Vertex) {
      object.put("@cat", "v");
      if (includeVertexEdges) {
        final Vertex vertex = ((Vertex) document);
        if (useVertexEdgeSize) {
          object.put("@out", vertex.countEdges(Vertex.DIRECTION.OUT, null));
          object.put("@in", vertex.countEdges(Vertex.DIRECTION.IN, null));

        } else {
          final JSONArray outEdges = new JSONArray();
          for (final Edge e : vertex.getEdges(Vertex.DIRECTION.OUT))
            outEdges.put(e.getIdentity().toString());
          object.put("@out", outEdges);

          final JSONArray inEdges = new JSONArray();
          for (final Edge e : vertex.getEdges(Vertex.DIRECTION.IN))
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

  private static Object convertNonNumbers(Object value) {
    if (value != null)
      if (value.equals(Double.NaN) || value.equals(Float.NaN))
        // JSON DOES NOT SUPPORT NaN
        value = "NaN";
      else if (value.equals(Double.POSITIVE_INFINITY) || value.equals(Float.POSITIVE_INFINITY))
        // JSON DOES NOT SUPPORT INFINITY
        value = "PosInfinity";
      else if (value.equals(Double.NEGATIVE_INFINITY) || value.equals(Float.NEGATIVE_INFINITY))
        // JSON DOES NOT SUPPORT INFINITY
        value = "NegInfinity";
    return value;
  }
}
