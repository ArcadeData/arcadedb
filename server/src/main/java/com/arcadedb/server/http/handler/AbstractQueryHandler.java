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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;

import java.util.*;
import java.util.stream.*;

public abstract class AbstractQueryHandler extends DatabaseAbstractHandler {

  protected static final int DEFAULT_LIMIT = 20_000;

  public AbstractQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected void serializeResultSet(final Database database, final String serializer, final int limit, final JSONObject response,
      final ResultSet qResult) {
    if (qResult == null)
      return;

    switch (serializer) {
    case "graph": {
      // SERIALIZES THE GRAPH ELEMENTS IN VERTICES AND EDGES
      final JsonGraphSerializer serializerImpl = new JsonGraphSerializer().setExpandVertexEdges(false);
      serializerImpl.setUseCollectionSize(false).setUseCollectionSizeForEdges(true);

      final Set<Identifiable> includedVertices = new HashSet<>();
      final Set<Identifiable> includedEdges = new HashSet<>();
      final JSONArray vertices = new JSONArray();
      final JSONArray edges = new JSONArray();

      while (qResult.hasNext()) {
        final Result row = qResult.next();

        if (row.isVertex()) {
          final Vertex v = row.getVertex().get();
          if (includedVertices.add(v.getIdentity()))
            vertices.put(serializerImpl.serializeGraphElement(v));
        } else if (row.isEdge()) {
          final Edge e = row.getEdge().get();
          if (includedEdges.add(e.getIdentity()))
            edges.put(serializerImpl.serializeGraphElement(e));
        } else {
          analyzeResultContent(database, serializerImpl, includedVertices, vertices, edges, row);
        }
      }

      response.put("result", new JSONObject().put("vertices", vertices).put("edges", edges));
      break;
    }

    case "studio": {
      // USE BY STUDIO TO RENDER GRAPH AND TABLE AT THE SAME TIME
      final JsonGraphSerializer serializerImpl = new JsonGraphSerializer().setExpandVertexEdges(false);
      serializerImpl.setUseCollectionSize(false).setUseCollectionSizeForEdges(true);

      final Set<Identifiable> includedRecords = new HashSet<>();
      final Set<Identifiable> includedVertices = new HashSet<>();
      final JSONArray vertices = new JSONArray();
      final JSONArray edges = new JSONArray();
      final JSONArray records = new JSONArray();

      if (qResult != null) {
        while (qResult.hasNext()) {
          final Result row = qResult.next();

          boolean recordIncluded = true;
          if (!row.getIdentity().isEmpty()) {
            final RID rid = row.getIdentity().get();
            recordIncluded = includedRecords.add(rid);
            if (recordIncluded)
              records.put(serializerImpl.serializeResult(database, row));
          } else
            records.put(serializerImpl.serializeResult(database, row));

          if (row.isVertex()) {
            if (recordIncluded) {
              final Vertex v = row.getVertex().get();
              if (includedVertices.add(v.getIdentity()))
                vertices.put(serializerImpl.serializeGraphElement(v));
            }
          } else if (row.isEdge()) {
            final Edge e = row.getEdge().get();
            if (recordIncluded)
              edges.put(serializerImpl.serializeGraphElement(e));
            if (includedVertices.add(e.getIn())) {
              includedRecords.add(e.getIn());

              vertices.put(serializerImpl.serializeGraphElement(e.getInVertex()));
            }
            if (includedVertices.add(e.getOut())) {
              includedRecords.add(e.getOut());

              vertices.put(serializerImpl.serializeGraphElement(e.getOutVertex()));
            }
          } else {
            analyzeResultContent(database, serializerImpl, includedVertices, vertices, edges, row);
          }

          if (limit > -1 && records.length() >= limit)
            break;
        }
      }

      // FILTER OUT NOT CONNECTED EDGES
      for (final Identifiable entry : includedVertices) {
        final Vertex vertex = entry.asVertex(true);

        final Iterable<Edge> vEdgesOut = vertex.getEdges(Vertex.DIRECTION.OUT);
        for (final Edge e : vEdgesOut) {
          if (includedVertices.contains(e.getIn()) && !includedRecords.contains(e.getIdentity()))
            edges.put(serializerImpl.serializeGraphElement(e));
        }

        final Iterable<Edge> vEdgesIn = vertex.getEdges(Vertex.DIRECTION.IN);
        for (final Edge e : vEdgesIn) {
          if (includedVertices.contains(e.getOut()) && !includedRecords.contains(e.getIdentity()))
            edges.put(serializerImpl.serializeGraphElement(e));
        }
      }

      response.put("result", new JSONObject().put("vertices", vertices).put("edges", edges).put("records", records));
      break;
    }

    case "record": {
      final JsonSerializer serializerImpl = new JsonSerializer().setIncludeVertexEdges(false).setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false);
      final JSONArray result = new JSONArray();
      while (qResult.hasNext()) {
        final Result r = qResult.next();
        result.put(serializerImpl.serializeResult(database, r));
        if (limit > 0 && response.length() >= limit)
          break;
      }
      response.put("result", result);
      break;
    }

    default: {
      final JsonSerializer serializerImpl = new JsonSerializer().setIncludeVertexEdges(true).setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false);
      final JSONArray result = new JSONArray(limit > -1 ?
          qResult.stream().limit(limit + 1).map(r -> serializerImpl.serializeResult(database, r)).collect(Collectors.toList()) :
          qResult.stream().map(r -> serializerImpl.serializeResult(database, r)).collect(Collectors.toList()));
      response.put("result", result);
    }
    }
  }

  protected void analyzeResultContent(final Database database, final JsonGraphSerializer serializerImpl,
      final Set<Identifiable> includedVertices, final JSONArray vertices, final JSONArray edges, final Result row) {
    for (final String prop : row.getPropertyNames()) {
      final Object value = row.getProperty(prop);
      if (value == null)
        continue;

      if (prop.equals("@rid") && RID.is(value)) {
        analyzePropertyValue(database, serializerImpl, includedVertices, vertices, edges, new RID(database, value.toString()));
      } else
        analyzePropertyValue(database, serializerImpl, includedVertices, vertices, edges, value);
    }
  }

  protected void analyzePropertyValue(final Database database, final JsonGraphSerializer serializerImpl,
      final Set<Identifiable> includedVertices, final JSONArray vertices, final JSONArray edges, final Object value) {
    if (value instanceof Identifiable) {

      final DocumentType type;
      if (value instanceof Document)
        type = ((Document) value).getType();
      else {
        final RID rid = ((Identifiable) value).getIdentity();
        type = database.getSchema().getTypeByBucketId(rid.getBucketId());
      }

      if (type instanceof LocalVertexType) {
        if (includedVertices.add((Identifiable) value))
          vertices.put(serializerImpl.serializeGraphElement(((Identifiable) value).asVertex(true)));
      } else if (type instanceof LocalEdgeType) {
        final Edge edge = ((Identifiable) value).asEdge(true);

        edges.put(serializerImpl.serializeGraphElement(edge));

        if (includedVertices.add(edge.getIn()))
          vertices.put(serializerImpl.serializeGraphElement(edge.getInVertex()));
        if (includedVertices.add(edge.getOut()))
          vertices.put(serializerImpl.serializeGraphElement(edge.getOutVertex()));
      }
    } else if (value instanceof Result) {
      analyzeResultContent(database, serializerImpl, includedVertices, vertices, edges, (Result) value);
    } else if (value instanceof Collection) {
      for (final Iterator<?> it = ((Collection<?>) value).iterator(); it.hasNext(); ) {
        analyzePropertyValue(database, serializerImpl, includedVertices, vertices, edges, it.next());
      }
    }
  }

  protected Object mapParams(Map<String, Object> paramMap) {
    if (paramMap != null) {
      if (!paramMap.isEmpty() && paramMap.containsKey("0")) {
        // ORDINAL
        final Object[] array = new Object[paramMap.size()];
        for (int i = 0; i < array.length; ++i) {
          array[i] = paramMap.get("" + i);
        }
        return array;
      }
    } else
      paramMap = Collections.emptyMap();
    return paramMap;
  }
}
