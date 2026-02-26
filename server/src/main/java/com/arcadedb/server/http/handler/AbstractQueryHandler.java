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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
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
import java.util.logging.*;
import java.util.stream.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;

public abstract class AbstractQueryHandler extends DatabaseAbstractHandler {

  public static final int DEFAULT_LIMIT = 20_000;

  public AbstractQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected void serializeResultSet(final Database database, final String serializer, final int limit, final JSONObject response,
      final ResultSet qResult) {
    if (qResult == null)
      return;

    try {
    switch (serializer) {
    case "graph": {
      // SERIALIZES THE GRAPH ELEMENTS IN VERTICES AND EDGES
      final JsonGraphSerializer serializerImpl = JsonGraphSerializer
          .createJsonGraphSerializer()
          .setExpandVertexEdges(false);
      // Don't use collection size for edges - we want COLLECT(rel) to return edge objects, not counts
      serializerImpl.setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false);

      final Set<RID> includedVertices = new HashSet<>();
      final Set<RID> includedEdges = new HashSet<>();
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
          analyzeResultContent(database, serializerImpl, includedVertices, includedEdges, vertices, edges, row, limit);
        }

        if (limit > 0 && vertices.length() + edges.length() >= limit)
          break;
      }

      response.put("result", new JSONObject().put("vertices", vertices).put("edges", edges));
      break;
    }

    case "studio": {
      // USE BY STUDIO TO RENDER GRAPH AND TABLE AT THE SAME TIME
      final JsonGraphSerializer serializerImpl = JsonGraphSerializer.createJsonGraphSerializer()
          .setExpandVertexEdges(false);
      // Don't use collection size for edges - we want COLLECT(rel) to return edge objects, not counts (issue #3404)
      serializerImpl.setUseCollectionSize(false).setUseCollectionSizeForEdges(false);

      final Set<RID> includedVertices = new HashSet<>();
      final Set<RID> includedEdges = new HashSet<>();
      final JSONArray vertices = new JSONArray();
      final JSONArray edges = new JSONArray();
      final JSONArray records = new JSONArray();

      while (qResult.hasNext()) {
        final Result row = qResult.next();

        try {
          // Always add to records without deduplication - UNWIND queries can legitimately
          // return the same RID multiple times with different values (issue #1582)
          records.put(serializerImpl.serializeResult(database, row));

          if (row.isVertex()) {
            final Vertex v = row.getVertex().get();
            if (includedVertices.add(v.getIdentity()))
              vertices.put(serializerImpl.serializeGraphElement(v));
          } else if (row.isEdge()) {
            final Edge e = row.getEdge().get();
            if (includedEdges.add(e.getIdentity())) {
              edges.put(serializerImpl.serializeGraphElement(e));
              try {
                if (includedVertices.add(e.getIn())) {
                  vertices.put(serializerImpl.serializeGraphElement(e.getInVertex()));
                }
                if (includedVertices.add(e.getOut())) {
                  vertices.put(serializerImpl.serializeGraphElement(e.getOutVertex()));
                }
              } catch (RecordNotFoundException ex) {
                LogManager.instance().log(this, Level.SEVERE, "Record %s not found during serialization", ex.getRID());
              }
            }
          } else {
            analyzeResultContent(database, serializerImpl, includedVertices, includedEdges, vertices, edges, row, limit);
          }
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on serializing element (error=%s)", e.getMessage());
        }

        if (limit > 0 && records.length() >= limit)
          break;
      }

      // FILTER OUT NOT CONNECTED EDGES
      for (final Identifiable entry : includedVertices) {
        if (limit > 0 && vertices.length() + edges.length() >= limit)
          break;

        try {
          final Vertex vertex = entry.asVertex(true);

          final Iterable<Edge> vEdgesOut = vertex.getEdges(Vertex.DIRECTION.OUT);
          for (final Edge e : vEdgesOut) {
            if (includedVertices.contains(e.getIn()) && !includedEdges.contains(e.getIdentity())) {
              edges.put(serializerImpl.serializeGraphElement(e));
              includedEdges.add(e.getIdentity());
            }
          }

          final Iterable<Edge> vEdgesIn = vertex.getEdges(Vertex.DIRECTION.IN);
          for (final Edge e : vEdgesIn) {
            if (includedVertices.contains(e.getOut()) && !includedEdges.contains(e.getIdentity())) {
              edges.put(serializerImpl.serializeGraphElement(e));
              includedEdges.add(e.getIdentity());
            }
          }
        } catch (RecordNotFoundException e) {
          LogManager.instance().log(this, Level.SEVERE, "Vertex %s not found during serialization", e.getRID());
        }
      }

      response.put("result", new JSONObject().put("vertices", vertices).put("edges", edges).put("records", records));
      break;
    }

    case "record": {
      final JsonSerializer serializerImpl = JsonSerializer.createJsonSerializer()
          .setIncludeVertexEdges(false)
          .setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false);
      final JSONArray result = new JSONArray();
      while (qResult.hasNext()) {
        final Result r = qResult.next();
        result.put(serializerImpl.serializeResult(database, r));
        if (limit > 0 && result.length() >= limit)
          break;
      }
      response.put("result", result);
      break;
    }

    default: {
      final JsonSerializer serializerImpl = JsonSerializer.createJsonSerializer().setIncludeVertexEdges(true)
          .setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false);
      final JSONArray result = new JSONArray(limit > 0 ?
          qResult.stream().limit(limit + 1).map(r -> serializerImpl.serializeResult(database, r)).collect(Collectors.toList()) :
          qResult.stream().map(r -> serializerImpl.serializeResult(database, r)).collect(Collectors.toList()));
      response.put("result", result);
    }
    }
    } finally {
      qResult.close();
    }
  }

  protected void analyzeResultContent(final Database database, final JsonGraphSerializer serializerImpl,
      final Set<RID> includedVertices, final Set<RID> includedEdges, final JSONArray vertices, final JSONArray edges,
      final Result row, final int limit) {
    for (final String prop : row.getPropertyNames()) {
      try {
        final Object value = row.getProperty(prop);
        if (value == null)
          continue;

        if (limit > 0 && vertices.length() + edges.length() >= limit)
          break;

        if (prop.equals(RID_PROPERTY) && RID.is(value)) {
          analyzePropertyValue(database, serializerImpl, includedVertices, includedEdges, vertices, edges,
              new RID(database, value.toString()), limit);
        } else
          analyzePropertyValue(database, serializerImpl, includedVertices, includedEdges, vertices, edges, value, limit);
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on serializing collection element (error=%s)", e.getMessage());
      }
    }
  }

  protected void analyzePropertyValue(final Database database, final JsonGraphSerializer serializerImpl,
      final Set<RID> includedVertices, final Set<RID> includedEdges, final JSONArray vertices, final JSONArray edges,
      final Object value, final int limit) {
    if (value instanceof Identifiable identifiable) {

      final DocumentType type;
      if (value instanceof Document document)
        type = document.getType();
      else {
        final RID rid = identifiable.getIdentity();
        type = database.getSchema().getTypeByBucketId(rid.getBucketId());
      }

      if (type instanceof LocalVertexType) {
        if (includedVertices.add(((Identifiable) value).getIdentity()))
          vertices.put(serializerImpl.serializeGraphElement(((Identifiable) value).asVertex(true)));
      } else if (type instanceof LocalEdgeType) {
        final Edge edge = ((Identifiable) value).asEdge(true);
        if (includedEdges.add(edge.getIdentity())) {
          edges.put(serializerImpl.serializeGraphElement(edge));
          try {
            if (includedVertices.add(edge.getIn())) {
              final Vertex inV = edge.getInVertex();
              vertices.put(serializerImpl.serializeGraphElement(inV));
            }
            if (includedVertices.add(edge.getOut())) {
              final Vertex outV = edge.getOutVertex();
              vertices.put(serializerImpl.serializeGraphElement(outV));
            }
          } catch (RecordNotFoundException e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on loading connecting vertices for edge %s: vertex %s not found",
                edge.getIdentity(), e.getRID());
          }
        }
      }
    } else if (value instanceof Result result) {
      analyzeResultContent(database, serializerImpl, includedVertices, includedEdges, vertices, edges, result, limit);
    } else if (value instanceof Collection<?> collection) {
      for (final Iterator<?> it = collection.iterator(); it.hasNext(); ) {
        try {
          analyzePropertyValue(database, serializerImpl, includedVertices, includedEdges, vertices, edges, it.next(), limit);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on serializing collection element (error=%s)", e.getMessage());
        }
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
