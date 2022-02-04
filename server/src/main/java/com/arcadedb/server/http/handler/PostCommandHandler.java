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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.stream.*;

public class PostCommandHandler extends DatabaseAbstractHandler {

  private static final int DEFAULT_LIMIT = 20_000;

  public PostCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database) throws IOException {

    final String payload = parseRequestPayload(exchange);
    if (payload == null || payload.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
      return;
    }

    final JSONObject json = new JSONObject(payload);

    final Map<String, Object> requestMap = json.toMap();

    final String language = (String) requestMap.get("language");
    final String command = decode((String) requestMap.get("command"));
    final int limit = (int) requestMap.getOrDefault("limit", DEFAULT_LIMIT);
    final String serializer = (String) requestMap.getOrDefault("serializer", "record");

    if (command == null || command.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
      return;
    }

    final Map<String, Object> paramMap = (Map<String, Object>) requestMap.get("params");

    final ServerMetrics.MetricTimer timer = httpServer.getServer().getServerMetrics().timer("http.command");

    try {

      final ResultSet qResult = language.equalsIgnoreCase("sqlScript") ?
          executeScript(database, language, command, paramMap) :
          executeCommand(database, language, command, paramMap);

      if (qResult == null)
        throw new CommandExecutionException("Error on executing command");

      final JSONObject response = createResult(user);

      switch (serializer) {
      case "graph": {
        final JsonGraphSerializer serializerImpl = new JsonGraphSerializer().setExpandVertexEdges(false);
        final Set<Identifiable> includedRecords = new HashSet<>();
        final Set<Identifiable> includedVertices = new HashSet<>();
        final JSONArray vertices = new JSONArray();
        final JSONArray edges = new JSONArray();
        final JSONArray records = new JSONArray();

        while (qResult.hasNext()) {
          final Result row = qResult.next();

          boolean justIncluded = true;
          if (!row.getIdentity().isEmpty()) {
            final RID rid = row.getIdentity().get();
            justIncluded = includedRecords.add(rid);
            if (justIncluded)
              records.put(serializerImpl.serializeResult(row));
          }

          if (row.isVertex()) {
            if (justIncluded) {
              final Vertex v = row.getVertex().get();
              includedVertices.add(v.getIdentity());
              vertices.put(serializerImpl.serializeGraphElement(v));
            }
          } else if (row.isEdge()) {
            final Edge e = row.getEdge().get();
            if (justIncluded)
              edges.put(serializerImpl.serializeGraphElement(e));
            if (includedVertices.add(e.getIn())) {
              if (includedRecords.add(e.getIn()))
                records.put(serializerImpl.serializeDocument(e.getInVertex()));

              vertices.put(serializerImpl.serializeGraphElement(e.getInVertex()));
            }
            if (includedVertices.add(e.getOut())) {
              if (includedRecords.add(e.getOut()))
                records.put(serializerImpl.serializeDocument(e.getOutVertex()));

              vertices.put(serializerImpl.serializeGraphElement(e.getOutVertex()));
            }
          } else {
            analyzeResultContent(database, serializerImpl, includedVertices, vertices, edges, row);
            records.put(serializerImpl.serializeResult(row));
          }
        }

        // FILTER OUT NOT CONNECTED EDGES
        for (Identifiable entry : includedVertices) {
          final Vertex vertex = entry.asVertex(true);

          final Iterable<Edge> vEdgesOut = vertex.getEdges(Vertex.DIRECTION.OUT);
          for (Edge e : vEdgesOut) {
            if (includedVertices.contains(e.getIn()) && !includedRecords.contains(e.getIdentity()))
              edges.put(serializerImpl.serializeGraphElement(e));
          }

          final Iterable<Edge> vEdgesIn = vertex.getEdges(Vertex.DIRECTION.IN);
          for (Edge e : vEdgesIn) {
            if (includedVertices.contains(e.getOut()) && !includedRecords.contains(e.getIdentity()))
              edges.put(serializerImpl.serializeGraphElement(e));
          }
        }

        response.put("result", new JSONObject().put("vertices", vertices).put("edges", edges).put("records", records));
        break;
      }

      case "record": {
        final JsonSerializer serializerImpl = new JsonSerializer().setIncludeVertexEdges(false).setUseCollectionSize(false);
        final JSONArray result = new JSONArray(qResult.stream().limit(limit + 1).map(r -> serializerImpl.serializeResult(r)).collect(Collectors.toList()));
        response.put("result", result);
        break;
      }

      default: {
        final JsonSerializer serializerImpl = new JsonSerializer().setIncludeVertexEdges(true).setUseCollectionSize(false);
        final JSONArray result = new JSONArray(qResult.stream().limit(limit + 1).map(r -> serializerImpl.serializeResult(r)).collect(Collectors.toList()));
        response.put("result", result);
      }
      }

      final String responseAsString = response.toString();

      exchange.setStatusCode(200);
      exchange.getResponseSender().send(responseAsString);

    } finally {
      timer.stop();
    }
  }

  private void analyzeResultContent(final Database database, final JsonGraphSerializer serializerImpl, final Set<Identifiable> includedVertices,
      final JSONArray vertices, final JSONArray edges, final Result row) {
    for (String prop : row.getPropertyNames()) {
      final Object value = row.getProperty(prop);
      if (value == null)
        continue;

      if (prop.equals("@rid") && RID.is(value)) {
        analyzePropertyValue(database, serializerImpl, includedVertices, vertices, edges, new RID(database, value.toString()));
      } else
        analyzePropertyValue(database, serializerImpl, includedVertices, vertices, edges, value);
    }
  }

  private void analyzePropertyValue(final Database database, final JsonGraphSerializer serializerImpl, final Set<Identifiable> includedVertices,
      final JSONArray vertices, final JSONArray edges, final Object value) {
    if (value instanceof Identifiable) {
      final RID rid = ((Identifiable) value).getIdentity();

      final DocumentType type = database.getSchema().getTypeByBucketId(rid.getBucketId());
      if (type instanceof VertexType) {
        includedVertices.add((Identifiable) value);
        vertices.put(serializerImpl.serializeGraphElement(((Identifiable) value).asVertex(true)));
      } else if (type instanceof EdgeType) {
        final Edge edge = ((Identifiable) value).asEdge(true);

        edges.put(serializerImpl.serializeGraphElement(edge));

        includedVertices.add(edge.getIn());
        vertices.put(serializerImpl.serializeGraphElement(edge.getInVertex()));
        includedVertices.add(edge.getOut());
        vertices.put(serializerImpl.serializeGraphElement(edge.getOutVertex()));
      }
    } else if (value instanceof Result) {
      analyzeResultContent(database, serializerImpl, includedVertices, vertices, edges, (Result) value);
    } else if (value instanceof Collection) {
      for (Iterator<?> it = ((Collection<?>) value).iterator(); it.hasNext(); ) {
        analyzePropertyValue(database, serializerImpl, includedVertices, vertices, edges, it.next());
      }
    }
  }

  private ResultSet executeScript(final Database database, final String language, String command, final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);

    if (!command.endsWith(";"))
      command += ";";

    if (params instanceof Object[])
      return database.execute(language, command, (Object[]) params);

    return database.execute(language, command, (Map<String, Object>) params);
  }

  private ResultSet executeCommand(final Database database, final String language, String command, final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);

    if (params instanceof Object[])
      return database.command(language, command, (Object[]) params);

    return database.command(language, command, (Map<String, Object>) params);
  }

  private Object mapParams(Map<String, Object> paramMap) {
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
      paramMap = Collections.EMPTY_MAP;
    return paramMap;
  }
}
