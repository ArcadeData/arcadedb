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
package com.arcadedb.gremlin.integration.importer.format;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.integration.importer.format.CSVImporterFormat;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;

/**
 * Imports GraphSON files into ArcadeDB.
 * <p>
 * This importer handles both ArcadeDB-native GraphSON files (with RID-format IDs like #1:0)
 * and standard GraphSON files with custom IDs (like URIs or other string identifiers).
 * <p>
 * For non-RID IDs, the original ID is preserved in a property called {@code @id} to allow
 * querying vertices by their original identifiers.
 */
public class GraphSONImporterFormat extends CSVImporterFormat {

  /**
   * Property name used to store the original vertex/edge ID when it's not an ArcadeDB RID.
   */
  public static final String ORIGINAL_ID_PROPERTY = "@id";

  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.EntityType entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {

    // Read all lines from the GraphSON file
    final List<String> lines = new ArrayList<>();
    try (final InputStream is = parser.getInputStream();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.isBlank()) {
          lines.add(line);
        }
      }
    } catch (final IOException e) {
      throw new ImportException("Error reading GraphSON file", e);
    }

    if (lines.isEmpty()) {
      return; // Empty file
    }

    // Check if the file uses RID-format IDs by examining the first line
    final JSONObject firstVertex = new JSONObject(lines.get(0));
    final Object firstId = firstVertex.opt("id");
    final boolean usesNonRidIds = firstId != null && !isRidFormat(firstId);

    if (usesNonRidIds) {
      // Use custom import that handles non-RID IDs
      importWithIdMapping(lines, database);
    } else {
      // Use standard TinkerPop import for RID-format IDs
      // Convert the lines back to an InputStream
      final String content = String.join("\n", lines);
      try (final InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
        final ArcadeGraph graph = ArcadeGraph.open(database);
        graph.io(IoCore.graphson()).reader().create().readGraph(is, graph);
      } catch (final IOException e) {
        throw new ImportException("Error on importing GraphSON", e);
      }
    }
  }

  /**
   * Custom import method that handles GraphSON files with non-RID vertex IDs.
   * <p>
   * This method:
   * 1. Reads vertices and stores original IDs as properties
   * 2. Builds a mapping from original IDs to new ArcadeDB RIDs
   * 3. Creates edges using the ID mapping
   */
  private void importWithIdMapping(final List<String> lines, final DatabaseInternal database) {
    final Map<Object, RID> idMapping = new HashMap<>();
    final List<EdgeData> pendingEdges = new ArrayList<>();

    // First pass: create vertices and collect edge data
    database.begin();

    for (final String line : lines) {
      final JSONObject vertexJson = new JSONObject(line);
      final Object originalId = vertexJson.get("id");
      final String label = vertexJson.getString("label", "vertex");

      // Ensure vertex type exists
      if (!database.getSchema().existsType(label)) {
        database.getSchema().createVertexType(label);
      } else if (!(database.getSchema().getType(label) instanceof VertexType)) {
        throw new ImportException("Type '" + label + "' is not a vertex type");
      }

      // Create vertex
      final MutableVertex vertex = database.newVertex(label);

      // Store original ID as property
      vertex.set(ORIGINAL_ID_PROPERTY, String.valueOf(originalId));

      // Copy properties
      final JSONObject properties = vertexJson.getJSONObject("properties", null);
      if (properties != null) {
        for (final String propName : properties.keySet()) {
          final Object propValue = extractPropertyValue(properties, propName);
          if (propValue != null) {
            vertex.set(propName, propValue);
          }
        }
      }

      vertex.save();
      final RID newRid = vertex.getIdentity();
      idMapping.put(originalId, newRid);

      // Collect outgoing edges
      final JSONObject outEdges = vertexJson.getJSONObject("outE", null);
      if (outEdges != null) {
        for (final String edgeLabel : outEdges.keySet()) {
          final JSONArray edgeArray = outEdges.getJSONArray(edgeLabel, null);
          if (edgeArray != null) {
            for (int i = 0; i < edgeArray.length(); i++) {
              final JSONObject edgeJson = edgeArray.getJSONObject(i);
              final Object edgeId = edgeJson.opt("id");
              final Object inV = edgeJson.get("inV");
              final JSONObject edgeProps = edgeJson.getJSONObject("properties", null);

              pendingEdges.add(new EdgeData(originalId, inV, edgeLabel, edgeId, edgeProps));
            }
          }
        }
      }
    }

    database.commit();

    // Second pass: create edges using the ID mapping
    if (!pendingEdges.isEmpty()) {
      database.begin();

      for (final EdgeData edgeData : pendingEdges) {
        final RID outRid = idMapping.get(edgeData.outV);
        final RID inRid = idMapping.get(edgeData.inV);

        if (outRid == null) {
          LogManager.instance().log(this, Level.WARNING,
              "Skipping edge: source vertex with ID '%s' not found", edgeData.outV);
          continue;
        }

        if (inRid == null) {
          LogManager.instance().log(this, Level.WARNING,
              "Skipping edge: target vertex with ID '%s' not found", edgeData.inV);
          continue;
        }

        // Ensure edge type exists
        if (!database.getSchema().existsType(edgeData.label)) {
          database.getSchema().createEdgeType(edgeData.label);
        } else if (!(database.getSchema().getType(edgeData.label) instanceof EdgeType)) {
          throw new ImportException("Type '" + edgeData.label + "' is not an edge type");
        }

        // Create edge
        final Vertex outVertex = outRid.asVertex();
        final MutableEdge edge = outVertex.newEdge(edgeData.label, inRid.asVertex());

        // Store original edge ID if present
        if (edgeData.edgeId != null) {
          edge.set(ORIGINAL_ID_PROPERTY, String.valueOf(edgeData.edgeId));
        }

        // Copy edge properties
        if (edgeData.properties != null) {
          for (final String propName : edgeData.properties.keySet()) {
            final Object propValue = extractPropertyValue(edgeData.properties, propName);
            if (propValue != null) {
              edge.set(propName, propValue);
            }
          }
        }

        edge.save();
      }

      database.commit();
    }
  }

  /**
   * Extracts a property value from a GraphSON properties object.
   * Handles both simple values and GraphSON-typed values.
   */
  private Object extractPropertyValue(final JSONObject properties, final String propName) {
    final Object propObj = properties.get(propName);

    if (propObj instanceof JSONArray propArray) {
      // GraphSON format: properties are arrays of {id, value} objects
      if (propArray.length() > 0) {
        final Object firstObj = propArray.get(0);
        if (firstObj instanceof JSONObject first && first.has("value")) {
          return extractTypedValue(first.get("value"));
        }
      }
      return null;
    } else if (propObj instanceof JSONObject propJson) {
      // Check for typed value format
      if (propJson.has("@type") && propJson.has("@value")) {
        return extractTypedValue(propObj);
      } else if (propJson.has("value")) {
        return extractTypedValue(propJson.get("value"));
      }
      return propObj.toString();
    } else {
      return extractTypedValue(propObj);
    }
  }

  /**
   * Extracts a value from GraphSON typed format.
   * Handles formats like {"@type": "g:Int32", "@value": 42}
   */
  private Object extractTypedValue(final Object value) {
    if (value instanceof JSONObject json) {
      if (json.has("@type") && json.has("@value")) {
        final String type = json.getString("@type");
        final Object innerValue = json.get("@value");

        return switch (type) {
          case "g:Int32" -> innerValue instanceof Number ? ((Number) innerValue).intValue() : Integer.parseInt(innerValue.toString());
          case "g:Int64" -> innerValue instanceof Number ? ((Number) innerValue).longValue() : Long.parseLong(innerValue.toString());
          case "g:Float" -> innerValue instanceof Number ? ((Number) innerValue).floatValue() : Float.parseFloat(innerValue.toString());
          case "g:Double" -> innerValue instanceof Number ? ((Number) innerValue).doubleValue() : Double.parseDouble(innerValue.toString());
          case "g:String" -> innerValue.toString();
          default -> innerValue;
        };
      }
      return json.toString();
    }
    return value;
  }

  /**
   * Checks if the given ID is in ArcadeDB RID format (#bucket:position).
   */
  private boolean isRidFormat(final Object id) {
    if (id instanceof String s) {
      return RID.is(s);
    } else if (id instanceof Map) {
      @SuppressWarnings("unchecked")
      final Map<String, Object> map = (Map<String, Object>) id;
      return map.containsKey("bucketId") && map.containsKey("bucketPosition");
    }
    return false;
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.EntityType entityType, final Parser parser, final ImporterSettings settings, final AnalyzedSchema analyzedSchema)
      throws IOException {
    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public String getFormat() {
    return "graphson";
  }

  /**
   * Holds edge data during import to defer edge creation until all vertices exist.
   */
  private static class EdgeData {
    final Object outV;
    final Object inV;
    final String label;
    final Object edgeId;
    final JSONObject properties;

    EdgeData(final Object outV, final Object inV, final String label, final Object edgeId, final JSONObject properties) {
      this.outV = outV;
      this.inV = inV;
      this.label = label;
      this.edgeId = edgeId;
      this.properties = properties;
    }
  }
}
