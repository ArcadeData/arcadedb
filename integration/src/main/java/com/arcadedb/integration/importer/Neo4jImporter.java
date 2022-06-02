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
package com.arcadedb.integration.importer;

import com.arcadedb.Constants;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.math.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;
import java.util.zip.*;

/**
 * Importer of a Neo4j database exported in JSONL format. To export a Neo4j database follow the instructions in https://neo4j.com/labs/apoc/4.3/export/json/.
 * The resulting file contains one json per line.
 * <br>
 * Neo4j is a registered mark of Neo4j, Inc.
 *
 * @author Luca Garulli
 */
public class Neo4jImporter {
  private              String                         databasePath;
  private              String                         inputFile;
  private              boolean                        overwriteDatabase     = false;
  private              Type                           typeForDecimals       = Type.DECIMAL;
  private  final       Map<String, Long>              totalVerticesByType   = new HashMap<>();
  private              long                           totalVerticesParsed   = 0L;
  private  final       Map<String, Long>              totalEdgesByType      = new HashMap<>();
  private              long                           totalEdgesParsed      = 0L;
  private              long                           totalAttributesParsed = 0L;
  private              long                           errors                = 0L;
  private              long                           warnings              = 0L;
  private              DatabaseFactory                factory;
  private              Database                       database;
  private              int                            batchSize             = 10_000;
  private              long                           processedItems        = 0L;
  private              long                           skippedEdges          = 0L;
  private              long                           savedVertices         = 0L;
  private              long                           savedEdges            = 0L;
  private              long                           beginTime;
  private              long                           beginTimeVerticesCreation;
  private              long                           beginTimeEdgesCreation;
  private              InputStream                    inputStream;
  private              boolean                        error                 = false;
  private              File                           file;
  private final        Map<String, Map<String, Type>> schemaProperties      = new HashMap<>();
  private final static SimpleDateFormat               dateTimeISO8601Format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  private enum PHASE {OFF, CREATE_SCHEMA, CREATE_VERTICES, CREATE_EDGES}

  public Neo4jImporter(final String[] args) {
    printHeader();

    String state = null;
    for (String arg : args) {
      if (arg.equals("-?"))
        printHelp();
      else if (arg.equals("-d"))
        state = "databasePath";
      else if (arg.equals("-i"))
        state = "inputFile";
      else if (arg.equals("-o"))
        overwriteDatabase = true;
      else if (arg.equals("-b"))
        state = "batchSize";
      else if (arg.equals("-decimalType"))
        state = "decimalType";
      else if (state != null) {
        if (state.equals("databasePath"))
          databasePath = arg;
        else if (state.equals("inputFile"))
          inputFile = arg;
        else if (state.equals("batchSize"))
          batchSize = Integer.parseInt(arg);
        else if (state.equals("decimalType"))
          typeForDecimals = Type.valueOf(arg.toUpperCase());
      }
    }

    if (inputFile == null)
      syntaxError("Missing input file. Use -f <file-path>");
  }

  public static void main(final String[] args) throws IOException {
    new Neo4jImporter(args).run();
  }

  public void run() throws IOException {
    file = new File(inputFile);
    if (!file.exists()) {
      error = true;
      throw new IllegalArgumentException("File '" + inputFile + "' not found");
    }

    if (databasePath == null)
      log("Checking Neo4j database from file '%s'...", inputFile);
    else {
      log("Importing Neo4j database from file '%s' to '%s'", inputFile, databasePath);

      factory = new DatabaseFactory(databasePath);
      if (factory.exists()) {
        if (!overwriteDatabase) {
          error("Database already exists on path '%s'", databasePath);
          return;
        } else {
          database = factory.open();
          error("Found existent database at '%s', dropping it and recreate a new one", databasePath);
          database.drop();
        }
      }

      // CREATE THE DATABASE
      database = factory.create();
    }

    try {
      beginTime = System.currentTimeMillis();

      // PARSE THE FILE THE 1ST TIME TO CREATE THE SCHEMA AND CACHE EDGES IN RAM
      log("Creation of the schema: types, properties and indexes");
      syncSchema();

      // PARSE THE FILE AGAIN TO CREATE VERTICES
      log("- Creation of vertices started");
      beginTimeVerticesCreation = System.currentTimeMillis();
      parseVertices();

      // PARSE THE FILE AGAIN TO CREATE EDGES
      log("Creation of edges started: creating edges between vertices");
      beginTimeEdgesCreation = System.currentTimeMillis();
      parseEdges();

      final long elapsed = (System.currentTimeMillis() - beginTime) / 1000;

      log("***************************************************************************************************");
      log("Import of Neo4j database completed in %,d secs with %,d errors and %,d warnings.", elapsed, errors, warnings);
      log("\nSUMMARY\n");
      log("- Vertices.............: %,d", totalVerticesParsed);
      for (Map.Entry<String, Long> entry : totalVerticesByType.entrySet()) {
        final String typeName = entry.getKey();
        final Long verticesByClass = totalVerticesByType.get(typeName);
        final long entries = verticesByClass != null ? verticesByClass : 0L;
        String additional = "";

        log("-- %-20s: %,d %s", typeName, entries, additional);
      }
      log("- Edges................: %,d", totalEdgesParsed);
      for (Map.Entry<String, Long> entry : totalEdgesByType.entrySet()) {
        final String typeName = entry.getKey();
        final Long edgesByClass = totalEdgesByType.get(typeName);
        final long entries = edgesByClass != null ? edgesByClass : 0L;
        String additional = "";

        log("-- %-20s: %,d %s", typeName, entries, additional);
      }
      log("- Total attributes.....: %,d", totalAttributesParsed);
      log("***************************************************************************************************");
      log("");
      log("NOTES:");

      if (database != null)
        log("- you can find your new ArcadeDB database in '" + database.getDatabasePath() + "'");

    } finally {
      if (database != null)
        database.close();
    }
  }

  public boolean isError() {
    return error;
  }

  private void syncSchema() throws IOException {
    readFile(json -> {
      switch (json.getString("type")) {
      case "node":
        final Pair<String, List<String>> labels = typeNameFromLabels(json);

        if (!database.getSchema().existsType(labels.getFirst())) {
          final VertexType type = database.getSchema().getOrCreateVertexType(labels.getFirst());
          if (labels.getSecond() != null)
            for (String parent : labels.getSecond())
              type.addSuperType(parent);

          database.transaction(() -> {
            final Property id = type.createProperty("id", Type.STRING);
            id.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
          });
        }

        inferPropertyType(json, labels.getFirst());

        break;

      case "relationship":
        final String edgeLabel = json.has("label") && !json.isNull("label") ? json.getString("label") : null;
        if (edgeLabel != null)
          database.getSchema().getOrCreateEdgeType(edgeLabel);

        inferPropertyType(json, edgeLabel);
        break;
      }
      return null;
    });
  }

  private void inferPropertyType(final JSONObject json, final String label) {
    // TRY TO INFER PROPERTY TYPES
      Map<String, Type> typeProperties = schemaProperties.computeIfAbsent(label, k -> new HashMap<>());

      final JSONObject properties = json.getJSONObject("properties");
    for (String propName : properties.keySet()) {
      ++totalAttributesParsed;

      Type currentType = typeProperties.get(propName);
      if (currentType == null) {
        Object propValue = properties.get(propName);
        if (!propValue.equals(JSONObject.NULL)) {

          if (propValue instanceof String) {
            // CHECK IF IT'S A DATE
            try {
              dateTimeISO8601Format.parse((String) propValue);
              currentType = Type.DATETIME;
            } catch (ParseException e) {
              currentType = Type.STRING;
            }
          } else {
            if (propValue instanceof JSONObject)
              propValue = ((JSONObject) propValue).toMap();
            else if (propValue instanceof JSONArray)
              propValue = ((JSONArray) propValue).toList();

            currentType = Type.getTypeByValue(propValue);
          }

          typeProperties.put(propName, currentType);
        }
      }
    }
  }

  private void parseVertices() throws IOException {
    database.begin();

    final AtomicInteger lineNumber = new AtomicInteger();

    readFile(json -> {
      lineNumber.incrementAndGet();

      switch (json.getString("type")) {
      case "node":
        ++processedItems;
        ++totalVerticesParsed;
        if (processedItems > 0 && processedItems % 1_000_000 == 0) {
          final long elapsed = System.currentTimeMillis() - beginTimeVerticesCreation;
          log("- Status update: created %,d vertices, skipped %,d edges (%,d vertices/sec)", savedVertices, skippedEdges, (savedVertices / elapsed * 1000));
        }

        final Pair<String, List<String>> type = typeNameFromLabels(json);
        if (type == null) {
          log("- found vertex in line %d without labels. Skip it.", lineNumber.get());
          ++warnings;
          return null;
        }

        final String typeName = type.getFirst();

        final String id = json.getString("id");

        final MutableVertex vertex = database.newVertex(typeName);

        vertex.fromMap(setProperties(json.getJSONObject("properties"), schemaProperties.get(typeName)));
        vertex.set("id", id);
        vertex.save();
        ++savedVertices;

        incrementVerticesByType(typeName);

        if (processedItems > 0 && processedItems % batchSize == 0) {
          database.commit();
          database.begin();
        }

        break;

      case "relationship":
        ++skippedEdges;
        break;
      }
      return null;
    });

    database.commit();

    final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

    log("- Creation of vertices completed: created %,d vertices, skipped %,d edges (%,d vertices/sec elapsed=%,d secs)", savedVertices, skippedEdges,
        elapsedInSecs > 0 ? (savedVertices / elapsedInSecs) : 0, elapsedInSecs);
  }

  private void parseEdges() throws IOException {
    database.begin();

    final AtomicInteger lineNumber = new AtomicInteger();

    readFile(json -> {
      lineNumber.incrementAndGet();

      switch (json.getString("type")) {
      case "node":
        break;

      case "relationship":
        ++processedItems;
        ++totalEdgesParsed;
        if (processedItems > 0 && processedItems % 1_000_000 == 0) {
          final long elapsed = System.currentTimeMillis() - beginTimeEdgesCreation;
          log("- Status update: created %,d edges %s (%,d edges/sec)", savedEdges, totalEdgesByType, (savedEdges / elapsed * 1000));
        }

        final String type = json.getString("label");
        if (type == null) {
          log("- found edge in line %d without labels. Skip it.", lineNumber.get());
          ++warnings;
          return null;
        }

        final JSONObject start = json.getJSONObject("start");
        final Pair<String, List<String>> startType = typeNameFromLabels(start);
        final String startId = start.getString("id");

        final IndexCursor beginCursor = database.lookupByKey(startType.getFirst(), "id", startId);
        if (!beginCursor.hasNext()) {
          log("- cannot create relationship with id '%s'. Vertex id '%s' not found for labels. Skip it.", json.getString("id"), startId);
          ++warnings;
          return null;
        }

        final Vertex fromVertex = beginCursor.next().asVertex();

        final JSONObject end = json.getJSONObject("end");
        final Pair<String, List<String>> endType = typeNameFromLabels(end);
        final String endId = end.getString("id");

        final IndexCursor endCursor = database.lookupByKey(endType.getFirst(), "id", endId);
        if (!endCursor.hasNext()) {
          log("- cannot create relationship with id '%s'. Vertex id '%s' not found for labels. Skip it.", json.getString("id"), endId);
          ++warnings;
          return null;
        }

        final Identifiable toVertex = endCursor.next();

        final MutableEdge edge = fromVertex.newEdge(type, toVertex, true);

        edge.fromMap(setProperties(json.getJSONObject("properties"), schemaProperties.get(type)));
        edge.save();
        ++savedEdges;

        incrementEdgesByType(type);

        if (processedItems > 0 && processedItems % batchSize == 0) {
          database.commit();
          database.begin();
        }
        break;
      }
      return null;
    });

    database.commit();

    final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

    log("- Creation of edged completed: created %,d edges, (%,d edges/sec elapsed=%,d secs)", savedEdges, elapsedInSecs > 0 ? (savedEdges / elapsedInSecs) : 0,
        elapsedInSecs);
  }

  private Map<String, Object> setProperties(final JSONObject properties, final Map<String, Type> typeSchema) {
    final Map<String, Object> result = new HashMap<>();

    for (String propName : properties.keySet()) {
      Object propValue = properties.get(propName);

      if (propValue == JSONObject.NULL)
        propValue = null;
      else if (propValue instanceof JSONObject)
        propValue = setProperties((JSONObject) propValue, null);
      else if (propValue instanceof JSONArray)
        propValue = ((JSONArray) propValue).toList();
      else if (propValue instanceof String && typeSchema != null && typeSchema.get(propName) == Type.DATETIME) {
        try {
          propValue = dateTimeISO8601Format.parse((String) propValue).getTime();
        } catch (ParseException e) {
          log("Invalid date '%s', ignoring conversion to timestamp and leaving it as string", propValue);
          ++errors;
        }
      } else if (propValue instanceof BigDecimal) {
        propValue = typeForDecimals.newInstance(propValue);
      }

      result.put(propName, propValue);
    }

    return result;
  }

  private void readFile(Callable<Void, JSONObject> callback) throws IOException {
    inputStream = file.getName().endsWith("gz") ? new GZIPInputStream(new FileInputStream(file)) : new FileInputStream(file);
    try {
      final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, DatabaseFactory.getDefaultCharset()));
      try {
        for (long lineNumber = 0; reader.ready(); ++lineNumber) {
          final String line = reader.readLine();

          try {
            final JSONObject json = new JSONObject(line);

            switch (json.getString("type")) {
            case "node":
              callback.call(json);
              break;

            case "relationship":
              callback.call(json);
              break;

            default:
              log("Invalid 'type' content on line %d of the input JSONL file. The line will be ignored. JSON: %s", lineNumber, line);
              ++errors;
            }

          } catch (JSONException e) {
            log("Error on parsing json on line %d of the input JSONL file. The line will be ignored. JSON: %s", lineNumber, line);
            ++errors;
          }
        }
      } finally {
        reader.close();
      }
    } finally {
      inputStream.close();
      inputStream = null;
    }
  }

  private void incrementVerticesByType(final String label) {
    final Long vertices = totalVerticesByType.get(label);
    totalVerticesByType.put(label, vertices == null ? 1 : vertices + 1);
  }

  private void incrementEdgesByType(final String label) {
    final Long edges = totalEdgesByType.get(label);
    totalEdgesByType.put(label, edges == null ? 1 : edges + 1);
  }

  private Pair<String, List<String>> typeNameFromLabels(final JSONObject json) {
    final JSONArray nodeLabels = json.has("labels") && !json.isNull("labels") ? json.getJSONArray("labels") : null;
    if (nodeLabels != null && nodeLabels.length() > 0) {
      if (nodeLabels.length() > 1) {
        // MULTI LABEL, CREATE A NEW MIXED TYPE THAT EXTEND ALL THE LABELS BY USING INHERITANCE
        final Stream<String> list = nodeLabels.toList().stream().map(String.class::cast).sorted(Comparator.naturalOrder());
        return new Pair<>(list.collect(Collectors.joining("_")), list.collect(Collectors.toList()));
      } else
        return new Pair<>((String) nodeLabels.get(0), null);
    }
    return null;
  }

  private void log(final String text, final Object... args) {
    if (args.length == 0)
      System.out.println(text);
    else
      System.out.printf((text) + "%n", args);
  }

  private void error(final String text, final Object... args) {
    if (args.length == 0)
      System.out.println(text);
    else
      System.out.println(String.format(text, args));
  }

  private void syntaxError(final String s) {
    log("Syntax error: " + s);
    error = true;
    printHelp();
  }

  private void printHeader() {
    log(Constants.PRODUCT + " " + Constants.getVersion() + " - Neo4j Importer");
  }

  private void printHelp() {
    log("Use:");
    log("-d <database-path>: create a database from the Neo4j export");
    log("-i <input-file>: path to the Neo4j export file in JSONL format");
    log("-o: overwrite an existent database");
    log("-decimalType <type>: use <type> for decimals. <type> can be FLOAT, DOUBLE and DECIMAL. By default decimalType is DECIMAL.");
  }
}
