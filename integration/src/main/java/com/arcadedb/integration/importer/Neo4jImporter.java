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
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Importer of a Neo4j database exported in JSONL format. To export a Neo4j database follow the instructions in https://neo4j.com/labs/apoc/4.3/export/json/.
 * The resulting file contains one json per line.
 * <br>
 * Uses {@link GraphBatch} for high-performance bulk import: vertices are created with pre-allocated edge segments,
 * and edges are buffered in flat primitive arrays then flushed sorted by vertex for sequential I/O.
 * <br>
 * ID mapping uses a primitive {@link LongLongMap} when Neo4j IDs are numeric (the common case with APOC exports),
 * falling back to a {@code HashMap<String, Long>} for non-numeric IDs. The primitive map uses ~24 bytes/entry
 * vs ~156 bytes/entry for a {@code HashMap<String, RID>}, saving ~85% RAM on large imports.
 * <br>
 * Neo4j is a registered mark of Neo4j, Inc.
 *
 * @author Luca Garulli
 */
public class Neo4jImporter {
  protected            Database                       database;
  protected            Callable<Void, JSONObject>     parsingCallback;
  protected            int                            indexPageSize            = LSMTreeIndexAbstract.DEF_PAGE_SIZE;
  protected            int                            bucketsPerType           = 1;
  private              boolean                        closeDatabaseAfterImport = true;
  private              InputStream                    inputStream;
  private              String                         databasePath;
  private              String                         inputFile;
  private              boolean                        overwriteDatabase        = false;
  private              Type                           typeForDecimals          = Type.DECIMAL;
  private final        Map<String, Long>              totalVerticesByType      = new HashMap<>();
  private              long                           totalVerticesParsed      = 0L;
  private final        Map<String, Long>              totalEdgesByType         = new HashMap<>();
  private              long                           totalEdgesParsed         = 0L;
  private              long                           totalAttributesParsed    = 0L;
  private              int                            batchSize                = 10_000;
  private              long                           beginTimeVerticesCreation;
  private              long                           beginTimeEdgesCreation;
  private              boolean                        error                    = false;
  private final        ImporterContext                context;
  private final        Map<String, Map<String, Type>> schemaProperties         = new HashMap<>();
  private final static SimpleDateFormat               dateTimeISO8601Format    = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  // Neo4j ID -> packed ArcadeDB RID mapping, populated during vertex pass.
  // Uses primitive LongLongMap for numeric IDs (common case), falls back to HashMap for non-numeric IDs.
  private              LongLongMap                    numericIdMap;
  private              Map<String, Long>              stringIdMap;
  private              boolean                        useNumericIds            = true;

  public Neo4jImporter(final InputStream inputStream, final String... args) {
    parseArguments(args);
    this.inputStream = inputStream;
    this.context = new ImporterContext();
    if (inputStream == null)
      syntaxError("Input Stream is null");
  }

  public Neo4jImporter(final String... args) {
    parseArguments(args);
    this.context = new ImporterContext();
    if (inputFile == null)
      syntaxError("Missing input file. Use -f <file-path>");
  }

  public Neo4jImporter(final Database database, final ImporterContext context) {
    this.database = database;
    this.context = context;
    this.closeDatabaseAfterImport = false;
    initPackingConstants();
  }

  public static void main(final String[] args) throws IOException {
    new Neo4jImporter(args).run();
  }

  public void run() throws IOException {
    if (databasePath == null)
      log("Checking Neo4j database from file '%s'...", inputFile);
    else {
      log("Importing Neo4j database from file '%s' to '%s'", inputFile, databasePath);

      DatabaseFactory factory = new DatabaseFactory(databasePath);
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
      context.startedOn = System.currentTimeMillis();

      // PARSE THE FILE THE 1ST TIME TO CREATE THE SCHEMA AND CACHE EDGES IN RAM
      log("- Creation of the schema: types, properties and indexes");
      syncSchema();

      // PARSE THE FILE AGAIN TO CREATE VERTICES USING GRAPHBATCH
      log("- Creation of vertices started");
      beginTimeVerticesCreation = System.currentTimeMillis();
      parseVertices();

      // PARSE THE FILE AGAIN TO CREATE EDGES USING GRAPHBATCH
      log("- Creation of edges started: creating edges between vertices");
      beginTimeEdgesCreation = System.currentTimeMillis();
      parseEdges();

      final long elapsed = (System.currentTimeMillis() - context.startedOn) / 1000;

      log("***************************************************************************************************");
      log("Import of Neo4j database completed in %,d secs with %,d errors and %,d warnings.", elapsed, context.errors.get(),
          context.warnings.get());
      log("\nSUMMARY\n");
      log("- Vertices.............: %,d", totalVerticesParsed);
      for (final Map.Entry<String, Long> entry : totalVerticesByType.entrySet()) {
        final String typeName = entry.getKey();
        final Long verticesByClass = totalVerticesByType.get(typeName);
        final long entries = verticesByClass != null ? verticesByClass : 0L;
        final String additional = "";

        log("-- %-20s: %,d %s", typeName, entries, additional);
      }
      log("- Edges................: %,d", totalEdgesParsed);
      for (final Map.Entry<String, Long> entry : totalEdgesByType.entrySet()) {
        final String typeName = entry.getKey();
        final Long edgesByClass = totalEdgesByType.get(typeName);
        final long entries = edgesByClass != null ? edgesByClass : 0L;
        final String additional = "";

        log("-- %-20s: %,d %s", typeName, entries, additional);
      }
      log("- Total attributes.....: %,d", totalAttributesParsed);
      log("***************************************************************************************************");
      log("");
      log("NOTES:");

      if (database != null)
        log("- you can find your new ArcadeDB database in '" + database.getDatabasePath() + "'");

    } finally {
      numericIdMap = null;
      stringIdMap = null;
      if (database != null && closeDatabaseAfterImport)
        database.close();
    }
  }

  public boolean isError() {
    return error;
  }

  private void syncSchema() throws IOException {
    final VertexType rootNodeType = database.getSchema().buildVertexType().withName("Node")
        .withTotalBuckets(bucketsPerType).withIgnoreIfExists(true).create();
    rootNodeType.getOrCreateProperty("id", Type.STRING);
    rootNodeType.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" }, indexPageSize);

    readFile(json -> {
      switch (json.getString("type")) {
      case "node":
        final Pair<String, List<String>> labels = typeNameFromLabels(json);

        if (!database.getSchema().existsType(labels.getFirst())) {
          final VertexType type = database.getSchema().buildVertexType().withName(labels.getFirst())
              .withTotalBuckets(bucketsPerType).withIgnoreIfExists(true).create();
          if (labels.getSecond() != null) {
            for (final String parent : labels.getSecond()) {
              final VertexType parentType = database.getSchema().getOrCreateVertexType(parent);
              parentType.addSuperType(rootNodeType);

              type.addSuperType(parentType);
            }
          } else
            type.addSuperType(rootNodeType);
        }

        inferPropertyType(json, labels.getFirst());

        break;

      case "relationship":
        final String edgeLabel = json.has("label") && !json.isNull("label") ? json.getString("label") : null;
        if (edgeLabel != null)
          database.getSchema().buildEdgeType().withName(edgeLabel).withTotalBuckets(bucketsPerType).withIgnoreIfExists(true)
              .create();

        inferPropertyType(json, edgeLabel);
        break;
      }
      return null;
    });
  }

  private void inferPropertyType(final JSONObject json, final String label) {
    if (!json.has("properties"))
      return;

    // TRY TO INFER PROPERTY TYPES
    final Map<String, Type> typeProperties = schemaProperties.computeIfAbsent(label, k -> new HashMap<>());

    final JSONObject properties = json.getJSONObject("properties");
    for (final String propName : properties.keySet()) {
      ++totalAttributesParsed;

      Type currentType = typeProperties.get(propName);
      if (currentType == null) {
        Object propValue = properties.get(propName);
        if (propValue != null && !propValue.equals(JSONObject.NULL)) {

          if (propValue instanceof String string) {
            // CHECK IF IT'S A DATE
            try {
              dateTimeISO8601Format.parse(string);
              currentType = Type.DATETIME;
            } catch (final ParseException e) {
              currentType = Type.STRING;
            }
          } else {
            if (propValue instanceof JSONObject object)
              propValue = object.toMap();
            else if (propValue instanceof JSONArray array)
              propValue = array.toList();

            currentType = Type.getTypeByValue(propValue);
          }

          typeProperties.put(propName, currentType);
        }
      }
    }
  }

  private void parseVertices() throws IOException {
    final AtomicInteger lineNumber = new AtomicInteger();
    numericIdMap = new LongLongMap(1024);
    useNumericIds = true;

    try (final GraphBatch batch = database.batch()
        .withBidirectional(false)
        .withWAL(false)
        .withPreAllocateEdgeChunks(true)
        .withCommitEvery(0)
        .build()) {

      database.begin();

      readFileSimple(json -> {
        lineNumber.incrementAndGet();

        switch (json.getString("type")) {
        case "node":
          context.parsed.incrementAndGet();
          ++totalVerticesParsed;
          if (context.parsed.get() > 0 && context.parsed.get() % 1_000_000 == 0) {
            final long elapsed = System.currentTimeMillis() - beginTimeVerticesCreation;
            log("- Status update: created %,d vertices, skipped %,d edges (%,d vertices/sec)", context.createdVertices.get(),
                context.skippedEdges.get(), (context.createdVertices.get() / elapsed * 1000));
          }

          final Pair<String, List<String>> type = typeNameFromLabels(json);
          if (type == null) {
            log("- found vertex in line %d without labels. Skip it.", lineNumber.get());
            context.warnings.incrementAndGet();
            return null;
          }

          final String typeName = type.getFirst();
          final String id = json.getString("id");

          try {
            final Map<String, Object> props;
            if (json.has("properties"))
              props = setProperties(json.getJSONObject("properties"), schemaProperties.get(typeName));
            else
              props = new HashMap<>();
            props.put("id", id);

            final MutableVertex vertex = batch.createVertex(typeName, props);
            final long packedRID = packRID(vertex.getIdentity());
            putId(id, packedRID);
            context.createdVertices.incrementAndGet();

            incrementVerticesByType(typeName);
          } catch (Exception e) {
            error("- Error on saving vertex with id %s: %s", id, e.getMessage());
            context.errors.incrementAndGet();
          }

          if (context.createdVertices.get() > 0 && context.createdVertices.get() % batchSize == 0) {
            database.commit();
            database.begin();
          }

          break;

        case "relationship":
          context.skippedEdges.incrementAndGet();
          break;
        }

        if (parsingCallback != null)
          parsingCallback.call(json);

        return null;
      });

      if (database.isTransactionActive())
        database.commit();
    }

    final long elapsedInSecs = (System.currentTimeMillis() - context.startedOn) / 1000;

    log("- Creation of vertices completed: created %,d vertices, skipped %,d edges (%,d vertices/sec elapsed=%,d secs)",
        context.createdVertices.get(), context.skippedEdges.get(),
        elapsedInSecs > 0 ? (context.createdVertices.get() / elapsedInSecs) : 0, elapsedInSecs);
    log("- ID mapping mode: %s", useNumericIds ? "numeric (primitive long[])" : "string (HashMap)");
  }

  private void parseEdges() throws IOException {
    final AtomicInteger lineNumber = new AtomicInteger();

    try (final GraphBatch batch = database.batch()
        .withBatchSize(batchSize)
        .withBidirectional(true)
        .withWAL(false)
        .withCommitEvery(batchSize)
        .build()) {

      readFileSimple(json -> {
        lineNumber.incrementAndGet();

        switch (json.getString("type")) {
        case "node":
          break;

        case "relationship":
          context.parsed.incrementAndGet();
          ++totalEdgesParsed;
          if (context.parsed.get() > 0 && context.parsed.get() % 1_000_000 == 0) {
            final long elapsed = System.currentTimeMillis() - beginTimeEdgesCreation;
            log("- Status update: created %,d edges %s (%,d edges/sec)", context.createdEdges.get(), totalEdgesByType,
                (context.createdEdges.get() / elapsed * 1000));
          }

          final String type = json.getString("label");
          if (type == null) {
            log("- found edge in line %d without labels. Skip it.", lineNumber.get());
            context.warnings.incrementAndGet();
            return null;
          }

          final JSONObject start = json.getJSONObject("start");
          final String startId = start.getString("id");
          final long fromPacked = getId(startId);
          if (fromPacked == LongLongMap.EMPTY) {
            log("- cannot create relationship with id '%s'. Vertex id '%s' not found. Skip it.", json.getString("id"), startId);
            context.warnings.incrementAndGet();
            return null;
          }

          final JSONObject end = json.getJSONObject("end");
          final String endId = end.getString("id");
          final long toPacked = getId(endId);
          if (toPacked == LongLongMap.EMPTY) {
            log("- cannot create relationship with id '%s'. Vertex id '%s' not found. Skip it.", json.getString("id"), endId);
            context.warnings.incrementAndGet();
            return null;
          }

          final RID fromRID = unpackRID(fromPacked);
          final RID toRID = unpackRID(toPacked);

          try {
            if (json.has("properties")) {
              final Map<String, Object> edgeProps = setProperties(json.getJSONObject("properties"), schemaProperties.get(type));
              if (!edgeProps.isEmpty()) {
                // Flatten map to key-value array for GraphBatch
                final Object[] propsArray = new Object[edgeProps.size() * 2];
                int i = 0;
                for (final Map.Entry<String, Object> entry : edgeProps.entrySet()) {
                  propsArray[i++] = entry.getKey();
                  propsArray[i++] = entry.getValue();
                }
                batch.newEdge(fromRID, type, toRID, propsArray);
              } else
                batch.newEdge(fromRID, type, toRID);
            } else
              batch.newEdge(fromRID, type, toRID);

            context.createdEdges.incrementAndGet();
            incrementEdgesByType(type);
          } catch (Exception e) {
            error("- Error on saving edge between %s and %s: %s", fromRID, toRID, e.getMessage());
            context.errors.incrementAndGet();
          }

          break;
        }

        if (parsingCallback != null)
          parsingCallback.call(json);

        return null;
      });
    }

    final long elapsedInSecs = (System.currentTimeMillis() - context.startedOn) / 1000;

    log("- Creation of edges completed: created %,d edges, (%,d edges/sec elapsed=%,d secs)", context.createdEdges.get(),
        elapsedInSecs > 0 ? (context.createdEdges.get() / elapsedInSecs) : 0, elapsedInSecs);
  }

  // ═══════════════════════════════════════════════════════════════════
  //  ID mapping: numeric (primitive) or string (HashMap) mode
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Stores a Neo4j ID -> packed RID mapping. Tries numeric mode first; on the first non-numeric ID,
   * migrates all existing entries to the string HashMap and switches permanently.
   */
  private void putId(final String id, final long packedRID) {
    if (useNumericIds) {
      try {
        final long numericId = Long.parseLong(id);
        numericIdMap.put(numericId, packedRID);
        return;
      } catch (final NumberFormatException e) {
        // Non-numeric ID detected, migrate to string mode
        migrateToStringMode();
      }
    }
    stringIdMap.put(id, packedRID);
  }

  /**
   * Looks up a Neo4j ID and returns the packed RID, or {@link LongLongMap#EMPTY} if not found.
   */
  private long getId(final String id) {
    if (useNumericIds) {
      try {
        return numericIdMap.get(Long.parseLong(id));
      } catch (final NumberFormatException e) {
        return LongLongMap.EMPTY;
      }
    }
    final Long packed = stringIdMap.get(id);
    return packed != null ? packed : LongLongMap.EMPTY;
  }

  /**
   * Migrates from primitive LongLongMap to HashMap when a non-numeric ID is encountered.
   */
  private void migrateToStringMode() {
    log("- Non-numeric Neo4j ID detected, switching to string-based ID mapping");
    useNumericIds = false;
    stringIdMap = new HashMap<>(numericIdMap.size() * 2);
    numericIdMap.forEach((key, value) -> stringIdMap.put(Long.toString(key), value));
    numericIdMap = null;
  }

  // Bit layout for packing RID (bucketId, position) into a single long.
  // Default: 10 bits for bucket (max 1,023) and 54 bits for position (max ~18 quadrillion).
  // Adjustable via -bucketBits <n> to support databases with more buckets at the cost of position range.
  private              int  bucketBits  = 10;
  private              int  maxBucketId;
  private              long posMask;

  private void initPackingConstants() {
    maxBucketId = (1 << bucketBits) - 1;
    posMask = (1L << (64 - bucketBits)) - 1;
  }

  /** Packs a RID (bucketId, position) into a single long. */
  private long packRID(final RID rid) {
    final int bucketId = rid.getBucketId();
    if (bucketId > maxBucketId)
      throw new IllegalStateException(
          "Bucket ID " + bucketId + " exceeds the " + bucketBits + "-bit maximum (" + maxBucketId
              + "). Use -bucketBits <n> to increase (e.g. -bucketBits 16 supports up to 65,535 buckets).");
    return ((long) bucketId << (64 - bucketBits)) | (rid.getPosition() & posMask);
  }

  /** Unpacks a long back into a RID. */
  private RID unpackRID(final long packed) {
    return new RID((int) (packed >>> (64 - bucketBits)), packed & posMask);
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Property conversion
  // ═══════════════════════════════════════════════════════════════════

  private Map<String, Object> setProperties(final JSONObject properties, final Map<String, Type> typeSchema) {
    final Map<String, Object> result = new HashMap<>();

    for (final String propName : properties.keySet()) {
      Object propValue = properties.get(propName);

      if (propValue == JSONObject.NULL)
        propValue = null;
      else if (propValue instanceof JSONObject object)
        propValue = setProperties(object, null);
      else if (propValue instanceof JSONArray array)
        propValue = array.toList();
      else if (propValue instanceof String string && typeSchema != null && typeSchema.get(propName) == Type.DATETIME) {
        try {
          propValue = dateTimeISO8601Format.parse(string).getTime();
        } catch (final ParseException e) {
          log("Invalid date '%s', ignoring conversion to timestamp and leaving it as string", propValue);
          context.errors.incrementAndGet();
        }
      } else if (propValue instanceof BigDecimal) {
        propValue = typeForDecimals.newInstance(propValue);
      }

      result.put(propName, propValue);
    }

    return result;
  }

  public InputStream openInputStream() throws IOException {
    if (inputStream != null) {
      inputStream.reset();
      return inputStream;
    }

    final File file = new File(inputFile);
    if (!file.exists()) {
      error = true;
      throw new IllegalArgumentException("File '" + inputFile + "' not found");
    }

    return file.getName().endsWith("gz") ? new GZIPInputStream(new FileInputStream(file)) : new FileInputStream(file);
  }

  /**
   * Reads the JSONL file line by line, calling the callback for each valid JSON record.
   * Used by syncSchema (which only reads, no record creation) with its own transaction management.
   */
  private void readFile(final Callable<Void, JSONObject> callback) throws IOException {
    database.begin();

    try (InputStream inputStream = openInputStream()) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, DatabaseFactory.getDefaultCharset()))) {
        for (long lineNumber = 0; ; ++lineNumber) {
          try {
            final String line = reader.readLine();
            if (line == null)
              break;

            final JSONObject json = new JSONObject(line);
            final String type = json.getString("type");
            if ("node".equals(type) || "relationship".equals(type))
              callback.call(json);
            else {
              log("Invalid 'type' content on line %d of the input JSONL file. The line will be ignored. JSON: %s", lineNumber, line);
              context.errors.incrementAndGet();
            }
          } catch (final JSONException e) {
            log("Error on parsing json on line %d of the input JSONL file. The line will be ignored.", lineNumber);
            context.errors.incrementAndGet();
          }
        }
      }
    }

    if (database.isTransactionActive())
      database.commit();
  }

  /**
   * Simplified file reader for vertex/edge passes that use GraphBatch.
   * GraphBatch handles its own transaction management, so this method just reads and dispatches.
   */
  private void readFileSimple(final Callable<Void, JSONObject> callback) throws IOException {
    try (InputStream inputStream = openInputStream()) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, DatabaseFactory.getDefaultCharset()))) {
        for (long lineNumber = 0; ; ++lineNumber) {
          try {
            final String line = reader.readLine();
            if (line == null)
              break;

            final JSONObject json = new JSONObject(line);
            final String type = json.getString("type");
            if ("node".equals(type) || "relationship".equals(type))
              callback.call(json);
            else {
              log("Invalid 'type' content on line %d of the input JSONL file. The line will be ignored. JSON: %s", lineNumber, line);
              context.errors.incrementAndGet();
            }
          } catch (final JSONException e) {
            log("Error on parsing json on line %d of the input JSONL file. The line will be ignored.", lineNumber);
            context.errors.incrementAndGet();
          }
        }
      }
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
    if (nodeLabels != null && !nodeLabels.isEmpty()) {
      if (nodeLabels.length() > 1) {
        // MULTI LABEL, CREATE A NEW MIXED TYPE THAT EXTEND ALL THE LABELS BY USING INHERITANCE
        final List<String> list = nodeLabels.toList().stream().map(String.class::cast).sorted(Comparator.naturalOrder())
            .collect(Collectors.toList());
        return new Pair<>(String.join(Labels.LABEL_SEPARATOR, list), list);
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
      System.out.println(text.formatted(args));
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
    log("-bucketBits <n>: bits for bucket ID in RID packing (default 10, max bucket ID 1,023). Increase if you have many types/buckets.");
  }

  private void parseArguments(final String... args) {
    printHeader();

    String state = null;
    for (final String arg : args) {
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
      else if (arg.equals("-bucketBits"))
        state = "bucketBits";
      else if (state != null) {
        if (state.equals("databasePath"))
          databasePath = arg;
        else if (state.equals("inputFile"))
          inputFile = arg;
        else if (state.equals("batchSize"))
          batchSize = Integer.parseInt(arg);
        else if (state.equals("decimalType"))
          typeForDecimals = Type.valueOf(arg.toUpperCase(Locale.ENGLISH));
        else if (state.equals("bucketBits")) {
          bucketBits = Integer.parseInt(arg);
          if (bucketBits < 1 || bucketBits > 32)
            syntaxError("bucketBits must be between 1 and 32");
        }
      }
    }

    initPackingConstants();
  }

  // ═══════════════════════════════════════════════════════════════════
  //  LongLongMap: open-addressing primitive long-to-long hash map
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Open-addressing hash map from primitive long to primitive long, with no object overhead.
   * Uses ~24 bytes per entry at 70% load factor (16 bytes for key+value arrays + table overhead).
   * Modeled after {@code GraphImporter.IntIntMap} but with long keys and values.
   */
  static final class LongLongMap {
    static final long EMPTY = Long.MIN_VALUE;

    private long[] keys;
    private long[] values;
    private int    mask;
    private int    size;
    private int    threshold;

    LongLongMap(final int expected) {
      int cap = Integer.highestOneBit(Math.max(16, (int) (expected / 0.7))) << 1;
      keys = new long[cap];
      values = new long[cap];
      mask = cap - 1;
      threshold = (int) (cap * 0.7);
      Arrays.fill(keys, EMPTY);
    }

    void put(final long key, final long value) {
      if (key == EMPTY)
        throw new IllegalArgumentException("Key cannot be Long.MIN_VALUE (reserved as EMPTY sentinel)");
      if (size >= threshold)
        resize();
      int i = index(key);
      while (keys[i] != EMPTY && keys[i] != key)
        i = (i + 1) & mask;
      if (keys[i] == EMPTY)
        size++;
      keys[i] = key;
      values[i] = value;
    }

    long get(final long key) {
      int i = index(key);
      while (keys[i] != EMPTY) {
        if (keys[i] == key)
          return values[i];
        i = (i + 1) & mask;
      }
      return EMPTY;
    }

    int size() {
      return size;
    }

    void forEach(final LongLongConsumer consumer) {
      for (int i = 0; i < keys.length; i++)
        if (keys[i] != EMPTY)
          consumer.accept(keys[i], values[i]);
    }

    private int index(final long key) {
      // Fibonacci hashing for good distribution
      return (int) ((key * 0x9E3779B97F4A7C15L) >>> (64 - Integer.numberOfTrailingZeros(keys.length))) & mask;
    }

    private void resize() {
      final int newCap = keys.length << 1;
      final long[] oldKeys = keys;
      final long[] oldValues = values;
      keys = new long[newCap];
      values = new long[newCap];
      mask = newCap - 1;
      threshold = (int) (newCap * 0.7);
      Arrays.fill(keys, EMPTY);
      for (int i = 0; i < oldKeys.length; i++)
        if (oldKeys[i] != EMPTY) {
          int j = index(oldKeys[i]);
          while (keys[j] != EMPTY)
            j = (j + 1) & mask;
          keys[j] = oldKeys[i];
          values[j] = oldValues[i];
        }
    }

    @FunctionalInterface
    interface LongLongConsumer {
      void accept(long key, long value);
    }
  }
}
