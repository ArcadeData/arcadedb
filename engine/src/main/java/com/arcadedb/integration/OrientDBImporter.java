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

package com.arcadedb.integration;

import com.arcadedb.Constants;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.*;
import com.arcadedb.utility.FileUtils;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Importer from OrientDB. OrientDB is a registered mark of SAP.
 *
 * @author Luca Garulli
 */
public class OrientDBImporter {
  private String                     databasePath;
  private String                     inputFile;
  private String                     securityFileName;
  private String                     databaseName;
  private boolean                    overwriteDatabase      = false;
  private Map<String, Integer>       clustersNameToId       = new LinkedHashMap<>();
  private Map<Integer, String>       clustersIdToName       = new LinkedHashMap<>();
  private Map<String, OrientDBClass> classes                = new LinkedHashMap<>();
  private Map<String, Long>          totalRecordByType      = new HashMap<>();
  private long                       totalRecordParsed      = 0L;
  private long                       totalAttributesParsed  = 0L;
  private long                       errors                 = 0L;
  private long                       warnings               = 0L;
  private Set<String>                excludeClasses         = new HashSet<>(Arrays
      .asList(new String[] { "OUser", "ORole", "OSchedule", "OSequence", "OTriggered", "OSecurityPolicy", "ORestricted", "OIdentity", "OFunction", "V", "E" }));
  private DatabaseFactory            factory;
  private Database                   database;
  private Set<String>                edgeClasses            = new HashSet<>();
  private List<Map<String, Object>>  parsedUsers            = new ArrayList<>();
  private Map<RID, RID>              vertexRidMap           = new HashMap<>();
  private int                        batchSize              = 10_000;
  private PHASE                      phase                  = PHASE.OFF; // phase1 = create DB and cache edges in RAM, phase2 = create vertices and edges
  private long                       processedItems         = 0L;
  private long                       skippedEdges           = 0L;
  private long                       savedDocuments         = 0L;
  private long                       savedVertices          = 0L;
  private long                       savedEdges             = 0L;
  private Map<String, Long>          totalEdgesByVertexType = new HashMap<>();
  private long                       beginTime;
  private long                       beginTimeRecordsCreation;
  private long                       beginTimeEdgeCreation;
  private GZIPInputStream            inputStream;
  private JsonReader                 reader;
  private boolean                    error                  = false;

  private enum PHASE {OFF, CREATE_SCHEMA, CREATE_RECORDS, CREATE_EDGES}

  private class OrientDBClass {
    List<String>        superClasses = new ArrayList<>();
    List<Integer>       clusterIds   = new ArrayList<>();
    Map<String, String> properties   = new LinkedHashMap<>();
  }

  public OrientDBImporter(final String[] args) {
    printHeader();

    String state = null;
    for (String arg : args) {
      if (arg.equals("-?"))
        printHelp();
      else if (arg.equals("-d"))
        state = "databasePath";
      else if (arg.equals("-i"))
        state = "inputFile";
      else if (arg.equals("-s"))
        state = "securityFile";
      else if (arg.equals("-o"))
        overwriteDatabase = true;
      else if (arg.equals("-b"))
        state = "batchSize";
      else {
        if (state.equals("databasePath"))
          databasePath = arg;
        else if (state.equals("inputFile"))
          inputFile = arg;
        else if (state.equals("securityFile"))
          securityFileName = arg;
        else if (state.equals("batchSize"))
          batchSize = Integer.parseInt(arg);
      }
    }

    if (inputFile == null)
      syntaxError("Missing input file. Use -f <file-path>");
  }

  public static void main(final String[] args) throws IOException {
    new OrientDBImporter(args).run();
  }

  public void run() throws IOException {
    File file = new File(inputFile);
    if (!file.exists()) {
      error = true;
      throw new IllegalArgumentException("File '" + inputFile + "' not found");
    }

    if (databasePath == null)
      log("Checking OrientDB database from file '%s'...", inputFile);
    else {
      log("Importing OrientDB database from file '%s' to '%s'", inputFile, databasePath);

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

      phase = PHASE.CREATE_SCHEMA;

      // PARSE THE FILE THE 1ST TIME TO CREATE THE SCHEMA AND CACHE EDGES IN RAM
      log("Creation of the schema: types, properties and indexes");
      parseInputFile(file);

      phase = PHASE.CREATE_EDGES;

      // PARSE THE FILE AGAIN TO CREATE RECORDS AND EDGES
      log("- Creation of edges started: creating edges between vertices");
      beginTimeEdgeCreation = System.currentTimeMillis();
      parseInputFile(file);

      final long elapsed = (System.currentTimeMillis() - beginTime) / 1000;

      log("***************************************************************************************************");
      log("Import of OrientDB database completed in %,d secs with %,d errors and %,d warnings.", elapsed, errors, warnings);
      log("\nSUMMARY\n");
      log("- Records..............: %,d", totalRecordParsed);
      for (Map.Entry<String, OrientDBClass> entry : classes.entrySet()) {
        final String className = entry.getKey();
        final Long recordsByClass = totalRecordByType.get(className);
        final long entries = recordsByClass != null ? recordsByClass : 0L;
        String additional = "";

        if (excludeClasses.contains(className))
          additional = " (excluded)";

        log("-- %-20s: %,d %s", className, entries, additional);
      }
      log("- Total attributes.....: %,d", totalAttributesParsed);
      log("***************************************************************************************************");
      log("");
      log("NOTES:");

      if (securityFileName == null)
        log("- users stored in OUser class will not be imported because ArcadeDB has users only at server level. If you want to import such users into ArcadeDB server configuration, please run the importer with the option -s <securityFile>");
      else {
        writeSecurityFile();
        log("- you can find your security.json file to install in ArcadeDB server in '" + securityFileName + "'");
      }

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

  private void parseInputFile(final File file) throws IOException {
    inputStream = new GZIPInputStream(new FileInputStream(file));

    reader = new JsonReader(new InputStreamReader(inputStream));
    reader.setLenient(true);
    reader.beginObject();

    while (reader.hasNext()) {
      switch (reader.nextName()) {
      case "info":
        parseDatabaseInfo();
        break;

      case "clusters":
        parseClusters();
        break;

      case "schema":
        parseSchema();
        break;

      case "records":
        parseRecords();
        break;

      default:
        reader.skipValue();
      }
    }
    reader.endObject();
  }

  private void writeSecurityFile() throws IOException {
    final File securityFile = new File(securityFileName);

    final JSONObject securityFileContent = new JSONObject();

    final JSONObject users = new JSONObject();
    securityFileContent.put("users", users);

    for (Map<String, Object> u : parsedUsers) {
      String userName = (String) u.get("name");
      String password = (String) u.get("password");

      final JSONObject user = new JSONObject();

      final JSONArray databases = new JSONArray();
      databases.put(databaseName);
      user.put("name", userName);
      user.put("password", password);
      user.put("databases", databases);
      user.put("databaseBlackList", true);

      users.put(userName, user);
    }

    FileUtils.writeFile(securityFile, securityFileContent.toString());
  }

  private void parseRecords() throws IOException {
    reader.beginArray();

    processedItems = 0L;
    skippedEdges = 0L;

    database.begin();

    while (reader.peek() == JsonToken.BEGIN_OBJECT) {
      final Map<String, Object> attributes = parseRecord(reader, false);

      final String className = (String) attributes.get("@class");

      if (phase == PHASE.CREATE_SCHEMA || phase == PHASE.CREATE_RECORDS) {
        if (!edgeClasses.contains(className)) {
          createRecord(attributes);
        } else
          ++skippedEdges;

        if (processedItems > 0 && processedItems % 1_000_000 == 0) {
          final long elapsed = System.currentTimeMillis() - beginTimeRecordsCreation;
          log("- Status update: created %,d vertices and %,d documents, skipped %,d edges (%,d records/sec)", savedVertices, savedDocuments, skippedEdges,
              ((savedDocuments + savedVertices) / elapsed * 1000));
        }

      } else {
        if (edgeClasses.contains(className)) {
          createEdges(attributes);
          if (processedItems > 0 && processedItems % 1_000_000 == 0) {
            final long elapsed = System.currentTimeMillis() - beginTimeEdgeCreation;
            log("- Status update: created %,d edges %s (%,d edges/sec)", savedEdges, totalEdgesByVertexType, (savedEdges / elapsed * 1000));
          }
        }
      }

      ++processedItems;

      if (processedItems > 0 && processedItems % batchSize == 0) {
        database.commit();
        database.begin();
      }

      if (reader.peek() == JsonToken.NULL)
        // FIX A BUG ON ORIENTDB EXPORTER WHEN GENERATE AN EMPTY RECORD
        reader.skipValue();
    }

    database.commit();

    final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

    switch (phase) {
    case CREATE_RECORDS:
      log("- Creation of records completed: created %,d vertices and %,d documents, skipped %,d edges (%,d records/sec elapsed=%,d secs)", savedVertices,
          savedDocuments, skippedEdges, elapsedInSecs > 0 ? ((savedDocuments + savedVertices) / elapsedInSecs) : 0, elapsedInSecs);
      break;
    case CREATE_EDGES:
      log("- Creation of edges completed: created %,d edges %s (%,d edges/sec elapsed=%,d secs)", savedEdges, totalEdgesByVertexType,
          elapsedInSecs > 0 ? (savedEdges / elapsedInSecs) : 0, elapsedInSecs);
      break;
    default:
      error = true;
      throw new IllegalArgumentException("Invalid phase " + phase);
    }
    reader.endArray();
  }

  private MutableDocument createRecord(final Map<String, Object> attributes) throws IOException {
    MutableDocument record = null;

    final RID recordRid = new RID(null, (String) attributes.get("@rid"));
    final String recordType = (String) attributes.get("@type");
    if (recordType != null) {
      switch (recordType) {
      case "b":
        // BINARY
        if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 0) {
          // INTERNAL ORIENTDB RECORD, IGNORE THEM
        } else {
          error("- Unsupported binary record. Ignoring record: %s", attributes);
          ++warnings;
        }
        break;
      case "d":
        // DOCUMENT, VERTEX, EDGE
        final String className = (String) attributes.get("@class");
        if (className == null) {
          if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 1) {
            // INTERNAL ORIENTDB RECORD SCHEMA, IGNORE THEM
          } else if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 2)
            // INTERNAL ORIENTDB RECORD INDEX MGR, EXTRACT INDEXES
            parseIndexes(attributes);
          else {
            error("- Unsupported record without class. Ignoring record: %s", attributes);
            ++warnings;
          }
        } else {
          if (className.equals("OUser"))
            parsedUsers.add(attributes);

          incrementRecordByClass(className);
          ++totalRecordParsed;

          if (!excludeClasses.contains(className)) {
            final DocumentType type = database.getSchema().getType(className);

            if (type instanceof VertexType)
              record = database.newVertex(className);
            else if (type instanceof EdgeType)
              // SKIP IT. EDGES ARE CREATED FROM VERTICES
              return null;
            else
              record = database.newDocument(className);

            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
              final String attrName = entry.getKey();
              if (attrName.startsWith("@"))
                continue;

              Object attrValue = entry.getValue();

              if (attrValue instanceof Map) {
                //EMBEDDED
                attrValue = createRecord((Map<String, Object>) attrValue);
              } else if (attrValue instanceof List) {
                if (record instanceof Vertex && (attrName.startsWith("out_") || attrName.startsWith("in_")))
                  // EDGES WILL BE CREATE BELOW IN THE 2ND PHASE
                  attrValue = null;
              }

              if (attrValue != null)
                record.set(attrName, attrValue);
            }

            record.save();

            if (type instanceof VertexType)
              ++savedVertices;
            else
              ++savedDocuments;

            if (type instanceof VertexType) {
              // REMEMBER THE VERTEX TO ATTACH EDGES ON 2ND PHASE
              vertexRidMap.put(recordRid, record.getIdentity());
            }
          }
        }
        break;
      default:
        error("- Unsupported record type '%s'", recordType);
      }
    }

    return record;
  }

  private void incrementRecordByClass(final String className) {
    Long recordsByClass = totalRecordByType.get(className);
    if (recordsByClass == null) {
      recordsByClass = 0L;
      totalRecordByType.put(className, recordsByClass);
    }
    totalRecordByType.put(className, recordsByClass + 1);
  }

  private void createEdges(final Map<String, Object> attributes) {
    final String recordType = (String) attributes.get("@type");
    if (recordType == null || !recordType.equals("d"))
      return;

    // DOCUMENT, VERTEX, EDGE
    final String className = (String) attributes.get("@class");
    if (className == null)
      return;

    if (excludeClasses.contains(className))
      return;

    final DocumentType type = database.getSchema().getType(className);
    if (!(type instanceof EdgeType))
      return;

    Map<String, Object> properties = null;
    for (Map.Entry<String, Object> attr : attributes.entrySet())
      if (!attr.getKey().startsWith("@") && !attr.getKey().equals("out") && !attr.getKey().equals("in")) {
        if (properties == null)
          properties = new HashMap<>();
        properties.put(attr.getKey(), attr.getValue());
      }

    final RID out = new RID(database, (String) attributes.get("out"));
    final RID newOut = vertexRidMap.get(out);

    final RID in = new RID(database, (String) attributes.get("in"));
    final RID newIn = vertexRidMap.get(in);

    final Vertex sourceVertex = (Vertex) database.lookupByRID(newOut, false);

    sourceVertex.newEdge(className, newIn, true, properties);
    ++savedEdges;

    Long edgesByVertexType = totalEdgesByVertexType.get(className);
    if (edgesByVertexType == null) {
      edgesByVertexType = 0L;
      totalEdgesByVertexType.put(className, edgesByVertexType);
    }
    totalEdgesByVertexType.put(className, edgesByVertexType + 1);

    incrementRecordByClass(className);
  }

  private Map<String, Object> parseRecord(JsonReader reader, final boolean ignore) throws IOException {
    final Map<String, Object> attributes = ignore ? null : new LinkedHashMap<>();

    reader.beginObject();
    while (reader.peek() != JsonToken.END_OBJECT) {
      final String attributeName = reader.nextName();
      final Object attributeValue;

      final JsonToken propertyType = reader.peek();
      switch (propertyType) {
      case STRING:
        attributeValue = reader.nextString();
        break;
      case NUMBER:
        attributeValue = reader.nextDouble();
        break;
      case BOOLEAN:
        attributeValue = reader.nextBoolean();
        break;
      case NULL:
        reader.nextNull();
        attributeValue = null;
        break;
      case BEGIN_OBJECT:
        attributeValue = parseRecord(reader, ignore);
        break;
      case BEGIN_ARRAY:
        attributeValue = parseArray(reader, ignore);
        break;
      default:
        log("Skipping property '%s' of type '%s'", attributeName, propertyType);
        continue;
      }

      if (!ignore) {
        attributes.put(attributeName, attributeValue);
        ++totalAttributesParsed;
      }
    }

    reader.endObject();
    return attributes;
  }

  private List<Object> parseArray(JsonReader reader, final boolean ignore) throws IOException {
    final List<Object> list = ignore ? null : new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      final Object entryValue;

      final JsonToken entryType = reader.peek();
      switch (entryType) {
      case STRING:
        entryValue = reader.nextString();
        break;
      case NUMBER:
        entryValue = reader.nextDouble();
        break;
      case BOOLEAN:
        entryValue = reader.nextBoolean();
        break;
      case NULL:
        reader.nextNull();
        entryValue = null;
        break;
      case BEGIN_OBJECT:
        entryValue = parseRecord(reader, ignore);
        break;
      case BEGIN_ARRAY:
        entryValue = parseArray(reader, ignore);
        break;
      default:
        log("Skipping entry of type '%s'", entryType);
        continue;
      }

      if (!ignore)
        list.add(entryValue);
    }
    reader.endArray();
    return list;
  }

  private void parseSchema() throws IOException {
    reader.beginObject();

    while (reader.peek() != JsonToken.END_OBJECT) {
      final String name = reader.nextName();

      switch (name) {
      case "classes":
        reader.beginArray();

        while (reader.peek() != JsonToken.END_ARRAY) {
          reader.beginObject();

          String className = null;
          OrientDBClass cls = new OrientDBClass();

          while (reader.peek() != JsonToken.END_OBJECT) {
            switch (reader.nextName()) {
            case "name":
              className = reader.nextString();
              break;

            case "cluster-ids":
              reader.beginArray();
              while (reader.peek() != JsonToken.END_ARRAY)
                cls.clusterIds.add(reader.nextInt());
              reader.endArray();
              break;

            case "super-classes":
              reader.beginArray();
              while (reader.peek() != JsonToken.END_ARRAY)
                cls.superClasses.add(reader.nextString());
              reader.endArray();
              break;

            case "properties":
              String propertyName = null;
              String propertyType = null;

              reader.beginArray();
              while (reader.peek() != JsonToken.END_ARRAY) {
                reader.beginObject();

                while (reader.peek() != JsonToken.END_OBJECT) {
                  switch (reader.nextName()) {
                  case "name":
                    propertyName = reader.nextString();
                    break;
                  case "type":
                    propertyType = reader.nextString();
                    break;
                  default:
                    reader.skipValue();
                  }
                }

                cls.properties.put(propertyName, propertyType);
                reader.endObject();
              }
              reader.endArray();
              break;

            default:
              reader.skipValue();
            }
          }
          reader.endObject();

          classes.put(className, cls);
        }
        reader.endArray();
        break;

      default:
        reader.skipValue();
      }
    }

    reader.endObject();

    for (String className : classes.keySet())
      createType(className);
  }

  private void parseIndexes(final Map<String, Object> attributes) throws IOException {
    final List<Map<String, Object>> parsedIndexes = (List<Map<String, Object>>) attributes.get("indexes");
    for (Map<String, Object> parsedIndex : parsedIndexes) {
      final String indexName = (String) parsedIndex.get("name");
      final boolean unique = parsedIndex.get("type").toString().startsWith("UNIQUE");

      final Map<String, Object> indexDefinition = (Map<String, Object>) parsedIndex.get("indexDefinition");
      final String className = (String) indexDefinition.get("className");

      if (!classes.containsKey(className))
        continue;

      if (excludeClasses.contains(className))
        continue;

      final String fieldName = (String) indexDefinition.get("field");
      final String[] properties = new String[] { fieldName };
      final boolean nullValuesIgnored = (boolean) indexDefinition.get("nullValuesIgnored");

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, unique, className, properties, LSMTreeIndexAbstract.DEF_PAGE_SIZE,
          nullValuesIgnored ? LSMTreeIndexAbstract.NULL_STRATEGY.SKIP : LSMTreeIndexAbstract.NULL_STRATEGY.ERROR, null);

      log("- Created index %s on %s%s", unique ? "UNIQUE" : "NOT UNIQUE", className, Arrays.toString(properties));
    }

    if (phase == PHASE.CREATE_SCHEMA) {
      log("Creation of records started: creating vertices and documents records (edges on the next phase)");
      phase = PHASE.CREATE_RECORDS;
      beginTimeRecordsCreation = System.currentTimeMillis();
    }
  }

  private void createType(final String className) {
    final OrientDBClass classInfo = classes.get(className);

    int type = 0;
    if (!classInfo.superClasses.isEmpty()) {
      for (String c : classInfo.superClasses)
        createType(c);

      type = getClassType(classInfo.superClasses);
    }

    if (database.getSchema().existsType(className))
      // ALREADY CREATED
      return;

    if (excludeClasses.contains(className))
      // SPECIAL ORIENTDB CLASSES, NO NEED IN ARCADEDB
      return;

    final DocumentType t;

    switch (type) {
    case 1:
      t = database.getSchema().createVertexType(className);
      break;

    case 2:
      t = database.getSchema().createEdgeType(className);
      edgeClasses.add(className);
      break;

    default:
      t = database.getSchema().createDocumentType(className);
    }

    // CREATE PROPERTIES
    for (Map.Entry<String, String> entry : classInfo.properties.entrySet()) {
      t.createProperty(entry.getKey(), Type.valueOf(entry.getValue()));
    }

    log("- Created type '%s' with the following properties %s", className, classInfo.properties);
  }

  private int getClassType(List<String> list) {
    int type = 0;
    for (int i = 0; type == 0 && i < list.size(); ++i) {
      final String su = list.get(i);
      if (su.equals("V")) {
        type = 1;
        break;
      } else if (su.equals("E")) {
        type = 2;
        break;
      }

      final OrientDBClass c = classes.get(su);
      if (c != null) {
        if (!c.superClasses.isEmpty())
          type = getClassType(c.superClasses);
      }
    }
    return type;
  }

  private void parseClusters() throws IOException {
    reader.beginArray();
    while (reader.hasNext()) {
      reader.beginObject();

      String clusterName = null;
      int clusterId = -1;

      while (reader.hasNext()) {
        switch (reader.nextName()) {
        case "name":
          clusterName = reader.nextString();
          break;

        case "id":
          clusterId = reader.nextInt();
          break;

        default:
          reader.skipValue();
        }

        if (clusterName != null && clusterId > -1) {
          clustersNameToId.put(clusterName, clusterId);
          clustersIdToName.put(clusterId, clusterName);
          clusterName = null;
          clusterId = -1;
          skipUntilEndOfObject(reader, JsonToken.END_OBJECT);

          if (reader.peek() != JsonToken.BEGIN_OBJECT)
            break;
          reader.beginObject();
        }
      }
      reader.endObject();
    }
    reader.endArray();
  }

  private void parseDatabaseInfo() throws IOException {
    reader.beginObject();

    switch (reader.nextName()) {
    case "name":
      databaseName = reader.nextString();
      break;

    default:
      reader.skipValue();
    }

    skipUntilEndOfObject(reader, JsonToken.END_OBJECT);
    reader.endObject();
  }

  private void skipUntilEndOfObject(final JsonReader reader, final JsonToken token) throws IOException {
    while (reader.hasNext()) {
      final JsonToken t = reader.peek();
      reader.skipValue();

      if (t == token)
        return;
    }
  }

  private void skipUntilPropertyName(final JsonReader reader, final String propertyName) throws IOException {
    while (reader.hasNext()) {
      final JsonToken t = reader.peek();

      if (t == JsonToken.NAME) {
        if (reader.nextName().equals(propertyName))
          return;
      } else
        reader.skipValue();
    }
  }

  private void log(final String text, final Object... args) {
    if (args.length == 0)
      System.out.println(text);
    else
      System.out.println(String.format(text, args));
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
    log(Constants.PRODUCT + " " + Constants.getVersion() + " - OrientDB Importer");
  }

  private void printHelp() {
    log("Use:");
    log("-d <database-path>: create a database from the OrientDB export");
    log("-i <input-file>: path to the OrientDB export file. The default name is export.json.gz");
    log("-s <security-file>: path to the security file generated from OrientDB users to use in ArcadeDB server");
  }
}
