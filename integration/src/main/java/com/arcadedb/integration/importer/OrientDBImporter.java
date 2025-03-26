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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.CompressedRID2RIDIndex;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.zip.*;

import static com.google.gson.stream.JsonToken.BEGIN_OBJECT;
import static com.google.gson.stream.JsonToken.END_ARRAY;
import static com.google.gson.stream.JsonToken.END_OBJECT;
import static com.google.gson.stream.JsonToken.NULL;

/**
 * Importer from OrientDB. OrientDB is a registered mark of SAP.
 *
 * @author Luca Garulli
 */
public class OrientDBImporter {
  private static final int                        CONCURRENT_MAX_RETRY            = 3;
  private final        File                       file;
  private final        Map<String, OrientDBClass> classes                         = new LinkedHashMap<>();
  private final        Map<String, Long>          totalRecordByType               = new HashMap<>();
  private final        Set<String>                excludeClasses                  = Set.of("OUser", "ORole", "OSchedule",
      "OSequence", "OTriggered", "OSecurityPolicy",
      "ORestricted", "OIdentity", "OFunction", "_studio");
  private final        Set<String>                edgeClasses                     = new HashSet<>();
  private final        List<Map<String, Object>>  parsedUsers                     = new ArrayList<>();
  private              CompressedRID2RIDIndex     compressedRecordsRidMap;
  private final        Set<RID>                   documentsWithLinksToUpdate      = new HashSet<>();
  private final        Map<String, AtomicLong>    totalEdgesByVertexType          = new HashMap<>();
  private final        ImporterSettings           settings;
  private              List<Map<String, Object>>  parsedIndexes;
  private final        ConsoleLogger              logger;
  private              String                     databasePath;
  private              String                     inputFile;
  private              String                     databaseName;
  private              boolean                    createSecurityFiles             = false;
  private              boolean                    overwriteDatabase               = false;
  private              long                       totalAttributesParsed           = 0L;
  private              long                       errors                          = 0L;
  private              long                       warnings                        = 0L;
  private              DatabaseFactory            factory;
  private              Database                   database;
  private              int                        batchSize                       = 10_000;
  private              PHASE                      phase                           = PHASE.OFF; // phase1 = create DB and cache edges in RAM, phase2 = create vertices and edges
  private              long                       skippedRecordBecauseNullKey     = 0L;
  private              long                       skippedEdgeBecauseMissingVertex = 0l;
  private              long                       beginTime;
  private              long                       beginTimeRecordsCreation;
  private              long                       beginTimeEdgeCreation;
  private              JsonReader                 reader;
  private              boolean                    error                           = false;
  private              ImporterContext            context                         = new ImporterContext();

  private enum PHASE {OFF, ANALYZE, CREATE_SCHEMA, CREATE_RECORDS, CREATE_EDGES}

  private static class OrientDBProperty {
    String              type;
    String              ofType;
    Boolean             readOnly;
    Boolean             mandatory;
    Boolean             notNull;
    Object              defaultValue;
    Object              min;
    Object              max;
    String              regexp;
    Map<String, Object> customFields;
  }

  private static class OrientDBClass {
    String                        name;
    List<String>                  superClasses = new ArrayList<>();
    List<Integer>                 clusterIds   = new ArrayList<>();
    Map<String, OrientDBProperty> properties   = new LinkedHashMap<>();
  }

  public OrientDBImporter(final String[] args) {
    this.settings = new ImporterSettings();

    String state = null;
    for (final String arg : args) {
      if (arg.equals("-?"))
        printHelp();
      else if (arg.equals("-d"))
        state = "databasePath";
      else if (arg.equals("-i"))
        state = "inputFile";
      else if (arg.equals("-s"))
        createSecurityFiles = true;
      else if (arg.equals("-o"))
        overwriteDatabase = true;
      else if (arg.equals("-b"))
        state = "batchSize";
      else if (arg.equals("-v"))
        state = "verboseLevel";
      else if (state != null) {
        if (state.equals("databasePath"))
          databasePath = arg;
        else if (state.equals("inputFile"))
          inputFile = arg;
        else if (state.equals("batchSize"))
          batchSize = Integer.parseInt(arg);
        else if (state.equals("verboseLevel"))
          settings.verboseLevel = Integer.parseInt(arg);
      }
    }

    logger = new ConsoleLogger(settings.verboseLevel);

    printHeader();

    if (inputFile == null)
      syntaxError("Missing input file. Use -f <file-path>");

    file = new File(inputFile);
  }

  public OrientDBImporter(final DatabaseInternal database, final ImporterSettings settings) throws ClassNotFoundException {
    this.settings = settings;
    this.database = database;
    this.databasePath = database.getDatabasePath();
    this.batchSize = settings.commitEvery;
    this.file = null;
    this.logger = new ConsoleLogger(settings.verboseLevel);
  }

  public static void main(final String[] args) throws Exception {
    new OrientDBImporter(args).run();
  }

  public GZIPInputStream openInputStream() throws IOException {
    return new GZIPInputStream(new FileInputStream(file));
  }

  public Database run() throws IOException, ClassNotFoundException {
    if (file != null && !file.exists()) {
      error = true;
      throw new IllegalArgumentException("File '" + inputFile + "' not found");
    }

    final String from = inputFile != null ? "'" + inputFile + "'" : "stream";

    if (databasePath == null)
      logger.logLine(1, "Checking OrientDB database from %s...", from);
    else {
      logger.logLine(1, "Importing OrientDB database from %s to '%s'", from, databasePath);

      if (!createDatabase())
        return null;
    }

    beginTime = System.currentTimeMillis();

    if (settings.expectedVertices < 1) {
      // COUNT RECORDS FIRST
      phase = PHASE.ANALYZE;

      // PARSE THE FILE THE 1ST TIME TO CREATE THE SCHEMA AND CACHE EDGES IN RAM
      logger.logLine(1, "No `expectedVertex` setting specified: parsing the file to count the total documents and vertex");
      parseInputFile();
      settings.expectedVertices = context.parsedDocumentAndVertices.get();

      if (settings.expectedVertices < 1) {
        // USE THE DEFAULT
        settings.expectedVertices = 500_000;
        logger.logLine(1, "Cannot find any documents or vertices");
      }
    }

    this.compressedRecordsRidMap = new CompressedRID2RIDIndex(database, (int) settings.expectedVertices,
        (int) settings.expectedEdges);

    phase = PHASE.CREATE_SCHEMA;
    totalAttributesParsed = 0L;

    // PARSE THE FILE THE 1ST TIME TO CREATE THE SCHEMA AND CACHE EDGES IN RAM
    logger.logLine(1, "Creation of the schema: types, properties and indexes");
    parseInputFile();

    phase = PHASE.CREATE_EDGES;

    // PARSE THE FILE AGAIN TO CREATE RECORDS AND EDGES
    logger.logLine(1, "Creation of edges started: creating edges between vertices");
    beginTimeEdgeCreation = System.currentTimeMillis();
    parseInputFile();

    createIndexes();

    if (database.getSchema().existsType("V") && database.countType("V", false) == 0) {
      logger.logLine(2, "Dropping empty 'V' base vertex type (in OrientDB all the vertices have their own class");
      database.getSchema().dropType("V");
    }

    if (database.getSchema().existsType("E") && database.countType("E", false) == 0) {
      logger.logLine(2, "Dropping empty 'E' base edge type (in OrientDB all the edges have their own class");
      database.getSchema().dropType("E");
    }

    final long elapsed = (System.currentTimeMillis() - beginTime) / 1000;

    long totalRecords = 0L;
    for (Long t : totalRecordByType.values())
      totalRecords += t;

    logger.logLine(1, "***************************************************************************************************");
    logger.logLine(1, "Import of OrientDB database completed in %,d secs with %,d errors and %,d warnings.", elapsed, errors,
        warnings);
    logger.logLine(1, "\nSUMMARY\n");
    logger.logLine(1, "- Records..................................: %,d", totalRecords);
    for (final Map.Entry<String, OrientDBClass> entry : classes.entrySet()) {
      final String className = entry.getKey();
      final Long recordsByClass = totalRecordByType.get(className);
      final long entries = recordsByClass != null ? recordsByClass : 0L;
      String additional = "";

      if (excludeClasses.contains(className))
        additional = " (excluded)";

      logger.logLine(1, "-- %-40s: %,d %s", className, entries, additional);
    }
    logger.logLine(1, "- Total attributes.........................: %,d", totalAttributesParsed);
    logger.logLine(1, "***************************************************************************************************");
    logger.logLine(1, "");
    logger.logLine(1, "NOTES:");

    if (createSecurityFiles) {
      writeSecurityFiles();
      logger.logLine(1,
          "- generated file server-users.jsonl to copy under ArcadeDB server 'config' directory. If the server already contains users, instead of replacing it, you can append the content");
    } else
      logger.logLine(1,
          "- users stored in OUser class will not be imported because ArcadeDB has users only at server level. If you want to import such users into ArcadeDB server configuration, please run the importer with the option -s <securityFileDirectory>");

    if (database != null)
      logger.logLine(1, "- you can find your new ArcadeDB database in '" + database.getDatabasePath() + "'");

    return database;
  }

  private boolean createDatabase() {
    if (database == null) {
      factory = new DatabaseFactory(databasePath);
      if (factory.exists()) {
        if (!overwriteDatabase) {
          logger.errorLine("Database already exists on path '%s'", databasePath);
          ++errors;
          return false;
        } else {
          database = factory.open();
          logger.errorLine("Found existent database at '%s', dropping it and recreate a new one", databasePath);
          database.drop();
        }
      }

      // CREATE THE DATABASE
      database = factory.create();
    }
    return true;
  }

  public boolean isError() {
    return error;
  }

  public ImporterContext getContext() {
    return context;
  }

  public OrientDBImporter setContext(final ImporterContext context) {
    this.context = context;
    return this;
  }

  private void parseInputFile() throws IOException {
    final GZIPInputStream inputStream = openInputStream();

    reader = new JsonReader(new InputStreamReader(inputStream, DatabaseFactory.getDefaultCharset()));
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
        try {
          reader.skipValue();
        } catch (final EOFException e) {
          return;
        }
      }
    }
    reader.endObject();
  }

  private void writeSecurityFiles() throws IOException {
    final File securityFile = new File("./server-users.jsonl");

    final StringBuilder buffer = new StringBuilder();

    for (final Map<String, Object> u : parsedUsers) {
      final JSONObject user = new JSONObject();

      user.put("name", u.get("name"));
      user.put("password", u.get("password"));
      user.put("databases", new JSONArray(new String[] { databaseName }));

      buffer.append(user).append("\n");
    }

    FileUtils.writeFile(securityFile, buffer.toString());
  }

  private void parseRecords() throws IOException {
    reader.beginArray();

    final AtomicLong processedItems = new AtomicLong();
    context.skippedEdges.set(0);
    context.parsed.set(0);

    final List<Map<String, Object>> batch = new ArrayList<>(batchSize);

    while (reader.peek() == BEGIN_OBJECT) {
      for (int i = 0; i < batchSize && reader.peek() == BEGIN_OBJECT; i++) {
        final Map<String, Object> attributes = parseRecord(reader, false);
        batch.add(attributes);
        context.parsed.incrementAndGet();

        if (reader.peek() == NULL)
          // FIX A BUG ON ORIENTDB EXPORTER WHEN GENERATE AN EMPTY RECORD
          reader.skipValue();
      }

      database.transaction(() -> {
        try {
          executeBatch(processedItems, batch);
        } catch (IOException e) {
          throw new ImportException("Error on importing batch of records", e);
        }
      }, false, CONCURRENT_MAX_RETRY);

      batch.clear();
    }

    final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

    switch (phase) {
    case ANALYZE:
      break;

    case CREATE_RECORDS:
      logger.logLine(1,
          "Creation of records completed: created %,d vertices and %,d documents, skipped %,d edges (%,d records/sec elapsed=%,d secs)",
          context.createdVertices.get(), context.createdDocuments.get(), context.skippedEdges.get(),
          elapsedInSecs > 0 ? ((context.createdDocuments.get() + context.createdVertices.get()) / elapsedInSecs) : 0,
          elapsedInSecs);

      updateDocumentLinks();

      break;
    case CREATE_EDGES:
      logger.logLine(1, "Creation of edges completed: created %,d edges %s (%,d edges/sec elapsed=%,d secs)",
          context.createdEdges.get(), totalEdgesByVertexType, elapsedInSecs > 0 ? (context.createdEdges.get() / elapsedInSecs) : 0,
          elapsedInSecs);
      break;
    default:
      ++errors;
      error = true;
      throw new IllegalArgumentException("Invalid phase " + phase);
    }
    reader.endArray();
  }

  private void executeBatch(final AtomicLong processedItems, final List<Map<String, Object>> batch) throws IOException {
    for (int i = 0; i < batch.size(); i++) {
      final Map<String, Object> attributes = batch.get(i);

      final String className = (String) attributes.get("@class");

      if (phase == PHASE.CREATE_SCHEMA || phase == PHASE.CREATE_RECORDS)
        createRecords(processedItems, attributes, className);
      else if (phase == PHASE.CREATE_EDGES)
        createEdges(processedItems, attributes, className);
      else if (phase == PHASE.ANALYZE) {
        if (!edgeClasses.contains(className))
          context.parsedDocumentAndVertices.incrementAndGet();
      }

      processedItems.incrementAndGet();
    }
  }

  private void updateDocumentLinks() {
    if (!documentsWithLinksToUpdate.isEmpty()) {
      logger.logLine(1, "Updating LINKs in %,d documents...", documentsWithLinksToUpdate.size());

      database.begin();

      for (RID rid : documentsWithLinksToUpdate) {
        final MutableDocument record = database.lookupByRID(rid, true).asDocument().modify();
        for (String pName : record.getPropertyNames()) {
          final Object pValue = record.get(pName);
          final RID converted = convertRIDs(pValue);
          if (converted != null)
            record.set(pName, converted);
        }
        record.save();

        context.updatedDocuments.incrementAndGet();

        if (context.updatedDocuments.get() > 0 && context.updatedDocuments.get() % batchSize == 0) {
          database.commit();
          database.begin();
        }
      }
      database.commit();
      logger.logLine(1, "- Updated LINKs in %,d records", context.updatedDocuments.get());
    }
  }

  private RID convertRID(final Object value) {
    final RID rid = value instanceof RID rid1 ? rid1 : new RID(database, value.toString());
    return compressedRecordsRidMap.get(rid);
  }

  private RID convertRIDs(final Object pValue) {
    if (RID.is(pValue))
      return convertRID(pValue);
    else if (pValue instanceof List list) {
      for (int i = 0; i < list.size(); i++) {
        final Object item = list.get(i);
        if (RID.is(item))
          list.set(i, convertRID(item));
        else if (item instanceof List)
          convertRIDs(item);
        else if (item instanceof Map)
          convertRIDs(item);

      }
    } else if (pValue instanceof Map map) {
      final List keys = new ArrayList(map.keySet());
      for (int i = 0; i < keys.size(); i++) {
        final Object key = keys.get(i);
        final Object value = map.get(key);
        if (RID.is(value))
          map.put(key, convertRID(value));
        else if (value instanceof List)
          convertRIDs(value);
        else if (value instanceof Map)
          convertRIDs(value);
      }
    }
    return null;
  }

  private void createEdges(final AtomicLong processedItems, final Map<String, Object> attributes, final String className) {
    if (edgeClasses.contains(className)) {
      createEdges(attributes);
      if (processedItems.get() > 0 && processedItems.get() % 1_000_000 == 0) {
        final long elapsed = System.currentTimeMillis() - beginTimeEdgeCreation;
        logger.logLine(2, "- Status update: created %,d edges %s (%,d edges/sec)", context.createdEdges.get(),
            totalEdgesByVertexType, (context.createdEdges.get() / elapsed * 1000));
      }
    }
  }

  private void createRecords(final AtomicLong processedItems, final Map<String, Object> attributes, final String className)
      throws IOException {
    if (!edgeClasses.contains(className)) {
      createRecord(attributes);
    } else
      context.skippedEdges.incrementAndGet();

    if (processedItems.get() > 0 && processedItems.get() % 1_000_000 == 0) {
      final long elapsed = System.currentTimeMillis() - beginTimeRecordsCreation;
      logger.logLine(2, "- Status update: created %,d vertices and %,d documents, skipped %,d edges (%,d records/sec)",
          context.createdVertices.get(), context.createdDocuments.get(), context.skippedEdges.get(),
          ((context.createdDocuments.get() + context.createdVertices.get()) / elapsed * 1000));
    }
  }

  private MutableDocument createRecord(final Map<String, Object> attributes) throws IOException {
    MutableDocument record = null;

    final String rid = (String) attributes.get("@rid");
    if (rid == null)
      // EMBEDDED RECORD?
      return null;

    final RID recordRid = new RID(database, rid);
    final String recordType = (String) attributes.get("@type");
    if (recordType != null) {
      switch (recordType) {
      case "b":
        // BINARY
        if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 0) {
          // INTERNAL ORIENTDB RECORD, IGNORE THEM
        } else {
          logger.errorLine("- Unsupported binary record. Ignoring record: %s", attributes);
          ++warnings;
        }
        break;
      case "d":
        // DOCUMENT, VERTEX, EDGE
        final String className = (String) attributes.get("@class");
        if (className == null) {
          if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 1) {
            // INTERNAL ORIENTDB RECORD SCHEMA, IGNORE THEM
          } else if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 2) {
            // INTERNAL ORIENTDB RECORD INDEX MGR, EXTRACT INDEXES
            parsedIndexes = (List<Map<String, Object>>) attributes.get("indexes");

            if (phase == PHASE.CREATE_SCHEMA) {
              logger.logLine(1, "Creation of records started: creating vertices and documents records (edges on the next phase)");
              phase = PHASE.CREATE_RECORDS;
              beginTimeRecordsCreation = System.currentTimeMillis();
            }
          } else {
            logger.errorLine("- Unsupported record without class. Ignoring record: %s", attributes);
            ++warnings;
          }
        } else {
          if (className.equals("OUser"))
            parsedUsers.add(attributes);

          incrementRecordByClass(className);

          if (!excludeClasses.contains(className)) {
            final DocumentType type = database.getSchema().getType(className);

            if (!checkForNullIndexes(attributes, type))
              return null;

            if (type instanceof LocalVertexType)
              record = database.newVertex(className);
            else if (type instanceof LocalEdgeType)
              // SKIP IT. EDGES ARE CREATED FROM VERTICES
              return null;
            else
              record = database.newDocument(className);

            final boolean rememberToUpdateThisDocumentBecauseContainsLinks = containsLinks(record, attributes);

            for (final Map.Entry<String, Object> entry : attributes.entrySet()) {
              final String attrName = entry.getKey();
              if (attrName.startsWith("@"))
                continue;

              Object attrValue = entry.getValue();

              if (attrValue instanceof Map) {
                final Map<String, Object> attrValueMap = (Map<String, Object>) attrValue;
                if (!attrValueMap.containsKey("@class")) {
                  // EMBEDDED MAP
                } else if (!attrValueMap.containsKey("@rid")) {
                  createEmbeddedDocument(record, attrName, attrValueMap);
                  // ALREADY SET BY THE NEW EMBEDDED DOCUMENT API
                  continue;
                } else
                  // CHECK IF IS POSSIBLE TO HAVE AN EMBEDDED RECORD WITH RID
                  attrValue = createRecord(attrValueMap);
              } else if (attrValue instanceof List) {
                if (record instanceof Vertex && (attrName.startsWith("out_") || attrName.startsWith("in_")))
                  // EDGES WILL BE CREATE BELOW IN THE 2ND PHASE
                  attrValue = null;
              }

              if (attrValue != null)
                record.set(attrName, attrValue);
            }

            record.save();

            final RID recordRID = record.getIdentity();

            if (type instanceof LocalVertexType)
              context.createdVertices.incrementAndGet();
            else
              context.createdDocuments.incrementAndGet();

            // REMEMBER THE DOCUMENT FOR LINKS AND VERTEX TO ATTACH EDGES ON 2ND PHASE
            compressedRecordsRidMap.put(recordRid, recordRID);

            if (rememberToUpdateThisDocumentBecauseContainsLinks) {
              documentsWithLinksToUpdate.add(recordRID);
              context.documentsWithLinksToUpdate.set(documentsWithLinksToUpdate.size());
            }
          }
        }
        break;
      default:
        ++errors;
        logger.errorLine("- Unsupported record type '%s'", recordType);
      }
    }

    return record;
  }

  private boolean containsLinks(final Record record, final Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      final String entryName = entry.getKey();
      final Object entryValue = entry.getValue();
      if (entryValue instanceof Map) {
        if (containsLinks(null, (Map<String, Object>) entryValue))
          return true;
      } else if (entryValue instanceof List) {
        if (record instanceof Vertex && (entryName.startsWith("out_") || entryName.startsWith("in_")))
          // SKIP EDGE LIST
          ;
        else {
          final List<?> list = (List<?>) entryValue;
          for (int i = 0; i < list.size(); i++) {
            final Object item = list.get(i);

            if (item instanceof Map)
              if (containsLinks(null, (Map<String, Object>) item))
                return true;
              else if (RID.is(item))
                return true;
          }
        }

      } else if (RID.is(entryValue))
        return true;
    }
    return false;
  }

  private MutableEmbeddedDocument createEmbeddedDocument(final MutableDocument record, final String propertyName,
      final Map<String, Object> attributes) throws IOException {
    MutableEmbeddedDocument embedded = null;

    // DOCUMENT, VERTEX, EDGE
    final String className = (String) attributes.remove("@class");

    incrementRecordByClass(className);
    context.parsed.incrementAndGet();

    if (!excludeClasses.contains(className)) {
      embedded = record.newEmbeddedDocument(className, propertyName);

      for (final Map.Entry<String, Object> entry : attributes.entrySet()) {
        final String attrName = entry.getKey();
        if (attrName.startsWith("@"))
          continue;

        final Object attrValue = entry.getValue();

        if (attrValue != null)
          embedded.set(attrName, attrValue);
      }

      context.createdEmbeddedDocuments.incrementAndGet();
    }

    return embedded;
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
    if (!(type instanceof LocalEdgeType))
      return;

    if (!checkForNullIndexes(attributes, type))
      return;

    final Map<String, Object> properties = new LinkedHashMap<>();
    for (final Map.Entry<String, Object> attr : attributes.entrySet())
      if (!attr.getKey().startsWith("@") && !attr.getKey().equals("out") && !attr.getKey().equals("in")) {
        properties.put(attr.getKey(), attr.getValue());
      }

    final RID out = new RID(database, (String) attributes.get("out"));
    final RID newOut = compressedRecordsRidMap.get(out);
    if (newOut == null) {
      ++skippedEdgeBecauseMissingVertex;
      ++warnings;

      if (settings.verboseLevel < 3 && skippedEdgeBecauseMissingVertex == 100)
        logger.logLine(2,
            "- Skipped 100 edges because one vertex is not in the database. Not reporting further case to reduce the output");
      else if (settings.verboseLevel > 2 || skippedEdgeBecauseMissingVertex < 100)
        logger.logLine(2, "- Skip edge %s because source vertex (out) was not imported", attributes);

      return;
    }

    final RID in = new RID(database, (String) attributes.get("in"));
    final RID newIn = compressedRecordsRidMap.get(in);
    if (newIn == null) {
      ++skippedEdgeBecauseMissingVertex;
      ++warnings;

      if (settings.verboseLevel < 3 && skippedEdgeBecauseMissingVertex == 100)
        logger.logLine(2,
            "- Skipped 100 edges because one vertex is not in the database. Not reporting further case to reduce the output");
      else if (settings.verboseLevel > 2 || skippedEdgeBecauseMissingVertex < 100)
        logger.logLine(2, "- Skip edge %s because destination vertex (in) was not imported", attributes);

      return;
    }

    final Vertex sourceVertex = (Vertex) database.lookupByRID(newOut, false);

    sourceVertex.newEdge(className, newIn, properties);

    context.createdEdges.incrementAndGet();

    final AtomicLong edgesByVertexType = totalEdgesByVertexType.computeIfAbsent(className, k -> new AtomicLong());
    edgesByVertexType.incrementAndGet();

    incrementRecordByClass(className);
  }

  private Map<String, Object> parseRecord(final JsonReader reader, final boolean ignore) throws IOException {
    final Map<String, Object> attributes = ignore ? null : new LinkedHashMap<>();

    reader.beginObject();
    while (reader.peek() != END_OBJECT) {
      final String attributeName = reader.nextName();

      final Object attributeValue = parseAttributeValue(reader, attributeName, ignore);

      if (!ignore) {
        attributes.put(attributeName, attributeValue);
        ++totalAttributesParsed;
      }
    }

    reader.endObject();
    return attributes;
  }

  private Object parseAttributeValue(final JsonReader reader, final String attributeName, final boolean ignore) throws IOException {
    Object attributeValue = null;

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
      logger.logLine(2, "Skipping property '%s' of type '%s'", attributeName, propertyType);
      ++errors;
    }
    return attributeValue;
  }

  private List<Object> parseArray(final JsonReader reader, final boolean ignore) throws IOException {
    final List<Object> list = ignore ? null : new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != END_ARRAY) {
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
        ++errors;
        logger.logLine(2, "Skipping entry of type '%s'", entryType);
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

    while (reader.peek() != END_OBJECT) {
      final String name = reader.nextName();

      switch (name) {
      case "classes":
        reader.beginArray();

        while (reader.peek() != END_ARRAY) {
          reader.beginObject();

          final OrientDBClass cls = new OrientDBClass();

          while (reader.peek() != END_OBJECT) {
            final String attrName = reader.nextName();
            switch (attrName) {
            case "name":
              cls.name = reader.nextString();
              break;

            case "cluster-ids":
              reader.beginArray();
              while (reader.peek() != END_ARRAY)
                cls.clusterIds.add(reader.nextInt());
              reader.endArray();
              break;

            case "super-classes":
              reader.beginArray();
              while (reader.peek() != END_ARRAY)
                cls.superClasses.add(reader.nextString());
              reader.endArray();
              break;

            case "properties":
              String propertyName = null;
              reader.beginArray();
              while (reader.peek() != END_ARRAY) {
                reader.beginObject();

                final OrientDBProperty propertyType = new OrientDBProperty();

                while (reader.peek() != END_OBJECT) {
                  final String pName = reader.nextName();
                  switch (pName) {
                  case "name":
                    propertyName = reader.nextString();
                    break;
                  case "type":
                    propertyType.type = reader.nextString();
                    break;
                  case "linkedType":
                  case "linkedClass":
                    propertyType.ofType = reader.nextString();
                    break;
                  case "customFields":
                    propertyType.customFields = parseRecord(reader, false);
                    if (propertyType.customFields.isEmpty())
                      propertyType.customFields = null;
                    break;
                  case "mandatory":
                    propertyType.mandatory = reader.nextBoolean();
                    break;
                  case "readonly":
                    propertyType.readOnly = reader.nextBoolean();
                    break;
                  case "not-null":
                    propertyType.notNull = reader.nextBoolean();
                    break;
                  case "min":
                    propertyType.min = parseAttributeValue(reader, propertyName + ".max", false);
                    break;
                  case "max":
                    propertyType.max = parseAttributeValue(reader, propertyName + ".max", false);
                    break;
                  case "regexp":
                    propertyType.regexp = reader.nextString();
                    break;
                  case "default-value":
                    propertyType.defaultValue = parseAttributeValue(reader, propertyName + ".defaultValue", false);
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

          classes.put(cls.name, cls);
        }
        reader.endArray();
        break;

      default:
        reader.skipValue();
      }
    }

    reader.endObject();

    for (final String className : classes.keySet())

      createType(className);

  }

  private void createIndexes() {
    for (final Map<String, Object> parsedIndex : parsedIndexes) {
      parsedIndex.get("name");
      final boolean unique = parsedIndex.get("type").toString().startsWith("UNIQUE");

      final Map<String, Object> indexDefinition = (Map<String, Object>) parsedIndex.get("indexDefinition");
      final String className = (String) indexDefinition.get("className");
      final String keyType = (String) indexDefinition.get("keyType");

      if (!classes.containsKey(className))
        continue;

      if (excludeClasses.contains(className))
        continue;

      final String fieldName = (String) indexDefinition.get("field");
      final String[] properties = new String[] { fieldName };
      boolean nullValuesIgnored = (boolean) indexDefinition.get("nullValuesIgnored");

      final DocumentType type = database.getSchema().getType(className);
      if (!type.existsPolymorphicProperty(fieldName)) {
        if (keyType == null) {
          logger.logLine(2, "- Skipped %s index creation on %s%s because the property is not defined and the key type is unknown",
              unique ? "UNIQUE" : "NOTUNIQUE", className, Arrays.toString(properties));
          ++warnings;
          continue;
        }

        type.createProperty(fieldName, Type.valueOf(keyType));
      }

      // PATCH TO ALWAYS USE SKIP BECAUSE IN ORIENTDB AN INDEX WITHOUT THE IGNORE SETTINGS CAN STILL HAVE NULL PROPERTIES INDEXES.
      nullValuesIgnored = true;

      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = nullValuesIgnored ?
          LSMTreeIndexAbstract.NULL_STRATEGY.SKIP :
          LSMTreeIndexAbstract.NULL_STRATEGY.ERROR;

      database.getSchema()
          .getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, unique, className, properties, LSMTreeIndexAbstract.DEF_PAGE_SIZE,
              nullStrategy, null);

      logger.logLine(2, "- Created index %s on %s%s", unique ? "UNIQUE" : "NOTUNIQUE", className, Arrays.toString(properties));
    }
  }

  private void createType(final String className) {
    final OrientDBClass classInfo = classes.get(className);

    int type;
    if (className.equals("V")) {
      type = 1;
    } else if (className.equals("E")) {
      type = 2;
    } else
      type = 0;

    if (!classInfo.superClasses.isEmpty()) {
      for (final String c : classInfo.superClasses)
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

    final int bucketsPerType = settings.getIntValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey(),
        GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getValueAsInteger());

    switch (type) {
    case 1:
      t = database.getSchema().createVertexType(className, bucketsPerType);
      break;

    case 2:
      t = database.getSchema().createEdgeType(className, bucketsPerType);
      edgeClasses.add(className);
      break;

    default:
      t = database.getSchema().createDocumentType(className, bucketsPerType);
      break;
    }

    for (final String c : classInfo.superClasses) {
      if (!excludeClasses.contains(c))
        t.addSuperType(c);
    }

    // CREATE PROPERTIES
    for (final Map.Entry<String, OrientDBProperty> entry : classInfo.properties.entrySet()) {
      final OrientDBProperty orientdbProperty = entry.getValue();

      String orientdbType = orientdbProperty.type;
      switch (orientdbType) {
      case "EMBEDDEDLIST":
      case "EMBEDDEDSET":
      case "LINKLIST":
      case "LINKSET":
        orientdbType = "LIST";
        break;
      case "EMBEDDEDMAP":
      case "LINKMAP":
        orientdbType = "MAP";
        break;
      }

      try {
        final Property property = t.createProperty(entry.getKey(), Type.valueOf(orientdbType), orientdbProperty.ofType);

        if (orientdbProperty.customFields != null)
          for (Map.Entry<String, Object> customEntry : orientdbProperty.customFields.entrySet())
            property.setCustomValue(customEntry.getKey(), customEntry.getValue());

        if (orientdbProperty.mandatory != null)
          property.setMandatory(orientdbProperty.mandatory);

        if (orientdbProperty.readOnly != null)
          property.setReadonly(orientdbProperty.readOnly);

        if (orientdbProperty.notNull != null)
          property.setNotNull(orientdbProperty.notNull);

        if (orientdbProperty.min != null)
          property.setMin(orientdbProperty.min.toString());

        if (orientdbProperty.max != null)
          property.setMax(orientdbProperty.max.toString());

        if (orientdbProperty.regexp != null)
          property.setRegexp(orientdbProperty.regexp);

        if (orientdbProperty.defaultValue != null)
          property.setDefaultValue(orientdbProperty.defaultValue);

      } catch (final Exception e) {
        logger.logLine(1, "- Unknown type '%s', ignoring creation of property in the schema for '%s.%s'", orientdbType, t.getName(),
            entry.getKey());
        ++warnings;
      }
    }

    logger.logLine(2, "- Created type '%s' which extends from %s with the following properties %s", className,
        classInfo.superClasses, classInfo.properties);
  }

  private int getClassType(final List<String> list) {
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
      if (c != null && !c.superClasses.isEmpty())
        type = getClassType(c.superClasses);
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
          clusterName = null;
          clusterId = -1;
          skipUntilEndOfObject(reader, END_OBJECT);

          if (reader.peek() != BEGIN_OBJECT)
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

    skipUntilEndOfObject(reader, END_OBJECT);
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

//  private void skipUntilPropertyName(final JsonReader reader, final String propertyName) throws IOException {
//    while (reader.hasNext()) {
//      final JsonToken t = reader.peek();
//
//      if (t == NAME) {
//        if (reader.nextName().equals(propertyName))
//          return;
//      } else
//        reader.skipValue();
//    }
//  }

  private void syntaxError(final String s) {
    logger.errorLine("Syntax error: " + s);
    error = true;
    printHelp();
  }

  private void printHeader() {
    logger.logLine(1, Constants.PRODUCT + " " + Constants.getVersion() + " - OrientDB Importer");
  }

  private void printHelp() {
    logger.errorLine("Use:");
    logger.errorLine("-d <database-path>: create a database from the OrientDB export");
    logger.errorLine("-i <input-file>: path to the OrientDB export file. The default name is export.json.gz");
    logger.errorLine("-s <security-file>: path to the security file generated from OrientDB users to use in ArcadeDB server");
    logger.errorLine("-v <verbose-level>: 0 = error only, 1 = main steps, 2 = detailed steps, 3 = everything");
  }

  private boolean checkForNullIndexes(final Map<String, Object> properties, final DocumentType type) {
    boolean valid = true;
    final Collection<TypeIndex> indexes = type.getAllIndexes(true);
    for (final Index index : indexes) {
      if (index.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.ERROR) {
        final String indexedPropName = index.getPropertyNames().get(0);
        final Object value = properties.get(indexedPropName);
        if (value == null) {
          ++skippedRecordBecauseNullKey;
          ++errors;

          if (settings.verboseLevel < 3 && skippedRecordBecauseNullKey == 100)
            logger.logLine(2,
                "- Skipped 100 records where indexed field is null and the index is not accepting NULLs. Not reporting further case to reduce the output");
          else if (settings.verboseLevel > 2 || skippedRecordBecauseNullKey < 100)
            logger.logLine(2, "- Skipped record %s because indexed field '%s' is null and the index is not accepting NULLs",
                properties, indexedPropName);

          valid = false;
        }
      }
    }

    return valid;
  }

  private void incrementRecordByClass(final String className) {
    final Long recordsByClass = totalRecordByType.computeIfAbsent(className, k -> 0L);
    totalRecordByType.put(className, recordsByClass + 1);
  }
}
