/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db.tool;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODocumentFieldWalker;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.tool.importer.OConverterData;
import com.orientechnologies.orient.core.db.tool.importer.OLinksRewriter;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.metadata.security.OIdentity;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OJSONReader;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.ORidSet;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

/** Import data from a file into a database. */
public class ODatabaseImport extends ODatabaseImpExpAbstract {
  public static final String EXPORT_IMPORT_CLASS_NAME = "___exportImportRIDMap";
  public static final String EXPORT_IMPORT_INDEX_NAME = EXPORT_IMPORT_CLASS_NAME + "Index";

  public static final int IMPORT_RECORD_DUMP_LAP_EVERY_MS = 5000;

  private Map<OPropertyImpl, String> linkedClasses = new HashMap<>();
  private Map<OClass, List<String>> superClasses = new HashMap<>();
  private OJSONReader jsonReader;
  private ORecord record;
  private boolean schemaImported = false;
  private int exporterVersion = -1;
  private ORID schemaRecordId;
  private ORID indexMgrRecordId;

  private boolean deleteRIDMapping = true;

  private boolean preserveClusterIDs = true;
  private boolean migrateLinks = true;
  private boolean merge = false;
  private boolean rebuildIndexes = true;

  private Set<String> indexesToRebuild = new HashSet<>();
  private Map<String, String> convertedClassNames = new HashMap<>();

  private Map<Integer, Integer> clusterToClusterMapping = new HashMap<>();

  private int maxRidbagStringSizeBeforeLazyImport = 100_000_000;

  // Jackson Stream Parser support
  private final JsonFactory factory = new JsonFactory();
  private InputStream input;

  public ODatabaseImport(
      final ODatabaseDocumentInternal database,
      final String fileName,
      final OCommandOutputListener outputListener)
      throws IOException {
    super(database, fileName, outputListener);

    // FIXME: unclosed stream
    final BufferedInputStream bufferedInputStream =
        new BufferedInputStream(new FileInputStream(this.fileName));
    bufferedInputStream.mark(1024);
    InputStream inputStream;
    try {
      inputStream = new GZIPInputStream(bufferedInputStream, 16384); // 16KB
    } catch (final Exception ignore) {
      bufferedInputStream.reset();
      inputStream = bufferedInputStream;
    }
    createJsonReaderDefaultListenerAndDeclareIntent(database, outputListener, inputStream);
  }

  public ODatabaseImport(
      final ODatabaseDocumentInternal database,
      final InputStream inputStream,
      final OCommandOutputListener outputListener)
      throws IOException {
    super(database, "streaming", outputListener);
    createJsonReaderDefaultListenerAndDeclareIntent(database, outputListener, inputStream);
  }

  private void createJsonReaderDefaultListenerAndDeclareIntent(
      final ODatabaseDocumentInternal database,
      final OCommandOutputListener outputListener,
      final InputStream inputStream) {
    if (outputListener == null) {
      listener = text -> {};
    }
    jsonReader = new OJSONReader(new InputStreamReader(inputStream));
    database.declareIntent(new OIntentMassiveInsert());

    input = inputStream;
  }

  @Override
  public ODatabaseImport setOptions(final String options) {
    super.setOptions(options);
    return this;
  }

  @Override
  public void run() {
    importDatabase();
    // TODO: importDatabaseV2();
  }

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-deleteRIDMapping"))
      deleteRIDMapping = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-preserveClusterIDs"))
      preserveClusterIDs = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-merge")) merge = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-migrateLinks"))
      migrateLinks = Boolean.parseBoolean(items.get(0));
    else if (option.equalsIgnoreCase("-rebuildIndexes"))
      rebuildIndexes = Boolean.parseBoolean(items.get(0));
    else super.parseSetting(option, items);
  }

  // TODO: WIP - using existing OJSONReader
  public ODatabaseImport importDatabaseV3() {
    final boolean preValidation = database.isValidationEnabled();
    try (final JsonParser parser = factory.createParser(input)) {
      listener.onMessage(
          "\nStarted import of database '" + database.getURL() + "' from " + fileName + "...");
      final long time = System.nanoTime();

      jsonReader.readNext(parser, JsonToken.START_OBJECT);
      database.setValidationEnabled(false);
      // status concept seems deprecated - status `IMPORTING` never checked
      database.setStatus(STATUS.IMPORTING);

      if (!merge) {
        removeDefaultNonSecurityClasses();
        database.getMetadata().getIndexManagerInternal().reload();
      }

      for (final OIndex index :
          database.getMetadata().getIndexManagerInternal().getIndexes(database)) {
        if (index.isAutomatic()) indexesToRebuild.add(index.getName());
      }

      boolean clustersImported = false;
      while (!parser.isClosed()) {
        // while (jsonReader.hasNext() && jsonReader.lastChar() != '}') {
        // final String tag = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
        final String tag = jsonReader.readString(parser, JsonToken.FIELD_NAME);

        if (tag.equals("info")) {
          importInfoV2(parser);
        } else if (tag.equals("clusters")) {
          importClusters();
          clustersImported = true;
        } else if (tag.equals("schema")) importSchema(clustersImported);
        else if (tag.equals("records")) importRecords();
        else if (tag.equals("indexes")) importIndexes();
        else if (tag.equals("manualIndexes")) importManualIndexes();
        else if (tag.equals("brokenRids")) {
          processBrokenRids();
        } else
          throw new ODatabaseImportException("Invalid format. Found unsupported tag '" + tag + "'");
      }
      if (rebuildIndexes) {
        rebuildIndexes();
      }

      // This is needed to insure functions loaded into an open
      // in memory database are available after the import.
      // see issue #5245
      database.getMetadata().reload();

      database.getStorage().synch();
      // status concept seems deprecated, but status `OPEN` is checked elsewhere
      database.setStatus(STATUS.OPEN);

      if (isDeleteRIDMapping()) {
        removeExportImportRIDsMap();
      }
      listener.onMessage(
          "\n\nDatabase import completed in " + ((System.nanoTime() - time) / 1000000) + " ms");
    } catch (final Exception e) {
      final StringWriter writer = new StringWriter();
      writer.append(
          "Error on database import happened just before line "
              + jsonReader.getLineNumber()
              + ", column "
              + jsonReader.getColumnNumber()
              + "\n");
      final PrintWriter printWriter = new PrintWriter(writer);
      e.printStackTrace(printWriter);
      printWriter.flush();

      listener.onMessage(writer.toString());

      try {
        writer.close();
      } catch (final IOException e1) {
        throw new ODatabaseExportException(
            "Error on importing database '" + database.getName() + "' from file: " + fileName, e1);
      }
      throw new ODatabaseExportException(
          "Error on importing database '" + database.getName() + "' from file: " + fileName, e);
    } finally {
      database.setValidationEnabled(preValidation);
      close();
    }
    return this;
  }

  // TODO: WIP - adding jackson stream parser replacing old logic
  public ODatabaseImport importDatabaseV2() {
    final boolean preValidation = database.isValidationEnabled();

    try (final JsonParser parser = factory.createParser(input)) {
      listener.onMessage(
          "\nStarted import of database '" + database.getURL() + "' from " + fileName + "...");
      final long time = System.nanoTime();

      database.setValidationEnabled(false);
      // status concept seems deprecated - status `IMPORTING` never checked
      database.setStatus(STATUS.IMPORTING);

      if (!merge) {
        removeDefaultNonSecurityClasses();
        database.getMetadata().getIndexManagerInternal().reload();
      }

      for (final OIndex index :
          database.getMetadata().getIndexManagerInternal().getIndexes(database)) {
        if (index.isAutomatic()) indexesToRebuild.add(index.getName());
      }

      boolean clustersImported = false;
      while (!parser.isClosed()) {
        JsonToken jsonToken = parser.nextToken();

        if (JsonToken.FIELD_NAME.equals(jsonToken)) {
          if (parser.getValueAsString().equals("info")) {
            importInfo(parser);
          } else if (parser.getValueAsString().equals("clusters")) {
            importClusters(parser);
            clustersImported = true;
          } else if (parser.getValueAsString().equals("schema")) {
            importSchema(parser, clustersImported);
          } else if (parser.getValueAsString().equals("records")) {
            importRecords(parser);
          }
          /*FIXME:
          else if (tag.equals("indexes")) importIndexes();
          else if (tag.equals("manualIndexes")) importManualIndexes();
          else if (tag.equals("brokenRids")) {
            processBrokenRids();
          } else
            throw new ODatabaseImportException("Invalid format. Found unsupported tag '" + tag + "'");*/
        }
      }
      if (rebuildIndexes) {
        rebuildIndexes();
      }

      // This is needed to insure functions loaded into an open
      // in memory database are available after the import.
      // see issue #5245
      database.getMetadata().reload();

      database.getStorage().synch();
      // status concept seems deprecated, but status `OPEN` is checked elsewhere
      database.setStatus(STATUS.OPEN);

      if (isDeleteRIDMapping()) {
        removeExportImportRIDsMap();
      }
      listener.onMessage(
          "\n\nDatabase import completed in " + ((System.nanoTime() - time) / 1000000) + " ms");
    } catch (final Exception e) {
      final StringWriter writer = new StringWriter();
      writer.append(
          "Error on database import happened just before line "
              + jsonReader.getLineNumber()
              + ", column "
              + jsonReader.getColumnNumber()
              + "\n");
      final PrintWriter printWriter = new PrintWriter(writer);
      e.printStackTrace(printWriter);
      printWriter.flush();

      listener.onMessage(writer.toString());

      try {
        writer.close();
      } catch (final IOException e1) {
        throw new ODatabaseExportException(
            "Error on importing database '" + database.getName() + "' from file: " + fileName, e1);
      }
      throw new ODatabaseExportException(
          "Error on importing database '" + database.getName() + "' from file: " + fileName, e);
    } finally {
      database.setValidationEnabled(preValidation);
      close();
    }
    return this;
  }

  private void importInfo(final JsonParser parser) throws IOException {
    listener.onMessage("\nImporting database info...");

    JsonToken jsonToken = parser.nextToken();
    while (!JsonToken.END_OBJECT.equals(jsonToken)) {
      if (JsonToken.FIELD_NAME.equals(jsonToken)) {
        if (parser.getValueAsString().equals("exporter-version")) {
          parser.nextToken();
          exporterVersion = parser.getValueAsInt();
        } else if (parser.getValueAsString().equals("schemaRecordId")) {
          parser.nextToken();
          schemaRecordId = new ORecordId(parser.getValueAsString());
        } else if (parser.getValueAsString().equals("indexMgrRecordId")) {
          parser.nextToken();
          indexMgrRecordId = new ORecordId(parser.getValueAsString());
        }
      }
      jsonToken = parser.nextToken();
    }

    if (schemaRecordId == null) {
      schemaRecordId = new ORecordId(database.getStorage().getConfiguration().getSchemaRecordId());
    }
    if (indexMgrRecordId == null) {
      indexMgrRecordId =
          new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId());
    }
    listener.onMessage("OK");
  }

  private long importClusters(final JsonParser parser) throws IOException {
    listener.onMessage("\nImporting clusters...");
    long total = 0;
    boolean recreateManualIndex = false;
    if (exporterVersion <= 4) {
      removeDefaultClusters();
      recreateManualIndex = true;
    }
    final Set<String> indexesToRebuild = new HashSet<>();

    JsonToken jsonToken = parser.nextToken();
    @SuppressWarnings("unused")
    ORecordId rid = null;
    while (!JsonToken.END_ARRAY.equals(jsonToken)) {
      if (JsonToken.START_OBJECT.equals(jsonToken) || JsonToken.END_OBJECT.equals(jsonToken)) {
        jsonToken = parser.nextToken();
      } else if (JsonToken.FIELD_NAME.equals(jsonToken)) {
        if (parser.getValueAsString().equals("name")) {
          jsonToken = parser.nextToken();
          String name = parser.getValueAsString();
          if (name.length() == 0) {
            name = null;
          }
          name = OClassImpl.decodeClassName(name);
          if (name != null) {
            // CHECK IF THE CLUSTER IS INCLUDED
            if (includeClusters != null) {
              if (!includeClusters.contains(name)) {
                jsonToken = parser.nextToken();
                continue;
              }
            } else if (excludeClusters != null) {
              if (excludeClusters.contains(name)) {
                jsonToken = parser.nextToken();
                continue;
              }
            }
            int id = -1;
            if (exporterVersion < 9) {
              jsonToken = parser.nextToken();
              if (parser.getValueAsString().equals("id")) {
                jsonToken = parser.nextToken();
                id = parser.getValueAsInt();
                /*id =
                jsonReader
                    .readNext(OJSONReader.FIELD_ASSIGNMENT)
                    .checkContent("\"id\"")
                    .readInteger(OJSONReader.COMMA_SEPARATOR);*/
                // TODO: dead code?
                /*String type =
                jsonReader
                    .readNext(OJSONReader.FIELD_ASSIGNMENT)
                    .checkContent("\"type\"")
                    .readString(OJSONReader.NEXT_IN_OBJECT);*/
              }
            } else {
              jsonToken = parser.nextToken();
              if (parser.getValueAsString().equals("id")) {
                jsonToken = parser.nextToken();
                id = parser.getValueAsInt();
                /*id =
                jsonReader
                    .readNext(OJSONReader.FIELD_ASSIGNMENT)
                    .checkContent("\"id\"")
                    .readInteger(OJSONReader.NEXT_IN_OBJECT);*/
              } else {
                throw new IllegalStateException();
              }
            }

            // TODO: dead code?
            /*String type;
            if (jsonReader.lastChar() == ',') {
              type =
                  jsonReader
                      .readNext(OJSONReader.FIELD_ASSIGNMENT)
                      .checkContent("\"type\"")
                      .readString(OJSONReader.NEXT_IN_OBJECT);
            } else {
              type = "PHYSICAL";
            }

            if (jsonReader.lastChar() == ',') {
              rid =
                  new ORecordId(
                      jsonReader
                          .readNext(OJSONReader.FIELD_ASSIGNMENT)
                          .checkContent("\"rid\"")
                          .readString(OJSONReader.NEXT_IN_OBJECT));
            } else {
              rid = null;
            }*/
            addCluster(name, id);
            total++;
          }
          // jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
        }
        jsonToken = parser.nextToken();
      } else {
        jsonToken = parser.nextToken();
      }
    }
    // jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
    rebuildIndexes(indexesToRebuild);
    recreateManualIndex(recreateManualIndex);
    listener.onMessage("\nDone. Imported " + total + " clusters");
    handleDatabaseLoadNull();
    return total;
  }

  private void addCluster(String name, int id) {
    listener.onMessage(
        "\n- Creating cluster " + (name != null ? "'" + name + "'" : "NULL") + "...");

    int clusterId = name != null ? database.getClusterIdByName(name) : -1;
    if (clusterId == -1) {
      // CREATE IT
      if (!preserveClusterIDs) clusterId = database.addCluster(name);
      else {
        clusterId = database.addCluster(name, id, null);
        assert clusterId == id;
      }
    }

    if (clusterId != id) {
      if (!preserveClusterIDs) {
        if (database.countClusterElements(clusterId - 1) == 0) {
          listener.onMessage("Found previous version: migrating old clusters...");
          database.dropCluster(name);
          database.addCluster("temp_" + clusterId, null);
          clusterId = database.addCluster(name);
        } else
          throw new OConfigurationException(
              "Imported cluster '"
                  + name
                  + "' has id="
                  + clusterId
                  + " different from the original: "
                  + id
                  + ". To continue the import drop the cluster '"
                  + database.getClusterNameById(clusterId - 1)
                  + "' that has "
                  + database.countClusterElements(clusterId - 1)
                  + " records");
      } else {
        final OClass clazz = database.getMetadata().getSchema().getClassByClusterId(clusterId);
        if (clazz != null && clazz instanceof OClassEmbedded)
          ((OClassEmbedded) clazz).removeClusterId(clusterId, true);
        database.dropCluster(clusterId);
        database.addCluster(name, id, null);
      }
    }
    listener.onMessage("OK, assigned id=" + clusterId);
  }

  private void handleDatabaseLoadNull() {
    if (database.load(new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId()))
        == null) {
      ODocument indexDocument = new ODocument();
      indexDocument.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);

      database.getStorage().setIndexMgrRecordId(indexDocument.getIdentity().toString());
    }
  }

  private void rebuildIndexes(final Set<String> indexesToRebuild) {
    listener.onMessage("\nRebuilding indexes of truncated clusters ...");
    for (final String indexName : indexesToRebuild)
      database
          .getMetadata()
          .getIndexManagerInternal()
          .getIndex(database, indexName)
          .rebuild(
              new OProgressListener() {
                private long last = 0;

                @Override
                public void onBegin(Object iTask, long iTotal, Object metadata) {
                  listener.onMessage(
                      "\n- Cluster content was updated: rebuilding index '" + indexName + "'...");
                }

                @Override
                public boolean onProgress(Object iTask, long iCounter, float iPercent) {
                  final long now = System.currentTimeMillis();
                  if (last == 0) last = now;
                  else if (now - last > 1000) {
                    listener.onMessage(
                        String.format(
                            "\nIndex '%s' is rebuilding (%.2f/100)", indexName, iPercent));
                    last = now;
                  }
                  return true;
                }

                @Override
                public void onCompletition(Object iTask, boolean iSucceed) {
                  listener.onMessage(" Index " + indexName + " was successfully rebuilt.");
                }
              });
    listener.onMessage("\nDone " + indexesToRebuild.size() + " indexes were rebuilt.");
  }

  public ODatabaseImport importDatabase() {
    final boolean preValidation = database.isValidationEnabled();
    try {
      listener.onMessage(
          "\nStarted import of database '" + database.getURL() + "' from " + fileName + "...");
      final long time = System.nanoTime();

      jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
      database.setValidationEnabled(false);
      // status concept seems deprecated - status `IMPORTING` never checked
      database.setStatus(STATUS.IMPORTING);

      if (!merge) {
        removeDefaultNonSecurityClasses();
        database.getMetadata().getIndexManagerInternal().reload();
      }

      for (final OIndex index :
          database.getMetadata().getIndexManagerInternal().getIndexes(database)) {
        if (index.isAutomatic()) indexesToRebuild.add(index.getName());
      }

      boolean clustersImported = false;
      while (jsonReader.hasNext() && jsonReader.lastChar() != '}') {
        final String tag = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);

        if (tag.equals("info")) {
          importInfo();
        } else if (tag.equals("clusters")) {
          importClusters();
          clustersImported = true;
        } else if (tag.equals("schema")) importSchema(clustersImported);
        else if (tag.equals("records")) importRecords();
        else if (tag.equals("indexes")) importIndexes();
        else if (tag.equals("manualIndexes")) importManualIndexes();
        else if (tag.equals("brokenRids")) {
          processBrokenRids();
        } else
          throw new ODatabaseImportException("Invalid format. Found unsupported tag '" + tag + "'");
      }
      if (rebuildIndexes) {
        rebuildIndexes();
      }

      // This is needed to insure functions loaded into an open
      // in memory database are available after the import.
      // see issue #5245
      database.getMetadata().reload();

      database.getStorage().synch();
      // status concept seems deprecated, but status `OPEN` is checked elsewhere
      database.setStatus(STATUS.OPEN);

      if (isDeleteRIDMapping()) {
        removeExportImportRIDsMap();
      }
      listener.onMessage(
          "\n\nDatabase import completed in " + ((System.nanoTime() - time) / 1000000) + " ms");
    } catch (final Exception e) {
      final StringWriter writer = new StringWriter();
      writer.append(
          "Error on database import happened just before line "
              + jsonReader.getLineNumber()
              + ", column "
              + jsonReader.getColumnNumber()
              + "\n");
      final PrintWriter printWriter = new PrintWriter(writer);
      e.printStackTrace(printWriter);
      printWriter.flush();

      listener.onMessage(writer.toString());

      try {
        writer.close();
      } catch (final IOException e1) {
        throw new ODatabaseExportException(
            "Error on importing database '" + database.getName() + "' from file: " + fileName, e1);
      }
      throw new ODatabaseExportException(
          "Error on importing database '" + database.getName() + "' from file: " + fileName, e);
    } finally {
      database.setValidationEnabled(preValidation);
      close();
    }
    return this;
  }

  private void processBrokenRids() throws IOException, ParseException {
    final Set<ORID> brokenRids = new HashSet<>();
    processBrokenRids(brokenRids);
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
  }

  // just read collection so import process can continue
  private void processBrokenRids(final Set<ORID> brokenRids) throws IOException, ParseException {
    if (exporterVersion >= 12) {
      listener.onMessage(
          "Reading of set of RIDs of records which were detected as broken during database export\n");
      jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

      while (true) {
        jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);

        final ORecordId recordId = new ORecordId(jsonReader.getValue());
        brokenRids.add(recordId);

        if (jsonReader.lastChar() == ']') break;
      }
    }
    if (migrateLinks) {
      if (exporterVersion >= 12)
        listener.onMessage(
            brokenRids.size()
                + " were detected as broken during database export, links on those records will be removed from"
                + " result database");
      migrateLinksInImportedDocuments(brokenRids);
    }
  }

  public void rebuildIndexes() {
    database.getMetadata().getIndexManagerInternal().reload();

    OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    listener.onMessage("\nRebuild of stale indexes...");
    for (String indexName : indexesToRebuild) {

      if (indexManager.getIndex(database, indexName) == null) {
        listener.onMessage(
            "\nIndex " + indexName + " is skipped because it is absent in imported DB.");
        continue;
      }

      listener.onMessage("\nStart rebuild index " + indexName);
      database.command("rebuild index " + indexName).close();
      listener.onMessage("\nRebuild  of index " + indexName + " is completed.");
    }
    listener.onMessage("\nStale indexes were rebuilt...");
  }

  public ODatabaseImport removeExportImportRIDsMap() {
    listener.onMessage("\nDeleting RID Mapping table...");

    OSchema schema = database.getMetadata().getSchema();
    if (schema.getClass(EXPORT_IMPORT_CLASS_NAME) != null) {
      schema.dropClass(EXPORT_IMPORT_CLASS_NAME);
    }

    listener.onMessage("OK\n");
    return this;
  }

  public void close() {
    database.declareIntent(null);
  }

  public boolean isMigrateLinks() {
    return migrateLinks;
  }

  public void setMigrateLinks(boolean migrateLinks) {
    this.migrateLinks = migrateLinks;
  }

  public boolean isRebuildIndexes() {
    return rebuildIndexes;
  }

  public void setRebuildIndexes(boolean rebuildIndexes) {
    this.rebuildIndexes = rebuildIndexes;
  }

  public boolean isPreserveClusterIDs() {
    return preserveClusterIDs;
  }

  public void setPreserveClusterIDs(boolean preserveClusterIDs) {
    this.preserveClusterIDs = preserveClusterIDs;
  }

  public boolean isMerge() {
    return merge;
  }

  public void setMerge(boolean merge) {
    this.merge = merge;
  }

  public boolean isDeleteRIDMapping() {
    return deleteRIDMapping;
  }

  public void setDeleteRIDMapping(boolean deleteRIDMapping) {
    this.deleteRIDMapping = deleteRIDMapping;
  }

  public void setOption(final String option, String value) {
    parseSetting("-" + option, Arrays.asList(value));
  }

  protected void removeDefaultClusters() {
    listener.onMessage(
        "\nWARN: Exported database does not support manual index separation."
            + " Manual index cluster will be dropped.");

    // In v4 new cluster for manual indexes has been implemented. To keep database consistent we
    // should shift back all clusters and recreate cluster for manual indexes in the end.
    database.dropCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);

    final OSchema schema = database.getMetadata().getSchema();
    if (schema.existsClass(OUser.CLASS_NAME)) schema.dropClass(OUser.CLASS_NAME);
    if (schema.existsClass(ORole.CLASS_NAME)) schema.dropClass(ORole.CLASS_NAME);
    if (schema.existsClass(OSecurityShared.RESTRICTED_CLASSNAME))
      schema.dropClass(OSecurityShared.RESTRICTED_CLASSNAME);
    if (schema.existsClass(OFunction.CLASS_NAME)) schema.dropClass(OFunction.CLASS_NAME);
    if (schema.existsClass("ORIDs")) schema.dropClass("ORIDs");
    if (schema.existsClass(OClassTrigger.CLASSNAME)) schema.dropClass(OClassTrigger.CLASSNAME);

    database.dropCluster(OStorage.CLUSTER_DEFAULT_NAME);

    database.getStorage().setDefaultClusterId(database.addCluster(OStorage.CLUSTER_DEFAULT_NAME));

    // Starting from v4 schema has been moved to internal cluster.
    // Create a stub at #2:0 to prevent cluster position shifting.
    new ODocument().save(OStorage.CLUSTER_DEFAULT_NAME);

    database.getSharedContext().getSecurity().create(database);
  }

  // TODO: WIP - reusing OJSONReader
  private void importInfoV2(final JsonParser parser) throws IOException, ParseException {
    listener.onMessage("\nImporting database info...");

    final JsonToken jsonToken = jsonReader.readNext(parser, JsonToken.START_OBJECT);
    while (!JsonToken.END_OBJECT.equals(jsonToken)) {
      final String fieldName = jsonReader.readString(parser, JsonToken.FIELD_NAME);
      if (fieldName.equals("exporter-version")) exporterVersion = jsonReader.readInteger(parser);
      else if (fieldName.equals("schemaRecordId"))
        schemaRecordId = new ORecordId(jsonReader.readString(parser, JsonToken.VALUE_STRING));
      else if (fieldName.equals("indexMgrRecordId"))
        indexMgrRecordId = new ORecordId(jsonReader.readString(parser, JsonToken.VALUE_STRING));
      else jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
    }
    jsonReader.readNext(parser, JsonToken.END_OBJECT);

    if (schemaRecordId == null)
      schemaRecordId = new ORecordId(database.getStorage().getConfiguration().getSchemaRecordId());

    if (indexMgrRecordId == null)
      indexMgrRecordId =
          new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId());

    listener.onMessage("OK");
  }

  private void importInfo() throws IOException, ParseException {
    listener.onMessage("\nImporting database info...");

    jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
    while (jsonReader.lastChar() != '}') {
      final String fieldName = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
      if (fieldName.equals("exporter-version"))
        exporterVersion = jsonReader.readInteger(OJSONReader.NEXT_IN_OBJECT);
      else if (fieldName.equals("schemaRecordId"))
        schemaRecordId = new ORecordId(jsonReader.readString(OJSONReader.NEXT_IN_OBJECT));
      else if (fieldName.equals("indexMgrRecordId"))
        indexMgrRecordId = new ORecordId(jsonReader.readString(OJSONReader.NEXT_IN_OBJECT));
      else jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
    }
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);

    if (schemaRecordId == null)
      schemaRecordId = new ORecordId(database.getStorage().getConfiguration().getSchemaRecordId());

    if (indexMgrRecordId == null)
      indexMgrRecordId =
          new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId());

    listener.onMessage("OK");
  }

  private void removeDefaultNonSecurityClasses() {
    listener.onMessage(
        "\nNon merge mode (-merge=false): removing all default non security classes");

    final OSchema schema = database.getMetadata().getSchema();
    final Collection<OClass> classes = schema.getClasses();
    final OClass role = schema.getClass(ORole.CLASS_NAME);
    final OClass user = schema.getClass(OUser.CLASS_NAME);
    final OClass identity = schema.getClass(OIdentity.CLASS_NAME);
    // final OClass oSecurityPolicy = schema.getClass(OSecurityPolicy.class.getSimpleName());
    final Map<String, OClass> classesToDrop = new HashMap<>();
    final Set<String> indexNames = new HashSet<>();
    for (final OClass dbClass : classes) {
      final String className = dbClass.getName();
      if (!dbClass.isSuperClassOf(role)
          && !dbClass.isSuperClassOf(user)
          && !dbClass.isSuperClassOf(identity) /*&& !dbClass.isSuperClassOf(oSecurityPolicy)*/) {
        classesToDrop.put(className, dbClass);
        for (final OIndex index : dbClass.getIndexes()) {
          indexNames.add(index.getName());
        }
      }
    }

    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    for (final String indexName : indexNames) {
      indexManager.dropIndex(database, indexName);
    }

    int removedClasses = 0;
    while (!classesToDrop.isEmpty()) {
      final AbstractList<String> classesReadyToDrop = new ArrayList<>();
      for (final String className : classesToDrop.keySet()) {
        boolean isSuperClass = false;
        for (OClass dbClass : classesToDrop.values()) {
          final List<OClass> parentClasses = dbClass.getSuperClasses();
          if (parentClasses != null) {
            for (OClass parentClass : parentClasses) {
              if (className.equalsIgnoreCase(parentClass.getName())) {
                isSuperClass = true;
                break;
              }
            }
          }
        }
        if (!isSuperClass) {
          classesReadyToDrop.add(className);
        }
      }
      for (final String className : classesReadyToDrop) {
        schema.dropClass(className);
        classesToDrop.remove(className);
        removedClasses++;
        listener.onMessage("\n- Class " + className + " was removed.");
      }
    }
    schema.reload();
    listener.onMessage("\nRemoved " + removedClasses + " classes.");
  }

  private void importManualIndexes() throws IOException, ParseException {
    listener.onMessage("\nImporting manual index entries...");

    ODocument document = new ODocument();

    OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    // FORCE RELOADING
    indexManager.reload();

    int n = 0;
    do {
      jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

      jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
      final String indexName = jsonReader.readString(OJSONReader.NEXT_IN_ARRAY);

      if (indexName == null || indexName.length() == 0) return;

      listener.onMessage("\n- Index '" + indexName + "'...");

      final OIndex index =
          database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);

      long tot = 0;

      jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

      do {
        final String value = jsonReader.readString(OJSONReader.NEXT_IN_ARRAY).trim();
        if ("[]".equals(value)) {
          return;
        }

        if (!value.isEmpty()) {
          document = (ODocument) ORecordSerializerJSON.INSTANCE.fromString(value, document, null);
          document.setLazyLoad(false);

          final OIdentifiable oldRid = document.field("rid");
          assert oldRid != null;

          final OIdentifiable newRid;
          if (!document.<Boolean>field("binary")) {
            try (final OResultSet result =
                database.query(
                    "select value from " + EXPORT_IMPORT_CLASS_NAME + " where key = ?",
                    String.valueOf(oldRid))) {
              if (!result.hasNext()) {
                newRid = oldRid;
              } else {
                newRid = new ORecordId(result.next().<String>getProperty("value"));
              }
            }

            index.put(document.field("key"), newRid.getIdentity());
          } else {
            ORuntimeKeyIndexDefinition<?> runtimeKeyIndexDefinition =
                (ORuntimeKeyIndexDefinition<?>) index.getDefinition();
            OBinarySerializer<?> binarySerializer = runtimeKeyIndexDefinition.getSerializer();

            try (final OResultSet result =
                database.query(
                    "select value from " + EXPORT_IMPORT_CLASS_NAME + " where key = ?",
                    String.valueOf(document.<OIdentifiable>field("rid")))) {
              if (!result.hasNext()) {
                newRid = document.field("rid");
              } else {
                newRid = new ORecordId(result.next().<String>getProperty("value"));
              }
            }

            index.put(binarySerializer.deserialize(document.field("key"), 0), newRid);
          }
          tot++;
        }
      } while (jsonReader.lastChar() == ',');

      if (index != null) {
        listener.onMessage("OK (" + tot + " entries)");
        n++;
      } else listener.onMessage("ERR, the index wasn't found in configuration");

      jsonReader.readNext(OJSONReader.END_OBJECT);
      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);

    } while (jsonReader.lastChar() == ',');

    listener.onMessage("\nDone. Imported " + String.format("%,d", n) + " indexes.");

    jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
  }

  private void importSchema(final JsonParser parser, final boolean clustersImported)
      throws IOException, ParseException {
    if (!clustersImported) {
      removeDefaultClusters();
    }
    listener.onMessage("\nImporting database schema...");

    JsonToken jsonToken = parser.nextToken();
    @SuppressWarnings("unused")
    int schemaVersion = 0;
    long classImported = 0;
    while (!JsonToken.END_OBJECT.equals(jsonToken)) {
      if (JsonToken.FIELD_NAME.equals(jsonToken) && "version".equals(parser.getValueAsString())) {
        parser.nextToken();
        schemaVersion = parser.getValueAsInt();
        jsonToken = parser.nextToken();
      } else if (JsonToken.FIELD_NAME.equals(jsonToken)
          && "blob-clusters".equals(parser.getValueAsString())) {
        // FIXME: implement (tests insufficient)
        while (!JsonToken.END_ARRAY.equals(jsonToken)) {
          jsonToken = parser.nextToken();
          System.out.println("within blob-clusters: " + jsonToken);
          /*String blobClusterIds = jsonReader.readString(OJSONReader.END_COLLECTION, true).trim();
          blobClusterIds = blobClusterIds.substring(1, blobClusterIds.length() - 1);

          if (!"".equals(blobClusterIds)) {
            // READ BLOB CLUSTER IDS
            for (String i :
                OStringSerializerHelper.split(
                    blobClusterIds, OStringSerializerHelper.RECORD_SEPARATOR)) {
              Integer cluster = Integer.parseInt(i);
              if (!database.getBlobClusterIds().contains(cluster)) {
                String name = database.getClusterNameById(cluster);
                database.addBlobCluster(name);
              }
            }
          }
          jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
          jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);*/
        }
        jsonToken = parser.nextToken();
      } else if (JsonToken.FIELD_NAME.equals(jsonToken)
          && "globalProperties".equals(parser.getValueAsString())) {
        // This can be removed after the M1 expires
        // FIXME: implement (tests insufficient)
        while (!JsonToken.END_ARRAY.equals(jsonToken)) {
          jsonToken = parser.nextToken();
          System.out.println("within globalProperties: " + jsonToken);
          /*jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);
          do {
            jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
            jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"name\"");
            String name = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"global-id\"");
            String id = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"type\"");
            String type = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            // getDatabase().getMetadata().getSchema().createGlobalProperty(name, OType.valueOf(type),
            // Integer.valueOf(id));
            jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
          } while (jsonReader.lastChar() == ',');
          jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
          jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);*/
        }
      } else if (JsonToken.FIELD_NAME.equals(jsonToken)
          && "classes".equals(parser.getValueAsString())) {
        while (!JsonToken.END_ARRAY.equals(jsonToken)) {
          jsonToken = parser.nextToken();

          String className = null;
          // FIXME:
          int classDefClusterId = -666;
          OClassImpl cls = null;
          while (!JsonToken.END_OBJECT.equals(jsonToken)) {
            // @COMPATIBILITY 1.0rc4 IGNORE THE ID

            if (JsonToken.FIELD_NAME.equals(jsonToken)) {
              if (parser.getValueAsString().equals("name")) {
                parser.nextToken();
                className = parser.getValueAsString();
              } else if (parser.getValueAsString().equals("default-cluster-id")) {
                parser.nextToken();
                classDefClusterId = parser.getIntValue();
              } else if (parser.getValueAsString().equals("cluster-ids")) {
                if (classDefClusterId == -666) {
                  classDefClusterId = database.getDefaultClusterId();
                }
                if (className.contains(".")) {
                  // MIGRATE OLD NAME WITH . TO _
                  final String newClassName = className.replace('.', '_');
                  convertedClassNames.put(className, newClassName);
                  listener.onMessage(
                      "\nWARNING: class '"
                          + className
                          + "' has been renamed in '"
                          + newClassName
                          + "'\n");
                  className = newClassName;
                }
                cls = (OClassImpl) database.getMetadata().getSchema().getClass(className);
                if (cls != null) {
                  if (cls.getDefaultClusterId() != classDefClusterId)
                    cls.setDefaultClusterId(classDefClusterId);
                } else if (clustersImported) {
                  cls =
                      (OClassImpl)
                          database
                              .getMetadata()
                              .getSchema()
                              .createClass(className, new int[] {classDefClusterId});
                } else if (className.equalsIgnoreCase("ORestricted")) {
                  cls =
                      (OClassImpl)
                          database.getMetadata().getSchema().createAbstractClass(className);
                } else {
                  cls = (OClassImpl) database.getMetadata().getSchema().createClass(className);
                }
                while (!JsonToken.END_ARRAY.equals(jsonToken)) {
                  jsonToken = parser.nextToken();
                  if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                    int clusterId = parser.getValueAsInt();
                    // ASSIGN OTHER CLUSTER IDS
                    if (clusterId != -1) {
                      cls.addClusterId(clusterId);
                    }
                  }
                }
              } else if (parser.getValueAsString().equals("strictMode")) {
                parser.nextToken();
                final boolean strictMode = parser.getValueAsBoolean();
                cls.setStrictMode(strictMode);
              } else if (parser.getValueAsString().equals("abstract")) {
                parser.nextToken();
                final boolean abstractValue = parser.getValueAsBoolean();
                cls.setAbstract(abstractValue);
              } else if (parser.getValueAsString().equals("oversize")) {
                parser.nextToken();
                final float oversize = parser.getFloatValue();
                cls.setOverSize(oversize);
              } else if (parser.getValueAsString().equals("short-name")) {
                parser.nextToken();
                final String shortName = parser.getValueAsString();
                if (!cls.getName().equalsIgnoreCase(shortName)) cls.setShortName(shortName);
              } else if (parser.getValueAsString().equals("super-class")) {
                // @compatibility <2.1 SINGLE CLASS ONLY
                parser.nextToken();
                final String classSuper = parser.getValueAsString();
                final List<String> superClassNames = new ArrayList<>();
                superClassNames.add(classSuper);
                superClasses.put(cls, superClassNames);
              } else if (parser.getValueAsString().equals("super-classes")) {
                // MULTIPLE CLASSES
                final List<String> superClassNames = new ArrayList<>();
                while (!JsonToken.END_ARRAY.equals(jsonToken)) {
                  while (!JsonToken.VALUE_STRING.equals(jsonToken)) {
                    jsonToken = parser.nextToken();
                  }
                  final String clsName = parser.getValueAsString();
                  superClassNames.add(clsName);
                  jsonToken = parser.nextToken();
                }
                superClasses.put(cls, superClassNames);
              } else if (parser.getValueAsString().equals("properties")) {
                while (!JsonToken.START_OBJECT.equals(jsonToken)) {
                  jsonToken = parser.nextToken();
                }
                // GET PROPERTIES
                while (!JsonToken.END_ARRAY.equals(jsonToken)) {
                  importProperty(parser, cls);
                  jsonToken = parser.nextToken();
                }
              } else if (parser.getValueAsString().equals("customFields")) {
                final Map<String, String> customFields = importCustomFields(parser);
                for (final Entry<String, String> entry : customFields.entrySet()) {
                  cls.setCustom(entry.getKey(), entry.getValue());
                }
              } else if (parser.getValueAsString().equals("cluster-selection")) {
                // @SINCE 1.7
                parser.nextToken();
                final String clusterSelection = parser.getValueAsString();
                cls.setClusterSelection(clusterSelection);
              }
            }
            jsonToken = parser.nextToken();
          }
          classImported++;
          jsonToken = parser.nextToken();
        }
        listener.onMessage("OK (" + classImported + " classes)");
        schemaImported = true;
      } else if (!JsonToken.START_ARRAY.equals(jsonToken)) {
        System.out.println(jsonToken);
        jsonToken = parser.nextToken();
      } else {
        final StringBuffer sb = new StringBuffer();
        sb.append("\t" + jsonToken + "=" + parser.getValueAsString()).append(":::");
        jsonToken = parser.nextToken();
        sb.append(jsonToken + " " + parser.getValueAsString());
        System.out.println(sb.toString());
      }
    }

    this.rebuildCompleteClassInheritence();
    this.setLinkedClasses();

    if (exporterVersion < 11) {
      OClass role = database.getMetadata().getSchema().getClass("ORole");
      role.dropProperty("rules");
    }
    listener.onMessage("OK (" + classImported + " classes)");
    schemaImported = true;

    this.rebuildCompleteClassInheritence();

    // SET ALL THE LINKED CLASSES
    setLinkedClasses();

    if (exporterVersion < 11) {
      final OClass role = database.getMetadata().getSchema().getClass("ORole");
      role.dropProperty("rules");
    }

    /*  listener.onMessage("OK (" + classImported + " classes)");
      schemaImported = true;
      jsonReader.readNext(OJSONReader.END_OBJECT);
      jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on importing schema", e);
      listener.onMessage("ERROR (" + classImported + " entries): " + e);
    }*/
  }

  private void setLinkedClasses() {
    for (final Entry<OPropertyImpl, String> linkedClass : linkedClasses.entrySet()) {
      linkedClass
          .getKey()
          .setLinkedClass(database.getMetadata().getSchema().getClass(linkedClass.getValue()));
    }
  }

  @Deprecated
  private void importSchema(boolean clustersImported) throws IOException, ParseException {
    if (!clustersImported) {
      removeDefaultClusters();
    }

    listener.onMessage("\nImporting database schema...");

    jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
    @SuppressWarnings("unused")
    int schemaVersion =
        jsonReader
            .readNext(OJSONReader.FIELD_ASSIGNMENT)
            .checkContent("\"version\"")
            .readNumber(OJSONReader.ANY_NUMBER, true);
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
    jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);
    // This can be removed after the M1 expires
    if (jsonReader.getValue().equals("\"globalProperties\"")) {
      jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);
      do {
        jsonReader.readNext(OJSONReader.BEGIN_OBJECT);
        jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"name\"");
        String name = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"global-id\"");
        String id = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).checkContent("\"type\"");
        String type = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        // getDatabase().getMetadata().getSchema().createGlobalProperty(name, OType.valueOf(type),
        // Integer.valueOf(id));
        jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
      } while (jsonReader.lastChar() == ',');
      jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
      jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);
    }

    if (jsonReader.getValue().equals("\"blob-clusters\"")) {
      String blobClusterIds = jsonReader.readString(OJSONReader.END_COLLECTION, true).trim();
      blobClusterIds = blobClusterIds.substring(1, blobClusterIds.length() - 1);

      if (!"".equals(blobClusterIds)) {
        // READ BLOB CLUSTER IDS
        for (String i :
            OStringSerializerHelper.split(
                blobClusterIds, OStringSerializerHelper.RECORD_SEPARATOR)) {
          Integer cluster = Integer.parseInt(i);
          if (!database.getBlobClusterIds().contains(cluster)) {
            String name = database.getClusterNameById(cluster);
            database.addBlobCluster(name);
          }
        }
      }

      jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
      jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);
    }

    jsonReader.checkContent("\"classes\"").readNext(OJSONReader.BEGIN_COLLECTION);

    long classImported = 0;

    try {
      do {
        jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

        String className =
            jsonReader
                .readNext(OJSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"name\"")
                .readString(OJSONReader.COMMA_SEPARATOR);

        String next = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).getValue();

        if (next.equals("\"id\"")) {
          // @COMPATIBILITY 1.0rc4 IGNORE THE ID
          next = jsonReader.readString(OJSONReader.COMMA_SEPARATOR);
          next = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).getValue();
        }

        final int classDefClusterId;
        if (jsonReader.isContent("\"default-cluster-id\"")) {
          next = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
          classDefClusterId = Integer.parseInt(next);
        } else classDefClusterId = database.getDefaultClusterId();

        int realClassDefClusterId = classDefClusterId;
        if (!clusterToClusterMapping.isEmpty()
            && clusterToClusterMapping.get(classDefClusterId) != null) {
          realClassDefClusterId = clusterToClusterMapping.get(classDefClusterId);
        }
        String classClusterIds =
            jsonReader
                .readNext(OJSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"cluster-ids\"")
                .readString(OJSONReader.END_COLLECTION, true)
                .trim();

        jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);

        if (className.contains(".")) {
          // MIGRATE OLD NAME WITH . TO _
          final String newClassName = className.replace('.', '_');
          convertedClassNames.put(className, newClassName);

          listener.onMessage(
              "\nWARNING: class '" + className + "' has been renamed in '" + newClassName + "'\n");

          className = newClassName;
        }

        OClassImpl cls = (OClassImpl) database.getMetadata().getSchema().getClass(className);

        if (cls != null) {
          if (cls.getDefaultClusterId() != realClassDefClusterId)
            cls.setDefaultClusterId(realClassDefClusterId);
        } else if (clustersImported) {
          cls =
              (OClassImpl)
                  database
                      .getMetadata()
                      .getSchema()
                      .createClass(className, new int[] {realClassDefClusterId});
        } else if (className.equalsIgnoreCase("ORestricted")) {
          cls = (OClassImpl) database.getMetadata().getSchema().createAbstractClass(className);
        } else {
          cls = (OClassImpl) database.getMetadata().getSchema().createClass(className);
        }

        if (classClusterIds != null && clustersImported) {
          // REMOVE BRACES
          classClusterIds = classClusterIds.substring(1, classClusterIds.length() - 1);

          // ASSIGN OTHER CLUSTER IDS
          for (int i : OStringSerializerHelper.splitIntArray(classClusterIds)) {
            if (i != -1) {
              if (!clusterToClusterMapping.isEmpty()
                  && clusterToClusterMapping.get(classDefClusterId) != null) {
                i = clusterToClusterMapping.get(i);
              }
              cls.addClusterId(i);
            }
          }
        }

        String value;
        while (jsonReader.lastChar() == ',') {
          jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);
          value = jsonReader.getValue();

          if (value.equals("\"strictMode\"")) {
            cls.setStrictMode(jsonReader.readBoolean(OJSONReader.NEXT_IN_OBJECT));
          } else if (value.equals("\"abstract\"")) {
            cls.setAbstract(jsonReader.readBoolean(OJSONReader.NEXT_IN_OBJECT));
          } else if (value.equals("\"oversize\"")) {
            final String oversize = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            cls.setOverSize(Float.parseFloat(oversize));
          } else if (value.equals("\"strictMode\"")) { // TODO: check redundant?
            final String strictMode = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            cls.setStrictMode(Boolean.parseBoolean(strictMode));
          } else if (value.equals("\"short-name\"")) {
            final String shortName = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            if (!cls.getName().equalsIgnoreCase(shortName)) cls.setShortName(shortName);
          } else if (value.equals("\"super-class\"")) {
            // @compatibility <2.1 SINGLE CLASS ONLY
            final String classSuper = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
            final List<String> superClassNames = new ArrayList<String>();
            superClassNames.add(classSuper);
            superClasses.put(cls, superClassNames);
          } else if (value.equals("\"super-classes\"")) {
            // MULTIPLE CLASSES
            jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

            final List<String> superClassNames = new ArrayList<String>();
            while (jsonReader.lastChar() != ']') {
              jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);

              final String clsName = jsonReader.getValue();

              superClassNames.add(OIOUtils.getStringContent(clsName));
            }
            jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);

            superClasses.put(cls, superClassNames);
          } else if (value.equals("\"properties\"")) {
            // GET PROPERTIES
            jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

            while (jsonReader.lastChar() != ']') {
              importProperty(cls);

              if (jsonReader.lastChar() == '}') jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
            }
            jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
          } else if (value.equals("\"customFields\"")) {
            Map<String, String> customFields = importCustomFields();
            for (Entry<String, String> entry : customFields.entrySet()) {
              cls.setCustom(entry.getKey(), entry.getValue());
            }
          } else if (value.equals("\"cluster-selection\"")) {
            // @SINCE 1.7
            cls.setClusterSelection(jsonReader.readString(OJSONReader.NEXT_IN_OBJECT));
          }
        }

        classImported++;

        jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
      } while (jsonReader.lastChar() == ',');

      this.rebuildCompleteClassInheritence();
      this.setLinkedClasses();

      if (exporterVersion < 11) {
        OClass role = database.getMetadata().getSchema().getClass("ORole");
        role.dropProperty("rules");
      }

      listener.onMessage("OK (" + classImported + " classes)");
      schemaImported = true;
      jsonReader.readNext(OJSONReader.END_OBJECT);
      jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
    } catch (final Exception e) {
      OLogManager.instance().error(this, "Error on importing schema", e);
      listener.onMessage("ERROR (" + classImported + " entries): " + e);
    }
  }

  private void rebuildCompleteClassInheritence() {
    for (final Entry<OClass, List<String>> entry : superClasses.entrySet()) {
      for (final String superClassName : entry.getValue()) {
        final OClass superClass = database.getMetadata().getSchema().getClass(superClassName);

        if (!entry.getKey().getSuperClasses().contains(superClass)) {
          entry.getKey().addSuperClass(superClass);
        }
      }
    }
  }

  private void importProperty(final JsonParser parser, final OClass iClass)
      throws IOException, ParseException {
    JsonToken jsonToken = parser.currentToken();
    if (!JsonToken.START_OBJECT.equals(jsonToken)) {
      throw new IllegalStateException("Expected JSON Object, but found " + jsonToken);
    }
    String propertyName = null;
    OType propertyType = null;

    String min = null;
    String max = null;
    String linkedClass = null;
    OType linkedType = null;
    boolean mandatory = false;
    boolean readonly = false;
    boolean notNull = false;
    String collate = null;
    String regexp = null;
    String defaultValue = null;

    Map<String, String> customFields = null;
    while (!JsonToken.END_OBJECT.equals(jsonToken)) {
      if (JsonToken.FIELD_NAME.equals(jsonToken)) {
        // @COMPATIBILITY 1.0rc4 IGNORE THE ID

        // jsonToken = parser.nextToken();
        if (parser.getValueAsString().equals("name")) {
          parser.nextToken();
          propertyName = parser.getValueAsString();
        } else if (parser.getValueAsString().equals("type")) {
          parser.nextToken();
          final String type = parser.getValueAsString();
          propertyType = OType.valueOf(type);
        } else if (parser.getValueAsString().equals("customFields")) {
          jsonToken = parser.nextToken();
          System.out.println(jsonToken + "-" + parser.getValueAsString());
        } else {
          final String value = parser.getValueAsString();
          if (value.equals("min")) {
            parser.nextToken();
            min = parser.getValueAsString();
          } else if (value.equals("max")) {
            parser.nextToken();
            max = parser.getValueAsString();
          } else if (value.equals("linked-class")) {
            parser.nextToken();
            linkedClass = parser.getValueAsString();
          } else if (value.equals("mandatory")) {
            parser.nextToken();
            mandatory = parser.getValueAsBoolean();
          } else if (value.equals("readonly")) {
            parser.nextToken();
            readonly = parser.getValueAsBoolean();
          } else if (value.equals("not-null")) {
            parser.nextToken();
            notNull = parser.getValueAsBoolean();
          } else if (value.equals("linked-type")) {
            parser.nextToken();
            linkedType = OType.valueOf(parser.getValueAsString());
          } else if (value.equals("collate")) {
            parser.nextToken();
            collate = parser.getValueAsString();
          } else if (value.equals("default-value")) {
            parser.nextToken();
            defaultValue = parser.getValueAsString();
          } else if (value.equals("customFields")) {
            parser.nextToken();
            customFields = importCustomFields(parser);
          } else if (value.equals("regexp")) {
            parser.nextToken();
            regexp = parser.getValueAsString();
          }
        }
      }
      jsonToken = parser.nextToken();
    }

    OPropertyImpl prop = (OPropertyImpl) iClass.getProperty(propertyName);
    if (prop == null) {
      // CREATE IT
      prop = (OPropertyImpl) iClass.createProperty(propertyName, propertyType, (OType) null, true);
    }
    prop.setMandatory(mandatory);
    prop.setReadonly(readonly);
    prop.setNotNull(notNull);

    if (min != null) prop.setMin(min);
    if (max != null) prop.setMax(max);
    if (linkedClass != null) linkedClasses.put(prop, linkedClass);
    if (linkedType != null) prop.setLinkedType(linkedType);
    if (collate != null) prop.setCollate(collate);
    if (regexp != null) prop.setRegexp(regexp);
    if (defaultValue != null) {
      // was VALUE before
      prop.setDefaultValue(defaultValue);
    }
    if (customFields != null) {
      for (Entry<String, String> entry : customFields.entrySet()) {
        prop.setCustom(entry.getKey(), entry.getValue());
      }
    }
  }

  @Deprecated
  private void importProperty(final OClass iClass) throws IOException, ParseException {
    jsonReader.readNext(OJSONReader.NEXT_OBJ_IN_ARRAY);

    if (jsonReader.lastChar() == ']') return;

    final String propName =
        jsonReader
            .readNext(OJSONReader.FIELD_ASSIGNMENT)
            .checkContent("\"name\"")
            .readString(OJSONReader.COMMA_SEPARATOR);

    String next = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).getValue();

    if (next.equals("\"id\"")) {
      // @COMPATIBILITY 1.0rc4 IGNORE THE ID
      next = jsonReader.readString(OJSONReader.COMMA_SEPARATOR);
      next = jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT).getValue();
    }
    next = jsonReader.checkContent("\"type\"").readString(OJSONReader.NEXT_IN_OBJECT);

    final OType type = OType.valueOf(next);

    String attrib;
    String value = null;

    String min = null;
    String max = null;
    String linkedClass = null;
    OType linkedType = null;
    boolean mandatory = false;
    boolean readonly = false;
    boolean notNull = false;
    String collate = null;
    String regexp = null;
    String defaultValue = null;

    Map<String, String> customFields = null;

    while (jsonReader.lastChar() == ',') {
      jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);

      attrib = jsonReader.getValue();
      if (!attrib.equals("\"customFields\""))
        value =
            jsonReader.readString(
                OJSONReader.NEXT_IN_OBJECT, false, OJSONReader.DEFAULT_JUMP, null, false);

      if (attrib.equals("\"min\"")) min = value;
      else if (attrib.equals("\"max\"")) max = value;
      else if (attrib.equals("\"linked-class\"")) linkedClass = value;
      else if (attrib.equals("\"mandatory\"")) mandatory = Boolean.parseBoolean(value);
      else if (attrib.equals("\"readonly\"")) readonly = Boolean.parseBoolean(value);
      else if (attrib.equals("\"not-null\"")) notNull = Boolean.parseBoolean(value);
      else if (attrib.equals("\"linked-type\"")) linkedType = OType.valueOf(value);
      else if (attrib.equals("\"collate\"")) collate = value;
      else if (attrib.equals("\"default-value\"")) defaultValue = value;
      else if (attrib.equals("\"customFields\"")) customFields = importCustomFields();
      else if (attrib.equals("\"regexp\"")) regexp = value;
    }

    OPropertyImpl prop = (OPropertyImpl) iClass.getProperty(propName);
    if (prop == null) {
      // CREATE IT
      prop = (OPropertyImpl) iClass.createProperty(propName, type, (OType) null, true);
    }
    prop.setMandatory(mandatory);
    prop.setReadonly(readonly);
    prop.setNotNull(notNull);

    if (min != null) prop.setMin(min);
    if (max != null) prop.setMax(max);
    if (linkedClass != null) linkedClasses.put(prop, linkedClass);
    if (linkedType != null) prop.setLinkedType(linkedType);
    if (collate != null) prop.setCollate(collate);
    if (regexp != null) prop.setRegexp(regexp);
    if (defaultValue != null) {
      prop.setDefaultValue(value);
    }
    if (customFields != null) {
      for (Entry<String, String> entry : customFields.entrySet()) {
        prop.setCustom(entry.getKey(), entry.getValue());
      }
    }
  }

  private Map<String, String> importCustomFields(final JsonParser parser)
      throws ParseException, IOException {
    Map<String, String> result = new HashMap<>();

    JsonToken jsonToken = parser.currentToken();
    while (!JsonToken.END_OBJECT.equals(jsonToken)) {
      if (JsonToken.FIELD_NAME.equals(jsonToken)) {
        // FIXME
        // final String key = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
        // final String value = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

        // result.put(key, value);
      }
      jsonToken = parser.nextToken();
    }
    return result;
  }

  @Deprecated
  private Map<String, String> importCustomFields() throws ParseException, IOException {
    Map<String, String> result = new HashMap<>();

    jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

    while (jsonReader.lastChar() != '}') {
      final String key = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
      final String value = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

      result.put(key, value);
    }

    jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

    return result;
  }

  @Deprecated
  private long importClusters() throws ParseException, IOException {
    listener.onMessage("\nImporting clusters...");

    long total = 0;

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    boolean recreateManualIndex = false;
    if (exporterVersion <= 4) {
      removeDefaultClusters();
      recreateManualIndex = true;
    }

    final Set<String> indexesToRebuild = new HashSet<>();

    @SuppressWarnings("unused")
    ORecordId rid = null;
    while (jsonReader.lastChar() != ']') {
      jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

      String name =
          jsonReader
              .readNext(OJSONReader.FIELD_ASSIGNMENT)
              .checkContent("\"name\"")
              .readString(OJSONReader.COMMA_SEPARATOR);

      if (name.length() == 0) name = null;

      name = OClassImpl.decodeClassName(name);
      if (name != null)
        // CHECK IF THE CLUSTER IS INCLUDED
        if (includeClusters != null) {
          if (!includeClusters.contains(name)) {
            jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
            continue;
          }
        } else if (excludeClusters != null) {
          if (excludeClusters.contains(name)) {
            jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
            continue;
          }
        }

      int clusterIdFromJson;
      if (exporterVersion < 9) {
        clusterIdFromJson =
            jsonReader
                .readNext(OJSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"id\"")
                .readInteger(OJSONReader.COMMA_SEPARATOR);
        String type =
            jsonReader
                .readNext(OJSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"type\"")
                .readString(OJSONReader.NEXT_IN_OBJECT);
      } else
        clusterIdFromJson =
            jsonReader
                .readNext(OJSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"id\"")
                .readInteger(OJSONReader.NEXT_IN_OBJECT);

      String type;
      if (jsonReader.lastChar() == ',')
        type =
            jsonReader
                .readNext(OJSONReader.FIELD_ASSIGNMENT)
                .checkContent("\"type\"")
                .readString(OJSONReader.NEXT_IN_OBJECT);
      else type = "PHYSICAL";

      if (jsonReader.lastChar() == ',') {
        rid =
            new ORecordId(
                jsonReader
                    .readNext(OJSONReader.FIELD_ASSIGNMENT)
                    .checkContent("\"rid\"")
                    .readString(OJSONReader.NEXT_IN_OBJECT));
      } else rid = null;

      listener.onMessage(
          "\n- Creating cluster " + (name != null ? "'" + name + "'" : "NULL") + "...");

      int createdClusterId = name != null ? database.getClusterIdByName(name) : -1;
      if (createdClusterId == -1) {
        // CREATE IT
        if (!preserveClusterIDs) createdClusterId = database.addCluster(name);
        else if (getDatabase().getClusterNameById(clusterIdFromJson) == null) {
          createdClusterId = database.addCluster(name, clusterIdFromJson, null);
          assert createdClusterId == clusterIdFromJson;
        } else {
          createdClusterId = database.addCluster(name);
          listener.onMessage(
              "\n- WARNING cluster with id " + clusterIdFromJson + " already exists");
        }
      }

      if (createdClusterId != clusterIdFromJson) {
        if (!preserveClusterIDs) {
          if (database.countClusterElements(createdClusterId - 1) == 0) {
            listener.onMessage("Found previous version: migrating old clusters...");
            database.dropCluster(name);
            database.addCluster("temp_" + createdClusterId, null);
            createdClusterId = database.addCluster(name);
          } else
            throw new OConfigurationException(
                "Imported cluster '"
                    + name
                    + "' has id="
                    + createdClusterId
                    + " different from the original: "
                    + clusterIdFromJson
                    + ". To continue the import drop the cluster '"
                    + database.getClusterNameById(createdClusterId - 1)
                    + "' that has "
                    + database.countClusterElements(createdClusterId - 1)
                    + " records");
        } else {

          final OClass clazz =
              database.getMetadata().getSchema().getClassByClusterId(createdClusterId);
          if (clazz != null && clazz instanceof OClassEmbedded)
            ((OClassEmbedded) clazz).removeClusterId(createdClusterId, true);

          database.dropCluster(createdClusterId);
          createdClusterId = database.addCluster(name, clusterIdFromJson, null);
        }
      }
      clusterToClusterMapping.put(clusterIdFromJson, createdClusterId);

      listener.onMessage("OK, assigned id=" + createdClusterId + ", was " + clusterIdFromJson);

      total++;

      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
    }
    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);

    listener.onMessage("\nRebuilding indexes of truncated clusters ...");

    for (final String indexName : indexesToRebuild)
      database
          .getMetadata()
          .getIndexManagerInternal()
          .getIndex(database, indexName)
          .rebuild(
              new OProgressListener() {
                private long last = 0;

                @Override
                public void onBegin(Object iTask, long iTotal, Object metadata) {
                  listener.onMessage(
                      "\n- Cluster content was updated: rebuilding index '" + indexName + "'...");
                }

                @Override
                public boolean onProgress(Object iTask, long iCounter, float iPercent) {
                  final long now = System.currentTimeMillis();
                  if (last == 0) last = now;
                  else if (now - last > 1000) {
                    listener.onMessage(
                        String.format(
                            "\nIndex '%s' is rebuilding (%.2f/100)", indexName, iPercent));
                    last = now;
                  }
                  return true;
                }

                @Override
                public void onCompletition(Object iTask, boolean iSucceed) {
                  listener.onMessage(" Index " + indexName + " was successfully rebuilt.");
                }
              });
    listener.onMessage("\nDone " + indexesToRebuild.size() + " indexes were rebuilt.");

    if (recreateManualIndex) {
      database.addCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      database.getMetadata().getIndexManagerInternal().create();

      listener.onMessage("\nManual index cluster was recreated.");
    }
    listener.onMessage("\nDone. Imported " + total + " clusters");

    if (database.load(new ORecordId(database.getStorage().getConfiguration().getIndexMgrRecordId()))
        == null) {
      ODocument indexDocument = new ODocument();
      indexDocument.save(OMetadataDefault.CLUSTER_INTERNAL_NAME);

      database.getStorage().setIndexMgrRecordId(indexDocument.getIdentity().toString());
    }
    return total;
  }

  // FIXME
  private ORID importRecord(final JsonParser parser, final HashSet<ORID> recordsBeforeImport)
      throws Exception {
    OPair<String, Map<String, ORidSet>> recordParse =
        jsonReader.readRecordString(this.maxRidbagStringSizeBeforeLazyImport);
    String value = recordParse.getKey().trim();

    if (value.isEmpty()) {
      return null;
    }

    // JUMP EMPTY RECORDS
    while (!value.isEmpty() && value.charAt(0) != '{') {
      value = value.substring(1);
    }

    record = null;

    // big ridbags (ie. supernodes) sometimes send the system OOM, so they have to be discarded at
    // this stage
    // and processed later. The following collects the positions ("value" inside the string) of
    // skipped fields.
    Set<Integer> skippedPartsIndexes = new HashSet<>();

    try {
      try {
        record =
            ORecordSerializerJSON.INSTANCE.fromString(
                value,
                record,
                null,
                null,
                false,
                maxRidbagStringSizeBeforeLazyImport,
                skippedPartsIndexes);
      } catch (OSerializationException e) {
        if (e.getCause() instanceof OSchemaException) {
          // EXTRACT CLASS NAME If ANY
          final int pos = value.indexOf("\"@class\":\"");
          if (pos > -1) {
            final int end = value.indexOf("\"", pos + "\"@class\":\"".length() + 1);
            final String value1 = value.substring(0, pos + "\"@class\":\"".length());
            final String clsName = value.substring(pos + "\"@class\":\"".length(), end);
            final String value2 = value.substring(end);

            final String newClassName = convertedClassNames.get(clsName);

            value = value1 + newClassName + value2;
            // OVERWRITE CLASS NAME WITH NEW NAME
            record =
                ORecordSerializerJSON.INSTANCE.fromString(
                    value,
                    record,
                    null,
                    null,
                    false,
                    maxRidbagStringSizeBeforeLazyImport,
                    skippedPartsIndexes);
          }
        } else
          throw OException.wrapException(
              new ODatabaseImportException("Error on importing record"), e);
      }

      // Incorrect record format , skip this record
      if (record == null || record.getIdentity() == null) {
        OLogManager.instance().warn(this, "Broken record was detected and will be skipped");
        return null;
      }

      if (schemaImported && record.getIdentity().equals(schemaRecordId)) {
        recordsBeforeImport.remove(record.getIdentity());
        // JUMP THE SCHEMA
        return null;
      }

      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(
            database.getClusterNameById(record.getIdentity().getClusterId()))) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(
            database.getClusterNameById(record.getIdentity().getClusterId()))) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record instanceof ODocument && excludeClasses != null) {
        if (excludeClasses.contains(((ODocument) record).getClassName())) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record.getIdentity().getClusterId() == 0
          && record.getIdentity().getClusterPosition() == 1) {
        recordsBeforeImport.remove(record.getIdentity());
        // JUMP INTERNAL RECORDS
        return null;
      }

      if (exporterVersion >= 3) {
        int oridsId = database.getClusterIdByName("ORIDs");
        int indexId = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

        if (record.getIdentity().getClusterId() == indexId
            || record.getIdentity().getClusterId() == oridsId) {
          recordsBeforeImport.remove(record.getIdentity());
          // JUMP INDEX RECORDS
          return null;
        }
      }

      final int manualIndexCluster =
          database.getClusterIdByName(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      final int internalCluster =
          database.getClusterIdByName(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      final int indexCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

      if (exporterVersion >= 4) {
        if (record.getIdentity().getClusterId() == manualIndexCluster) {
          // JUMP INDEX RECORDS
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record.getIdentity().equals(indexMgrRecordId)) {
        recordsBeforeImport.remove(record.getIdentity());
        return null;
      }

      final ORID rid = record.getIdentity();

      final int clusterId = rid.getClusterId();

      if ((clusterId != manualIndexCluster
          && clusterId != internalCluster
          && clusterId != indexCluster)) {
        if (recordsBeforeImport.remove(rid)) {
          final ORecord loadedRecord = database.load(rid);
          if (loadedRecord == null) {
            throw new IllegalStateException(
                "Record with rid = " + rid + " should exist in database " + database.getName());
          }

          if (!record.getClass().isAssignableFrom(loadedRecord.getClass())) {
            throw new IllegalStateException(
                "Imported record and record stored in database under id "
                    + rid.toString()
                    + " have different types. "
                    + "Stored record class is : "
                    + record.getClass()
                    + " and imported "
                    + loadedRecord.getClass()
                    + " .");
          }

          ORecordInternal.setVersion(record, loadedRecord.getVersion());
        } else {
          ORecordInternal.setVersion(record, 0);
          ORecordInternal.setIdentity(record, new ORecordId());
        }

        record.setDirty();

        if (!preserveRids
            && record instanceof ODocument
            && ODocumentInternal.getImmutableSchemaClass(database, ((ODocument) record)) != null)
          record.save();
        else record.save(database.getClusterNameById(clusterId));

        if (!rid.equals(record.getIdentity())) {
          // SAVE IT ONLY IF DIFFERENT
          new ODocument(EXPORT_IMPORT_CLASS_NAME)
              .field("key", rid.toString())
              .field("value", record.getIdentity().toString())
              .save();
        }
      }

      // import skipped records (too big to be imported before)
      if (skippedPartsIndexes.size() > 0) {
        for (Integer skippedPartsIndex : skippedPartsIndexes) {
          importSkippedRidbag(record, value, skippedPartsIndex);
        }
      }

      if (recordParse.value.size() > 0) {
        importSkippedRidbag(record, recordParse.getValue());
      }

    } catch (Exception t) {
      if (record != null)
        OLogManager.instance()
            .error(
                this,
                "Error importing record "
                    + record.getIdentity()
                    + ". Source line "
                    + jsonReader.getLineNumber()
                    + ", column "
                    + jsonReader.getColumnNumber(),
                t);
      else
        OLogManager.instance()
            .error(
                this,
                "Error importing record. Source line "
                    + jsonReader.getLineNumber()
                    + ", column "
                    + jsonReader.getColumnNumber(),
                t);

      if (!(t instanceof ODatabaseException)) {
        throw t;
      }
    }
    return record.getIdentity();
  }

  // TODO: WIP
  @Deprecated
  private ORID importRecord(final HashSet<ORID> recordsBeforeImport) throws Exception {
    OPair<String, Map<String, ORidSet>> recordParse =
        jsonReader.readRecordString(this.maxRidbagStringSizeBeforeLazyImport);
    String value = recordParse.getKey().trim();

    if (value.isEmpty()) {
      return null;
    }

    // JUMP EMPTY RECORDS
    while (!value.isEmpty() && value.charAt(0) != '{') {
      value = value.substring(1);
    }

    record = null;

    // big ridbags (ie. supernodes) sometimes send the system OOM, so they have to be discarded at
    // this stage
    // and processed later. The following collects the positions ("value" inside the string) of
    // skipped fields.
    Set<Integer> skippedPartsIndexes = new HashSet<>();

    try {

      try {
        if (exporterVersion < 13) {
          record =
              ORecordSerializerJSON.INSTANCE.fromString(
                  value,
                  record,
                  null,
                  null,
                  false,
                  maxRidbagStringSizeBeforeLazyImport,
                  skippedPartsIndexes);
        } else {
          // FIXME: switch to `fromStream` + adapt e2e stream handling
          record =
              ORecordSerializerJSON.INSTANCE.fromString(
                  value,
                  record,
                  null,
                  null,
                  false,
                  maxRidbagStringSizeBeforeLazyImport,
                  skippedPartsIndexes);
        }
      } catch (final OSerializationException e) {
        if (e.getCause() instanceof OSchemaException) {
          // EXTRACT CLASS NAME If ANY
          final int pos = value.indexOf("\"@class\":\"");
          if (pos > -1) {
            final int end = value.indexOf("\"", pos + "\"@class\":\"".length() + 1);
            final String value1 = value.substring(0, pos + "\"@class\":\"".length());
            final String clsName = value.substring(pos + "\"@class\":\"".length(), end);
            final String value2 = value.substring(end);

            final String newClassName = convertedClassNames.get(clsName);

            value = value1 + newClassName + value2;
            // OVERWRITE CLASS NAME WITH NEW NAME
            record =
                ORecordSerializerJSON.INSTANCE.fromString(
                    value,
                    record,
                    null,
                    null,
                    false,
                    maxRidbagStringSizeBeforeLazyImport,
                    skippedPartsIndexes);
          }
        } else
          throw OException.wrapException(
              new ODatabaseImportException("Error on importing record"), e);
      }

      // Incorrect record format , skip this record
      if (record == null || record.getIdentity() == null) {
        OLogManager.instance().warn(this, "Broken record was detected and will be skipped");
        return null;
      }

      if (schemaImported && record.getIdentity().equals(schemaRecordId)) {
        recordsBeforeImport.remove(record.getIdentity());
        // JUMP THE SCHEMA
        return null;
      }

      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(
            database.getClusterNameById(record.getIdentity().getClusterId()))) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(
            database.getClusterNameById(record.getIdentity().getClusterId()))) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record instanceof ODocument && excludeClasses != null) {
        if (excludeClasses.contains(((ODocument) record).getClassName())) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record.getIdentity().getClusterId() == 0
          && record.getIdentity().getClusterPosition() == 1) {
        recordsBeforeImport.remove(record.getIdentity());
        // JUMP INTERNAL RECORDS
        return null;
      }

      if (exporterVersion >= 3) {
        int oridsId = database.getClusterIdByName("ORIDs");
        int indexId = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

        if (record.getIdentity().getClusterId() == indexId
            || record.getIdentity().getClusterId() == oridsId) {
          recordsBeforeImport.remove(record.getIdentity());
          // JUMP INDEX RECORDS
          return null;
        }
      }

      final int manualIndexCluster =
          database.getClusterIdByName(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      final int internalCluster =
          database.getClusterIdByName(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      final int indexCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

      if (exporterVersion >= 4) {
        if (record.getIdentity().getClusterId() == manualIndexCluster) {
          // JUMP INDEX RECORDS
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record.getIdentity().equals(indexMgrRecordId)) {
        recordsBeforeImport.remove(record.getIdentity());
        return null;
      }

      final ORID rid = record.getIdentity();

      final int clusterId = rid.getClusterId();

      if ((clusterId != manualIndexCluster
          && clusterId != internalCluster
          && clusterId != indexCluster)) {
        if (recordsBeforeImport.remove(rid)) {
          final ORecord loadedRecord = database.load(rid);
          if (loadedRecord == null) {
            throw new IllegalStateException(
                "Record with rid = " + rid + " should exist in database " + database.getName());
          }

          if (!record.getClass().isAssignableFrom(loadedRecord.getClass())) {
            throw new IllegalStateException(
                "Imported record and record stored in database under id "
                    + rid.toString()
                    + " have different types. "
                    + "Stored record class is : "
                    + record.getClass()
                    + " and imported "
                    + loadedRecord.getClass()
                    + " .");
          }

          ORecordInternal.setVersion(record, loadedRecord.getVersion());
        } else {
          ORecordInternal.setVersion(record, 0);
          ORecordInternal.setIdentity(record, new ORecordId());
        }

        record.setDirty();

        if (!preserveRids
            && record instanceof ODocument
            && ODocumentInternal.getImmutableSchemaClass(database, ((ODocument) record)) != null)
          record.save();
        else record.save(database.getClusterNameById(clusterId));

        if (!rid.equals(record.getIdentity())) {
          // SAVE IT ONLY IF DIFFERENT
          new ODocument(EXPORT_IMPORT_CLASS_NAME)
              .field("key", rid.toString())
              .field("value", record.getIdentity().toString())
              .save();
        }
      }

      // import skipped records (too big to be imported before)
      if (skippedPartsIndexes.size() > 0) {
        for (Integer skippedPartsIndex : skippedPartsIndexes) {
          importSkippedRidbag(record, value, skippedPartsIndex);
        }
      }

      if (recordParse.value.size() > 0) {
        importSkippedRidbag(record, recordParse.getValue());
      }

    } catch (Exception t) {
      if (record != null)
        OLogManager.instance()
            .error(
                this,
                "Error importing record "
                    + record.getIdentity()
                    + ". Source line "
                    + jsonReader.getLineNumber()
                    + ", column "
                    + jsonReader.getColumnNumber(),
                t);
      else
        OLogManager.instance()
            .error(
                this,
                "Error importing record. Source line "
                    + jsonReader.getLineNumber()
                    + ", column "
                    + jsonReader.getColumnNumber(),
                t);

      if (!(t instanceof ODatabaseException)) {
        throw t;
      }
    }
    return record.getIdentity();
  }

  private void recreateManualIndex(boolean recreateManualIndex) {
    if (recreateManualIndex) {
      database.addCluster(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      database.getMetadata().getIndexManagerInternal().create();

      listener.onMessage("\nManual index cluster was recreated.");
    }
  }

  private long importRecords(final JsonParser parser) throws Exception {
    long total = 0;

    final OSchema schema = database.getMetadata().getSchema();
    if (schema.getClass(EXPORT_IMPORT_CLASS_NAME) != null) {
      schema.dropClass(EXPORT_IMPORT_CLASS_NAME);
    }

    final OClass cls = schema.createClass(EXPORT_IMPORT_CLASS_NAME);
    cls.createProperty("key", OType.STRING);
    cls.createProperty("value", OType.STRING);
    cls.createIndex(EXPORT_IMPORT_INDEX_NAME, OClass.INDEX_TYPE.DICTIONARY, "key");

    long totalRecords = 0;
    listener.onMessage("\n\nImporting records...");

    // the only security records are left at this moment so we need to overwrite them
    // and then remove left overs
    final HashSet<ORID> recordsBeforeImport = new HashSet<>();

    for (final String clusterName : database.getClusterNames()) {
      final Iterator<ORecord> recordIterator = database.browseCluster(clusterName);
      while (recordIterator.hasNext()) {
        recordsBeforeImport.add(recordIterator.next().getIdentity());
      }
    }

    ORID rid;
    ORID lastRid = new ORecordId();
    final long begin = System.nanoTime();
    long lastLapRecords = 0;
    long last = begin;
    Set<String> involvedClusters = new HashSet<>();

    final JsonToken jsonToken = parser.currentToken();

    while (!JsonToken.END_ARRAY.equals(jsonToken)) {
      rid = importRecord(parser, recordsBeforeImport);

      total++;
      if (rid != null) {
        ++lastLapRecords;
        ++totalRecords;

        if (rid.getClusterId() != lastRid.getClusterId() || involvedClusters.isEmpty())
          involvedClusters.add(database.getClusterNameById(rid.getClusterId()));
        lastRid = rid;
      }

      final long now = System.currentTimeMillis();
      if (now - last > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
        final List<String> sortedClusters = new ArrayList<>(involvedClusters);
        Collections.sort(sortedClusters);

        listener.onMessage(
            String.format(
                "\n- Imported %,d records into clusters: %s. "
                    + "Total JSON records imported so for %,d .Total records imported so far: %,d (%,.2f/sec)",
                lastLapRecords,
                total,
                sortedClusters.size(),
                totalRecords,
                (float) lastLapRecords * 1000 / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

        // RESET LAP COUNTERS
        last = now;
        lastLapRecords = 0;
        involvedClusters.clear();
      }
      record = null;
    }

    if (!merge) {
      // remove all records which were absent in new database but
      // exist in old database
      for (final ORID leftOverRid : recordsBeforeImport) {
        database.delete(leftOverRid);
      }
    }
    database.getMetadata().reload();

    final Set<ORID> brokenRids = new HashSet<>();
    processBrokenRids(brokenRids);
    listener.onMessage(
        String.format(
            "\n\nDone. Imported %,d records in %,.2f secs\n",
            totalRecords, ((float) (System.nanoTime() - begin)) / 1000000));
    // jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);
    return total;
  }

  @Deprecated
  private long importRecords() throws Exception {
    long total = 0;

    final OSchema schema = database.getMetadata().getSchema();
    if (schema.getClass(EXPORT_IMPORT_CLASS_NAME) != null) {
      schema.dropClass(EXPORT_IMPORT_CLASS_NAME);
    }

    final OClass cls = schema.createClass(EXPORT_IMPORT_CLASS_NAME);
    cls.createProperty("key", OType.STRING);
    cls.createProperty("value", OType.STRING);
    cls.createIndex(EXPORT_IMPORT_INDEX_NAME, OClass.INDEX_TYPE.DICTIONARY, "key");

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    long totalRecords = 0;

    listener.onMessage("\n\nImporting records...");

    // the only security records are left at this moment so we need to overwrite them
    // and then remove left overs
    final HashSet<ORID> recordsBeforeImport = new HashSet<>();

    for (final String clusterName : database.getClusterNames()) {
      final Iterator<ORecord> recordIterator = database.browseCluster(clusterName);
      while (recordIterator.hasNext()) {
        recordsBeforeImport.add(recordIterator.next().getIdentity());
      }
    }

    ORID rid;
    ORID lastRid = new ORecordId();
    final long begin = System.currentTimeMillis();
    long lastLapRecords = 0;
    long last = begin;
    Set<String> involvedClusters = new HashSet<>();

    OLogManager.instance().debug(this, "Detected exporter version " + exporterVersion + ".");
    while (jsonReader.lastChar() != ']') {
      // TODO: add special handling for `exporterVersion` / `ODatabaseExport.EXPORTER_VERSION` >= 13
      rid = importRecord(recordsBeforeImport);

      total++;
      if (rid != null) {
        ++lastLapRecords;
        ++totalRecords;

        if (rid.getClusterId() != lastRid.getClusterId() || involvedClusters.isEmpty())
          involvedClusters.add(database.getClusterNameById(rid.getClusterId()));
        lastRid = rid;
      }

      final long now = System.currentTimeMillis();
      if (now - last > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
        final List<String> sortedClusters = new ArrayList<>(involvedClusters);
        Collections.sort(sortedClusters);

        listener.onMessage(
            String.format(
                "\n- Imported %,d records into clusters: %s. "
                    + "Total JSON records imported so for %,d .Total records imported so far: %,d (%,.2f/sec)",
                lastLapRecords,
                total,
                sortedClusters.size(),
                totalRecords,
                (float) lastLapRecords * 1000 / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

        // RESET LAP COUNTERS
        last = now;
        lastLapRecords = 0;
        involvedClusters.clear();
      }

      record = null;
    }

    if (!merge) {
      // remove all records which were absent in new database but
      // exist in old database
      for (final ORID leftOverRid : recordsBeforeImport) {
        database.delete(leftOverRid);
      }
    }
    database.getMetadata().reload();

    final Set<ORID> brokenRids = new HashSet<>();
    processBrokenRids(brokenRids);

    listener.onMessage(
        String.format(
            "\n\nDone. Imported %,d records in %,.2f secs\n",
            totalRecords, ((float) (System.currentTimeMillis() - begin)) / 1000));

    jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);

    return total;
  }

  /*@Deprecated
  private ORID importRecord(final HashSet<ORID> recordsBeforeImport) throws Exception {
    final OPair<String, Map<String, ORidSet>> recordParse =
        jsonReader.readRecordString(this.maxRidbagStringSizeBeforeLazyImport);
    String value = recordParse.getKey().trim();

    if (value.isEmpty()) {
      return null;
    }

    // JUMP EMPTY RECORDS
    while (!value.isEmpty() && value.charAt(0) != '{') {
      value = value.substring(1);
    }

    record = null;

    // big ridbags (ie. supernodes) sometimes send the system OOM, so they have to be discarded at
    // this stage
    // and processed later. The following collects the positions ("value" inside the string) of
    // skipped fields.
    Set<Integer> skippedPartsIndexes = new HashSet<>();

    try {
      try {
        record =
            ORecordSerializerJSON.INSTANCE.fromString(
                value,
                record,
                null,
                null,
                false,
                maxRidbagStringSizeBeforeLazyImport,
                skippedPartsIndexes);
      } catch (OSerializationException e) {
        if (e.getCause() instanceof OSchemaException) {
          // EXTRACT CLASS NAME If ANY
          final int pos = value.indexOf("\"@class\":\"");
          if (pos > -1) {
            final int end = value.indexOf("\"", pos + "\"@class\":\"".length() + 1);
            final String value1 = value.substring(0, pos + "\"@class\":\"".length());
            final String clsName = value.substring(pos + "\"@class\":\"".length(), end);
            final String value2 = value.substring(end);

            final String newClassName = convertedClassNames.get(clsName);

            value = value1 + newClassName + value2;
            // OVERWRITE CLASS NAME WITH NEW NAME
            record =
                ORecordSerializerJSON.INSTANCE.fromString(
                    value,
                    record,
                    null,
                    null,
                    false,
                    maxRidbagStringSizeBeforeLazyImport,
                    skippedPartsIndexes);
          }
        } else
          throw OException.wrapException(
              new ODatabaseImportException("Error on importing record"), e);
      }

      // Incorrect record format , skip this record
      if (record == null || record.getIdentity() == null) {
        OLogManager.instance().warn(this, "Broken record was detected and will be skipped");
        return null;
      }

      if (schemaImported && record.getIdentity().equals(schemaRecordId)) {
        recordsBeforeImport.remove(record.getIdentity());
        // JUMP THE SCHEMA
        return null;
      }

      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(
            database.getClusterNameById(record.getIdentity().getClusterId()))) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(
            database.getClusterNameById(record.getIdentity().getClusterId()))) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record instanceof ODocument && excludeClasses != null) {
        if (excludeClasses.contains(((ODocument) record).getClassName())) {
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record.getIdentity().getClusterId() == 0
          && record.getIdentity().getClusterPosition() == 1) {
        recordsBeforeImport.remove(record.getIdentity());
        // JUMP INTERNAL RECORDS
        return null;
      }

      if (exporterVersion >= 3) {
        int oridsId = database.getClusterIdByName("ORIDs");
        int indexId = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

        if (record.getIdentity().getClusterId() == indexId
            || record.getIdentity().getClusterId() == oridsId) {
          recordsBeforeImport.remove(record.getIdentity());
          // JUMP INDEX RECORDS
          return null;
        }
      }

      final int manualIndexCluster =
          database.getClusterIdByName(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
      final int internalCluster =
          database.getClusterIdByName(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      final int indexCluster = database.getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);

      if (exporterVersion >= 4) {
        if (record.getIdentity().getClusterId() == manualIndexCluster) {
          // JUMP INDEX RECORDS
          recordsBeforeImport.remove(record.getIdentity());
          return null;
        }
      }

      if (record.getIdentity().equals(indexMgrRecordId)) {
        recordsBeforeImport.remove(record.getIdentity());
        return null;
      }

      final ORID rid = record.getIdentity();

      final int clusterId = rid.getClusterId();

      if ((clusterId != manualIndexCluster
          && clusterId != internalCluster
          && clusterId != indexCluster)) {
        if (recordsBeforeImport.remove(rid)) {
          final ORecord loadedRecord = database.load(rid);
          if (loadedRecord == null) {
            throw new IllegalStateException(
                "Record with rid = " + rid + " should exist in database " + database.getName());
          }

          if (!record.getClass().isAssignableFrom(loadedRecord.getClass())) {
            throw new IllegalStateException(
                "Imported record and record stored in database under id "
                    + rid.toString()
                    + " have different types. "
                    + "Stored record class is : "
                    + record.getClass()
                    + " and imported "
                    + loadedRecord.getClass()
                    + " .");
          }

          ORecordInternal.setVersion(record, loadedRecord.getVersion());
        } else {
          ORecordInternal.setVersion(record, 0);
          ORecordInternal.setIdentity(record, new ORecordId());
        }

        record.setDirty();

        if (!preserveRids
            && record instanceof ODocument
            && ODocumentInternal.getImmutableSchemaClass(database, ((ODocument) record)) != null)
          record.save();
        else record.save(database.getClusterNameById(clusterId));

        if (!rid.equals(record.getIdentity())) {
          // SAVE IT ONLY IF DIFFERENT
          new ODocument(EXPORT_IMPORT_CLASS_NAME)
              .field("key", rid.toString())
              .field("value", record.getIdentity().toString())
              .save();
        }
      }

      // import skipped records (too big to be imported before)
      if (skippedPartsIndexes.size() > 0) {
        for (Integer skippedPartsIndex : skippedPartsIndexes) {
          importSkippedRidbag(record, value, skippedPartsIndex);
        }
      }

      if (recordParse.value.size() > 0) {
        importSkippedRidbag(record, recordParse.getValue());
      }

    } catch (Exception t) {
      if (record != null)
        OLogManager.instance()
            .error(
                this,
                "Error importing record "
                    + record.getIdentity()
                    + ". Source line "
                    + jsonReader.getLineNumber()
                    + ", column "
                    + jsonReader.getColumnNumber(),
                t);
      else
        OLogManager.instance()
            .error(
                this,
                "Error importing record. Source line "
                    + jsonReader.getLineNumber()
                    + ", column "
                    + jsonReader.getColumnNumber(),
                t);

      if (!(t instanceof ODatabaseException)) {
        throw t;
      }
    }
    return record.getIdentity();
  }*/

  private void importSkippedRidbag(ORecord record, Map<String, ORidSet> bags) {
    if (bags == null) {
      return;
    }
    OElement doc = (OElement) record;
    bags.forEach(
        (field, ridset) -> {
          ORidBag ridbag = ((OElement) record).getProperty(field);
          ridset.forEach(
              rid -> {
                ridbag.add(rid);
                doc.save();
              });
        });
  }

  private void importSkippedRidbag(ORecord record, String value, Integer skippedPartsIndex) {
    OElement doc = (OElement) record;

    StringBuilder builder = new StringBuilder();

    int nextIndex =
        OStringSerializerHelper.parse(
            value,
            builder,
            skippedPartsIndex,
            -1,
            ORecordSerializerJSON.PARAMETER_SEPARATOR,
            true,
            true,
            false,
            -1,
            false,
            ' ',
            '\n',
            '\r',
            '\t');

    String fieldName = OIOUtils.getStringContent(builder.toString());
    ORidBag bag = doc.getProperty(fieldName);

    if (!(value.charAt(nextIndex) == '[')) {
      throw new ODatabaseImportException("Cannot import field: " + fieldName + " (too big)");
    }

    StringBuilder ridBuffer = new StringBuilder();

    for (int i = nextIndex + 1; i < value.length() + 2; i++) {
      if (value.charAt(i) == ',' || value.charAt(i) == ']') {
        String ridString = OIOUtils.getStringContent(ridBuffer.toString().trim());
        if (ridString.length() > 0) {
          ORecordId rid = new ORecordId(ridString);
          bag.add(rid);
          record.save();
        }
        ridBuffer = new StringBuilder();
        if (value.charAt(i) == ']') {
          break;
        }
      } else {
        ridBuffer.append(value.charAt(i));
      }
    }
  }

  private void importIndexes() throws IOException, ParseException {
    listener.onMessage("\n\nImporting indexes ...");

    OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    indexManager.reload();

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    int n = 0;
    while (jsonReader.lastChar() != ']') {
      jsonReader.readNext(OJSONReader.NEXT_OBJ_IN_ARRAY);
      if (jsonReader.lastChar() == ']') {
        break;
      }

      String blueprintsIndexClass = null;
      String indexName = null;
      String indexType = null;
      String indexAlgorithm = null;
      Set<String> clustersToIndex = new HashSet<>();
      OIndexDefinition indexDefinition = null;
      ODocument metadata = null;
      Map<String, String> engineProperties = null;

      while (jsonReader.lastChar() != '}') {
        final String fieldName = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);
        if (fieldName.equals("name")) indexName = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        else if (fieldName.equals("type"))
          indexType = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        else if (fieldName.equals("algorithm"))
          indexAlgorithm = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
        else if (fieldName.equals("clustersToIndex")) clustersToIndex = importClustersToIndex();
        else if (fieldName.equals("definition")) {
          indexDefinition = importIndexDefinition();
          jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
        } else if (fieldName.equals("metadata")) {
          final String jsonMetadata = jsonReader.readString(OJSONReader.END_OBJECT, true);
          metadata = new ODocument().fromJSON(jsonMetadata);
          jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
        } else if (fieldName.equals("engineProperties")) {
          final String jsonEngineProperties = jsonReader.readString(OJSONReader.END_OBJECT, true);
          Map<String, Object> map = new ODocument().fromJSON(jsonEngineProperties).toMap();
          if (map != null) {
            engineProperties = new HashMap<>(map.size());
            for (Entry<String, Object> entry : map.entrySet()) {
              map.put(entry.getKey(), entry.getValue());
            }
          }
          jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
        } else if (fieldName.equals("blueprintsIndexClass"))
          blueprintsIndexClass = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
      }

      if (indexName == null) throw new IllegalArgumentException("Index name is missing");

      jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);

      // drop automatically created indexes
      if (!indexName.equalsIgnoreCase(EXPORT_IMPORT_INDEX_NAME)) {
        listener.onMessage("\n- Index '" + indexName + "'...");

        indexManager.dropIndex(database, indexName);
        indexesToRebuild.remove(indexName);
        List<Integer> clusterIds = new ArrayList<>();

        for (final String clusterName : clustersToIndex) {
          int id = database.getClusterIdByName(clusterName);
          if (id != -1) clusterIds.add(id);
          else
            listener.onMessage(
                String.format(
                    "found not existent cluster '%s' in index '%s' configuration, skipping",
                    clusterName, indexName));
        }
        int[] clusterIdsToIndex = new int[clusterIds.size()];

        int i = 0;
        for (Integer clusterId : clusterIds) {
          clusterIdsToIndex[i] = clusterId;
          i++;
        }

        if (indexDefinition == null) {
          indexDefinition = new OSimpleKeyIndexDefinition(OType.STRING);
        }

        boolean oldValue =
            OGlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.getValueAsBoolean();
        OGlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.setValue(
            indexDefinition.isNullValuesIgnored());
        final OIndex index =
            indexManager.createIndex(
                database,
                indexName,
                indexType,
                indexDefinition,
                clusterIdsToIndex,
                null,
                metadata,
                indexAlgorithm);
        OGlobalConfiguration.INDEX_IGNORE_NULL_VALUES_DEFAULT.setValue(oldValue);
        if (blueprintsIndexClass != null) {
          ODocument configuration = index.getConfiguration();
          configuration.field("blueprintsIndexClass", blueprintsIndexClass);
          indexManager.save();
        }

        n++;
        listener.onMessage("OK");
      }
    }

    listener.onMessage("\nDone. Created " + n + " indexes.");
    jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);
  }

  private Set<String> importClustersToIndex() throws IOException, ParseException {
    final Set<String> clustersToIndex = new HashSet<>();

    jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

    while (jsonReader.lastChar() != ']') {
      final String clusterToIndex = jsonReader.readString(OJSONReader.NEXT_IN_ARRAY);
      clustersToIndex.add(clusterToIndex);
    }

    jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);
    return clustersToIndex;
  }

  private OIndexDefinition importIndexDefinition() throws IOException, ParseException {
    jsonReader.readString(OJSONReader.BEGIN_OBJECT);
    jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);

    final String className = jsonReader.readString(OJSONReader.NEXT_IN_OBJECT);

    jsonReader.readNext(OJSONReader.FIELD_ASSIGNMENT);

    final String value = jsonReader.readString(OJSONReader.END_OBJECT, true);

    final OIndexDefinition indexDefinition;
    final ODocument indexDefinitionDoc =
        (ODocument) ORecordSerializerJSON.INSTANCE.fromString(value, null, null);
    try {
      final Class<?> indexDefClass = Class.forName(className);
      indexDefinition = (OIndexDefinition) indexDefClass.getDeclaredConstructor().newInstance();
      indexDefinition.fromStream(indexDefinitionDoc);
    } catch (final ClassNotFoundException e) {
      throw new IOException("Error during deserialization of index definition", e);
    } catch (final NoSuchMethodException e) {
      throw new IOException("Error during deserialization of index definition", e);
    } catch (final InvocationTargetException e) {
      throw new IOException("Error during deserialization of index definition", e);
    } catch (final InstantiationException e) {
      throw new IOException("Error during deserialization of index definition", e);
    } catch (final IllegalAccessException e) {
      throw new IOException("Error during deserialization of index definition", e);
    }

    jsonReader.readNext(OJSONReader.NEXT_IN_OBJECT);

    return indexDefinition;
  }

  private void migrateLinksInImportedDocuments(Set<ORID> brokenRids) throws IOException {
    listener.onMessage(
        "\n\nStarted migration of links (-migrateLinks=true). Links are going to be updated according to new RIDs:");

    final long begin = System.currentTimeMillis();
    long last = begin;
    long documentsLastLap = 0;

    long totalDocuments = 0;
    Collection<String> clusterNames = database.getClusterNames();
    for (String clusterName : clusterNames) {
      if (OMetadataDefault.CLUSTER_INDEX_NAME.equals(clusterName)
          || OMetadataDefault.CLUSTER_INTERNAL_NAME.equals(clusterName)
          || OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME.equals(clusterName)) continue;

      long documents = 0;
      String prefix = "";

      listener.onMessage("\n- Cluster " + clusterName + "...");

      final int clusterId = database.getClusterIdByName(clusterName);
      final long clusterRecords = database.countClusterElements(clusterId);
      OStorage storage = database.getStorage();

      OPhysicalPosition[] positions =
          storage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(0));
      while (positions.length > 0) {
        for (OPhysicalPosition position : positions) {
          ORecord record = database.load(new ORecordId(clusterId, position.clusterPosition));
          if (record instanceof ODocument) {
            ODocument document = (ODocument) record;
            rewriteLinksInDocument(database, document, brokenRids);

            documents++;
            documentsLastLap++;
            totalDocuments++;

            final long now = System.currentTimeMillis();
            if (now - last > IMPORT_RECORD_DUMP_LAP_EVERY_MS) {
              listener.onMessage(
                  String.format(
                      "\n--- Migrated %,d of %,d records (%,.2f/sec)",
                      documents,
                      clusterRecords,
                      (float) documentsLastLap * 1000 / (float) IMPORT_RECORD_DUMP_LAP_EVERY_MS));

              // RESET LAP COUNTERS
              last = now;
              documentsLastLap = 0;
              prefix = "\n---";
            }
          }
        }

        positions = storage.higherPhysicalPositions(clusterId, positions[positions.length - 1]);
      }

      listener.onMessage(
          String.format(
              "%s Completed migration of %,d records in current cluster", prefix, documents));
    }

    listener.onMessage(String.format("\nTotal links updated: %,d", totalDocuments));
  }

  protected static void rewriteLinksInDocument(
      ODatabaseSession session, ODocument document, Set<ORID> brokenRids) {
    doRewriteLinksInDocument(session, document, brokenRids);

    document.save();
  }

  protected static void doRewriteLinksInDocument(
      ODatabaseSession session, ODocument document, Set<ORID> brokenRids) {
    final OLinksRewriter rewriter = new OLinksRewriter(new OConverterData(session, brokenRids));
    final ODocumentFieldWalker documentFieldWalker = new ODocumentFieldWalker();
    documentFieldWalker.walkDocument(document, rewriter);
  }

  public int getMaxRidbagStringSizeBeforeLazyImport() {
    return maxRidbagStringSizeBeforeLazyImport;
  }

  public void setMaxRidbagStringSizeBeforeLazyImport(int maxRidbagStringSizeBeforeLazyImport) {
    this.maxRidbagStringSizeBeforeLazyImport = maxRidbagStringSizeBeforeLazyImport;
  }
}
