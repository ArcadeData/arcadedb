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

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.*;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public class Importer {
  protected DatabaseInternal database;
  protected Source           source;
  protected Timer            timer;

  // SETTINGS
  protected Parser           parser;
  protected ImporterSettings settings = new ImporterSettings();
  protected ImporterContext  context  = new ImporterContext();

  public Importer(final String[] args) {
    settings.parseParameters(args);
  }

  public static void main(final String[] args) {
    new Importer(args).load();
    System.exit(0);
  }

  public void load() {
    Source source = null;

    try {
      final String cfgValue = settings.options.get("maxValueSampling");

      final AnalyzedSchema analyzedSchema = new AnalyzedSchema(cfgValue != null ? Integer.parseInt(cfgValue) : 100);

      openDatabase();

      startImporting();

      loadFromSource(settings.documents, AnalyzedEntity.ENTITY_TYPE.DOCUMENT, analyzedSchema);
      loadFromSource(settings.vertices, AnalyzedEntity.ENTITY_TYPE.VERTEX, analyzedSchema);
      loadFromSource(settings.edges, AnalyzedEntity.ENTITY_TYPE.EDGE, analyzedSchema);

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on parsing source %s", e, source);
    } finally {
      if (database != null) {
        database.async().waitCompletion();
        stopImporting();
        closeDatabase();
      }
      closeInputFile();
    }
  }

  protected void loadFromSource(final String url, final AnalyzedEntity.ENTITY_TYPE entityType, final AnalyzedSchema analyzedSchema)
      throws IOException {
    if (url == null)
      // SKIP IT
      return;

    final SourceDiscovery sourceDiscovery = new SourceDiscovery(url);
    final SourceSchema sourceSchema = sourceDiscovery.getSchema(settings, entityType, analyzedSchema);
    if (sourceSchema == null) {
      LogManager.instance().log(this, Level.WARNING, "XML importing aborted because unable to determine the schema");
      return;
    }

    updateDatabaseSchema(sourceSchema.getSchema());

    source = sourceDiscovery.getSource();
    parser = new Parser(source, 0);
    parser.reset();

    sourceSchema.getContentImporter().load(sourceSchema, entityType, parser, database, context, settings);
  }

  protected void printProgress() {
    try {
      long deltaInSecs = (System.currentTimeMillis() - context.lastLapOn) / 1000;
      if (deltaInSecs == 0)
        deltaInSecs = 1;

      if (source == null || source.compressed || source.totalSize < 0) {
        LogManager.instance().log(this, Level.INFO,
            "Parsed %d (%d/sec) - %d documents (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec) - %d skipped edges - %d linked edges (%d/sec - %d%%)",
            null, context.parsed.get(), ((context.parsed.get() - context.lastParsed) / deltaInSecs), context.createdDocuments.get(),
            (context.createdDocuments.get() - context.lastDocuments) / deltaInSecs, context.createdVertices.get(),
            (context.createdVertices.get() - context.lastVertices) / deltaInSecs, context.createdEdges.get(),
            (context.createdEdges.get() - context.lastEdges) / deltaInSecs, context.skippedEdges.get(), context.linkedEdges.get(),
            (context.linkedEdges.get() - context.lastLinkedEdges) / deltaInSecs,
            context.createdEdges.get() > 0 ? (int) (context.linkedEdges.get() * 100 / context.createdEdges.get()) : 0);
      } else {
        final int progressPerc = (int) (parser.getPosition() * 100 / source.totalSize);
        LogManager.instance().log(this, Level.INFO,
            "Parsed %d (%d/sec - %d%%) - %d records (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec) - %d skipped edges - %d linked edges (%d/sec - %d%%)",
            null, context.parsed.get(), ((context.parsed.get() - context.lastParsed) / deltaInSecs), progressPerc,
            context.createdDocuments.get(), (context.createdDocuments.get() - context.lastDocuments) / deltaInSecs,
            context.createdVertices.get(), (context.createdVertices.get() - context.lastVertices) / deltaInSecs, context.createdEdges.get(),
            (context.createdEdges.get() - context.lastEdges) / deltaInSecs, context.skippedEdges.get(), context.linkedEdges.get(),
            (context.linkedEdges.get() - context.lastLinkedEdges) / deltaInSecs,
            context.createdEdges.get() > 0 ? (int) (context.linkedEdges.get() * 100 / context.createdEdges.get()) : 0);
      }
      context.lastLapOn = System.currentTimeMillis();
      context.lastParsed = context.parsed.get();

      context.lastDocuments = context.createdDocuments.get();
      context.lastVertices = context.createdVertices.get();
      context.lastEdges = context.createdEdges.get();
      context.lastLinkedEdges = context.linkedEdges.get();

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on print statistics", e);
    }
  }

  protected void startImporting() {
    context.startedOn = context.lastLapOn = System.currentTimeMillis();

    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        printProgress();
      }
    }, 5000, 5000);
  }

  protected void stopImporting() {
    if (timer != null)
      timer.cancel();
    printProgress();
  }

  protected void closeInputFile() {
    if (source != null)
      source.close();
  }

  protected void closeDatabase() {
    if (database != null) {
      if (database.isTransactionActive())
        database.commit();
      database.close();
    }
  }

  protected void openDatabase() {
    if (database != null && database.isOpen())
      throw new IllegalStateException("Database already open");

    final DatabaseFactory factory = new DatabaseFactory(settings.database);

    if (settings.forceDatabaseCreate) {
      if (factory.exists())
        factory.open().drop();
    }

    if (factory.exists()) {
      LogManager.instance().log(this, Level.INFO, "Opening database '%s'...", null, settings.database);
      database = (DatabaseInternal) factory.open();
    } else {
      LogManager.instance().log(this, Level.INFO, "Creating database '%s'...", null, settings.database);
      database = (DatabaseInternal) factory.create();
    }

    database.begin();

    if (settings.documents != null)
      settings.documentTypeName = getOrCreateDocumentType(settings.documentTypeName).getName();
    if (settings.vertices != null)
      settings.vertexTypeName = getOrCreateVertexType(settings.vertexTypeName).getName();
    if (settings.edges != null)
      settings.edgeTypeName = getOrCreateEdgeType(settings.edgeTypeName).getName();

    database.commit();

    database.setReadYourWrites(false); // THIS IS FUNDAMENTAL TO SPEEDUP EDGE-CONTAINER LOOKUPS
    database.async().setParallelLevel(settings.parallel);
    database.async().setCommitEvery(settings.commitEvery);
    database.async().setTransactionUseWAL(settings.wal);
    database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);

    database.begin();
  }

  protected void beginTxIfNeeded() {
    if (!database.isTransactionActive())
      database.begin();
  }

  protected DocumentType getOrCreateDocumentType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type DOCUMENT", null, name);

      beginTxIfNeeded();
      final DocumentType type = database.getSchema().createDocumentType(name, settings.parallel);
      if (settings.typeIdProperty != null) {
        type.createProperty(settings.typeIdProperty, Type.getTypeByName(settings.typeIdType));
//        database.getSchema()
//            .createIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, settings.typeIdPropertyIsUnique, name, new String[] { settings.typeIdProperty });
        LogManager.instance()
            .log(this, Level.INFO, "- Creating indexed property '%s' of type '%s'", null, settings.typeIdProperty, settings.typeIdType);
      }
      return type;
    }
    return database.getSchema().getType(name);
  }

  protected VertexType getOrCreateVertexType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type VERTEX", null, name);

      beginTxIfNeeded();
      final VertexType type = database.getSchema().createVertexType(name, settings.parallel);
      if (settings.typeIdProperty != null) {
        type.createProperty(settings.typeIdProperty, Type.getTypeByName(settings.typeIdType));
//        database.getSchema()
//            .createIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, settings.typeIdPropertyIsUnique, name, new String[] { settings.typeIdProperty });
        LogManager.instance()
            .log(this, Level.INFO, "- Creating indexed property '%s' of type '%s'", null, settings.typeIdProperty, settings.typeIdType);
      }
      return type;
    }

    return (VertexType) database.getSchema().getType(name);
  }

  protected EdgeType getOrCreateEdgeType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type EDGE", null, name);

      beginTxIfNeeded();
      return database.getSchema().createEdgeType(name, settings.parallel);
    }

    return (EdgeType) database.getSchema().getType(name);
  }

  protected void updateDatabaseSchema(final AnalyzedSchema schema) {
    if (schema == null)
      return;

    if (!database.isTransactionActive())
      database.begin();

    final Dictionary dictionary = database.getSchema().getDictionary();

    LogManager.instance().log(this, Level.INFO, "Checking schema...");

    for (AnalyzedEntity entity : schema.getEntities()) {
      final DocumentType type;
      switch (entity.getType()) {
      case VERTEX:
        type = getOrCreateVertexType(entity.getName());
        break;
      case EDGE:
        type = getOrCreateEdgeType(entity.getName());
        break;
      case DOCUMENT:
        type = getOrCreateDocumentType(entity.getName());
        break;
      default:
        throw new IllegalArgumentException("Record type '" + entity.getType() + "' is not supported");
      }

      for (AnalyzedProperty propValue : entity.getProperties()) {
        final String propName = propValue.getName();

        if (type.existsProperty(propName)) {
          // CHECK TYPE
          final Property property = type.getPolymorphicProperty(propName);
          if (property.getType() != propValue.getType()) {
            LogManager.instance()
                .log(this, Level.WARNING, "- found schema property %s.%s of type %s, while analyzing the source type %s was found", null,
                    entity, propName, property.getType(), propValue.getType());
          }
        } else {
          // CREATE IT
          LogManager.instance().log(this, Level.INFO, "- creating property %s.%s of type %s", null, entity, propName, propValue.getType());
          type.createProperty(propName, propValue.getType());
        }

        for (String sample : propValue.getContents()) {
          dictionary.getIdByName(sample, true);
        }
      }
    }

    ((SchemaImpl) database.getSchema()).saveConfiguration();

    database.commit();
    database.begin();
  }

  protected void dumpSchema(final AnalyzedSchema schema, final long parsedObjects) {
    LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
    LogManager.instance().log(this, Level.INFO, "Objects found %d", null, parsedObjects);
    for (AnalyzedEntity entity : schema.getEntities()) {
      LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
      LogManager.instance().log(this, Level.INFO, "Entity '%s':", null, entity);

      for (AnalyzedProperty p : entity.getProperties()) {
        LogManager.instance().log(this, Level.INFO, "- %s (%s)", null, p.getName(), p.getType());
        if (p.isCollectingSamples())
          LogManager.instance().log(this, Level.INFO, "    contents (%d items): %s", null, p.getContents().size(), p.getContents());
      }
    }
    LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
  }
}
