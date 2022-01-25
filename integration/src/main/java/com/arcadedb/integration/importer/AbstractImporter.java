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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.*;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public abstract class AbstractImporter {
  protected Parser           parser;
  protected ImporterSettings settings                       = new ImporterSettings();
  protected ImporterContext  context                        = new ImporterContext();
  protected DatabaseInternal database;
  protected Source           source;
  protected Timer            timer;
  protected boolean          databaseCreatedDuringImporting = true;
  protected ConsoleLogger    logger;

  public AbstractImporter(final String[] args) {
    settings.parseParameters(args);
  }

  public AbstractImporter(final DatabaseInternal database) {
    this.database = database;
    this.databaseCreatedDuringImporting = false;
  }

  protected void printProgress() {
    if (settings.verboseLevel < 2)
      return;

    try {
      long deltaInSecs = (System.currentTimeMillis() - context.lastLapOn) / 1000;
      if (deltaInSecs == 0)
        deltaInSecs = 1;

      if (source == null || source.compressed || source.totalSize < 0) {
        logger.logLine(2,//
            "- Status update: parsed %,d (%,d/sec) - %,d documents (%,d/sec) - %,d vertices (%,d/sec) - %,d edges (%,d/sec) - %,d skipped edges - %,d linked edges (%,d/sec - %,d%%)",
//
            context.parsed.get(), ((context.parsed.get() - context.lastParsed) / deltaInSecs), context.createdDocuments.get(),
            (context.createdDocuments.get() - context.lastDocuments) / deltaInSecs, context.createdVertices.get(),
            (context.createdVertices.get() - context.lastVertices) / deltaInSecs, context.createdEdges.get(),
            (context.createdEdges.get() - context.lastEdges) / deltaInSecs, context.skippedEdges.get(), context.linkedEdges.get(),
            (context.linkedEdges.get() - context.lastLinkedEdges) / deltaInSecs,
            context.createdEdges.get() > 0 ? (int) (context.linkedEdges.get() * 100 / context.createdEdges.get()) : 0);
      } else {
        final int progressPerc = (int) (parser.getPosition() * 100 / source.totalSize);
        logger.logLine(2,//
            "Status update: parsed %,d (%,d/sec - %,d%%) - %,d records (%,d/sec) - %,d vertices (%,d/sec) - %,d edges (%,d/sec) - %,d skipped edges - %,d linked edges (%,d/sec - %,d%%)",
            context.parsed.get(), ((context.parsed.get() - context.lastParsed) / deltaInSecs), progressPerc, context.createdDocuments.get(),
            (context.createdDocuments.get() - context.lastDocuments) / deltaInSecs, context.createdVertices.get(),
            (context.createdVertices.get() - context.lastVertices) / deltaInSecs, context.createdEdges.get(),
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
      logger.errorLine("Error on print statistics: " + e.getMessage());
    }
  }

  protected void startImporting() {
    if (logger == null)
      logger = new ConsoleLogger(settings.verboseLevel);

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
    if (!databaseCreatedDuringImporting)
      return;

    if (database != null) {
      if (database.isTransactionActive())
        database.commit();
      database.close();
    }
  }

  protected void openDatabase() {
    if (database != null && database.isOpen())
      return;

    final DatabaseFactory factory = new DatabaseFactory(settings.database);

    if (settings.forceDatabaseCreate) {
      if (factory.exists())
        FileUtils.deleteRecursively(new File(settings.database));
    }

    if (factory.exists()) {
      LogManager.instance().log(this, Level.INFO, "Opening database '%s'...", settings.database);
      database = (DatabaseInternal) factory.open();
    } else {
      LogManager.instance().log(this, Level.INFO, "Creating database '%s'...", settings.database);
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
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type DOCUMENT", name);

      beginTxIfNeeded();
      final DocumentType type = database.getSchema().createDocumentType(name, settings.parallel);
      if (settings.typeIdProperty != null) {
        type.createProperty(settings.typeIdProperty, Type.getTypeByName(settings.typeIdType));
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, settings.typeIdPropertyIsUnique, name, settings.typeIdProperty);
        LogManager.instance().log(this, Level.INFO, "- Creating indexed property '%s' of type '%s'", settings.typeIdProperty, settings.typeIdType);
      }
      return type;
    }
    return database.getSchema().getType(name);
  }

  protected VertexType getOrCreateVertexType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type VERTEX", name);

      beginTxIfNeeded();
      final VertexType type = database.getSchema().createVertexType(name, settings.parallel);
      if (settings.typeIdProperty != null) {
        type.createProperty(settings.typeIdProperty, Type.getTypeByName(settings.typeIdType));
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, settings.typeIdPropertyIsUnique, name, settings.typeIdProperty);
        LogManager.instance().log(this, Level.INFO, "- Creating indexed property '%s' of type '%s'", settings.typeIdProperty, settings.typeIdType);
      }
      return type;
    }

    return (VertexType) database.getSchema().getType(name);
  }

  protected EdgeType getOrCreateEdgeType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type EDGE", name);

      beginTxIfNeeded();
      return database.getSchema().createEdgeType(name, settings.parallel);
    }

    return (EdgeType) database.getSchema().getType(name);
  }

  protected void updateDatabaseSchema(final AnalyzedSchema schema) {
    if (schema == null)
      return;

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
                .log(this, Level.WARNING, "- found schema property %s.%s of type %s, while analyzing the source type %s was found", entity, propName,
                    property.getType(), propValue.getType());
          }
        } else {
          // CREATE IT
          LogManager.instance().log(this, Level.INFO, "- creating property %s.%s of type %s", entity, propName, propValue.getType());
          type.createProperty(propName, propValue.getType());
        }

        for (String sample : propValue.getContents()) {
          dictionary.getIdByName(sample, true);
        }
      }
    }

    database.getSchema().getEmbedded().saveConfiguration();
  }

  protected void dumpSchema(final AnalyzedSchema schema, final long parsedObjects) {
    LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
    LogManager.instance().log(this, Level.INFO, "Objects found %d", parsedObjects);
    for (AnalyzedEntity entity : schema.getEntities()) {
      LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
      LogManager.instance().log(this, Level.INFO, "Entity '%s':", entity);

      for (AnalyzedProperty p : entity.getProperties()) {
        LogManager.instance().log(this, Level.INFO, "- %s (%s)", p.getName(), p.getType());
        if (p.isCollectingSamples())
          LogManager.instance().log(this, Level.INFO, "    contents (%d items): %s", p.getContents().size(), p.getContents());
      }
    }
    LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
  }
}
