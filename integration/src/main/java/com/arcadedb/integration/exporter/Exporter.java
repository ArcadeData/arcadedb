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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.exporter.format.AbstractExporterFormat;
import com.arcadedb.integration.exporter.format.JsonlExporterFormat;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.log.LogManager;

import java.lang.reflect.InvocationTargetException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public class Exporter {
  protected ExporterSettings       settings = new ExporterSettings();
  protected ExporterContext        context  = new ExporterContext();
  protected DatabaseInternal       database;
  protected Timer                  timer;
  protected ConsoleLogger          logger;
  protected AbstractExporterFormat formatImplementation;

  public Exporter(final String[] args) {
    settings.parseParameters(args);
  }

  public Exporter(final Database database, final String file) {
    this.database = (DatabaseInternal) database;
    settings.file = file;
  }

  public static void main(final String[] args) {
    new Exporter(args).exportDatabase();
    System.exit(0);
  }

  public Exporter setFormat(final String format) {
    settings.format = format;
    return this;
  }

  public Exporter setOverwrite(final boolean overwrite) {
    settings.overwriteFile = overwrite;
    return this;
  }

  public void exportDatabase() {
    try {
      startExporting();

      openDatabase();

      formatImplementation = createFormatImplementation();
      formatImplementation.exportDatabase();

      long elapsedInSecs = (System.currentTimeMillis() - context.startedOn) / 1000;
      if (elapsedInSecs == 0)
        elapsedInSecs = 1;

      final long totalRecords = context.vertices.get() + context.edges.get() + context.documents.get();

      logger.logLine(0,//
          "Database exported successfully: %,d records exported in %s secs (%,d records/secs %,d documents %,d vertices %,d edges)",//
          totalRecords, elapsedInSecs, (totalRecords / elapsedInSecs), context.documents.get(), context.vertices.get(), context.edges.get());

    } catch (Exception e) {
      throw new ExportException("Error on writing to '" + settings.file + "'", e);
    } finally {
      if (database != null) {
        stopExporting();
        closeDatabase();
      }
    }
  }

  protected void startExporting() {
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

  protected void stopExporting() {
    if (timer != null)
      timer.cancel();
  }

  protected void openDatabase() {
    if (database != null && database.isOpen())
      return;

    final DatabaseFactory factory = new DatabaseFactory(settings.databaseURL);

    if (!factory.exists()) {
      LogManager.instance().log(this, Level.SEVERE, "Database '%s' not found", null, settings.databaseURL);
      return;
    }

    logger.logLine(0, "Opening database '%s'...", settings.databaseURL);
    database = (DatabaseInternal) factory.open();
  }

  protected void printProgress() {
    if (settings.verboseLevel < 2)
      return;

    try {
      long deltaInSecs = (System.currentTimeMillis() - context.lastLapOn) / 1000;
      if (deltaInSecs == 0)
        deltaInSecs = 1;

      logger.logLine(2,//
          "- Status update: %,d documents (%,d/sec) - %,d vertices (%,d/sec) - %,d edges (%,d/sec)",//
          context.documents.get(), (context.documents.get() - context.lastDocuments) / deltaInSecs,//
          context.vertices.get(), (context.vertices.get() - context.lastVertices) / deltaInSecs,//
          context.edges.get(), (context.edges.get() - context.lastEdges) / deltaInSecs);

      context.lastLapOn = System.currentTimeMillis();

      context.lastDocuments = context.documents.get();
      context.lastVertices = context.vertices.get();
      context.lastEdges = context.edges.get();

    } catch (Exception e) {
      logger.errorLine("Error on print statistics: " + e.getMessage());
    }
  }

  protected void closeDatabase() {
    if (database != null) {
      if (database.isTransactionActive())
        database.commit();
      database.close();
    }
  }

  protected AbstractExporterFormat createFormatImplementation() {
    switch (settings.format.toLowerCase()) {
    case JsonlExporterFormat.NAME:
      return new JsonlExporterFormat(database, settings, context, logger);

    case "graphml": {
      try {
        final Class<AbstractExporterFormat> clazz = (Class<AbstractExporterFormat>) Class.forName("com.arcadedb.gremlin.integration.exporter.format.GraphMLExporterFormat");
        return clazz.getConstructor( DatabaseInternal.class,  ExporterSettings.class, ExporterContext.class, ConsoleLogger.class)
                .newInstance(database, settings, context, logger);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
        LogManager.instance().log(this, Level.SEVERE, "Impossible to find exporter for 'graphml' ", e);

      }

    }
    case "graphson": {
      try {
        final Class<AbstractExporterFormat> clazz = (Class<AbstractExporterFormat>) Class.forName("com.arcadedb.gremlin.integration.exporter.format.GraphSONExporterFormat");
        return clazz.getConstructor( DatabaseInternal.class,  ExporterSettings.class, ExporterContext.class, ConsoleLogger.class)
                .newInstance(database, settings, context, logger);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
        LogManager.instance().log(this, Level.SEVERE, "Impossible to find exporter for 'graphson' ", e);
      }

    }
    default:
      throw new ExportException("Format '" + settings.format + "' not supported");
    }
  }
}
