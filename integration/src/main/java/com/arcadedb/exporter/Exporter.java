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

package com.arcadedb.exporter;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.importer.AnalyzedSchema;
import com.arcadedb.importer.ConsoleLogger;
import com.arcadedb.log.LogManager;

import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public class Exporter {
  protected ExporterSettings settings = new ExporterSettings();
  protected ExporterContext  context  = new ExporterContext();
  protected DatabaseInternal database;
  protected Timer            timer;
  protected ConsoleLogger    logger;

  public Exporter(final String[] args) {
    settings.parseParameters(args);
  }

  public Exporter(final DatabaseInternal database, final String file) {
    this.database = database;
    settings.file = file;
  }

  public static void main(final String[] args) {
    new Exporter(args).write();
    System.exit(0);
  }

  public void write() {
    try {
      final String cfgValue = settings.options.get("maxValueSampling");

      final AnalyzedSchema analyzedSchema = new AnalyzedSchema(cfgValue != null ? Integer.parseInt(cfgValue) : 100);

      openDatabase();

      startExporting();

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on writing to %s", e, settings.file);
    } finally {
      if (database != null) {
        stopExporting();
        closeDatabase();
      }
      closeOutputFile();
    }
  }

  protected void startExporting() {
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
    printProgress();
  }

  protected void closeOutputFile() {
  }

  protected void openDatabase() {
    if (database != null && database.isOpen())
      return;

    final DatabaseFactory factory = new DatabaseFactory(settings.databaseURL);

    if (!factory.exists()) {
      LogManager.instance().log(this, Level.SEVERE, "Database '%s' not found", null, settings.databaseURL);
      return;
    }

    LogManager.instance().log(this, Level.INFO, "Opening database '%s'...", null, settings.databaseURL);
    database = (DatabaseInternal) factory.open();
    database.begin();
  }

  protected void printProgress() {
    if (settings.verboseLevel < 2)
      return;

    try {
      long deltaInSecs = (System.currentTimeMillis() - context.lastLapOn) / 1000;
      if (deltaInSecs == 0)
        deltaInSecs = 1;

      if (logger == null)
        logger = new ConsoleLogger(settings.verboseLevel);

      logger.log(2,//
          "- Status update: %d documents (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec)",//
          context.documents.get(), (context.documents.get() - context.lastDocuments) / deltaInSecs,//
          context.vertices.get(), (context.vertices.get() - context.lastVertices) / deltaInSecs,//
          context.edges.get(), (context.edges.get() - context.lastEdges) / deltaInSecs);

      context.lastLapOn = System.currentTimeMillis();

      context.lastDocuments = context.documents.get();
      context.lastVertices = context.vertices.get();
      context.lastEdges = context.edges.get();

    } catch (Exception e) {
      logger.error("Error on print statistics: " + e.getMessage());
    }
  }

  protected void closeDatabase() {
    if (database != null) {
      if (database.isTransactionActive())
        database.commit();
      database.close();
    }
  }
}
