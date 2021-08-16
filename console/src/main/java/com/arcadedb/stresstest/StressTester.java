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
package com.arcadedb.stresstest;

import com.arcadedb.Constants;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.stresstest.workload.CheckWorkload;
import com.arcadedb.stresstest.workload.OWorkload;
import com.arcadedb.stresstest.workload.OWorkloadFactory;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The main class of the OStressTester. It is instantiated from the OStressTesterCommandLineParser and takes care of launching the
 * needed threads (OOperationsExecutor) for executing the operations of the test.
 *
 * @author
 */
public class StressTester {
  /**
   * The access mode to the database
   */
  public enum OMode {
    EMBEDDED, REMOTE, HA
  }

  private final DatabaseIdentifier    databaseIdentifier;
  private       ConsoleProgressWriter consoleProgressWriter;
  private final StressTesterSettings  settings;

  private static final OWorkloadFactory workloadFactory = new OWorkloadFactory();
  private              List<OWorkload>  workloads       = new ArrayList<OWorkload>();

  public StressTester(final List<OWorkload> workloads, DatabaseIdentifier databaseIdentifier, final StressTesterSettings settings) {
    this.workloads = workloads;
    this.databaseIdentifier = databaseIdentifier;
    this.settings = settings;
  }

  public static void main(String[] args) {
    System.out.println(String.format("%s Stress Tool v.%s - %s", Constants.PRODUCT, Constants.getRawVersion(), Constants.COPYRIGHT));

    int returnValue = 1;
    try {
      final StressTester stressTester = StressTesterCommandLineParser.getStressTester(args);
      returnValue = stressTester.execute();
    } catch (Exception ex) {
      System.err.println(ex.getMessage());
    }
    System.exit(returnValue);
  }

  @SuppressWarnings("unchecked")
  public int execute() {
    int returnCode = 0;

    // creates the temporary DB where to execute the test
    final DatabaseFactory dbFactory = new DatabaseFactory(databaseIdentifier.getUrl());
    if (dbFactory.exists())
      throw new RuntimeException("Database " + databaseIdentifier.getUrl() + " already exists.");

    databaseIdentifier.setEmbeddedDatabase((EmbeddedDatabase) dbFactory.create());

    System.out.println(String.format("Created database [%s].", databaseIdentifier.getUrl()));

    try {
      for (OWorkload workload : workloads) {
        consoleProgressWriter = new ConsoleProgressWriter(workload);

        consoleProgressWriter.start();

        consoleProgressWriter.printMessage(String.format("\nStarting workload %s (concurrencyLevel=%d)...", workload.getName(), settings.concurrencyLevel));

        final long startTime = System.currentTimeMillis();

        workload.execute(settings, databaseIdentifier);

        final long endTime = System.currentTimeMillis();

        consoleProgressWriter.sendShutdown();

        System.out.println(String.format("\n- Total execution time: %.3f secs", ((float) (endTime - startTime) / 1000f)));

        System.out.println(workload.getFinalResult());

        dumpHaMetrics();

        if (settings.checkDatabase && workload instanceof CheckWorkload) {
          System.out.println(String.format("- Checking database..."));
          ((CheckWorkload) workload).check(databaseIdentifier);
          System.out.println(String.format("- Check completed"));
        }
      }

      if (settings.resultOutputFile != null)
        writeFile();

    } catch (Exception ex) {
      System.err.println("\nAn error has occurred while running the stress test: " + ex.getMessage());
      returnCode = 1;
    } finally {
      databaseIdentifier.drop();
      consoleProgressWriter.printMessage(String.format("\nDropped database [%s].", databaseIdentifier.getUrl()));

    }

    return returnCode;
  }

  private void dumpHaMetrics() {
  }

  private void writeFile() {
    try {
      final StringBuilder output = new StringBuilder();
      output.append("{\"result\":[");
      int i = 0;
      for (OWorkload workload : workloads) {
        if (i++ > 0)
          output.append(",");
        output.append(workload.getFinalResultAsJson());
      }
      output.append("]}");

      FileUtils.writeFile(new File(settings.resultOutputFile), output.toString());
    } catch (IOException e) {
      System.err.println("\nError on writing the result file : " + e.getMessage());
    }
  }

  public int getThreadsNumber() {
    return settings.concurrencyLevel;
  }

  public OMode getMode() {
    return databaseIdentifier.getMode();
  }

  public DatabaseIdentifier getDatabaseIdentifier() {
    return databaseIdentifier;
  }

  public String getPassword() {
    return databaseIdentifier.getPassword();
  }

  public int getTransactionsNumber() {
    return settings.operationsPerTransaction;
  }

  public static OWorkloadFactory getWorkloadFactory() {
    return workloadFactory;
  }
}
