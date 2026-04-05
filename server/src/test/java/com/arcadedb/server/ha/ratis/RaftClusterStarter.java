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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Starts a 3-node Ratis HA cluster for manual testing of Studio.
 * Run this, then open http://localhost:2480 in your browser and click the Cluster tab.
 *
 * Ctrl+C to stop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RaftClusterStarter {

  private static final int SERVER_COUNT = 3;
  private static final int BASE_HA_PORT = 2424;
  private static final int BASE_HTTP_PORT = 2480;
  private static final String DB_NAME = "demodb";

  public static void main(final String[] args) throws Exception {
    System.out.println("=== ArcadeDB Ratis HA Cluster Starter ===");
    System.out.println("Servers: " + SERVER_COUNT);
    System.out.println("HA ports: " + BASE_HA_PORT + "-" + (BASE_HA_PORT + SERVER_COUNT - 1));
    System.out.println("HTTP ports: " + BASE_HTTP_PORT + "-" + (BASE_HTTP_PORT + SERVER_COUNT - 1));
    System.out.println();

    // Clean up from previous runs
    for (int i = 0; i < SERVER_COUNT; i++)
      FileUtils.deleteRecursively(new File("./target/cluster-db" + i));
    FileUtils.deleteRecursively(new File("./target/ratis-storage"));
    new File("./target/config/server-users.jsonl").delete();

    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("arcadedb");

    // Create a demo database on server 0, copy to others
    final String dbPath0 = "./target/cluster-db0/" + DB_NAME;
    try (final Database db = new DatabaseFactory(dbPath0).create()) {
      db.transaction(() -> {
        final var personType = db.getSchema().buildVertexType().withName("Person").withTotalBuckets(3).create();
        personType.createProperty("name", String.class);
        personType.createProperty("age", Integer.class);

        db.getSchema().createEdgeType("Friend");

        // Add some sample data
        db.newVertex("Person").set("name", "Alice").set("age", 30).save();
        db.newVertex("Person").set("name", "Bob").set("age", 25).save();
        db.newVertex("Person").set("name", "Carol").set("age", 35).save();
      });
      System.out.println("Demo database '" + DB_NAME + "' created with 3 vertices.");
    }

    for (int i = 1; i < SERVER_COUNT; i++) {
      try {
        FileUtils.copyDirectory(new File(dbPath0), new File("./target/cluster-db" + i + "/" + DB_NAME));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    // Build server list
    final StringBuilder serverList = new StringBuilder();
    for (int i = 0; i < SERVER_COUNT; i++) {
      if (i > 0) serverList.append(",");
      serverList.append("localhost:").append(BASE_HA_PORT + i);
    }

    // Start all servers
    final ArcadeDBServer[] servers = new ArcadeDBServer[SERVER_COUNT];
    for (int i = 0; i < SERVER_COUNT; i++) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/cluster-db" + i);
      config.setValue(GlobalConfiguration.HA_ENABLED, true);
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList.toString());
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS, String.valueOf(BASE_HA_PORT + i));
      config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "demo-cluster");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(BASE_HTTP_PORT + i));
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");

      servers[i] = new ArcadeDBServer(config);
      servers[i].start();
      System.out.println("Server " + i + " started (HTTP: " + (BASE_HTTP_PORT + i) + ", HA: " + (BASE_HA_PORT + i) + ")");
    }

    // Wait for leader election
    System.out.println("\nWaiting for Ratis leader election...");
    boolean leaderFound = false;
    for (int attempt = 0; attempt < 30 && !leaderFound; attempt++) {
      for (final ArcadeDBServer s : servers)
        if (s.getHA() != null && s.getHA().isLeader()) {
          System.out.println("Leader elected: " + s.getServerName());
          leaderFound = true;
          break;
        }
      if (!leaderFound)
        Thread.sleep(500);
    }
    if (!leaderFound)
      System.out.println("WARNING: No leader elected after 15 seconds");

    System.out.println("\n==========================================");
    System.out.println("  Cluster is ready!");
    System.out.println("  Studio:   http://localhost:2480");
    System.out.println("  User:     root");
    System.out.println("  Password: arcadedb");
    System.out.println("  Database: " + DB_NAME);
    System.out.println("==========================================");
    System.out.println("Press Ctrl+C to stop.\n");

    // Keep running until Ctrl+C
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("\nShutting down cluster...");
      for (int i = servers.length - 1; i >= 0; i--)
        if (servers[i] != null)
          try { servers[i].stop(); } catch (final Exception e) { /* ignore */ }
      System.out.println("Done.");
    }));

    Thread.currentThread().join();
  }
}
