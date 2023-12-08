/*
 * Copyright 2023 Arcade Data Ltd
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
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.remote.RemoteDatabase;
import org.apache.commons.configuration2.Configuration;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * ArcadeDB Gremlin implementation factory class. Utilizes a pool of ArcadeGraph to avoid creating a new instance every time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class ArcadeGraphFactory implements Closeable {
  private final ConcurrentLinkedQueue<PooledArcadeGraph> pooledInstances       = new ConcurrentLinkedQueue();
  private final Database                                 localDatabase;
  private final String                                   host;
  private final int                                      port;
  private final String                                   databaseName;
  private final String                                   userName;
  private final String                                   userPassword;
  private       int                                      maxInstances          = 32;
  private final AtomicInteger                            totalInstancesCreated = new AtomicInteger(0);

  public class PooledArcadeGraph extends ArcadeGraph {
    private final ArcadeGraphFactory factory;

    protected PooledArcadeGraph(final ArcadeGraphFactory factory, final Configuration configuration) {
      super(configuration);
      this.factory = factory;
    }

    protected PooledArcadeGraph(final ArcadeGraphFactory factory, final BasicDatabase database) {
      super(database);
      this.factory = factory;
    }

    @Override
    public void close() {
      factory.release(this);
    }

    public void dispose() {
      super.close();
    }
  }

  /**
   * Creates a new ArcadeGraphFactory with remote database connection. By default maximum 32 instances of ArcadeGraph
   * can be created. You can change this configuration with the method #setMaxInstances().
   *
   * @param host         ArcadeDB remote server ip address or host name
   * @param port         ArcadeDB remote server TCP/IP port
   * @param databaseName Database name
   * @param userName     User name
   * @param userPassword User password
   */
  private ArcadeGraphFactory(final String host, final int port, final String databaseName, final String userName,
      final String userPassword) {
    this.host = host;
    this.port = port;
    this.databaseName = databaseName;
    this.userName = userName;
    this.userPassword = userPassword;
    this.localDatabase = null;
  }

  /**
   * Creates a new ArcadeGraphFactory with local database connection. By default maximum 32 instances of ArcadeGraph
   * can be created. You can change this configuration with the method #setMaxInstances().
   *
   * @param databasePath ArcadeDB local database path
   */
  private ArcadeGraphFactory(final String databasePath) {
    this.localDatabase = new DatabaseFactory(databasePath).open();
    this.host = null;
    this.port = 0;
    this.databaseName = null;
    this.userName = null;
    this.userPassword = null;
  }

  public static ArcadeGraphFactory withRemote(final String host, final int port, final String databaseName, final String userName,
      final String userPassword) {
    return new ArcadeGraphFactory(host, port, databaseName, userName, userPassword);
  }

  public static ArcadeGraphFactory withLocal(final String databasePath) {
    return new ArcadeGraphFactory(databasePath);
  }

  /**
   * Closes the factory and dispose all the remaining ArcadeGraph instances in the pool.
   */
  @Override
  public void close() {
    while (!pooledInstances.isEmpty()) {
      final PooledArcadeGraph instance = pooledInstances.poll();
      if (instance != null)
        instance.dispose();
    }

    if (localDatabase != null)
      localDatabase.close();
  }

  public ArcadeGraph get() {
    PooledArcadeGraph instance = pooledInstances.poll();
    if (instance == null) {
      if (totalInstancesCreated.get() >= maxInstances)
        throw new IllegalArgumentException("Unable to create more than " + maxInstances
            + " instances in the pool. Assure the instances were correctly released with Graph.close()");

      if (localDatabase != null)
        instance = new PooledArcadeGraph(this, localDatabase);
      else
        instance = new PooledArcadeGraph(this, new RemoteDatabase(host, port, databaseName, userName, userPassword));
      totalInstancesCreated.incrementAndGet();
    }
    return instance;
  }

  public void setMaxInstances(final int maxInstances) {
    this.maxInstances = maxInstances;
  }

  public int getMaxInstances() {
    return maxInstances;
  }

  public int getTotalInstancesCreated() {
    return totalInstancesCreated.get();
  }

  private void release(final PooledArcadeGraph pooledArcadeGraph) {
    pooledInstances.offer(pooledArcadeGraph);
  }
}
