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
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.remote.RemoteDatabase;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import java.io.Closeable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

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

  /**
   * A borrowed {@link ArcadeGraph}, where {@code close()} means "give it back" rather than "tear it down". A caller
   * must not touch the instance after calling {@code close()}: it is handed straight to the next borrower, possibly
   * on another thread, and unlike a non-pooled graph it is not marked closed - so continuing to use it interleaves
   * with whoever holds it now instead of failing loudly.
   */
  public class PooledArcadeGraph extends ArcadeGraph {
    private final ArcadeGraphFactory factory;

    protected PooledArcadeGraph(final ArcadeGraphFactory factory, final Configuration configuration) {
      super(configuration);
      this.factory = factory;
    }

    protected PooledArcadeGraph(final ArcadeGraphFactory factory, final BasicDatabase database, final boolean sharedDatabase) {
      super(database, sharedDatabase);
      this.factory = factory;
    }

    /**
     * Returns the instance to the pool. A borrowed instance must always be handed back clean: any transaction still
     * in flight is ended here, otherwise the next borrower inherits (and commits) another caller's writes, which
     * across threads is cross-request data mixing (issue #6821).
     */
    @Override
    public void close() {
      RuntimeException failure = null;
      try {
        // HONOUR THE CONFIGURED CLOSE BEHAVIOUR FIRST (ROLLBACK BY DEFAULT), AS A NON-POOLED ArcadeGraph.close() DOES.
        // UNCONDITIONALLY, EVEN WITH NO TRANSACTION OPEN: AbstractThreadLocalTransaction.doClose() ALSO CLEARS THE
        // onClose()/onReadWrite() CONSUMERS, AND THE TRANSACTION OBJECT OUTLIVES THE BORROW. SKIPPING THE CALL WOULD
        // LEAVE A BORROWER'S onClose(COMMIT) IN PLACE FOR THE NEXT ONE, WHICH IS #6821 THROUGH A SIDE DOOR.
        tx().close();
      } catch (final RuntimeException e) {
        // A BORROWER WHOSE COMMIT-ON-CLOSE FAILED HAS TO HEAR ABOUT IT, BUT ONLY ONCE THE INSTANCE IS BACK IN THE
        // POOL AND CLEAN: HOLD THE FAILURE UNTIL THE finally BELOW HAS RUN.
        failure = e;
      } finally {
        boolean clean = true;

        try {
          // WHATEVER THE CLOSE BEHAVIOUR DID, THE INSTANCE MUST NOT GO BACK ON THE QUEUE WITH AN OPEN TRANSACTION.
          if (getDatabase().isTransactionActive())
            getDatabase().rollback();
        } catch (final Exception e) {
          clean = false;
          LogManager.instance()
              .log(this, Level.WARNING, "Error on rolling back the pending transaction while releasing a pooled ArcadeGraph instance", e);
        }

        try {
          // TinkerPop CLEARS THE CONSUMERS ONLY AFTER RUNNING THEM, SO ONE THAT THREW IS STILL ARMED FOR THE NEXT
          // BORROWER OF THIS SLOT. PUT THE DEFAULTS BACK EXPLICITLY RATHER THAN TRUST doClose() TO HAVE GOT THERE.
          tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK).onReadWrite(Transaction.READ_WRITE_BEHAVIOR.AUTO);
        } catch (final Exception e) {
          clean = false;
          LogManager.instance()
              .log(this, Level.WARNING, "Error on resetting the transaction behaviour of a pooled ArcadeGraph instance", e);
        }

        if (clean)
          factory.release(this);
        else
          // CLEANUP FAILED, SO WHAT THIS INSTANCE STILL CARRIES IS EXACTLY WHAT #6821 IS ABOUT - AN OPEN TRANSACTION,
          // OR THE PREVIOUS BORROWER'S CALLBACKS. IT MUST NOT REACH ANOTHER BORROWER.
          factory.quarantine(this);
      }

      if (failure != null)
        throw failure;
    }

    /**
     * Always refused. A borrowed instance never owns the database: over a local pool it is the factory's, shared with
     * every other instance, and over a remote pool {@code drop()} would delete it on the server for every other
     * instance AND every other client connected to it. The per-instance {@code sharedDatabase} flag cannot express
     * this, because a remote instance does own its own connection object and {@code close()} must still close it
     * (issue #6821).
     */
    @Override
    public void drop() {
      throw new UnsupportedOperationException(
          "Cannot drop a database from a pooled ArcadeGraph instance: the rest of the pool, and for a remote pool every other client of that server, is still using it");
    }

    /**
     * Tears the instance down for real, which is what {@link ArcadeGraphFactory#close()} does to the pool. Kept
     * separate from {@link #close()} because that one means "give it back", and so deliberately keeps the traversal
     * source and its driver cluster alive for the next borrower.
     */
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
        try {
          instance.dispose();
        } catch (final Exception e) {
          // ONE INSTANCE THAT FAILS TO DISPOSE MUST NOT STRAND THE REST: EVERY OTHER POOLED GRAPH STILL HOLDS A
          // DRIVER Cluster (A NETTY EVENT-LOOP GROUP AND A CONNECTION POOL) AND, OVER A REMOTE POOL, ITS OWN
          // CONNECTION - THE VERY RESOURCES #6822 IS ABOUT - AND THE SHARED LOCAL DATABASE BELOW WOULD STAY OPEN.
          LogManager.instance().log(this, Level.WARNING, "Error on disposing a pooled ArcadeGraph instance", e);
        }
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
        // THE LOCAL DATABASE IS SHARED BY EVERY POOLED INSTANCE AND OWNED BY THE FACTORY: DISPOSING ONE INSTANCE MUST
        // NOT CLOSE IT UNDER THE OTHERS. THE FACTORY CLOSES IT ITSELF IN close().
        instance = new PooledArcadeGraph(this, localDatabase, true);
      else
        instance = new PooledArcadeGraph(this, new RemoteDatabase(host, port, databaseName, userName, userPassword), false);
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

  /**
   * Drops an instance that could not be cleaned on release, instead of handing it to the next borrower. The counter
   * gets its slot back, so a replacement can be created and the pool does not quietly shrink towards zero.
   */
  private void quarantine(final PooledArcadeGraph pooledArcadeGraph) {
    totalInstancesCreated.decrementAndGet();
    try {
      pooledArcadeGraph.dispose();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on disposing a pooled ArcadeGraph instance that failed to clean up", e);
    }
  }
}
