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
package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.engine.WALFileFactoryEmbedded;
import com.arcadedb.engine.timeseries.TimeSeriesBucket;
import com.arcadedb.engine.timeseries.TimeSeriesTagDictionary;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.InvalidDatabaseInstanceException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableEdgeSegment;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.StripeDirectory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewPersistence;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.hash.HashIndexBucket;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.sparsevector.SparseSegmentComponent;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexGraphFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.opencypher.optimizer.statistics.GraphStatisticsCache;
import com.arcadedb.query.opencypher.query.CypherPlanCache;
import com.arcadedb.query.opencypher.query.CypherStatementCache;
import com.arcadedb.query.select.Select;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockException;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.RWLockContext;
import com.arcadedb.utility.RetryBackoff;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.stream.Stream;

/**
 * Local implementation of {@link Database}. It is based on files opened on the local file system.
 * <p>
 * Thread safe and therefore the same instance can be shared among threads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalDatabase extends RWLockContext implements DatabaseInternal {
  public static final int EDGE_LIST_INITIAL_CHUNK_SIZE         = 64;
  public static final int MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE = 8192;
  /** Header ({@code MutableEdgeSegment.CONTENT_START_POSITION}) plus room for a couple of maximum-width entries. */
  public static final int MIN_EDGE_LIST_CHUNK_SIZE             = 32;

  /** What {@link #quiesceAsync()} hands back on a database that never created an async executor: nothing to park. */
  private static final AsyncQuiesce NO_ASYNC_TO_QUIESCE = () -> {
  };

  private static final Set<String> SUPPORTED_FILE_EXT = Set.of(
      Dictionary.DICT_EXT,
      LocalBucket.BUCKET_EXT,
      LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT,
      LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
      LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT,
      LSMTreeIndexCompacted.UNIQUE_INDEX_EXT,
      LSMTreeIndexBloomFilter.FILE_EXT,
      LSMVectorIndex.FILE_EXT,
      LSMVectorIndexGraphFile.FILE_EXT,
      SparseSegmentComponent.FILE_EXT,
      TimeSeriesBucket.BUCKET_EXT,
      TimeSeriesTagDictionary.DICT_EXT,
      HashIndexBucket.UNIQUE_INDEX_EXT,
      HashIndexBucket.NOTUNIQUE_INDEX_EXT);

  /**
   * True when {@code fileName}'s extension is one the {@link FileManager} treats as a component file, i.e. one whose
   * content a point-in-time page snapshot (#6075) covers. The extension is taken from the NAME, never the path, so a
   * database directory containing a dot does not confuse it - the same rule
   * {@code FileManager.scanDirectoryForComponentFiles} applies when deciding what to register.
   * <p>
   * Exposed (#6116) so a reader walking the database DIRECTORY rather than the registered files - the HA
   * {@code /checksums} endpoint - can tell a page file from a configuration or time-series file by name alone. The
   * obvious alternative, asking the {@code FileManager} which files it currently has registered, is a moving target:
   * index compaction creates and drops component files WITHOUT the database write lock, so a set captured a moment
   * earlier can miss a file that is already on disk.
   */
  public static boolean isComponentFileName(final String fileName) {
    final int lastDot = fileName.lastIndexOf('.');
    return lastDot >= 0 && SUPPORTED_FILE_EXT.contains(fileName.substring(lastDot + 1));
  }

  public final       AtomicLong                                indexCompactions          = new AtomicLong();
  protected final    String                                    name;
  protected final    ComponentFile.MODE                        mode;
  protected final    ContextConfiguration                      configuration;
  protected final    String                                    databasePath;
  protected final    BinarySerializer                          serializer;
  protected final    RecordFactory                             recordFactory             = new RecordFactory();
  protected final    GraphEngine                               graphEngine;
  protected final    WALFileFactory                            walFactory;
  protected final    DocumentIndexer                           indexer;
  protected final    DatabaseStats                             stats                     = new DatabaseStats();
  protected          FileManager                               fileManager;
  protected          LocalSchema                               schema;
  protected          TransactionManager                        transactionManager;
  protected volatile DatabaseAsyncExecutorImpl                 async                     = null;
  protected final    Lock                                      asyncLock                 = new ReentrantLock();
  protected          boolean                                   autoTransaction           = false;
  protected volatile boolean                                   open                      = false;
  private            boolean                                   readYourWrites            = true;
  private final      Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks;
  private final      StatementCache                            statementCache;
  private final      ExecutionPlanCache                        executionPlanCache;
  private final      CypherStatementCache                      cypherStatementCache;
  private final      CypherPlanCache                           cypherPlanCache;
  private final      GraphStatisticsCache                      graphStatisticsCache      = new GraphStatisticsCache();
  private final      File                                      configurationFile;
  private            DatabaseInternal                          wrappedDatabaseInstance   = this;
  private final      SecurityManager                           security;
  private final      Map<String, Object>                       wrappers                  = new HashMap<>();
  private            File                                      lockFile;
  private            RandomAccessFile                          lockFileIO;
  private            FileChannel                               lockFileIOChannel;
  private            FileLock                                  lockFileLock;
  private final      RecordEventsRegistry                      events                    = new RecordEventsRegistry();
  private final      ConcurrentHashMap<String, QueryEngine>    reusableQueryEngines      = new ConcurrentHashMap<>();
  private final      ConcurrentHashMap<String, Object>         globalVariables           = new ConcurrentHashMap<>();
  private            TRANSACTION_ISOLATION_LEVEL               transactionIsolationLevel = TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
  private            long                                      openedOn;
  private            long                                      lastUpdatedOn;
  private            long                                      lastUsedOn;
  private            int                                       cachedHashCode            = 0;
  /** Guards against concurrent GraphBatch instances on this database. Never routed through a wrapper. */
  private final      AtomicBoolean                             batchInProgress           = new AtomicBoolean(false);

  protected LocalDatabase(final String path, final ComponentFile.MODE mode, final ContextConfiguration configuration,
      final SecurityManager security, final Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks) {
    try {
      this.mode = mode;
      this.configuration = configuration;
      this.security = security;
      this.callbacks = callbacks;
      this.serializer = new BinarySerializer(configuration);
      this.walFactory = mode == ComponentFile.MODE.READ_WRITE ? new WALFileFactoryEmbedded() : null;
      this.statementCache = new StatementCache(this,
          configuration.getValueAsInteger(GlobalConfiguration.SQL_STATEMENT_CACHE));
      this.executionPlanCache = new ExecutionPlanCache(this,
          configuration.getValueAsInteger(GlobalConfiguration.SQL_STATEMENT_CACHE));
      this.cypherStatementCache =
          new CypherStatementCache(configuration.getValueAsInteger(GlobalConfiguration.OPENCYPHER_STATEMENT_CACHE));
      this.cypherPlanCache = new CypherPlanCache(this,
          configuration.getValueAsInteger(GlobalConfiguration.OPENCYPHER_PLAN_CACHE));

      if (path.endsWith(File.separator))
        databasePath = path.substring(0, path.length() - 1);
      else
        databasePath = path;

      configurationFile = new File(databasePath + File.separator + "configuration.json");

      final int lastSeparatorPos = path.lastIndexOf(File.separator);
      if (lastSeparatorPos > -1)
        name = path.substring(lastSeparatorPos + 1);
      else
        name = path;

      checkDatabaseName();

      indexer = new DocumentIndexer(this);
      graphEngine = new GraphEngine(this);

    } catch (DatabaseOperationException e) {
      throw e;
    } catch (Exception e) {
      throw new DatabaseOperationException("Error on creating new database instance", e);
    }
  }

  public static int getNewEdgeListSize(final int previousSize) {
    if (previousSize == 0)
      // Floored: the chunk buffer is not auto-resizable, so a configured value that cannot hold the header plus one
      // entry fails at the first append with "Cannot resize the buffer" rather than at configuration time.
      return Math.max(MIN_EDGE_LIST_CHUNK_SIZE,
          GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.getValueAsInteger());

    int newSize = previousSize * 2;
    if (newSize > MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE)
      newSize = MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE;
    return newSize;
  }

  protected void open() {
    if (!new File(databasePath).exists())
      throw new DatabaseOperationException("Database '" + databasePath + "' does not exist");

    if (configurationFile.exists()) {
      try {
        final String content = FileUtils.readFileAsString(configurationFile);
        configuration.reset();
        configuration.fromJSON(content);
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on loading configuration from file '%s'", e,
            configurationFile);
      }
    }

    openInternal();

    try {
      executeCallbacks(CALLBACK_EVENT.DB_AFTER_OPEN);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on executing DB_AFTER_OPEN callbacks", e);
    }

    // Restore Graph Analytical Views persisted in schema extensions
    try {
      GraphAnalyticalViewPersistence.restoreAll(this);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error restoring Graph Analytical Views on database open", e);
    }
  }

  protected void create() {
    final File databaseDirectory = new File(databasePath);
    if (new File(databaseDirectory, LocalSchema.SCHEMA_FILE_NAME).exists() || new File(databaseDirectory,
        LocalSchema.SCHEMA_PREV_FILE_NAME).exists())
      throw new DatabaseOperationException("Database '" + databasePath + "' already exists");

    if (!databaseDirectory.exists() && !databaseDirectory.mkdirs())
      throw new DatabaseOperationException("Cannot create directory '" + databasePath + "'");

    openInternal();

    schema.saveConfiguration();

    try {
      saveConfiguration();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving configuration to file '%s'", e, configurationFile);
    }
  }

  @Override
  public void drop() {
    closeForDrop();

    executeInWriteLock(() -> {
      FileUtils.deleteRecursively(new File(databasePath));
      return null;
    });
  }

  /**
   * Closes the database with the same semantics {@link #drop()} uses - no index flush and no file sync, since
   * the content is about to be discarded - but leaves the files at {@link #getDatabasePath()} in place, so the
   * caller owns their removal. Callers that must not pay for the recursive delete on their own thread use this
   * to rename the directory aside and delete it elsewhere.
   */
  @Override
  public void closeForDrop() {
    checkDatabaseIsOpen(true, "Cannot drop database");

    if (isTransactionActive())
      throw new DatabaseOperationException("Cannot drop the database in transaction");

    closeInternal(true);
  }

  @Override
  public void close() {
    closeInternal(false);
  }

  /**
   * Test only API. Simulates a forced kill of the JVM leaving the database with the .lck file on the file system.
   * <p>
   * NOTE (#4927): kill() deliberately does NOT release this instance's PageManager lifecycle reference -
   * the documented kill-then-close contract relies on the following {@code close()} releasing it
   * (closeInternal's release runs with {@code open == false} too, exactly once via its CAS). A kill()
   * never followed by close() pins the page manager open for the JVM's life.
   */
  @Override
  public void kill() {
    if (async != null)
      async.kill();

    if (getTransaction().isActive())
      // ROLLBACK ANY PENDING OPERATION
      getTransaction().kill();

    // #5418: a real crash takes the index background threads down with the process, so the simulation must too -
    // otherwise a vector index inactivity timer survives the "crash" and later fires against a dead database.
    for (final Index idx : schema.getIndexes()) {
      try {
        ((IndexInternal) idx).releaseBackgroundResources();
      } catch (final Exception e) {
        // IGNORE IT: THIS IS A CRASH SIMULATION
      }
    }

    try {
      schema.close();
      PageManager.INSTANCE.simulateKillOfDatabase(this);
      fileManager.close();
      transactionManager.kill();

      if (lockFile != null) {
        try {
          if (lockFileLock != null) {
            lockFileLock.release();
          }
          if (lockFileIOChannel != null)
            lockFileIOChannel.close();
          if (lockFileIO != null)
            lockFileIO.close();
        } catch (final IOException e) {
          // IGNORE IT
        }
      }

    } finally {
      open = false;

      // CLEAR ANY THREAD-LOCAL CONTEXT POINTING AT THIS (NOW DEAD) DATABASE, OTHERWISE A SUBSEQUENT OPERATION ON THE SAME
      // THREAD (E.G. RESOLVING A BARE RID) WOULD STILL FIND THE KILLED DATABASE AS THE ACTIVE ONE. MIRRORS close().
      // UNLIKE closeInternal, THE RETURNED CONTEXTS ARE NOT ROLLED BACK, AND ONCE UNLINKED HERE THE DEAD-THREAD
      // SWEEP CAN NEVER REACH THEM EITHER. THAT IS SAFE ONLY IN THIS TEST-ONLY CRASH SIMULATION: THE FILE LOCKS
      // THEY COULD HOLD LIVE IN THIS INSTANCE'S TransactionManager LockManager, WHICH transactionManager.kill()
      // ALREADY CLOSED ABOVE - A REOPENED DATABASE STARTS WITH A FRESH LockManager, SO NOTHING CAN LEAK
      try {
        DatabaseContext.INSTANCE.removeAllContexts(databasePath);
      } catch (final Throwable e) {
        // IGNORE IT
      }

      Profiler.INSTANCE.unregisterDatabase(LocalDatabase.this);
    }
  }

  @Override
  public boolean isAsyncProcessing() {
    if (async != null) {
      asyncLock.lock();
      try {
        return async.isProcessing();
      } finally {
        asyncLock.unlock();
      }
    }
    return false;
  }

  @Override
  public void waitForAsyncCompletion() {
    // Read the field, never async(): that accessor CREATES the executor - worker threads included - and a database
    // that never used async must not grow a thread pool just because somebody asked whether it was idle.
    //
    // And read it WITHOUT asyncLock, unlike isAsyncProcessing() one method up. That lock exists to serialize the
    // lazy creation in async(); the field is volatile and, once assigned, is never assigned again - not even on
    // close - so the only two values this read can see are "no executor yet" and "the one and only executor". A
    // lock here would buy nothing and would be held across a blocking wait.
    final DatabaseAsyncExecutorImpl executor = async;
    if (executor == null)
      return;

    // REFUSED, not attempted, when the caller is one of the executor's own workers. waitCompletion() enqueues a
    // marker on every worker - this thread's own included - and blocks until each has run, and the only consumer of
    // a worker's queue is that worker: it would park on a marker nobody can dequeue and be lost for the life of the
    // process. There is no honest alternative to refusing, either. Silently skipping the wait would put back exactly
    // the bug this barrier exists to close, over the caller's OWN uncommitted batch; committing that batch here would
    // commit the enclosing task's writes mid-task, which is the per-task atomicity AsyncThread.executeTask guards.
    //
    // NeedRetryException, and worded like it, because that is what RebuildIndexStatement.buildIndex already threw for
    // this exact situation (issue #2097) - the guard now lives once, on the operation that cannot be satisfied,
    // instead of once per call site that happens to remember it.
    if (executor.isCurrentThreadOneOfMyWorkers())
      throw new NeedRetryException(
          "Cannot wait for the asynchronous executor of database '" + name + "' from one of its own worker threads: "
              + "the wait would never end. Run this command synchronously (awaitResponse=true) instead");

    // Unconditional, and that is the whole point (issue #6281). isProcessing() is not a precondition that can be
    // tested first: a worker keeps ONE transaction open across up to ASYNC_TX_BATCH_SIZE tasks, so an executor whose
    // queues are drained and whose workers are parked can still be holding thousands of uncommitted records.
    // waitCompletion() is the only operation that closes that batch - it enqueues a marker BEHIND everything already
    // submitted on every worker and that marker commits - so it has to run even when the executor looks idle.
    // The loop then covers what a single pass cannot: tasks submitted by OTHER threads while this one was waiting.
    do {
      executor.waitCompletion();
      // waitCompletion() answers an interrupt by restoring the flag and returning, so without this the loop would
      // spin: every further pass would be interrupted at its first queue offer and come straight back. The caller's
      // cancellation is left on the thread for it to observe.
      if (Thread.currentThread().isInterrupted())
        return;
    } while (isAsyncProcessing());
  }

  @Override
  public AsyncQuiesce quiesceAsync() {
    // The field, never async(), for the same reason waitForAsyncCompletion() reads it: quiescing an executor that
    // does not exist must not CREATE one. This is the reason BucketIndexBuilder no longer calls async() itself - it
    // did, unconditionally, so building an index on a database that had never touched the async API started a full
    // set of worker threads to park them (issue #6303, item 2).
    final DatabaseAsyncExecutorImpl executor = async;
    if (executor == null)
      return NO_ASYNC_TO_QUIESCE;

    return executor.quiesceWorkers();
  }

  /**
   * The asynchronous executor of this database <b>if one already exists</b>, and {@code null} otherwise - never a
   * newly created one, unlike {@link #async()}, whose lazy creation starts a full set of worker threads.
   * <p>
   * The same rule {@link #waitForAsyncCompletion()} and {@link #quiesceAsync()} follow, and for the same reason,
   * exported for the one caller that lives outside this class: {@code Profiler} reads async counters on every metrics
   * scrape AND on every database close, and going through {@code async()} there made merely observing a database
   * grow it a worker pool - one that the close path had already passed, so nothing was left to shut it down
   * (issue #6526 review).
   *
   * @see #async()
   */
  public DatabaseAsyncExecutorImpl getAsyncIfExists() {
    // The volatile field, unlocked: asyncLock serializes the lazy creation in async(), and once assigned this field
    // is never assigned again - not even on close - so the only two values a reader can see are "no executor yet"
    // and "the one and only executor".
    return async;
  }

  public DatabaseAsyncExecutor async() {
    if (async == null) {
      asyncLock.lock();
      try {
        if (async == null)
          async = new DatabaseAsyncExecutorImpl(wrappedDatabaseInstance, getConfiguration());
      } finally {
        asyncLock.unlock();
      }
    }
    return async;
  }

  /**
   * <b>Must stay lock-free, and must stay callable on a closed database (#5636.)</b> {@code Profiler} reads this
   * while holding its own monitor - both on a metrics scrape and from {@code unregisterDatabase} on the close path -
   * so acquiring a database lock here would put a database lock on the other side of a wait for that monitor, which
   * is a deadlock. It reads plain atomics today; keep it that way.
   */
  @Override
  public Map<String, Object> getStats() {
    final Map<String, Object> map = stats.toMap();
    map.put("indexCompactions", indexCompactions.get());
    return map;
  }

  @Override
  public String getDatabasePath() {
    return databasePath;
  }

  @Override
  public long getSize() {
    return executeInReadLock(() -> {
      checkDatabaseIsOpen();
      try {
        final Path dir = Path.of(databasePath);
        if (!Files.exists(dir))
          return 0L;
        try (Stream<Path> stream = Files.walk(dir)) {
          return stream.filter(Files::isRegularFile).mapToLong(p -> {
            try {
              return Files.size(p);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }).sum();
        }
      } catch (Exception e) {
        throw new DatabaseOperationException("Error calculating database size", e);
      }
    });
  }

  @Override
  public String getCurrentUserName() {
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    if (dbContext == null)
      return null;
    final SecurityDatabaseUser user = dbContext.getCurrentUser();
    return user != null ? user.getName() : null;
  }

  @Override
  public int getNestedTransactions() {
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    if (dbContext != null)
      return dbContext.transactions.size();
    return 0;
  }

  public TransactionContext getTransactionIfExists() {
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    if (dbContext != null) {
      final TransactionContext tx = dbContext.getLastTransaction();
      if (tx != null) {
        final DatabaseInternal txDb = tx.getDatabase();
        if (txDb == null) {
          tx.rollback();
          throw new InvalidDatabaseInstanceException("Invalid transactional context (db is null)");
        }
        if (txDb.getEmbedded() != this) {
          try {
            DatabaseContext.INSTANCE.init(this);
          } catch (final Exception e) {
            // IGNORE IT
          }
          throw new InvalidDatabaseInstanceException("Invalid transactional context (different db)");
        }
        return tx;
      }
    }

    return null;
  }

  @Override
  public void begin() {
    begin(transactionIsolationLevel);
  }

  @Override
  public void begin(final TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    executeInReadLock(() -> {
      checkDatabaseIsOpen();

      // FORCE THE RESET OF TL
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(databasePath);
      TransactionContext tx = current.getLastTransaction();
      if (tx.isActive()) {
        // CREATE A NESTED TX
        tx = new TransactionContext(getWrappedDatabaseInstance());
        current.pushTransaction(tx);
      }

      tx.begin(isolationLevel);

      return null;
    });
  }

  public void incrementStatsWriteTx() {
    stats.writeTx.incrementAndGet();
  }

  public void incrementStatsReadTx() {
    stats.readTx.incrementAndGet();
  }

  @Override
  public void commit() {
    executeInReadLock(() -> {
      checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current =
          DatabaseContext.INSTANCE.getContext(LocalDatabase.this.getDatabasePath());
      try {
        final Binary result = current.getLastTransaction().commit();
        if (result != null)
          stats.writeTx.incrementAndGet();
        else
          stats.readTx.incrementAndGet();
      } finally {
        current.popIfNotLastTransaction();
      }

      return null;
    });
  }

  @Override
  public void rollback() {
    stats.txRollbacks.incrementAndGet();

    executeInReadLock(() -> {
      try {
        checkTransactionIsActive(false);

        final DatabaseContext.DatabaseContextTL current =
            DatabaseContext.INSTANCE.getContext(LocalDatabase.this.getDatabasePath());
        current.popIfNotLastTransaction().rollback();

      } catch (final TransactionException e) {
        // ALREADY ROLLED BACK
      }
      return null;
    });
  }

  @Override
  public void rollbackAllNested() {
    if (!isTransactionActive())
      return;

    stats.txRollbacks.incrementAndGet();

    executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current =
          DatabaseContext.INSTANCE.getContext(LocalDatabase.this.getDatabasePath());

      TransactionContext tx;
      while ((tx = current.popIfNotLastTransaction()) != null) {
        try {
          if (tx.isActive())
            tx.rollback();
          else
            break;

        } catch (final InvalidDatabaseInstanceException e) {
          current.popIfNotLastTransaction().rollback();
        } catch (final TransactionException e) {
          // ALREADY ROLLED BACK
        }
      }
      return null;
    });
  }

  @Override
  public long countBucket(final String bucketName) {
    stats.countBucket.incrementAndGet();
    return (Long) executeInReadLock((Callable<Object>) () -> schema.getBucketByName(bucketName).count());
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    stats.countType.incrementAndGet();

    return (Long) executeInReadLock((Callable<Object>) () -> {
      final DocumentType type = schema.getType(typeName);

      // TimeSeries types store data in their own engine, not in regular buckets
      if (type instanceof LocalTimeSeriesType tsType) {
        try {
          return tsType.getEngine().countSamples();
        } catch (final IOException e) {
          throw new DatabaseOperationException("Error counting TimeSeries samples for type '" + typeName + "'", e);
        }
      }

      long total = 0;
      for (final Bucket b : type.getBuckets(polymorphic))
        total += b.count();

      return total;
    });
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    scanType(typeName, polymorphic, callback, null);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback,
      final ErrorRecordCallback errorRecordCallback) {
    stats.scanType.incrementAndGet();

    executeInReadLock(() -> {
      boolean success = false;
      final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);
      try {
        final DocumentType type = schema.getType(typeName);

        final AtomicBoolean continueScan = new AtomicBoolean(true);

        for (final Bucket b : type.getBuckets(polymorphic)) {
          b.scan((rid, view) -> {
            final Document record = (Document) recordFactory.newImmutableRecord(wrappedDatabaseInstance, type, rid,
                view, null);
            continueScan.set(callback.onRecord(record));
            return continueScan.get();
          }, errorRecordCallback);

          if (!continueScan.get())
            break;
        }

        success = true;

      } finally {
        if (implicitTransaction)
          if (success)
            wrappedDatabaseInstance.commit();
          else
            wrappedDatabaseInstance.rollback();
      }
      return null;
    });
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    scanBucket(bucketName, callback, null);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback,
      final ErrorRecordCallback errorRecordCallback) {
    stats.scanBucket.incrementAndGet();

    executeInReadLock(() -> {

      checkDatabaseIsOpen();

      final String typeName = schema.getTypeNameByBucketId(schema.getBucketByName(bucketName).getFileId());
      schema.getBucketByName(bucketName).scan((rid, view) -> {
        final Record record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, schema.getType(typeName), rid
            , view, null);
        return callback.onRecord(record);
      }, errorRecordCallback);
      return null;
    });
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    stats.iterateType.incrementAndGet();

    return executeInReadLock(() -> {
      checkDatabaseIsOpen();
      var type = schema.getType(typeName);
      var iter = new MultiIterator<Record>();

      // SET THE PROFILED LIMITS IF ANY
      iter.setLimit(getResultSetLimit());
      iter.setTimeout(getReadTimeout(), true);

      for (final Bucket b : type.getBuckets(polymorphic))
        iter.addIterator(b.iterator());
      return iter;
    });
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    stats.iterateBucket.incrementAndGet();

    return executeInReadLock(() -> {
      checkDatabaseIsOpen();
      try {
        final Bucket bucket = schema.getBucketByName(bucketName);
        return bucket.iterator();
      } catch (final Exception e) {
        throw new DatabaseOperationException("Error on executing scan of bucket '" + bucketName + "'", e);
      }
    });
  }

  public void checkPermissionsOnDatabase(final SecurityDatabaseUser.DATABASE_ACCESS access) {
    if (security == null)
      return;

    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    if (dbContext == null)
      return;
    final SecurityDatabaseUser user = dbContext.getCurrentUser();
    if (user == null)
      return;

    if (user.requestAccessOnDatabase(access))
      return;

    throw new SecurityException("User '" + user.getName() + "' is not allowed to " + access.fullName);
  }

  @Override
  public void checkPermissionsOnFile(final int fileId, final SecurityDatabaseUser.ACCESS access) {
    if (security == null)
      return;

    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    if (dbContext == null)
      return;
    final SecurityDatabaseUser user = dbContext.getCurrentUser();
    if (user == null)
      return;

    if (user.requestAccessOnFile(fileId, access))
      return;

    String resource = "file '" + schema.getFileById(fileId).getName() + "'";
    final DocumentType type = schema.getTypeByBucketId(fileId);
    if (type != null)
      resource = "type '" + type + "'";

    throw new SecurityException("User '" + user.getName() + "' is not allowed to " + access.fullName + " on " + resource);
  }

  @Override
  public long getResultSetLimit() {
    if (security == null)
      return -1L;

    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    if (dbContext == null)
      return -1L;
    final SecurityDatabaseUser user = dbContext.getCurrentUser();
    if (user == null)
      return -1L;

    return user.getResultSetLimit();
  }

  @Override
  public long getReadTimeout() {
    if (security == null)
      return -1L;

    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    if (dbContext == null)
      return -1L;
    final SecurityDatabaseUser user = dbContext.getCurrentUser();
    if (user == null)
      return -1L;

    return user.getReadTimeout();
  }

  @Override
  public boolean existsRecord(final RID rid) {
    stats.existsRecord.incrementAndGet();

    if (rid == null)
      throw new IllegalArgumentException("Record is null");

    return (boolean) executeInReadLock((Callable<Object>) () -> {
      checkDatabaseIsOpen();

      // CHECK IN TX CACHE FIRST
      final TransactionContext tx = getTransaction();
      Record record = tx.getRecordFromCache(rid);
      if (record != null)
        return true;

      // A dangling RID can reference a bucket that no longer exists: the record simply does not exist. See issue #4501.
      final LocalBucket bucket = schema.getBucketById(rid.getBucketId(), false);
      return bucket != null && bucket.existsRecord(rid);
    });
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    stats.readRecord.incrementAndGet();

    if (rid == null)
      throw new IllegalArgumentException("Record id is null");

    return (Record) executeInReadLock((Callable<Object>) () -> {
      checkDatabaseIsOpen();

      // CHECK IN TX CACHE FIRST
      final TransactionContext tx = getTransaction();
      Record record = tx.getRecordFromCache(rid);
      if (record != null)
        return record;

      final DocumentType type = schema.getTypeByBucketId(rid.getBucketId());

      final boolean loadRecordContent;
      if (!loadContent && tx.getIsolationLevel() == TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ)
        // FORCE LOAD OF CONTENT TO GUARANTEE THE LOADING OF MULTI-PAGE RECORD INTO THE TX CONTEXT
        loadRecordContent = true;
      else
        loadRecordContent = loadContent;

      if (loadRecordContent || type == null) {
        // A dangling index entry can reference an RID whose bucket no longer exists (e.g. the record/bucket
        // was removed but the index entry survived). Treat it as a missing record so callers that already
        // handle RecordNotFoundException (like index scans) can skip it, instead of aborting with a
        // SchemaException ("Bucket with id 'NNN' was not found"). See issue #4501.
        final LocalBucket bucket = schema.getBucketById(rid.getBucketId(), false);
        if (bucket == null)
          throw new RecordNotFoundException("Record " + rid + " not found", rid);
        final Binary buffer = bucket.getRecord(rid);
        record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, type, rid, buffer.copyOfContent(), null);
        record = invokeAfterReadEvents(record);
        if (record == null)
          throw new RecordNotFoundException("Record " + rid + " not found", rid);
        return record;
      }

      record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, type, rid, type.getType());

      return record;
    });
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String keyName, final Object keyValue) {
    return lookupByKey(type, new String[] { keyName }, new Object[] { keyValue });
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String[] keyNames, final Object[] keyValues) {
    stats.readRecord.incrementAndGet();

    return (IndexCursor) executeInReadLock((Callable<Object>) () -> {

      checkDatabaseIsOpen();
      final DocumentType t = schema.getType(type);

      final TypeIndex idx = t.getPolymorphicIndexByProperties(keyNames);
      if (idx == null)
        throw new IllegalArgumentException(
            "No index has been created on type '" + type + "' properties " + Arrays.toString(keyNames));

      return idx.get(keyValues);
    });
  }

  @Override
  public void registerCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    final List<Callable<Void>> callbacks = this.callbacks.computeIfAbsent(event, k -> new ArrayList<>());
    callbacks.add(callback);
  }

  @Override
  public void unregisterCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    final List<Callable<Void>> callbacks = this.callbacks.get(event);
    if (callbacks != null) {
      callbacks.remove(callback);
      if (callbacks.isEmpty())
        this.callbacks.remove(event);
    }
  }

  @Override
  public GraphEngine getGraphEngine() {
    return graphEngine;
  }

  @Override
  public TransactionManager getTransactionManager() {
    return transactionManager;
  }

  /**
   * Highest transaction id persisted in this database. Used by the HA bootstrap path
   * (issue #4147) as the recency signal when peers compare their local database state at
   * first cluster formation. Returns -1 if no transaction has ever been committed.
   */
  public long getLastTransactionId() {
    return transactionManager == null ? -1L : transactionManager.getLastTransactionId();
  }

  @Override
  public boolean isReadYourWrites() {
    return readYourWrites;
  }

  @Override
  public Database setReadYourWrites(final boolean readYourWrites) {
    this.readYourWrites = readYourWrites;
    return this;
  }

  @Override
  public TRANSACTION_ISOLATION_LEVEL getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  @Override
  public Database setTransactionIsolationLevel(final TRANSACTION_ISOLATION_LEVEL level) {
    transactionIsolationLevel = level;
    return this;
  }

  @Override
  public LocalDatabase setUseWAL(final boolean useWAL) {
    getTransaction().setUseWAL(useWAL);
    return this;
  }

  @Override
  public LocalDatabase setWALFlush(final WALFile.FlushType flush) {
    getTransaction().setWALFlush(flush);
    return this;
  }

  @Override
  public boolean isAsyncFlush() {
    return getTransaction().isAsyncFlush();
  }

  @Override
  public LocalDatabase setAsyncFlush(final boolean value) {
    getTransaction().setAsyncFlush(value);
    return this;
  }

  @Override
  public void createRecord(final MutableDocument record) {
    executeInReadLock(() -> {
      createRecordNoLock(record, null, false);
      return null;
    });
  }

  @Override
  public RecordEvents getEvents() {
    return events;
  }

  @Override
  public void createRecord(final Record record, final String bucketName) {
    executeInReadLock(() -> {
      createRecordNoLock(record, bucketName, false);
      return null;
    });
  }

  @Override
  public void createRecordNoLock(final Record record, final String bucketName, final boolean discardRecordAfter) {
    if (record.getIdentity() != null)
      throw new IllegalArgumentException("Cannot create record " + record.getIdentity() + " because it is already " +
          "persistent");

    if (mode == ComponentFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot create a new record");

    setDefaultValues(record);

    if (record instanceof MutableDocument doc)
      doc.validate();

    // INVOKE EVENT CALLBACKS
    if (!events.onBeforeCreate(record))
      return;
    if (record instanceof Document doc)
      if (!((RecordEventsRegistry) doc.getType().getEvents()).onBeforeCreate(record))
        return;

    boolean success = false;
    final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);
    try {
      final LocalBucket bucket;

      if (bucketName == null && record instanceof Document doc)
        bucket = (LocalBucket) doc.getType().getBucketIdByRecord(doc,
            DatabaseContext.INSTANCE.getContext(databasePath).perThreadBucketSelection);
      else {
        bucket = (LocalBucket) schema.getBucketByName(bucketName);
        // Reject direct writes to internal buckets (e.g. paired external-property buckets). They are infrastructure
        // for the serializer, not user data containers; allowing user DML to write here would corrupt the schema's
        // accounting of which records are real records vs. payload blobs.
        if (bucket.getPurpose() != LocalBucket.Purpose.PRIMARY)
          throw new IllegalArgumentException(
              "Bucket '" + bucketName + "' is internal (purpose=" + bucket.getPurpose() + ") and cannot be written to directly");
      }

      ((RecordInternal) record).setIdentity(bucket.createRecord(record, discardRecordAfter));

      final TransactionContext transaction = getTransaction();
      transaction.updateRecordInCache(record);
      transaction.updateBucketRecordDelta(bucket.getFileId(), +1);

      // A brand-new edge chunk cannot be edge-append rebased: the committed version of its page does not contain
      // this chunk yet, so replaying appends against it would target the wrong bytes. Exclude the whole page
      // (it may be shared with a pre-existing chunk) from the commutative append merge. Same for a new stripe
      // directory (super-node promotion, #5156). See TransactionContext.
      if (record instanceof MutableEdgeSegment || record instanceof StripeDirectory)
        transaction.poisonEdgeAppendPage(record.getIdentity());

      // TRACK USER DOCUMENTS (NOT INTERNAL RECORDS LIKE EDGE SEGMENTS) SO A ROLLBACK CAN RESET THEIR IDENTITY AND ALLOW
      // A CLEAN RE-INSERT INSTEAD OF AN UPDATE OF A MISSING RECORD (ISSUE #4562).
      if (record instanceof MutableDocument)
        transaction.registerNewRecord(record);

      if (record instanceof MutableDocument doc)
        indexer.createDocument(doc, doc.getType(), bucket);

      ((RecordInternal) record).unsetDirty();

      success = true;

      // INVOKE EVENT CALLBACKS
      events.onAfterCreate(record);
      if (record instanceof Document doc)
        ((RecordEventsRegistry) doc.getType().getEvents()).onAfterCreate(record);

    } finally {
      if (implicitTransaction) {
        if (success)
          wrappedDatabaseInstance.commit();
        else
          wrappedDatabaseInstance.rollback();
      }
    }
  }

  @Override
  public RID restoreRecord(final Record record, final LocalBucket bucket, final long position) {
    if (record.getIdentity() != null)
      throw new IllegalArgumentException(
          "Cannot restore record " + record.getIdentity() + " because it is already persistent");

    if (mode == ComponentFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot restore a record");

    // Restoring into an infrastructure bucket would recreate a serializer payload blob as if it were a user record,
    // corrupting the schema's accounting of which records are real - same reason createRecordNoLock refuses it.
    if (bucket.getPurpose() != LocalBucket.Purpose.PRIMARY)
      throw new IllegalArgumentException(
          "Bucket '" + bucket.getName() + "' is internal (purpose=" + bucket.getPurpose() + ") and cannot be restored into");

    // Auto-transaction parity with createRecordNoLock: on a database opened with setAutoTransaction(true) a plain
    // INSERT wraps itself in a transaction, and RESTORE must not be the one statement that throws instead. Without
    // it, both paths still fail identically outside a transaction (autoTransaction defaults to false).
    // The implicit transaction is opened AND committed inside the SAME read lock the restore write holds, the way
    // createRecord wraps all of createRecordNoLock. Committing after releasing it would leave a window - unique in
    // this class - for an executeInWriteLock caller (drop(), close()) to interleave between the physical page write
    // and the commit that persists it. The whole sequence below - validation and the beforeCreate listeners
    // included - is inside that lock for the same reason createRecord puts them there: a listener is arbitrary user
    // code, and it must not run against a database an executeInWriteLock caller is free to close underneath it.
    return executeInReadLock(() -> {
      boolean success = false;
      // Opened BEFORE the checks below, where createRecordNoLock opens it after them - forced, not an oversight:
      // checkRestoreTargetIsFree reads the target page through the transaction, so there has to be one to read it
      // through. The only visible difference is that with autoTransaction a RESTORE that fails validation opens and
      // rolls back an empty implicit transaction where the equivalent INSERT would not have opened one, which costs
      // nothing - a rollback with no modified page is a no-op.
      final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);
      try {
        // Checked before the record's own constraints (#6127 review): aiming at a RID that is still live is the
        // likeliest mistake with this statement and used to be the only error it could return, so a
        // mandatory-property violation must not be what a caller sees first and goes off to fix. Advisory only -
        // restoreRecordAtPosition runs the same check again, authoritatively, on the page it writes.
        bucket.checkRestoreTargetIsFree(position);

        // #6127: the schema contract, applied exactly as createRecordNoLock applies it. RESTORE used to skip both of
        // these on the grounds that an emergency repair must never be blocked - but a record written past its own
        // MANDATORY/NOTNULL constraints cannot even be UPDATEd afterwards (updateRecord validates too, so every later
        // write throws until the missing property is supplied), and CHECK DATABASE is a structural check that never
        // looks at schema constraints, so nothing downstream catches it either. Refusing up front costs the caller one
        // explicit `SET name = '<unknown>'` and yields a record the rest of the engine can actually work with.
        setDefaultValues(record);

        if (record instanceof MutableDocument doc)
          doc.validate();

        // #6127: the create events, likewise. Whoever may RESTORE may also INSERT, and the same triggers already run
        // on every INSERT, so firing them here adds no privilege or behavioural surface that was not already
        // reachable - while NOT firing them silently drifts any derived state a trigger maintains. Unlike
        // createRecordNoLock, a veto raises instead of returning quietly: a repair that reports success without
        // writing the record is the one outcome this statement must never produce.
        if (!events.onBeforeCreate(record))
          throw new DatabaseOperationException(
              "Cannot restore record at position " + position + " in bucket '" + bucket.getName()
                  + "': a database-level beforeCreate listener vetoed it");
        if (record instanceof Document doc)
          if (!((RecordEventsRegistry) doc.getType().getEvents()).onBeforeCreate(record))
            throw new DatabaseOperationException(
                "Cannot restore record at position " + position + " in bucket '" + bucket.getName()
                    + "': a beforeCreate listener on type '" + doc.getTypeName() + "' vetoed it");

        final RID restoredRid = restoreRecordInTransaction(record, bucket, position);
        success = true;

        // INVOKE EVENT CALLBACKS - before the implicit commit, exactly where createRecordNoLock fires them.
        events.onAfterCreate(record);
        if (record instanceof Document doc)
          ((RecordEventsRegistry) doc.getType().getEvents()).onAfterCreate(record);

        return restoredRid;
      } finally {
        if (implicitTransaction) {
          if (success)
            wrappedDatabaseInstance.commit();
          else
            wrappedDatabaseInstance.rollback();
        }
      }
    });
  }

  private RID restoreRecordInTransaction(final Record record, final LocalBucket bucket, final long position) {
    final RID rid = bucket.restoreRecordAtPosition(position, record);

    final TransactionContext transaction = getTransaction();
    transaction.updateRecordInCache(record);
    // #6069: restoreRecordAtPosition does the physical page write only, so fold the same +1 on the cached bucket
    // record-count delta that count(*) reads and a normal create applies.
    transaction.updateBucketRecordDelta(bucket.getFileId(), +1);

    // Same reason createRecordNoLock poisons it: a restored edge chunk or stripe directory is absent from the
    // committed version of its page, so replaying this transaction's appends against that page at commit would
    // target the wrong bytes. No current caller restores one - every RESTORE arm passes a document/vertex/edge
    // shell - but this is a general Record-typed primitive on a shared interface, and a future caller should get
    // the correct behaviour rather than a silent gap. (restoreRecordAtPosition already poisons the SLOT merge;
    // this is the separate commutative edge-append merge.)
    if (record instanceof MutableEdgeSegment || record instanceof StripeDirectory)
      transaction.poisonEdgeAppendPage(record.getIdentity());

    if (record instanceof MutableDocument doc) {
      // The record did not exist before this transaction, so for a rollback it is a NEW record: keep it out of
      // the reload loop and reset its identity to provisional (#4562, #4940) rather than leaving a dangling RID
      // on the caller's object.
      //
      // Registered BEFORE indexing, matching createRecordNoLock's order: indexer.createDocument can throw inline
      // (Index.put -> checkIsValid on a dropped/invalidated index, or convertKeys on a key it cannot coerce), and
      // registering after would skip the rollback identity reset on exactly those paths. Note this is NOT the
      // unique-constraint path - a duplicate key is detected at commit, by which point both calls have run.
      transaction.registerNewRecord(record);

      // #6120: the index entries. Without this the restored record is returned by a full scan but not by any
      // index-resolved query, and a UNIQUE index never learns the key came back - so a later restore or insert
      // could hand the same key to a second record unchallenged. Deliberately the same call createRecordNoLock
      // makes: a restored record is indexed exactly like an inserted one, duplicate rejection included.
      indexer.createDocument(doc, doc.getType(), bucket);
    }

    ((RecordInternal) record).unsetDirty();

    return rid;
  }

  @Override
  public void updateRecord(final Record record) {
    if (record.getIdentity() == null)
      throw new IllegalArgumentException("Cannot update the record because it is not persistent");

    if (mode == ComponentFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update a record");

    if (record instanceof MutableDocument document)
      document.validate();

    // INVOKE EVENT CALLBACKS
    if (!events.onBeforeUpdate(record))
      return;
    if (record instanceof Document document)
      if (!((RecordEventsRegistry) document.getType().getEvents()).onBeforeUpdate(record))
        return;

    stats.updateRecord.incrementAndGet();

    executeInReadLock(() -> {
      if (isTransactionActive()) {
        // MARK THE RECORD FOR UPDATE IN TX AND DEFER THE SERIALIZATION AT COMMIT TIME. THIS SPEEDS UP CASES WHEN THE
        // SAME RECORDS ARE UPDATE MULTIPLE TIME INSIDE
        // THE SAME TX. THE MOST CLASSIC EXAMPLE IS INSERTING EDGES: THE RECORD CHUNK IS UPDATED EVERYTIME A NEW EDGE
        // IS CREATED IN THE SAME CHUNK.
        // THE PAGE IS EARLY LOADED IN TX CACHE TO USE THE PAGE MVCC IN CASE OF CONCURRENT OPERATIONS ON THE MODIFIED
        // RECORD
        try {
          final TransactionContext tx = getTransaction();
          tx.addUpdatedRecord(record);

          if (record instanceof Document document) {
            // UPDATE THE INDEX IN MEMORY BEFORE UPDATING THE PAGE
            final List<IndexInternal> indexes = indexer.getInvolvedIndexes(document);
            if (!indexes.isEmpty()) {
              // UPDATE THE INDEXES TOO.
              // #4935: when the same record is updated more than once in this tx, diff against the previous
              // in-tx indexed state (snapshot below), not the committed buffer returned by
              // getOriginalDocument() - the buffer stays frozen until commit because serialization is
              // deferred, so diffing against it leaks a phantom index entry for every intermediate value.
              // The snapshot holds ONLY the indexed property values (not a full detach of the document) to
              // stay light on allocations for wide documents and bulk updates.
              final RID rid = record.getIdentity();
              final Document previous = tx.getLastIndexedSnapshot(rid);
              final Document originalRecord = previous != null ? previous : getOriginalDocument(record);
              // updateDocument returns a refreshed snapshot (built from the key values it already extracted
              // for the diff) ONLY when an index actually changed: otherwise the previous diff source
              // (committed buffer or an earlier snapshot) still describes the indexed state, and updates
              // that touch only non-indexed properties pay no snapshot cost at all.
              final Document refreshedSnapshot = indexer.updateDocument(originalRecord, document, indexes);
              if (refreshedSnapshot != null)
                tx.setLastIndexedSnapshot(rid, refreshedSnapshot);
            }
          }
        } catch (final IOException e) {
          throw new DatabaseOperationException("Error on update the record " + record.getIdentity() + " in " +
              "transaction", e);
        }
      } else
        updateRecordNoLock(record, false);

      // INVOKE EVENT CALLBACKS
      events.onAfterUpdate(record);
      if (record instanceof Document document)
        ((RecordEventsRegistry) document.getType().getEvents()).onAfterUpdate(record);

      return null;
    });
  }

  public Document getOriginalDocument(final Record record) {
    final Binary originalBuffer = ((RecordInternal) record).getBuffer();
    if (originalBuffer == null)
      throw new IllegalStateException("Cannot read original buffer for record " + record.getIdentity()
          + ". In case of tx retry check the record is created inside the transaction");
    originalBuffer.rewind();
    return (Document) recordFactory.newImmutableRecord(this, ((Document) record).getType(), record.getIdentity(),
        originalBuffer,
        null);
  }

  @Override
  public void updateRecordNoLock(final Record record, final boolean discardRecordAfter) {
    boolean success = false;
    final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);

    try {
      final List<IndexInternal> indexes = record instanceof Document d ? indexer.getInvolvedIndexes(d) :
          Collections.emptyList();

      if (!indexes.isEmpty()) {
        // UPDATE THE INDEXES TOO
        final Document originalRecord = getOriginalDocument(record);

        schema.getBucketById(record.getIdentity().getBucketId()).updateRecord(record, discardRecordAfter);

        indexer.updateDocument(originalRecord, (Document) record, indexes);
      } else
        // NO INDEXES
        schema.getBucketById(record.getIdentity().getBucketId()).updateRecord(record, discardRecordAfter);

      getTransaction().updateRecordInCache(record);
      getTransaction().removeImmutableRecordsOfSamePage(record.getIdentity());

      success = true;

    } finally {
      if (implicitTransaction) {
        if (success)
          wrappedDatabaseInstance.commit();
        else
          wrappedDatabaseInstance.rollback();
      }
    }
  }

  @Override
  public void deleteRecord(final Record record) {
    executeInReadLock(() -> {
      deleteRecordNoLock(record);
      return null;
    });
  }

  @Override
  public void deleteEdgeSkippingEndpoint(final Edge edge, final RID skipEndpoint) {
    executeInReadLock(() -> {
      deleteRecordNoLock(edge, skipEndpoint);
      return null;
    });
  }

  @Override
  public void deleteRecordNoLock(final Record record) {
    deleteRecordNoLock(record, null);
  }

  /**
   * @param skipEdgeEndpoint when {@code record} is an edge, the endpoint vertex whose edge list must NOT be
   *                         touched by the disconnection (#5760, see
   *                         {@link #deleteEdgeSkippingEndpoint(Edge, RID)}). Ignored for any other record type,
   *                         and always {@code null} on the ordinary delete path.
   */
  private void deleteRecordNoLock(final Record record, final RID skipEdgeEndpoint) {
    if (record.getIdentity() == null)
      throw new IllegalArgumentException("Cannot delete a non persistent record");

    if (mode == ComponentFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot delete record " + record.getIdentity());

    // INVOKE EVENT CALLBACKS
    if (!events.onBeforeDelete(record))
      return;
    if (record instanceof Document document)
      if (!((RecordEventsRegistry) document.getType().getEvents()).onBeforeDelete(record))
        return;

    boolean success = false;
    final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);

    try {
      final LocalBucket bucket = schema.getBucketById(record.getIdentity().getBucketId());

      // Set only when the index-cleanup read confirmed a structurally broken multi-page chain (below): the physical
      // removal must then also use the force path, otherwise deleteRecordInternal would re-hit the broken link and throw
      // the #4932 retry signal, leaving the record undeletable. Scoped to the exact record the caller asked to delete.
      boolean forceBrokenChainDelete = false;
      final boolean tolerateBrokenChain = configuration.getValueAsBoolean(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN);

      if (record instanceof Document document) {
        try {
          indexer.deleteDocument(document);
          // Cascade-delete EXTERNAL property values living in paired external buckets. This must run BEFORE the primary
          // record is deleted, so the buffer is still readable. Both deletes ride the same transaction.
          cascadeDeleteExternalValues(document);
        } catch (final SerializationException | NegativeArraySizeException | BufferUnderflowException
                       | IndexOutOfBoundsException | IllegalArgumentException e) {
          // The record buffer is corrupted (e.g. written by a version affected by issue #4319 in HA), so its indexed
          // keys and EXTERNAL pointers cannot be read for cleanup. A malformed buffer surfaces as one of a small family
          // of exceptions depending on which field decodes wrong: out-of-range length (SerializationException /
          // NegativeArraySizeException), a content offset past the end (IllegalArgumentException "Invalid position"), or
          // a truncated read (BufferUnderflowException / IndexOutOfBoundsException) - see issues #4420 and #4432.
          // Proceed with the physical deletion anyway so the stuck record can finally be removed; leftover index/external
          // entries are best-effort and a database check can repair them afterwards.
          LogManager.instance().log(this, Level.WARNING,
              "Cannot read record %s for index/external cleanup on delete (corrupted buffer): %s. Deleting the record anyway; "
                  + "run a database check to repair any dangling index entries.", record.getIdentity(), e.getMessage());
        } catch (final BrokenChunkChainException e) {
          // The loader itself confirmed the chunk chain is structurally broken (#6258), so there is nothing left to
          // disambiguate here: the body cannot be assembled and never will be. Same tolerant path as the branch below,
          // minus the structural probe that branch has to run because a ConcurrentModificationException does not say
          // which of the two problems it is. Still gated on the opt-in: forcing through is an admin decision either way.
          if (!tolerateBrokenChain)
            throw e;
          forceBrokenChainDelete = true;
          logBrokenChainForceDelete(record.getIdentity(), e);
        } catch (final ConcurrentModificationException e) {
          // The record body could not be assembled for a consistent read, so its indexed keys and EXTERNAL pointers could
          // not be read for cleanup. loadMultiPageRecord throws this after exhausting TX_RETRIES, but exhausted retries do
          // NOT prove corruption: its page-version validation also fails when concurrent writes touch OTHER records
          // sharing the chain's pages, so under a busy bucket this can be pure contention. Deleting anyway in that case
          // would leak index entries for a healthy record. Disambiguate with a version-blind STRUCTURAL walk of the chunk
          // chain: only a genuinely broken chain (a bad continuation pointer - the case that would otherwise make the
          // record undeletable forever) takes the tolerant path below; transient contention rethrows, preserving the
          // NeedRetryException semantics so the retry machinery re-runs the DELETE with intact index cleanup.
          if (!tolerateBrokenChain || !bucket.isChunkChainBroken(record.getIdentity()))
            throw e;
          forceBrokenChainDelete = true;
          logBrokenChainForceDelete(record.getIdentity(), e);
        }
      }

      if (record instanceof Edge edge) {
        graphEngine.deleteEdge(edge, skipEdgeEndpoint);
      } else if (record instanceof Vertex) {
        try {
          graphEngine.deleteVertex((VertexInternal) record, forceBrokenChainDelete);
        } catch (final BrokenChunkChainException e) {
          // The record body could not be assembled to reach the vertex's edge lists, and the loader has already
          // confirmed why (#6258): no structural probe needed, only the opt-in and the guard against re-forcing a
          // delete that was already forced.
          if (!tolerateBrokenChain || forceBrokenChainDelete)
            throw e;
          logBrokenChainForcePhysicalDelete(record.getIdentity(), e);
          graphEngine.deleteVertex((VertexInternal) record, true);
        } catch (final ConcurrentModificationException e) {
          // The physical removal can raise the #4932 retry signal even when index cleanup did not (e.g. the type has no
          // index left to read, so the broken chain is only discovered here). Fall back to force ONLY when the chain is
          // confirmed structurally broken; a genuine transient conflict (or an already-forced delete) rethrows to retry.
          if (!tolerateBrokenChain || forceBrokenChainDelete || !bucket.isChunkChainBroken(record.getIdentity()))
            throw e;
          logBrokenChainForcePhysicalDelete(record.getIdentity(), e);
          graphEngine.deleteVertex((VertexInternal) record, true);
        }
      } else {
        try {
          bucket.deleteRecord(record.getIdentity(), forceBrokenChainDelete);
        } catch (final ConcurrentModificationException e) {
          // NO BrokenChunkChainException ARM HERE, unlike the vertex branch above, and the asymmetry is real rather
          // than an omission (code review on #6258): deleteRecordInternal walks the chunk chain itself and never
          // loads the record, so it reports a break as the #4932 retry signal and the structural probe below is
          // still what tells the two apart. The vertex branch differs because reaching a vertex's edge lists means
          // READING it, which is where the loader's own verdict comes from. Add the arm here the day
          // deleteRecordInternal learns to name a broken chain as one.
          if (!tolerateBrokenChain || forceBrokenChainDelete || !bucket.isChunkChainBroken(record.getIdentity()))
            throw e;
          logBrokenChainForcePhysicalDelete(record.getIdentity(), e);
          bucket.deleteRecord(record.getIdentity(), true);
        }
      }

      success = true;
      stats.deleteRecord.incrementAndGet();

      // INVOKE EVENT CALLBACKS
      events.onAfterDelete(record);
      if (record instanceof Document document)
        ((RecordEventsRegistry) document.getType().getEvents()).onAfterDelete(record);

      final TransactionContext transaction = getTransaction();
      if (record.getIdentity().getPosition() >= 0)
        // A record-less RID (a lightweight edge) never allocated a record in the bucket, so there is nothing to
        // subtract. Folding a -1 anyway drifts the cached counter behind count(*), and the drift is persisted in
        // statistics.json, so it survives a reopen.
        transaction.updateBucketRecordDelta(bucket.getFileId(), -1);

    } finally {
      if (implicitTransaction) {
        if (success)
          wrappedDatabaseInstance.commit();
        else
          wrappedDatabaseInstance.rollback();
      }
    }
  }

  /** The INDEX/EXTERNAL cleanup could not read the record, so the delete proceeds without it. */
  private void logBrokenChainForceDelete(final RID rid, final Exception e) {
    LogManager.instance().log(this, Level.WARNING,
        "Cannot read record %s for index/external cleanup on delete (broken multi-page chunk chain): %s. Deleting the "
            + "record anyway; run a database check to repair any dangling index entries.", rid, e.getMessage());
  }

  /**
   * The PHYSICAL removal could not read the record - a vertex's edge lists, or the chunks to free. A different stage
   * from the index cleanup above, leaving different things behind, so it says so instead of reusing that message:
   * an operator running with {@code DELETE_TOLERATE_BROKEN_CHAIN} on was told to look for dangling index entries
   * when what survived was edges and orphaned chunks (code review on #6258).
   */
  private void logBrokenChainForcePhysicalDelete(final RID rid, final Exception e) {
    LogManager.instance().log(this, Level.WARNING,
        "Cannot read record %s to remove it physically (broken multi-page chunk chain): %s. Deleting it anyway; the "
            + "chunks it can no longer reach, and any edge left pointing at it, are repaired by a database check.",
        rid, e.getMessage());
  }

  /**
   * Deletes all external-bucket records referenced by the given document's TYPE_EXTERNAL property pointers, in the same
   * transaction as the primary delete. No-op if the type has no EXTERNAL properties or the document was not loaded with
   * a buffer.
   */
  private void cascadeDeleteExternalValues(final Document document) {
    if (!(document.getType() instanceof LocalDocumentType localType))
      return;
    if (!localType.hasExternalProperties())
      return;
    final Map<String, RID> externalRids = serializer.findExistingExternalRids(this, document);
    for (final RID extRid : externalRids.values()) {
      final LocalBucket externalBucket = schema.getBucketById(extRid.getBucketId(), false);
      if (externalBucket != null) {
        externalBucket.deleteRecord(extRid);
        // Keep the external bucket's count consistent (mirrors the +1 in BinarySerializer.writeExternalPropertyValue).
        getTransaction().updateBucketRecordDelta(externalBucket.getFileId(), -1);
      }
    }
  }

  @Override
  public boolean isTransactionActive() {
    final Transaction tx = getTransactionIfExists();
    return tx != null && tx.isActive();
  }

  @Override
  public LocalTransactionExplicitLock acquireLock() {
    checkTransactionIsActive(false);
    return getTransaction().lock();
  }

  @Override
  public void transaction(final TransactionScope txBlock) {
    transaction(txBlock, true, configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES), null, null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx) {
    return transaction(txBlock, joinCurrentTx, configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES), null,
        null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int attempts) {
    return transaction(txBlock, joinCurrentTx, attempts, null, null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, int attempts,
      final OkCallback ok,
      final ErrorCallback error) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    ArcadeDBException lastException = null;

    if (attempts < 1)
      attempts = 1;

    final int retryDelay = configuration.getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY);
    final int retryDelayBase = configuration.getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY_BASE);

    boolean duplicatedKeyRetried = false;

    for (int retry = 0; retry < attempts; ++retry) {
      boolean createdNewTx = true;

      try {
        if (joinCurrentTx && wrappedDatabaseInstance.isTransactionActive())
          createdNewTx = false;
        else
          wrappedDatabaseInstance.begin();

        txBlock.execute();

        if (createdNewTx && wrappedDatabaseInstance.isTransactionActive())
          wrappedDatabaseInstance.commit();

        if (ok != null)
          ok.call();

        // OK
        return createdNewTx;

      } catch (final NeedRetryException | DuplicatedKeyException e) {
        // #661: when we joined a transaction owned by the caller (createdNewTx == false) we must NOT retry
        // here. Retrying would roll back the caller's outer transaction - discarding work the caller did
        // before this call and resetting the RID of any records it created to null (surfacing later as
        // "Target vertex is not persistent" when a stale, now-unsaved record reference is reused) - and
        // re-running the block against the same conflicted state cannot succeed anyway. Propagate the
        // exception so the real transaction owner retries the whole logical unit with fresh bindings.
        if (!createdNewTx)
          throw e;

        // RETRY
        lastException = e;
        if (wrappedDatabaseInstance.isTransactionActive())
          wrappedDatabaseInstance.rollback();

        if (error != null)
          error.call(e);

        if (e instanceof DuplicatedKeyException) {
          // #4959: a genuine duplicate is deterministic and fails identically on every attempt. Only a
          // concurrency-induced duplicate can succeed on retry, and one retry is enough to disambiguate:
          // fail fast instead of burning all the remaining attempts plus their retry delays.
          //
          // #5061: a TRANSIENT duplicate from an in-flight sibling transaction that later rolls back
          // is unreachable - checkUniqueIndexKeys reads committed pages plus THIS transaction's own overlay
          // (TransactionIndexContext is per-transaction), so uncommitted sibling entries are invisible and a
          // detected duplicate is always against durable state (or this same transaction). The one retry
          // covers the only nondeterministic case: racing a COMMIT that lands between attempts.
          if (duplicatedKeyRetried)
            throw e;
          duplicatedKeyRetried = true;
        }

        if (retry < attempts - 1)
          delayBetweenRetries(retry, retryDelayBase, retryDelay);

      } catch (final Throwable e) {
        if (wrappedDatabaseInstance.isTransactionActive())
          wrappedDatabaseInstance.rollback();

        if (error != null)
          error.call(e);

        throw e;
      }
    }

    if (error != null)
      error.call(lastException);

    throw lastException;
  }

  @Override
  public RecordFactory getRecordFactory() {
    return recordFactory;
  }

  @Override
  public Schema getSchema() {
    checkDatabaseIsOpen();
    return schema;
  }

  @Override
  public BinarySerializer getSerializer() {
    return serializer;
  }

  @Override
  public PageManager getPageManager() {
    checkDatabaseIsOpen();
    return PageManager.INSTANCE;
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final LocalDocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(LocalDocumentType.class) && !(type instanceof LocalTimeSeriesType))
      throw new IllegalArgumentException("Cannot create a document of type '" + typeName + "' because is not a " +
          "document type");

    stats.createRecord.incrementAndGet();

    return new MutableDocument(wrappedDatabaseInstance, type, null);
  }

  @Override
  public MutableEmbeddedDocument newEmbeddedDocument(final EmbeddedModifier modifier, final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final LocalDocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(LocalDocumentType.class))
      throw new IllegalArgumentException(
          "Cannot create an embedded document of type '" + typeName + "' because it is a " + type.getClass().getName()
              + " instead of a document type ");

    return new MutableEmbeddedDocument(wrappedDatabaseInstance, type, modifier);
  }

  @Override
  public MutableVertex newVertex(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final LocalDocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(LocalVertexType.class))
      throw new IllegalArgumentException("Cannot create a vertex of type '" + typeName + "' because is not a vertex " +
          "type");

    stats.createRecord.incrementAndGet();

    return new MutableVertex(wrappedDatabaseInstance, (VertexType) type, null);
  }

  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames,
      final Object[] sourceVertexKeyValues, final String destinationVertexType,
      final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist,
      final String edgeType,
      final boolean bidirectional, final Object... properties) {
    if (sourceVertexKeyNames == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKeyNames.length != sourceVertexKeyValues.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKeyNames == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKeyNames.length != destinationVertexKeyValues.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<Identifiable> v1Result = lookupByKey(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues);

    final Vertex sourceVertex;
    if (!v1Result.hasNext()) {
      if (createVertexIfNotExist) {
        sourceVertex = newVertex(sourceVertexType);
        for (int i = 0; i < sourceVertexKeyNames.length; ++i)
          ((MutableVertex) sourceVertex).set(sourceVertexKeyNames[i], sourceVertexKeyValues[i]);
        ((MutableVertex) sourceVertex).save();
      } else
        throw new IllegalArgumentException(
            "Cannot find source vertex with key " + Arrays.toString(sourceVertexKeyNames) + "=" + Arrays.toString(
                sourceVertexKeyValues));
    } else
      sourceVertex = v1Result.next().getIdentity().asVertex();

    final Iterator<Identifiable> v2Result = lookupByKey(destinationVertexType, destinationVertexKeyNames,
        destinationVertexKeyValues);
    final Vertex destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKeyNames.length; ++i)
          ((MutableVertex) destinationVertex).set(destinationVertexKeyNames[i], destinationVertexKeyValues[i]);
        ((MutableVertex) destinationVertex).save();
      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKeyNames) + "=" + Arrays.toString(
                destinationVertexKeyValues));
    } else
      destinationVertex = v2Result.next().getIdentity().asVertex();

    stats.createRecord.incrementAndGet();

    return sourceVertex.newEdge(edgeType, destinationVertex, properties);
  }

  @Deprecated
  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType,
      final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist,
      final String edgeType,
      final boolean bidirectional, final Object... properties) {
    if (!bidirectional && schema.getType(edgeType) instanceof EdgeType type && type.isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues,
        createVertexIfNotExist, edgeType, properties);
  }

  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType,
      final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist,
      final String edgeType,
      final Object... properties) {
    if (sourceVertex == null)
      throw new IllegalArgumentException("Source vertex is null");

    if (destinationVertexKeyNames == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKeyNames.length != destinationVertexKeyValues.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<Identifiable> v2Result = lookupByKey(destinationVertexType, destinationVertexKeyNames,
        destinationVertexKeyValues);
    final Vertex destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKeyNames.length; ++i)
          ((MutableVertex) destinationVertex).set(destinationVertexKeyNames[i], destinationVertexKeyValues[i]);
        ((MutableVertex) destinationVertex).save();
      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKeyNames) + "=" + Arrays.toString(
                destinationVertexKeyValues));
    } else
      destinationVertex = v2Result.next().getIdentity().asVertex();

    stats.createRecord.incrementAndGet();

    return sourceVertex.newEdge(edgeType, destinationVertex, properties);
  }

  @Override
  public boolean isAutoTransaction() {
    return autoTransaction;
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    this.autoTransaction = autoTransaction;
  }

  @Override
  public FileManager getFileManager() {
    checkDatabaseIsOpen();
    return fileManager;
  }

  @Override
  public String getName() {
    return name;
  }

  /** Override root + dbName subdir, or null if not configured. The subdir prevents collisions across databases. */
  public String resolveExternalBucketPath() {
    final String configured = configuration.getValueAsString(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH);
    if (configured == null || configured.isEmpty())
      return null;
    return configured + File.separator + name;
  }

  @Override
  public ComponentFile.MODE getMode() {
    return mode;
  }

  @Override
  public boolean checkTransactionIsActive(final boolean createTx) {
    checkDatabaseIsOpen(true, "Cannot begin a transaction on a read only database");

    if (!isTransactionActive()) {
      if (createTx) {
        wrappedDatabaseInstance.begin();
        return true;
      }
      throw new TransactionException("Transaction not begun");
    }

    return false;
  }

  @Override
  public DocumentIndexer getIndexer() {
    return indexer;
  }

  @Override
  public QueryEngine getQueryEngine(final String language) {
    QueryEngine engine = reusableQueryEngines.get(language);
    if (engine == null) {
      engine = QueryEngineManager.getInstance().getEngine(language, this);
      if (engine.isReusable()) {
        final QueryEngine prev = reusableQueryEngines.putIfAbsent(language, engine);
        if (prev != null)
          engine = prev;
      }
    }

    return engine;
  }

  /**
   * Optimized overload for commands with no parameters - avoids varargs array allocation.
   */
  @Override
  public ResultSet command(final String language, final String query) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try (final QueryTracer.Span span = QueryTracer.Holder.begin(name, language, "command", query)) {
      return getQueryEngine(language).command(query, new ContextConfiguration());
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "command");
    }
  }

  @Override
  public ResultSet command(final String language, final String query, final Object... parameters) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try (final QueryTracer.Span span = QueryTracer.Holder.begin(name, language, "command", query)) {
      return getQueryEngine(language).command(query, new ContextConfiguration(), parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "command");
    }
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Object... parameters) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try (final QueryTracer.Span span = QueryTracer.Holder.begin(name, language, "command", query)) {
      return getQueryEngine(language).command(query, configuration, parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "command");
    }
  }

  @Override
  public ResultSet command(final String language, final String query, final Map<String, Object> parameters) {
    return command(language, query, new ContextConfiguration(), parameters);
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Map<String, Object> parameters) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try (final QueryTracer.Span span = QueryTracer.Holder.begin(name, language, "command", query)) {
      return getQueryEngine(language).command(query, configuration, parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "command");
    }
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Map<String, Object> params) {
    if (!"sql".equalsIgnoreCase(language))
      throw new CommandExecutionException("Language '" + language + "' does not support script");
    return command("sqlscript", script, params);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Object... args) {
    if (!"sqlscript".equalsIgnoreCase(language) && !"sql".equalsIgnoreCase(language))
      throw new CommandExecutionException("Language '" + language + "' does not support script");
    return command("sqlscript", script, args);
  }

  /**
   * Optimized overload for queries with no parameters - avoids varargs array allocation.
   */
  @Override
  public ResultSet query(final String language, final String query) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try (final QueryTracer.Span span = QueryTracer.Holder.begin(name, language, "query", query)) {
      return getQueryEngine(language).query(query, new ContextConfiguration());
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "query");
    }
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... parameters) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try (final QueryTracer.Span span = QueryTracer.Holder.begin(name, language, "query", query)) {
      return getQueryEngine(language).query(query, new ContextConfiguration(), parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "query");
    }
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> parameters) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try (final QueryTracer.Span span = QueryTracer.Holder.begin(name, language, "query", query)) {
      return getQueryEngine(language).query(query, new ContextConfiguration(), parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "query");
    }
  }

  @Override
  public Select select() {
    return new Select(this);
  }

  @Override
  public GraphBatch.Builder batch() {
    // Use the outermost wrapper so that commits flow through any HA/replication layer.
    // Without this, GraphBatch.commit() would short-circuit the Raft replication wrapper
    // installed by the HA plugin and writes would never reach followers (issue #4076).
    //
    // The guard owner is this instance and NOT the wrapper: the wrapper only delegates batch(),
    // so a release routed through it would never reach the flag below and the first batch would
    // lock out every later one on a replicated database (issue #5666).
    return GraphBatch.builder(wrappedDatabaseInstance, this);
  }

  /**
   * Reserves the single-batch slot of this database. Two concurrent {@link GraphBatch} instances on the same
   * database silently lose edges: the head pointer is deferred to close(), so the last writer wins and the
   * loser's segment chain is orphaned. Called by {@code GraphBatch.Builder.build()}, released by
   * {@link #batchFinished()}.
   *
   * @throws DatabaseOperationException if a batch is already open on this database
   */
  public void batchStarted() {
    if (!batchInProgress.compareAndSet(false, true))
      throw new DatabaseOperationException(
          "A GraphBatch is already in progress on this database. Concurrent batches silently lose edges. "
              + "Use a single GraphBatch (parallelFlush for parallel fan-out).");
  }

  /**
   * Releases the single-batch slot reserved by {@link #batchStarted()}. Idempotent, and safe to call on a
   * database that never opened a batch.
   */
  public void batchFinished() {
    batchInProgress.set(false);
  }

  @Override
  public int hashCode() {
    if (cachedHashCode == 0 && databasePath != null)
      cachedHashCode = databasePath.hashCode();
    return cachedHashCode;
  }

  /**
   * Returns true if two databases are the same.
   */
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Database other))
      return false;

    return Objects.equals(getDatabasePath(), other.getDatabasePath());
  }

  public DatabaseContext.DatabaseContextTL getContext() {
    return DatabaseContext.INSTANCE.getContext(databasePath);
  }

  public SecurityManager getSecurity() {
    return security;
  }

  /**
   * Executes a callback in a shared lock.
   */
  @Override
  public <RET> RET executeInReadLock(final Callable<RET> callable) {
    final ReentrantReadWriteLock.ReadLock readLock = readLock();
    try {

      return callable.call();

    } catch (final ClosedChannelException e) {
      LogManager.instance().log(this, Level.SEVERE, "Database '%s' has some files that are closed", e, name);
      close();
      throw new DatabaseOperationException("Database '" + name + "' has some files that are closed", e);

    } catch (final RuntimeException e) {
      throw e;

    } catch (final Throwable e) {
      throw new DatabaseOperationException("Error during read lock", e);

    } finally {
      readUnlock(readLock);
    }
  }

  /**
   * Executes a callback in an exclusive lock.
   */
  @Override
  public <RET> RET executeInWriteLock(final Callable<RET> callable) {
    final ReentrantReadWriteLock.WriteLock writeLock = writeLock();
    try {

      return callable.call();

    } catch (final ClosedChannelException e) {
      LogManager.instance().log(this, Level.SEVERE, "Database '%s' has some files that are closed", e, name);
      close();
      throw new DatabaseOperationException("Database '" + name + "' has some files that are closed", e);

    } catch (final RuntimeException e) {
      throw e;

    } catch (final Throwable e) {
      throw new DatabaseOperationException("Error during write lock", e);

    } finally {
      writeUnlock(writeLock);
    }
  }

  @Override
  public <RET> RET executeLockingFiles(final Collection<Integer> fileIds, Callable<RET> callable) {
    // #4959: file locks are keyed by requester (thread or session). Lock on behalf of the current
    // transaction's requester when one exists, so a thread acting for a session does not time out on locks
    // its own session already holds (a re-acquisition by the same requester is ALREADY_ACQUIRED and is not
    // released below, only the locks actually acquired here are). ACQUISITION PATH: captureRequester()
    // pins the identity on the owner thread (getTransactionIfExists resolves the CURRENT thread's tx, so
    // this thread IS the owner); see the INVARIANT on TransactionContext.requester (#4941).
    final TransactionContext tx = getTransactionIfExists();
    final Object requester = tx != null ? tx.captureRequester() : Thread.currentThread();

    List<Integer> lockedFiles = null;
    try {
      lockedFiles = transactionManager.tryLockFiles(fileIds, 5_000, requester);

      return callable.call();

    } catch (final RuntimeException e) {
      throw e;

    } catch (final Throwable e) {
      throw new DatabaseOperationException("Error during write lock", e);

    } finally {
      if (lockedFiles != null)
        transactionManager.unlockFilesInOrder(lockedFiles, requester);
    }
  }

  @Override
  public <RET> RET recordFileChanges(final Callable<Object> callback) {
    return (RET) executeInWriteLock(callback);
  }

  @Override
  public StatementCache getStatementCache() {
    return statementCache;
  }

  @Override
  public ExecutionPlanCache getExecutionPlanCache() {
    return executionPlanCache;
  }

  public CypherStatementCache getCypherStatementCache() {
    return cypherStatementCache;
  }

  public CypherPlanCache getCypherPlanCache() {
    return cypherPlanCache;
  }

  @Override
  public GraphStatisticsCache getGraphStatisticsCache() {
    return graphStatisticsCache;
  }

  @Override
  public WALFileFactory getWALFileFactory() {
    return walFactory;
  }

  @Override
  public void executeCallbacks(final CALLBACK_EVENT event) throws IOException {
    final List<Callable<Void>> callbacks = this.callbacks.get(event);
    if (callbacks != null && !callbacks.isEmpty()) {
      for (final Callable<Void> cb : callbacks) {
        try {
          cb.call();
        } catch (final RuntimeException | IOException e) {
          throw e;
        } catch (final Exception e) {
          throw new IOException("Error on executing test callback EVENT=" + event, e);
        }
      }
    }
  }

  public File getConfigurationFile() {
    return configurationFile;
  }

  @Override
  public DatabaseInternal getEmbedded() {
    return this;
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public Map<String, Object> alignToReplicas() {
    throw new UnsupportedOperationException("Align Database not supported");
  }

  @Override
  public DatabaseInternal getWrappedDatabaseInstance() {
    return wrappedDatabaseInstance;
  }

  public void setWrappedDatabaseInstance(final DatabaseInternal wrappedDatabaseInstance) {
    this.wrappedDatabaseInstance = wrappedDatabaseInstance;
  }

  public void registerReusableQueryEngine(final QueryEngine queryEngine) {
    reusableQueryEngines.put(queryEngine.getLanguage(), queryEngine);
  }

  public Map<String, Object> getWrappers() {
    return wrappers;
  }

  public void setWrapper(final String name, final Object instance) {
    if (instance == null)
      this.wrappers.remove(name);
    else
      this.wrappers.put(name, instance);
  }

  @Override
  public Object getGlobalVariable(String name) {
    if (name == null)
      return null;
    if (name.startsWith("$"))
      name = name.substring(1);
    return globalVariables.get(name);
  }

  @Override
  public Object setGlobalVariable(String name, final Object value) {
    if (name == null)
      throw new IllegalArgumentException("Variable name cannot be null");
    if (name.startsWith("$"))
      name = name.substring(1);
    SQLQueryEngine.validateVariableName(name);
    if (value == null)
      return globalVariables.remove(name);
    return globalVariables.put(name, value);
  }

  @Override
  public Object setGlobalVariableIfAbsent(String name, final Object value) {
    if (name == null)
      throw new IllegalArgumentException("Variable name cannot be null");
    if (name.startsWith("$"))
      name = name.substring(1);
    SQLQueryEngine.validateVariableName(name);
    return globalVariables.putIfAbsent(name, value);
  }

  @Override
  public Object setGlobalVariableIfPresent(String name, final Object value) {
    if (name == null)
      throw new IllegalArgumentException("Variable name cannot be null");
    if (name.startsWith("$"))
      name = name.substring(1);
    SQLQueryEngine.validateVariableName(name);
    final Object[] previous = new Object[1];
    globalVariables.computeIfPresent(name, (key, current) -> {
      previous[0] = current;
      return value;
    });
    return previous[0];
  }

  @Override
  public Map<String, Object> getGlobalVariables() {
    return CollectionUtils.immutableMap(globalVariables);
  }

  public QueryEngineManager getQueryEngineManager() {
    return QueryEngineManager.getInstance();
  }

  @Override
  public long getLastUpdatedOn() {
    return lastUpdatedOn;
  }

  @Override
  public long getLastUsedOn() {
    return lastUsedOn;
  }

  @Override
  public long getOpenedOn() {
    return openedOn;
  }

  public void saveConfiguration() throws IOException {
    FileUtils.writeFile(configurationFile, configuration.toJSON());
  }

  /**
   * Fires the AFTER READ listeners, under the same re-entrancy guard as the BEFORE READ ones - see
   * {@code LocalBucket.fireBeforeReadEvents}, which owns the explanation.
   * <p>
   * This side receives the materialised record, so it does not have to load anything itself and the shipped trigger
   * adapter never did. The body of an {@code AFTER READ} trigger is arbitrary SQL or JavaScript though, and one that
   * reads its own type re-enters the read that fired it exactly as the before side would. ONE flag covers both
   * directions because it answers one question - "is a read listener running on this thread for this database?" -
   * and while one is, no read event of either kind should fire.
   */
  @Override
  public Record invokeAfterReadEvents(Record record) {
    final DocumentType recordType = record instanceof Document document ? document.getType() : null;
    final RecordEventsRegistry typeEvents = recordType != null ? (RecordEventsRegistry) recordType.getEvents() : null;

    if (!events.hasAfterReadListeners() && (typeEvents == null || !typeEvents.hasAfterReadListeners()))
      return record;

    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
    if (context == null)
      return dispatchAfterRead(record, typeEvents);

    if (context.isFiringReadEvents())
      return record;

    context.setFiringReadEvents(true);
    try {
      return dispatchAfterRead(record, typeEvents);
    } finally {
      context.setFiringReadEvents(false);
    }
  }

  private Record dispatchAfterRead(Record record, final RecordEventsRegistry typeEvents) {
    // INVOKE EVENT CALLBACKS
    record = events.onAfterRead(record);
    if (record == null)
      return null;
    return typeEvents != null ? typeEvents.onAfterRead(record) : record;
  }

  private void lockDatabase() {
    try {
      lockFileIO = new RandomAccessFile(lockFile, "rw");
      lockFileIOChannel = lockFileIO.getChannel();
      lockFileLock = lockFileIOChannel.tryLock();
      if (lockFileLock == null) {
        lockFileIOChannel.close();
        lockFileIO.close();
        throw new LockException(
            "Database '" + name + "' is locked by another process (path=" + new File(databasePath).getAbsolutePath() + ")");
      }

      //LogManager.instance().log(this, Level.INFO, "LOCKED DATABASE FILE '%s' (thread=%s)", null, lockFile, Thread
      // .currentThread().getId());

    } catch (final Exception e) {
      try {
        if (lockFileIOChannel != null)
          lockFileIOChannel.close();
        if (lockFileIO != null)
          lockFileIO.close();
      } catch (final Exception e2) {
        // IGNORE
      }

      throw new LockException(
          "Database '" + name + "' is locked by another process (path=" + new File(databasePath).getAbsolutePath() +
              ")", e);
    }
  }

  private void checkDatabaseName() {
    if (name.contains("*") || name.contains(".."))
      throw new IllegalArgumentException("Invalid characters used in database name '" + name + "'");
  }

  private void closeInternal(final boolean drop) {
    // Graceful async drain FIRST, with the caller's interrupt flag INTACT so an interrupted caller bails
    // this wait fast; the warning distinguishes an interrupt from a real timeout.
    if (async != null) {
      try {
        // EXECUTE OUTSIDE LOCK
        // #5080: bound the graceful drain so a worker wedged inside a user task or callback cannot make
        // close()/drop() hang forever. On expiry, closeDurableParts() below force-shuts the workers (that
        // path is itself bounded: FORCE_EXIT offer + interrupt + a ~10s join, escalated to a second one).
        final long asyncCloseTimeout = configuration.getValueAsLong(GlobalConfiguration.ASYNC_CLOSE_TIMEOUT);
        if (!async.waitCompletion(asyncCloseTimeout)) {
          // waitCompletion also returns false when the caller thread is interrupted (it re-sets the flag
          // and returns), not only on timeout - distinguish the two so the message is accurate and never
          // prints "within 0 ms" for the wait-forever (interrupt) case.
          if (Thread.currentThread().isInterrupted())
            LogManager.instance().log(this, Level.WARNING, """
                Interrupted while draining the asynchronous tasks of database '%s' on close: forcing the \
                async workers down. A task blocked inside user code may not have completed""", name);
          else
            LogManager.instance().log(this, Level.WARNING, """
                Asynchronous tasks of database '%s' did not drain within %d ms on close: forcing the async \
                workers down. A task blocked inside user code may not have completed""", name, asyncCloseTimeout);
        }
      } catch (final Throwable e) {
        LogManager.instance()
            .log(this, Level.WARNING, """
                Error while draining the asynchronous manager during closing operation for database \
                '%s'""", e, name);
      }
    }

    // #5105 review: the DURABLE part of the close (async force-shutdown + the page flush in
    // executeInWriteLock) uses INTERRUPTIBLE steps - the bounded FORCE_EXIT offer/join, and PageManager
    // throws InterruptedIOException on a set flag. A set interrupt would therefore skip interrupting a
    // wedged worker AND truncate the flush into a needless crash-equivalent close. Run it all with the
    // flag CLEARED (unconditionally, so async == null is covered too) and restore it for the caller after.
    final boolean callerWasInterrupted = Thread.interrupted();
    try {
      closeDurableParts(drop);
    } finally {
      if (callerWasInterrupted)
        Thread.currentThread().interrupt();
    }
  }

  private void closeDurableParts(final boolean drop) {
    if (async != null) {
      try {
        async.close();
      } catch (final Throwable e) {
        LogManager.instance()
            .log(this, Level.WARNING, """
                Error on stopping asynchronous manager during closing operation for database \
                '%s'""", e, name);
      }
    }

    for (final Index idx : schema.getIndexes()) {
      final IndexInternal index = (IndexInternal) idx;

      if (!drop) {
        // FLUSH ALL INDEXES WHILE THE DATABASE IS STILL OPEN
        try {
          index.flush();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on flushing index %s: %s", e, idx.getName(),
              e.getMessage());
        }
      }

      // #5418: then stop whatever this index keeps running in the BACKGROUND. Nothing did before, so the vector
      // index inactivity rebuild timer outlived Database.close() and fired minutes later against a closed - or
      // by then already deleted - database: DatabaseIsClosedException, page-parse errors on dropped files, and
      // a race with JVM shutdown. Deliberately AFTER the flush above (an index whose graceful shutdown is a
      // graph build needs its build pool alive for it) and deliberately NOT index.close(), which also closes
      // the index files: those must stay open until the page flush in the write-lock section below has run,
      // and it is fileManager.close() that closes them, once, at the right point.
      try {
        index.releaseBackgroundResources();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error on releasing the background resources of index %s: %s", e,
            idx.getName(), e.getMessage());
      }
    }

    // Shutdown all Graph Analytical Views before closing the database
    try {
      GraphAnalyticalViewRegistry.shutdownAll(this);
    } catch (final Throwable e) {
      LogManager.instance().log(this, Level.WARNING,
          "Error on shutting down Graph Analytical Views during close for database '%s'", e, name);
    } finally {
      // Safety net: clear any orphaned traversal providers that were not cleaned up
      // by individual view shutdown() calls (e.g., if a view was registered directly
      // in GraphTraversalProviderRegistry without being in GraphAnalyticalViewRegistry)
      GraphTraversalProviderRegistry.clearAll(this);
    }

    executeInWriteLock(() -> {
      if (!open)
        return null;

      try {
        if (async != null)
          async.close();
      } catch (final Throwable e) {
        LogManager.instance()
            .log(this, Level.WARNING, """
                Error on stopping asynchronous manager during closing operation for database \
                '%s'""", e, name);
      }

      if (drop)
        // NOT redundant with the unconditional purge further down (#6133), and it has to stay BEFORE the wait:
        // the files of a dropped database are about to be deleted, so its queued pages must leave the pipeline
        // here or the wait below would sit through a backlog that nobody will ever need on disk. The later call
        // then finds nothing left to purge and only does the forgetting, which is all a drop still needs from it.
        PageManager.INSTANCE.removeModifiedPagesOfDatabase(this);

      // #4928: bounded wait. When it gives up (wedged flush / unwritable disk), this close becomes
      // CRASH-EQUIVALENT: the WAL files and the lock file are preserved below, so the next open runs
      // recovery and replays the pages that never reached the disk, instead of this close silently
      // deleting the only durable copy of them.
      boolean dataSafeOnDisk = PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(this);

      if (!drop && !fileManager.syncFiles()) {
        // #4934: the fsync failed, so the OS may have dropped the dirty pages (fsyncgate semantics) - the
        // pages the wait above considered flushed may never reach the platter. Deleting the WAL below would
        // then make the committed data unrecoverable after a power loss, with only a log line as evidence.
        // Treat the close as crash-equivalent instead: preserve WAL + lock file, recover on next open.
        dataSafeOnDisk = false;
      }

      final boolean preserveWalForRecovery = !dataSafeOnDisk && !drop;

      open = false;

      // #4928: the give-up close leaves the stuck pages in the shared flush thread's index, referencing a
      // now-closed database - they can never be flushed once open=false (flushPage early-returns). Purge them
      // so the JVM-wide flush thread does not leak entries; their content is safe in the preserved WAL.
      // #6133: done on EVERY close, not only the give-up one. On a clean close the page purge itself is a
      // no-op - the wait above proved this database's pipeline empty - but this call is also where the
      // JVM-wide flush thread FORGETS the database: its suspend and replay-drain locks, its deferred batches,
      // its flush-progress counter and its pending-page counter are all keyed by the Database instance, as is
      // the page manager's snapshot barrier monitor. Skipping it on the common path pinned one dead
      // LocalDatabase (and everything it references) per closed database for the lifetime of
      // PageManager.INSTANCE, which any process cycling through databases pays forever.
      PageManager.INSTANCE.removeModifiedPagesOfDatabase(this);

      PageManager.INSTANCE.removeAllReadPagesOfDatabase(this);

      try {
        final List<DatabaseContext.DatabaseContextTL> dbContexts =
            DatabaseContext.INSTANCE.removeAllContexts(databasePath);
        for (DatabaseContext.DatabaseContextTL dbContext : dbContexts) {
          if (!dbContext.transactions.isEmpty()) {
            // ROLLBACK ALL THE TX FROM LAST TO FIRST
            for (int i = dbContext.transactions.size() - 1; i > -1; --i) {
              final TransactionContext tx = dbContext.transactions.get(i);
              if (tx.isActive())
                // ROLLBACK ANY PENDING OPERATION
                tx.rollback();
            }
            dbContext.transactions.clear();
          }
        }
      } catch (final Throwable e) {
        LogManager.instance()
            .log(this, Level.WARNING, """
                Error on clearing transaction status during closing operation for database \
                '%s'""", e, name);
      }

      for (QueryEngine e : reusableQueryEngines.values())
        e.close();

      // Whether the WAL was ACTUALLY preserved: either this close's flush wait gave up, or the
      // TransactionManager found unacked WAL pages (a contained flush failure, #4928). Drives the lock-file
      // decision below so the next open runs recovery exactly when there is something to recover.
      boolean walPreservedForRecovery = preserveWalForRecovery;
      try {
        schema.close();
        fileManager.close();
        walPreservedForRecovery = transactionManager.close(drop, preserveWalForRecovery);
        statementCache.clear();
        reusableQueryEngines.clear();

      } catch (final Throwable e) {
        LogManager.instance()
            .log(this, Level.WARNING, """
                Error on closing internal components during closing operation for database \
                '%s'""", e, name);
      } finally {
        Profiler.INSTANCE.unregisterDatabase(LocalDatabase.this);
      }

      if (lockFile != null) {
        try {
          if (lockFileLock != null) {
            lockFileLock.release();
            //LogManager.instance().log(this, Level.INFO, "RELEASED DATABASE FILE '%s' (thread=%s)", null, lockFile,
            // Thread.currentThread().getId());
          }
          if (lockFileIOChannel != null)
            lockFileIOChannel.close();
          if (lockFileIO != null)
            lockFileIO.close();

          if (walPreservedForRecovery)
            // #4928: leave the lock file as the unclean-shutdown marker - together with the preserved WAL it
            // makes the next open run recovery and replay the pages this close could not flush.
            LogManager.instance().log(this, Level.SEVERE,
                "Database '%s' closed with unflushed pages: the lock file and the WAL were preserved so the next open will recover them",
                null, name);
          else {
            if (lockFile.exists())
              Files.delete(Path.of(lockFile.getAbsolutePath()));

            if (lockFile.exists() && !lockFile.delete())
              LogManager.instance().log(this, Level.WARNING, "Error on deleting lock file '%s'", lockFile);
          }
        } catch (final IOException e) {
          // IGNORE IT
          LogManager.instance().log(this, Level.WARNING, "Error on deleting lock file '%s'", e, lockFile);
        }
      }

      return null;
    });

    // Unconditional on purpose: a KILLED database (crash simulation) reaches close() with open == false and
    // must still unregister - removeActiveDatabaseInstance is naturally idempotent (false on the second
    // call), which is exactly how the pre-#4927 code stayed double-close-safe. The executor teardown stays
    // on the map-emptiness heuristic DELIBERATELY (#5070): unlike the flush thread, a shutdown
    // executor lazily re-creates itself on the next getExecutor(), so the mid-flight-open race self-heals.
    // A redundant double-close of the LAST database calls it twice (the empty map returns true again):
    // harmless, closeExecutor() is idempotent (early-returns on null/isShutdown under its class lock).
    if (DatabaseFactory.removeActiveDatabaseInstance(databasePath, this))
      GraphAnalyticalView.closeExecutor();

    // #4927: paired with the acquire in DatabaseFactory.open/create - the flush machinery is torn down by
    // the refcount reaching zero, never by the racy "was this the last registered instance" check (an open
    // in flight on another thread holds a reference before it registers, so it can no longer be pulled out
    // from under). The atomic flag makes the release EXACTLY ONCE per database instance (#5070): a
    // redundant double-close cannot steal another database's reference, and when registerActiveInstance
    // closes a same-path open race loser, the factory's catch sees the flag and does not release again.
    if (pageManagerReferenceReleased.compareAndSet(false, true))
      PageManager.INSTANCE.release();
  }

  /** #4927/#5070: whether this instance already consumed its PageManager lifecycle reference (see closeInternal). */
  private final AtomicBoolean pageManagerReferenceReleased = new AtomicBoolean(false);

  boolean isPageManagerReferenceReleased() {
    return pageManagerReferenceReleased.get();
  }

  /**
   * Takes ownership of the database before any of its content is touched: acquires the exclusive lock on
   * {@code database.lck} and settles the read-only rejection. Runs BEFORE the schema is loaded, because loading a
   * database that needs recovery writes to it - the sorted-index-build marker cleanup drops component files, and a
   * dictionary whose page never reached disk has its header page written and committed. Doing that ahead of the lock
   * let an open mutate a database another process owns.
   *
   * @return {@code true} when the marker file was present, so the WAL still has to be replayed by
   * {@link #performRecovery()} once the schema is loaded.
   */
  private boolean prepareRecovery() throws IOException {
    lockFile = new File(databasePath + "/database.lck");

    if (lockFile.exists()) {
      if (mode == ComponentFile.MODE.READ_ONLY) {
        // A READ_ONLY open cannot perform recovery: reject WITHOUT acquiring the exclusive write lock, so the OS file
        // lock on database.lck is never taken (and therefore cannot be leaked). The marker is left untouched so the
        // next READ_WRITE open still recovers.
        lockFile = null;
        throw new DatabaseMetadataException("Database needs recovery but has been open in read only mode");
      }

      lockDatabase();
      return true;
    }

    if (mode == ComponentFile.MODE.READ_WRITE) {
      lockFile.createNewFile();
      lockDatabase();
    } else
      lockFile = null;

    return false;
  }

  /**
   * Replays the WAL of a database that was not closed properly. Runs after the schema is loaded, because the replay
   * resolves file ids and the dictionary through the registered components.
   */
  private void performRecovery() throws IOException {
    LogManager.instance().log(this, Level.WARNING, "Database '%s' was not closed properly last time", null, name);

    // RESET THE COUNT OF RECORD IN CASE THE DATABASE WAS NOT CLOSED PROPERLY
    for (Bucket b : schema.getBuckets())
      ((LocalBucket) b).setCachedRecordCount(-1);

    executeCallbacks(CALLBACK_EVENT.DB_NOT_CLOSED);

    transactionManager.checkIntegrity();
  }

  /** Removes any {@code .pshadow} scratch file left behind by a snapshot window that a crash interrupted (#6075). */
  private void deleteOrphanSnapshotShadows() {
    final File[] orphans = new File(databasePath).listFiles(
        (dir, name) -> name.endsWith("." + PageSnapshot.SHADOW_FILE_EXT));
    if (orphans == null)
      return;
    for (final File orphan : orphans) {
      LogManager.instance().log(this, Level.FINE, "Deleting orphan snapshot shadow file '%s'", null, orphan.getName());
      if (!orphan.delete())
        LogManager.instance().log(this, Level.WARNING, "Cannot delete the orphan snapshot shadow file '%s'", null, orphan);
    }
  }

  private void openInternal() {
    try {
      DatabaseContext.INSTANCE.init(this);
      setLockingEnabled(configuration.getValueAsBoolean(GlobalConfiguration.BACKUP_ENABLED));

      // #6075 (challenge C8): the copy-on-write shadow of a snapshot window is pure scratch - recovery never reads
      // it - so a crash mid-window leaves nothing but an orphan file to delete here. Its extension is deliberately
      // absent from SUPPORTED_FILE_EXT, so the FileManager scan below never mistakes one for a data file.
      deleteOrphanSnapshotShadows();

      fileManager = new FileManager(databasePath, mode, SUPPORTED_FILE_EXT, resolveExternalBucketPath());
      fileManager.setDroppedFileHandler(file -> PageManager.INSTANCE.deferFileDrop(wrappedDatabaseInstance, file));
      transactionManager = new TransactionManager(wrappedDatabaseInstance);

      open = true;

      try {
        schema = new LocalSchema(wrappedDatabaseInstance, databasePath, security);

        // OWN THE DATABASE BEFORE READING IT: LOADING THE SCHEMA OF A CRASHED DATABASE WRITES TO IT.
        final boolean recoveryPending = prepareRecovery();

        if (fileManager.getFiles().isEmpty())
          schema.create(mode);
        else
          schema.load(mode, true);

        serializer.setDateImplementation(configuration.getValue(GlobalConfiguration.DATE_IMPLEMENTATION));
        serializer.setDateTimeImplementation(configuration.getValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION));

        if (recoveryPending)
          performRecovery();

        if (security != null)
          security.updateSchema(this);

        openedOn = lastUsedOn = lastUpdatedOn = System.currentTimeMillis();

        Profiler.INSTANCE.registerDatabase(this);

      } catch (final RuntimeException e) {
        open = false;
        PageManager.INSTANCE.removeAllReadPagesOfDatabase(this);
        throw e;
      } catch (final Exception e) {
        open = false;
        PageManager.INSTANCE.removeAllReadPagesOfDatabase(this);
        throw new DatabaseOperationException("Error on creating new database instance", e);
      }
    } catch (final Exception e) {
      open = false;

      // ISSUE #4511: RELEASE THE FILE LOCK AND CLOSE THE I/O RESOURCES ACQUIRED BEFORE THE FAILURE, OTHERWISE THE
      // DATABASE STAYS PERMANENTLY UNOPENABLE WITHIN THIS JVM (AND THE LOCK FILE CANNOT BE REMOVED ON WINDOWS).
      releaseResourcesOnOpenFailure();

      if (e instanceof DatabaseOperationException exception)
        throw exception;

      // PRESERVE THE READ_ONLY-NEEDS-RECOVERY REJECTION (AND ANY OTHER METADATA ERROR) SO THE CALLER SEES THE REASON
      // INSTEAD OF AN OPAQUE WRAPPED MESSAGE.
      if (e instanceof DatabaseMetadataException exception)
        throw exception;

      throw new DatabaseOperationException("Error on creating new database instance", e);
    }
  }

  /**
   * Releases the resources acquired during {@link #openInternal()} when the open fails partway through (issue #4511).
   * In particular it releases the JVM file lock and closes the lock-file I/O channels, the {@link FileManager} and the
   * {@link TransactionManager}. The {@code database.lck} marker is intentionally left on disk so the next open still
   * performs recovery. Every step is best-effort and isolated so a failure in one does not skip the others.
   */
  private void releaseResourcesOnOpenFailure() {
    try {
      if (lockFile != null) {
        if (lockFileLock != null) {
          lockFileLock.release();
          lockFileLock = null;
        }
        if (lockFileIOChannel != null) {
          lockFileIOChannel.close();
          lockFileIOChannel = null;
        }
        if (lockFileIO != null) {
          lockFileIO.close();
          lockFileIO = null;
        }
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on releasing lock file '%s' after a failed open", e, lockFile);
    }

    try {
      if (fileManager != null)
        fileManager.close();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on closing file manager after a failed open of database '%s'", e, name);
    }

    try {
      if (transactionManager != null)
        // preserveWalFiles: an open that failed is not a clean close and must never delete the WAL. The
        // deletion branch is directory-wide and mode-blind, so a rejected open (a READ_ONLY open of a
        // database needing recovery, for instance) would otherwise remove the very WAL files the next
        // recovery-capable open needs to replay - discarding every change that had not yet reached the
        // data files. This instance may not even own a WAL pool; it never owns the right to delete one.
        transactionManager.close(false, true);
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.WARNING, "Error on closing transaction manager after a failed open of database '%s'", e, name);
    }
  }

  /**
   * #5053: set when a commit fails AFTER its transaction was appended to the WAL (the point of no
   * return) but BEFORE its pages were published. From that moment the WAL and the live state diverge: the
   * transaction is durable (recovery will replay it) but invisible, and letting new transactions run would
   * let them bump the same page versions and append conflicting WAL records for the same target versions.
   * Fencing turns the divergence into the same crash-equivalent close/reopen cycle used elsewhere (#4928):
   * the orphaned record's pages were never flush-acked, so the ack-gated close preserves the WAL and the
   * lock file, and the next open replays it.
   * <p>
   * HA note: on a replica this fences the wrapped LocalDatabase too, halting replication apply until the
   * node restarts - intended: the fence surfaces exactly like a crash, and the standard restart
   * reconciliation (recovery replay, or snapshot re-install via the DatabaseReconciler) repairs the node.
   */
  private volatile String fenceReason = null;

  public void fenceForRecovery(final String reason) {
    fenceForRecovery(reason, null);
  }

  /**
   * Same as {@link #fenceForRecovery(String)}, plus the exception that crossed the WAL point of no return, if
   * one is known. Fencing is one of the most serious events a database can log - without the cause attached
   * here, the only place that exception was otherwise recorded is {@code TransactionContext.commit2ndPhase}'s
   * generic catch, at FINE (invisible under any default logging configuration): a fenced database with no
   * visible reason (#6505).
   */
  public void fenceForRecovery(final String reason, final Throwable cause) {
    if (fenceReason == null) {
      fenceReason = reason;
      LogManager.instance().log(this, Level.SEVERE,
          "Database '%s' fenced for recovery: %s. Close and reopen the database to replay the WAL", cause, name, reason);
    }
  }

  public boolean isFencedForRecovery() {
    return fenceReason != null;
  }

  protected void checkDatabaseIsOpen() {
    checkDatabaseIsOpen(false, null);
  }

  protected void checkDatabaseIsOpen(final boolean updateIntent, final String databaseReadOnlyErrorMessage) {
    if (!open)
      throw new DatabaseIsClosedException(name);
    if (fenceReason != null)
      // #5053: a commit failed AFTER its WAL append - the WAL holds a record whose pages were never
      // published, so the live in-memory state diverges from what recovery will reconstruct. Every further
      // operation is fenced until the database is closed (the close-time ack gate preserves the WAL, since
      // the orphaned record's pages were never flush-acked) and reopened, which replays the record.
      throw new DatabaseOperationException(
          "Database '" + name + "' is fenced after a failure past the WAL commit point: close and reopen the database to run recovery ("
              + fenceReason + ")");
    if (DatabaseContext.INSTANCE.getContextIfExists(databasePath) == null)
      DatabaseContext.INSTANCE.init(this);

    final long now = System.currentTimeMillis();

    if (updateIntent) {
      if (mode == ComponentFile.MODE.READ_ONLY)
        throw new DatabaseIsReadOnlyException(databaseReadOnlyErrorMessage);

      lastUpdatedOn = now;
    }

    lastUsedOn = now;
  }

  private void setDefaultValues(final Record record) {
    // The rule lives in DocumentType.applyDefaultValues, shared with ApplyDefaultsStep, so the two cannot diverge
    // again on what a null-evaluating default means (issue #6134).
    if (record instanceof MutableDocument doc)
      doc.getType().applyDefaultValues(doc);
  }

  /**
   * Sleeps an exponential-backoff-with-full-jitter interval before the next transaction retry (issue #5587):
   * the window widens with every failed {@code attempt} instead of staying flat, so a transaction that has
   * already lost several races backs off further than one on its first attempt. See {@link RetryBackoff} for
   * the shared policy.
   *
   * @param attempt        zero-based count of retries already performed in this transaction
   * @param retryDelayBase the backoff window's starting size ({@link GlobalConfiguration#TX_RETRY_DELAY_BASE})
   * @param retryDelayCap  the backoff window's cap, and the on/off switch ({@link
   *                       GlobalConfiguration#TX_RETRY_DELAY})
   */
  private void delayBetweenRetries(final int attempt, final int retryDelayBase, final int retryDelayCap) {
    if (retryDelayCap > 0) {
      LogManager.instance()
          .log(this, Level.FINE, "Wait up to %d ms before the next retry for transaction commit (attempt=%d, threadId=%d)",
              RetryBackoff.windowMs(attempt, retryDelayBase, retryDelayCap), attempt + 1, Thread.currentThread().getId());

      RetryBackoff.sleep(attempt, retryDelayBase, retryDelayCap);
    }
  }
}
