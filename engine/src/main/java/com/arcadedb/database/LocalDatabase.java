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
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.engine.WALFileFactoryEmbedded;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.InvalidDatabaseInstanceException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexGraphFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.opencypher.query.CypherPlanCache;
import com.arcadedb.query.opencypher.query.CypherStatementCache;
import com.arcadedb.query.select.Select;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockException;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.RWLockContext;

import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;
import java.util.stream.*;

/**
 * Local implementation of {@link Database}. It is based on files opened on the local file system.
 * <p>
 * Thread safe and therefore the same instance can be shared among threads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalDatabase extends RWLockContext implements DatabaseInternal {
  public static final  int                                       EDGE_LIST_INITIAL_CHUNK_SIZE         = 64;
  public static final  int                                       MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE = 8192;
  private static final Set<String>                               SUPPORTED_FILE_EXT                   = Set.of(
      Dictionary.DICT_EXT,
      LocalBucket.BUCKET_EXT,
      LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT,
      LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
      LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT,
      LSMTreeIndexCompacted.UNIQUE_INDEX_EXT,
      LSMVectorIndex.FILE_EXT,
      LSMVectorIndexGraphFile.FILE_EXT);
  public final         AtomicLong                                indexCompactions                     = new AtomicLong();
  protected final      String                                    name;
  protected final      ComponentFile.MODE                        mode;
  protected final      ContextConfiguration                      configuration;
  protected final      String                                    databasePath;
  protected final      BinarySerializer                          serializer;
  protected final      RecordFactory                             recordFactory                        = new RecordFactory();
  protected final      GraphEngine                               graphEngine;
  protected final      WALFileFactory                            walFactory;
  protected final      DocumentIndexer                           indexer;
  protected final      QueryEngineManager                        queryEngineManager;
  protected final      DatabaseStats                             stats                                = new DatabaseStats();
  protected            FileManager                               fileManager;
  protected            LocalSchema                               schema;
  protected            TransactionManager                        transactionManager;
  protected volatile   DatabaseAsyncExecutorImpl                 async                                = null;
  protected final      Lock                                      asyncLock                            = new ReentrantLock();
  protected            boolean                                   autoTransaction                      = false;
  protected volatile   boolean                                   open                                 = false;
  private              boolean                                   readYourWrites                       = true;
  private final        Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks;
  private final        StatementCache                            statementCache;
  private final        ExecutionPlanCache                        executionPlanCache;
  private final        CypherStatementCache cypherStatementCache;
  private final        CypherPlanCache      cypherPlanCache;
  private final        File                                      configurationFile;
  private              DatabaseInternal                          wrappedDatabaseInstance              = this;
  private              int                                       edgeListSize                         = EDGE_LIST_INITIAL_CHUNK_SIZE;
  private final        SecurityManager                           security;
  private final        Map<String, Object>                       wrappers                             = new HashMap<>();
  private              File                                      lockFile;
  private              RandomAccessFile                          lockFileIO;
  private              FileChannel                               lockFileIOChannel;
  private              FileLock                                  lockFileLock;
  private final        RecordEventsRegistry                      events                               = new RecordEventsRegistry();
  private final        ConcurrentHashMap<String, QueryEngine>    reusableQueryEngines                 = new ConcurrentHashMap<>();
  private              TRANSACTION_ISOLATION_LEVEL               transactionIsolationLevel            = TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
  private              long                                      openedOn;
  private              long                                      lastUpdatedOn;
  private              long                                      lastUsedOn;
  private              int                                       cachedHashCode                       = 0;

  protected LocalDatabase(final String path, final ComponentFile.MODE mode, final ContextConfiguration configuration,
      final SecurityManager security, final Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks) {
    try {
      this.mode = mode;
      this.configuration = configuration;
      this.security = security;
      this.callbacks = callbacks;
      this.serializer = new BinarySerializer(configuration);
      this.walFactory = mode == ComponentFile.MODE.READ_WRITE ? new WALFileFactoryEmbedded() : null;
      this.statementCache = new StatementCache(this, configuration.getValueAsInteger(GlobalConfiguration.SQL_STATEMENT_CACHE));
      this.executionPlanCache = new ExecutionPlanCache(this,
          configuration.getValueAsInteger(GlobalConfiguration.SQL_STATEMENT_CACHE));
      this.cypherStatementCache = new CypherStatementCache(this,
          configuration.getValueAsInteger(GlobalConfiguration.OPENCYPHER_STATEMENT_CACHE));
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
      queryEngineManager = new QueryEngineManager();
      graphEngine = new GraphEngine(this);

    } catch (DatabaseOperationException e) {
      throw e;
    } catch (Exception e) {
      throw new DatabaseOperationException("Error on creating new database instance", e);
    }
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
        LogManager.instance().log(this, Level.SEVERE, "Error on loading configuration from file '%s'", e, configurationFile);
      }
    }

    openInternal();
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
    checkDatabaseIsOpen(true, "Cannot drop database");

    if (isTransactionActive())
      throw new DatabaseOperationException("Cannot drop the database in transaction");

    closeInternal(true);

    executeInWriteLock(() -> {
      FileUtils.deleteRecursively(new File(databasePath));
      return null;
    });
  }

  @Override
  public void close() {
    closeInternal(false);
  }

  /**
   * Test only API. Simulates a forced kill of the JVM leaving the database with the .lck file on the file system.
   */
  @Override
  public void kill() {
    if (async != null)
      async.kill();

    if (getTransaction().isActive())
      // ROLLBACK ANY PENDING OPERATION
      getTransaction().kill();

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
        throw new DatabaseOperationException("Error calculating database size", e.getCause());
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

  public void incrementStatsTxCommits() {
    stats.txCommits.incrementAndGet();
  }

  @Override
  public void commit() {
    stats.txCommits.incrementAndGet();

    executeInReadLock(() -> {
      checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(LocalDatabase.this.getDatabasePath());
      try {
        current.getLastTransaction().commit();
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

        final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(LocalDatabase.this.getDatabasePath());
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
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(LocalDatabase.this.getDatabasePath());

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
            final Document record = (Document) recordFactory.newImmutableRecord(wrappedDatabaseInstance, type, rid, view, null);
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
  public void scanBucket(final String bucketName, final RecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
    stats.scanBucket.incrementAndGet();

    executeInReadLock(() -> {

      checkDatabaseIsOpen();

      final String typeName = schema.getTypeNameByBucketId(schema.getBucketByName(bucketName).getFileId());
      schema.getBucketByName(bucketName).scan((rid, view) -> {
        final Record record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, schema.getType(typeName), rid, view, null);
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

      return schema.getBucketById(rid.getBucketId()).existsRecord(rid);
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
        final Binary buffer = schema.getBucketById(rid.getBucketId()).getRecord(rid);
        record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, type, rid, buffer.copyOfContent(), null);
        return invokeAfterReadEvents(record);
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
      throw new IllegalArgumentException("Cannot create record " + record.getIdentity() + " because it is already persistent");

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
        bucket = (LocalBucket) doc.getType().getBucketIdByRecord(doc, DatabaseContext.INSTANCE.getContext(databasePath).asyncMode);
      else
        bucket = (LocalBucket) schema.getBucketByName(bucketName);

      ((RecordInternal) record).setIdentity(bucket.createRecord(record, discardRecordAfter));

      final TransactionContext transaction = getTransaction();
      transaction.updateRecordInCache(record);
      transaction.updateBucketRecordDelta(bucket.getFileId(), +1);

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

    executeInReadLock(() -> {
      if (isTransactionActive()) {
        // MARK THE RECORD FOR UPDATE IN TX AND DEFER THE SERIALIZATION AT COMMIT TIME. THIS SPEEDS UP CASES WHEN THE SAME RECORDS ARE UPDATE MULTIPLE TIME INSIDE
        // THE SAME TX. THE MOST CLASSIC EXAMPLE IS INSERTING EDGES: THE RECORD CHUNK IS UPDATED EVERYTIME A NEW EDGE IS CREATED IN THE SAME CHUNK.
        // THE PAGE IS EARLY LOADED IN TX CACHE TO USE THE PAGE MVCC IN CASE OF CONCURRENT OPERATIONS ON THE MODIFIED RECORD
        try {
          getTransaction().addUpdatedRecord(record);

          if (record instanceof Document document) {
            // UPDATE THE INDEX IN MEMORY BEFORE UPDATING THE PAGE
            final List<IndexInternal> indexes = indexer.getInvolvedIndexes(document);
            if (!indexes.isEmpty()) {
              // UPDATE THE INDEXES TOO
              final Document originalRecord = getOriginalDocument(record);
              indexer.updateDocument(originalRecord, document, indexes);
            }
          }
        } catch (final IOException e) {
          throw new DatabaseOperationException("Error on update the record " + record.getIdentity() + " in transaction", e);
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
    return (Document) recordFactory.newImmutableRecord(this, ((Document) record).getType(), record.getIdentity(), originalBuffer,
        null);
  }

  @Override
  public void updateRecordNoLock(final Record record, final boolean discardRecordAfter) {
    boolean success = false;
    final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);

    try {
      final List<IndexInternal> indexes = record instanceof Document d ? indexer.getInvolvedIndexes(d) : Collections.emptyList();

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
  public void deleteRecordNoLock(final Record record) {
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

      if (record instanceof Document document)
        indexer.deleteDocument(document);

      if (record instanceof Edge edge) {
        graphEngine.deleteEdge(edge);
      } else if (record instanceof Vertex) {
        graphEngine.deleteVertex((VertexInternal) record);
      } else
        bucket.deleteRecord(record.getIdentity());

      success = true;

      // INVOKE EVENT CALLBACKS
      events.onAfterDelete(record);
      if (record instanceof Document document)
        ((RecordEventsRegistry) document.getType().getEvents()).onAfterDelete(record);

      final TransactionContext transaction = getTransaction();
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
    return transaction(txBlock, joinCurrentTx, configuration.getValueAsInteger(GlobalConfiguration.TX_RETRIES), null, null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int attempts) {
    return transaction(txBlock, joinCurrentTx, attempts, null, null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, int attempts, final OkCallback ok,
      final ErrorCallback error) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    ArcadeDBException lastException = null;

    if (attempts < 1)
      attempts = 1;

    final int retryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();

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
        // RETRY
        lastException = e;
        if (wrappedDatabaseInstance.isTransactionActive())
          wrappedDatabaseInstance.rollback();

        if (error != null)
          error.call(e);

        if (retry < attempts - 1)
          delayBetweenRetries(retryDelay);

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
    if (!type.getClass().equals(LocalDocumentType.class))
      throw new IllegalArgumentException("Cannot create a document of type '" + typeName + "' because is not a document type");

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
      throw new IllegalArgumentException("Cannot create a vertex of type '" + typeName + "' because is not a vertex type");

    stats.createRecord.incrementAndGet();

    return new MutableVertex(wrappedDatabaseInstance, (VertexType) type, null);
  }

  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames,
      final Object[] sourceVertexKeyValues, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
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
  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final Object... properties) {
    if (!bidirectional && ((EdgeType) schema.getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues,
        createVertexIfNotExist, edgeType, properties);
  }

  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
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
      engine = queryEngineManager.getInstance(language, this);
      if (engine.isReusable()) {
        final QueryEngine prev = reusableQueryEngines.putIfAbsent(language, engine);
        if (prev != null)
          engine = prev;
      }
    }

    return engine;
  }

  @Override
  public ResultSet command(final String language, final String query, final Object... parameters) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    return getQueryEngine(language).command(query, new ContextConfiguration(), parameters);
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Object... parameters) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    return getQueryEngine(language).command(query, configuration, parameters);
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
    return getQueryEngine(language).command(query, configuration, parameters);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Map<String, Object> params) {
    if (!language.equalsIgnoreCase("sql"))
      throw new CommandExecutionException("Language '" + language + "' does not support script");
    return command("sqlscript", script, params);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Object... args) {
    if (!language.equalsIgnoreCase("sqlscript") && !language.equalsIgnoreCase("sql"))
      throw new CommandExecutionException("Language '" + language + "' does not support script");
    return command("sqlscript", script, args);
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... parameters) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    return getQueryEngine(language).query(query, new ContextConfiguration(), parameters);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> parameters) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    return getQueryEngine(language).query(query, new ContextConfiguration(), parameters);
  }

  @Override
  public Select select() {
    return new Select(this);
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
    List<Integer> lockedFiles = null;
    try {
      lockedFiles = transactionManager.tryLockFiles(fileIds, 5_000, Thread.currentThread());

      return callable.call();

    } catch (final RuntimeException e) {
      throw e;

    } catch (final Throwable e) {
      throw new DatabaseOperationException("Error during write lock", e);

    } finally {
      if (lockedFiles != null)
        transactionManager.unlockFilesInOrder(lockedFiles, Thread.currentThread());
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

  @Override
  public int getEdgeListSize() {
    return this.edgeListSize;
  }

  @Override
  public Database setEdgeListSize(final int size) {
    this.edgeListSize = size;
    return this;
  }

  @Override
  public int getNewEdgeListSize(final int previousSize) {
    if (previousSize == 0)
      return edgeListSize;

    int newSize = previousSize * 2;
    if (newSize > MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE)
      newSize = MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE;
    return newSize;
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

  public QueryEngineManager getQueryEngineManager() {
    return queryEngineManager;
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

  @Override
  public Record invokeAfterReadEvents(Record record) {
    // INVOKE EVENT CALLBACKS
    record = events.onAfterRead(record);
    if (record == null)
      return null;
    if (record instanceof Document document) {
      final DocumentType type = document.getType();
      if (type != null) {
        return ((RecordEventsRegistry) type.getEvents()).onAfterRead(record);
      }
    }
    return record;
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

      //LogManager.instance().log(this, Level.INFO, "LOCKED DATABASE FILE '%s' (thread=%s)", null, lockFile, Thread.currentThread().threadId());

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
          "Database '" + name + "' is locked by another process (path=" + new File(databasePath).getAbsolutePath() + ")", e);
    }
  }

  private void checkDatabaseName() {
    if (name.contains("*") || name.contains(".."))
      throw new IllegalArgumentException("Invalid characters used in database name '" + name + "'");
  }

  private void closeInternal(final boolean drop) {
    if (async != null) {
      try {
        // EXECUTE OUTSIDE LOCK
        async.waitCompletion();
        async.close();
      } catch (final Throwable e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error on stopping asynchronous manager during closing operation for database '%s'", e, name);
      }
    }

    if (!drop) {
      // FLUSH ALL INDEXES WHILE THE DATABASE IS STILL OPEN
      for (Index idx : schema.getIndexes()) {
        try {
          ((IndexInternal) idx).flush();
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on flushing index %s: %s", e, idx.getName(), e.getMessage());
        }
      }
    }

    executeInWriteLock(() -> {
      if (!open)
        return null;

      try {
        if (async != null)
          async.close();
      } catch (final Throwable e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error on stopping asynchronous manager during closing operation for database '%s'", e, name);
      }

      if (drop)
        PageManager.INSTANCE.removeModifiedPagesOfDatabase(this);

      PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(this);

      open = false;

      PageManager.INSTANCE.removeAllReadPagesOfDatabase(this);

      try {
        final List<DatabaseContext.DatabaseContextTL> dbContexts = DatabaseContext.INSTANCE.removeAllContexts(databasePath);
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
            .log(this, Level.WARNING, "Error on clearing transaction status during closing operation for database '%s'", e, name);
      }

      for (QueryEngine e : reusableQueryEngines.values())
        e.close();

      try {
        schema.close();
        fileManager.close();
        transactionManager.close(drop);
        statementCache.clear();
        reusableQueryEngines.clear();

      } catch (final Throwable e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error on closing internal components during closing operation for database '%s'", e, name);
      } finally {
        Profiler.INSTANCE.unregisterDatabase(LocalDatabase.this);
      }

      if (lockFile != null) {
        try {
          if (lockFileLock != null) {
            lockFileLock.release();
            //LogManager.instance().log(this, Level.INFO, "RELEASED DATABASE FILE '%s' (thread=%s)", null, lockFile, Thread.currentThread().threadId());
          }
          if (lockFileIOChannel != null)
            lockFileIOChannel.close();
          if (lockFileIO != null)
            lockFileIO.close();
          if (lockFile.exists())
            Files.delete(Path.of(lockFile.getAbsolutePath()));

          if (lockFile.exists() && !lockFile.delete())
            LogManager.instance().log(this, Level.WARNING, "Error on deleting lock file '%s'", lockFile);

        } catch (final IOException e) {
          // IGNORE IT
          LogManager.instance().log(this, Level.WARNING, "Error on deleting lock file '%s'", e, lockFile);
        }
      }

      return null;
    });

    if (DatabaseFactory.removeActiveDatabaseInstance(databasePath))
      PageManager.INSTANCE.close();
  }

  private void checkForRecovery() throws IOException {
    lockFile = new File(databasePath + "/database.lck");

    if (lockFile.exists()) {
      lockDatabase();

      // RECOVERY
      LogManager.instance().log(this, Level.WARNING, "Database '%s' was not closed properly last time", null, name);

      if (mode == ComponentFile.MODE.READ_ONLY)
        throw new DatabaseMetadataException("Database needs recovery but has been open in read only mode");

      // RESET THE COUNT OF RECORD IN CASE THE DATABASE WAS NOT CLOSED PROPERLY
      for (Bucket b : schema.getBuckets())
        ((LocalBucket) b).setCachedRecordCount(-1);

      executeCallbacks(CALLBACK_EVENT.DB_NOT_CLOSED);

      transactionManager.checkIntegrity();
    } else {
      if (mode == ComponentFile.MODE.READ_WRITE) {
        lockFile.createNewFile();
        lockDatabase();
      } else
        lockFile = null;
    }
  }

  private void openInternal() {
    try {
      DatabaseContext.INSTANCE.init(this);
      setLockingEnabled(configuration.getValueAsBoolean(GlobalConfiguration.BACKUP_ENABLED));

      fileManager = new FileManager(databasePath, mode, SUPPORTED_FILE_EXT);
      transactionManager = new TransactionManager(wrappedDatabaseInstance);

      open = true;

      try {
        schema = new LocalSchema(wrappedDatabaseInstance, databasePath, security);

        if (fileManager.getFiles().isEmpty())
          schema.create(mode);
        else
          schema.load(mode, true);

        serializer.setDateImplementation(configuration.getValue(GlobalConfiguration.DATE_IMPLEMENTATION));
        serializer.setDateTimeImplementation(configuration.getValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION));

        if (mode == ComponentFile.MODE.READ_WRITE)
          checkForRecovery();

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

      if (e instanceof DatabaseOperationException exception)
        throw exception;

      throw new DatabaseOperationException("Error on creating new database instance", e);
    }
  }

  protected void checkDatabaseIsOpen() {
    checkDatabaseIsOpen(false, null);
  }

  protected void checkDatabaseIsOpen(final boolean updateIntent, final String databaseReadOnlyErrorMessage) {
    if (!open)
      throw new DatabaseIsClosedException(name);
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
    if (record instanceof MutableDocument doc) {
      final DocumentType type = doc.getType();

      final Set<String> propertiesWithDefaultDefined = type.getPolymorphicPropertiesWithDefaultDefined();

      for (final String pName : propertiesWithDefaultDefined) {
        final Object pValue = doc.get(pName);
        if (pValue == null) {
          final Property p = type.getPolymorphicProperty(pName);
          Object defValue = p.getDefaultValue();
          doc.set(pName, defValue);
        }
      }
    }
  }

  private void delayBetweenRetries(final int retryDelay) {
    if (retryDelay > 0) {
      LogManager.instance()
          .log(this, Level.FINE, "Wait %d ms before the next retry for transaction commit (threadId=%d)", retryDelay,
              Thread.currentThread().threadId());

      try {
        Thread.sleep(1 + new Random().nextInt(retryDelay));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
