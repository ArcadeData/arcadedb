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
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.engine.WALFileFactoryEmbedded;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.InvalidDatabaseInstanceException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.ScriptExecutionPlan;
import com.arcadedb.query.sql.parser.BeginStatement;
import com.arcadedb.query.sql.parser.CommitStatement;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.LetStatement;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.LocalResultSetLifecycleDecorator;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedSchema;
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

public class EmbeddedDatabase extends RWLockContext implements DatabaseInternal {
  public static final  int                                       EDGE_LIST_INITIAL_CHUNK_SIZE         = 64;
  public static final  int                                       MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE = 8192;
  private static final Set<String>                               SUPPORTED_FILE_EXT                   = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList(Dictionary.DICT_EXT, Bucket.BUCKET_EXT, LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
          LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, LSMTreeIndexCompacted.UNIQUE_INDEX_EXT)));
  public final         AtomicLong                                indexCompactions                     = new AtomicLong();
  protected final      String                                    name;
  protected final      PaginatedFile.MODE                        mode;
  protected final      ContextConfiguration                      configuration;
  protected final      String                                    databasePath;
  protected final      BinarySerializer                          serializer                           = new BinarySerializer();
  protected final      RecordFactory                             recordFactory                        = new RecordFactory();
  protected final      GraphEngine                               graphEngine;
  protected final      WALFileFactory                            walFactory;
  protected final      DocumentIndexer                           indexer;
  protected final      QueryEngineManager                        queryEngineManager;
  protected final      DatabaseStats                             stats                                = new DatabaseStats();
  protected            FileManager                               fileManager;
  protected            PageManager                               pageManager;
  protected            EmbeddedSchema                            schema;
  protected            TransactionManager                        transactionManager;
  protected volatile   DatabaseAsyncExecutorImpl                 async                                = null;
  protected            Lock                                      asyncLock                            = new ReentrantLock();
  protected            boolean                                   autoTransaction                      = false;
  protected volatile   boolean                                   open                                 = false;
  private              boolean                                   readYourWrites                       = true;
  private final        Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks;
  private final        StatementCache                            statementCache;
  private final        ExecutionPlanCache                        executionPlanCache;
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
  private              ConcurrentHashMap<String, QueryEngine>    reusableQueryEngines                 = new ConcurrentHashMap<>();

  protected EmbeddedDatabase(final String path, final PaginatedFile.MODE mode, final ContextConfiguration configuration, final SecurityManager security,
      final Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks) {
    try {
      this.mode = mode;
      this.configuration = configuration;
      this.security = security;
      this.callbacks = callbacks;
      this.walFactory = mode == PaginatedFile.MODE.READ_WRITE ? new WALFileFactoryEmbedded() : null;
      this.statementCache = new StatementCache(this, configuration.getValueAsInteger(GlobalConfiguration.SQL_STATEMENT_CACHE));
      this.executionPlanCache = new ExecutionPlanCache(this, configuration.getValueAsInteger(GlobalConfiguration.SQL_STATEMENT_CACHE));

      if (path.endsWith("/") || path.endsWith("\\"))
        databasePath = path.substring(0, path.length() - 1);
      else
        databasePath = path;

      configurationFile = new File(databasePath + "/configuration.json");

      final int lastSeparatorPos = path.lastIndexOf(File.separator);
      if (lastSeparatorPos > -1)
        name = path.substring(lastSeparatorPos + 1);
      else
        name = path;

      checkDatabaseName();

      indexer = new DocumentIndexer(this);
      queryEngineManager = new QueryEngineManager();
      graphEngine = new GraphEngine(this);

    } catch (Exception e) {
      if (e instanceof DatabaseOperationException)
        throw (DatabaseOperationException) e;

      throw new DatabaseOperationException("Error on creating new database instance", e);
    }
  }

  protected void open() {
    if (!new File(databasePath).exists())
      throw new DatabaseOperationException("Database '" + databasePath + "' not exists");

    if (configurationFile.exists()) {
      try {
        final String content = FileUtils.readFileAsString(configurationFile, "UTF8");
        configuration.reset();
        configuration.fromJSON(content);
      } catch (IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on loading configuration from file '%s'", e, configurationFile);
      }
    }

    openInternal();
  }

  protected void create() {
    final File databaseDirectory = new File(databasePath);
    if (new File(databaseDirectory, EmbeddedSchema.SCHEMA_FILE_NAME).exists() || new File(databaseDirectory, EmbeddedSchema.SCHEMA_PREV_FILE_NAME).exists())
      throw new DatabaseOperationException("Database '" + databasePath + "' already exists");

    if (!databaseDirectory.exists() && !databaseDirectory.mkdirs())
      throw new DatabaseOperationException("Cannot create directory '" + databasePath + "'");

    openInternal();

    schema.saveConfiguration();

    try {
      saveConfiguration();
    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving configuration to file '%s'", e, configurationFile);
    }
  }

  @Override
  public void drop() {
    checkDatabaseIsOpen();

    if (isTransactionActive())
      throw new SchemaException("Cannot drop the database in transaction");

    if (mode == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot drop database");

    internalClose(true);

    executeInWriteLock(() -> {
      FileUtils.deleteRecursively(new File(databasePath));
      return null;
    });
  }

  @Override
  public void close() {
    internalClose(false);
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
      pageManager.kill();
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
        } catch (IOException e) {
          // IGNORE IT
        }
      }

    } finally {
      open = false;
      Profiler.INSTANCE.unregisterDatabase(EmbeddedDatabase.this);
    }
  }

  public DatabaseAsyncExecutor async() {
    if (async == null) {
      asyncLock.lock();
      try {
        if (async == null)
          async = new DatabaseAsyncExecutorImpl(wrappedDatabaseInstance);
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
  public String getCurrentUserName() {
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContext(databasePath);
    if (dbContext == null)
      return null;
    final SecurityDatabaseUser user = dbContext.getCurrentUser();
    return user != null ? user.getName() : null;
  }

  public TransactionContext getTransaction() {
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContext(databasePath);
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
          } catch (Exception e) {
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
    executeInReadLock(() -> {
      checkDatabaseIsOpen();

      // FORCE THE RESET OF TL
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(EmbeddedDatabase.this.getDatabasePath());
      TransactionContext tx = current.getLastTransaction();
      if (tx.isActive()) {
        // CREATE A NESTED TX
        tx = new TransactionContext(getWrappedDatabaseInstance());
        current.pushTransaction(tx);
      }

      tx.begin();

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

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(EmbeddedDatabase.this.getDatabasePath());
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

        final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(EmbeddedDatabase.this.getDatabasePath());
        current.popIfNotLastTransaction().rollback();

      } catch (TransactionException e) {
        // ALREADY ROLLBACKED
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
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(EmbeddedDatabase.this.getDatabasePath());

      TransactionContext tx;
      while ((tx = current.popIfNotLastTransaction()) != null) {
        try {
          if (tx.isActive())
            tx.rollback();
          else
            break;

        } catch (InvalidDatabaseInstanceException e) {
          current.popIfNotLastTransaction().rollback();
        } catch (TransactionException e) {
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
      for (Bucket b : type.getBuckets(polymorphic))
        total += b.count();

      return total;
    });
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    scanType(typeName, polymorphic, callback, null);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback, final ErrorRecordCallback errorRecordCallback) {
    stats.scanType.incrementAndGet();

    executeInReadLock(() -> {
      boolean success = false;
      final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);
      try {
        final DocumentType type = schema.getType(typeName);

        final AtomicBoolean continueScan = new AtomicBoolean(true);

        for (Bucket b : type.getBuckets(polymorphic)) {
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

      final String typeName = schema.getTypeNameByBucketId(schema.getBucketByName(bucketName).getId());
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

    return (Iterator<Record>) executeInReadLock(() -> {
      checkDatabaseIsOpen();
      final DocumentType type = schema.getType(typeName);
      final MultiIterator iter = new MultiIterator();

      // SET THE PROFILED LIMITS IF ANY
      iter.setLimit(getResultSetLimit());
      iter.setTimeout(getReadTimeout());

      for (Bucket b : type.getBuckets(polymorphic))
        iter.addIterator(b.iterator());
      return iter;
    });
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    stats.iterateBucket.incrementAndGet();

    readLock();
    try {

      checkDatabaseIsOpen();
      try {
        final Bucket bucket = schema.getBucketByName(bucketName);
        return bucket.iterator();
      } catch (Exception e) {
        throw new DatabaseOperationException("Error on executing scan of bucket '" + bucketName + "'", e);
      }

    } finally {
      readUnlock();
    }
  }

  public void checkPermissionsOnDatabase(final SecurityDatabaseUser.DATABASE_ACCESS access) {
    if (security == null)
      return;

    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContext(databasePath);
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

    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContext(databasePath);
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

    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContext(databasePath);
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

    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContext(databasePath);
    if (dbContext == null)
      return -1L;
    final SecurityDatabaseUser user = dbContext.getCurrentUser();
    if (user == null)
      return -1L;

    return user.getReadTimeout();
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    if (rid == null)
      throw new IllegalArgumentException("Record is null");

    stats.readRecord.incrementAndGet();

    return (Record) executeInReadLock((Callable<Object>) () -> {

      checkDatabaseIsOpen();

      // CHECK IN TX CACHE FIRST
      final TransactionContext tx = getTransaction();
      Record record = tx.getRecordFromCache(rid);
      if (record != null)
        return record;

      final DocumentType type = schema.getTypeByBucketId(rid.getBucketId());

      if (loadContent || type == null) {
        final Binary buffer = schema.getBucketById(rid.getBucketId()).getRecord(rid);
        record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, type, rid, buffer.copy(), null);
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
        throw new IllegalArgumentException("No index has been created on type '" + type + "' properties " + Arrays.toString(keyNames));

      return idx.get(keyValues);
    });
  }

  @Override
  public void registerCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    List<Callable<Void>> callbacks = this.callbacks.computeIfAbsent(event, k -> new ArrayList<>());
    callbacks.add(callback);
  }

  @Override
  public void unregisterCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    List<Callable<Void>> callbacks = this.callbacks.get(event);
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
  public EmbeddedDatabase setUseWAL(final boolean useWAL) {
    getTransaction().setUseWAL(useWAL);
    return this;
  }

  @Override
  public EmbeddedDatabase setWALFlush(final WALFile.FLUSH_TYPE flush) {
    getTransaction().setWALFlush(flush);
    return this;
  }

  @Override
  public boolean isAsyncFlush() {
    return getTransaction().isAsyncFlush();
  }

  @Override
  public EmbeddedDatabase setAsyncFlush(final boolean value) {
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

    if (mode == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot create a new record");

    setDefaultValues(record);

    // INVOKE EVENT CALLBACKS
    if (!events.onBeforeCreate(record))
      return;
    if (record instanceof Document)
      if (!((RecordEventsRegistry) ((Document) record).getType().getEvents()).onBeforeCreate(record))
        return;

    boolean success = false;
    final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);
    try {
      final Bucket bucket;

      if (bucketName == null && record instanceof Document) {
        final Document doc = (Document) record;
        bucket = doc.getType().getBucketIdByRecord(doc, DatabaseContext.INSTANCE.getContext(databasePath).asyncMode);
      } else
        bucket = schema.getBucketByName(bucketName);

      ((RecordInternal) record).setIdentity(bucket.createRecord(record, discardRecordAfter));
      getTransaction().updateRecordInCache(record);

      if (record instanceof MutableDocument) {
        final MutableDocument doc = (MutableDocument) record;
        indexer.createDocument(doc, doc.getType(), bucket);
      }

      ((RecordInternal) record).unsetDirty();

      success = true;

      // INVOKE EVENT CALLBACKS
      events.onAfterCreate(record);
      if (record instanceof Document)
        ((RecordEventsRegistry) ((Document) record).getType().getEvents()).onAfterCreate(record);

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

    if (mode == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update a record");

    // INVOKE EVENT CALLBACKS
    if (!events.onBeforeUpdate(record))
      return;
    if (record instanceof Document)
      if (!((RecordEventsRegistry) ((Document) record).getType().getEvents()).onBeforeUpdate(record))
        return;

    executeInReadLock(() -> {
      if (isTransactionActive()) {
        // MARK THE RECORD FOR UPDATE IN TX AND DEFER THE SERIALIZATION AT COMMIT TIME. THIS SPEEDS UP CASES WHEN THE SAME RECORDS ARE UPDATE MULTIPLE TIME INSIDE
        // THE SAME TX. THE MOST CLASSIC EXAMPLE IS INSERTING EDGES: THE RECORD CHUNK IS UPDATED EVERYTIME A NEW EDGE IS CREATED IN THE SAME CHUNK.
        // THE PAGE IS EARLY LOADED IN TX CACHE TO USE THE PAGE MVCC IN CASE OF CONCURRENT OPERATIONS ON THE MODIFIED RECORD
        try {
          getTransaction().addUpdatedRecord(record);

          if (record instanceof Document) {
            // UPDATE THE INDEX IN MEMORY BEFORE UPDATING THE PAGE
            final List<Index> indexes = indexer.getInvolvedIndexes((Document) record);
            if (!indexes.isEmpty()) {
              // UPDATE THE INDEXES TOO
              final Binary originalBuffer = ((RecordInternal) record).getBuffer();
              if (originalBuffer == null)
                throw new IllegalStateException("Cannot read original buffer for indexing");
              originalBuffer.rewind();
              final Document originalRecord = (Document) recordFactory.newImmutableRecord(this, ((Document) record).getType(), record.getIdentity(),
                  originalBuffer, null);
              indexer.updateDocument(originalRecord, (Document) record, indexes);
            }
          }
        } catch (IOException e) {
          throw new DatabaseOperationException("Error on update the record " + record.getIdentity() + " in transaction", e);
        }
      } else
        updateRecordNoLock(record, false);

      // INVOKE EVENT CALLBACKS
      events.onAfterUpdate(record);
      if (record instanceof Document)
        ((RecordEventsRegistry) ((Document) record).getType().getEvents()).onAfterUpdate(record);

      return null;
    });
  }

  @Override
  public void updateRecordNoLock(final Record record, final boolean discardRecordAfter) {
    boolean success = false;
    final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);

    try {
      final List<Index> indexes = record instanceof Document ? indexer.getInvolvedIndexes((Document) record) : Collections.emptyList();

      if (!indexes.isEmpty()) {
        // UPDATE THE INDEXES TOO
        final Binary originalBuffer = ((RecordInternal) record).getBuffer();
        if (originalBuffer == null)
          throw new IllegalStateException("Cannot read original buffer for indexing");
        originalBuffer.rewind();
        final Document originalRecord = (Document) recordFactory.newImmutableRecord(this, ((Document) record).getType(), record.getIdentity(), originalBuffer,
            null);

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

    if (mode == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot delete record " + record.getIdentity());

    // INVOKE EVENT CALLBACKS
    if (!events.onBeforeDelete(record))
      return;
    if (record instanceof Document)
      if (!((RecordEventsRegistry) ((Document) record).getType().getEvents()).onBeforeDelete(record))
        return;

    boolean success = false;
    final boolean implicitTransaction = checkTransactionIsActive(autoTransaction);

    try {
      final Bucket bucket = schema.getBucketById(record.getIdentity().getBucketId());

      if (record instanceof Document)
        indexer.deleteDocument((Document) record);

      if (record instanceof Edge) {
        graphEngine.deleteEdge((Edge) record);
      } else if (record instanceof Vertex) {
        graphEngine.deleteVertex((VertexInternal) record);
      } else
        bucket.deleteRecord(record.getIdentity());

      success = true;

      // INVOKE EVENT CALLBACKS
      events.onAfterDelete(record);
      if (record instanceof Document)
        ((RecordEventsRegistry) ((Document) record).getType().getEvents()).onAfterDelete(record);

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
    final Transaction tx = getTransaction();
    return tx != null && tx.isActive();
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
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, int retries) {
    return transaction(txBlock, joinCurrentTx, retries, null, null);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, int attempts, final OkCallback ok, final ErrorCallback error) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    ArcadeDBException lastException = null;

    if (attempts < 1)
      attempts = 1;

    for (int retry = 0; retry < attempts; ++retry) {
      boolean createdNewTx = true;

      try {
        if (joinCurrentTx && wrappedDatabaseInstance.isTransactionActive())
          createdNewTx = false;
        else
          wrappedDatabaseInstance.begin();

        txBlock.execute();

        if (createdNewTx)
          wrappedDatabaseInstance.commit();

        if (ok != null)
          ok.call();

        // OK
        return createdNewTx;

      } catch (NeedRetryException | DuplicatedKeyException e) {
        // RETRY
        lastException = e;
      } catch (Exception e) {
        final TransactionContext tx = getTransaction();
        if (tx != null && tx.isActive())
          rollback();

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
    return pageManager;
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final DocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(DocumentType.class))
      throw new IllegalArgumentException("Cannot create a document of type '" + typeName + "' because is not a document type");

    stats.createRecord.incrementAndGet();

    return new MutableDocument(wrappedDatabaseInstance, type, null);
  }

  @Override
  public MutableEmbeddedDocument newEmbeddedDocument(final EmbeddedModifier modifier, final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final DocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(DocumentType.class))
      throw new IllegalArgumentException("Cannot create an embedded document of type '" + typeName + "' because is not a document type");

    return new MutableEmbeddedDocument(wrappedDatabaseInstance, type, modifier);
  }

  @Override
  public MutableVertex newVertex(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final DocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(VertexType.class))
      throw new IllegalArgumentException("Cannot create a vertex of type '" + typeName + "' because is not a vertex type");

    stats.createRecord.incrementAndGet();

    return new MutableVertex(wrappedDatabaseInstance, type, null);
  }

  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames, final Object[] sourceVertexKeyValues,
      final String destinationVertexType, final String[] destinationVertexKeyNames, final Object[] destinationVertexKeyValues,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional, final Object... properties) {
    if (sourceVertexKeyNames == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKeyNames.length != sourceVertexKeyValues.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKeyNames == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKeyNames.length != destinationVertexKeyValues.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<Identifiable> v1Result = lookupByKey(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues);

    Vertex sourceVertex;
    if (!v1Result.hasNext()) {
      if (createVertexIfNotExist) {
        sourceVertex = newVertex(sourceVertexType);
        for (int i = 0; i < sourceVertexKeyNames.length; ++i)
          ((MutableVertex) sourceVertex).set(sourceVertexKeyNames[i], sourceVertexKeyValues[i]);
        ((MutableVertex) sourceVertex).save();
      } else
        throw new IllegalArgumentException(
            "Cannot find source vertex with key " + Arrays.toString(sourceVertexKeyNames) + "=" + Arrays.toString(sourceVertexKeyValues));
    } else
      sourceVertex = v1Result.next().getIdentity().asVertex();

    final Iterator<Identifiable> v2Result = lookupByKey(destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues);
    Vertex destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKeyNames.length; ++i)
          ((MutableVertex) destinationVertex).set(destinationVertexKeyNames[i], destinationVertexKeyValues[i]);
        ((MutableVertex) destinationVertex).save();
      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKeyNames) + "=" + Arrays.toString(destinationVertexKeyValues));
    } else
      destinationVertex = v2Result.next().getIdentity().asVertex();

    stats.createRecord.incrementAndGet();

    return sourceVertex.newEdge(edgeType, destinationVertex, bidirectional, properties);
  }

  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional,
      final Object... properties) {
    if (sourceVertex == null)
      throw new IllegalArgumentException("Source vertex is null");

    if (destinationVertexKeyNames == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKeyNames.length != destinationVertexKeyValues.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<Identifiable> v2Result = lookupByKey(destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues);
    Vertex destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKeyNames.length; ++i)
          ((MutableVertex) destinationVertex).set(destinationVertexKeyNames[i], destinationVertexKeyValues[i]);
        ((MutableVertex) destinationVertex).save();
      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKeyNames) + "=" + Arrays.toString(destinationVertexKeyValues));
    } else
      destinationVertex = v2Result.next().getIdentity().asVertex();

    stats.createRecord.incrementAndGet();

    return sourceVertex.newEdge(edgeType, destinationVertex, bidirectional, properties);
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
  public PaginatedFile.MODE getMode() {
    return mode;
  }

  @Override
  public boolean checkTransactionIsActive(boolean createTx) {
    checkDatabaseIsOpen();

    if (!isTransactionActive()) {
      if (createTx) {
        wrappedDatabaseInstance.begin();
        return true;
      }
      throw new DatabaseOperationException("Transaction not begun");
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
    checkDatabaseIsOpen();

    stats.commands.incrementAndGet();

    return getQueryEngine(language).command(query, parameters);
  }

  @Override
  public ResultSet command(final String language, final String query, final Map<String, Object> parameters) {
    checkDatabaseIsOpen();

    stats.commands.incrementAndGet();

    return getQueryEngine(language).command(query, parameters);
  }

  @Override
  public ResultSet execute(final String language, final String script, final Map<String, Object> params) {
    checkDatabaseIsOpen();

    BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(getWrappedDatabaseInstance());
    context.setInputParameters(params);

    final List<Statement> statements = ((SQLQueryEngine) getQueryEngine("sql")).parseScript(script, wrappedDatabaseInstance);
    return new LocalResultSetLifecycleDecorator(executeInternal(statements, context));
  }

  @Override
  public ResultSet execute(final String language, final String script, final Object... args) {
    checkDatabaseIsOpen();

    BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(getWrappedDatabaseInstance());
    context.setInputParameters(args);

    final List<Statement> statements = ((SQLQueryEngine) getQueryEngine("sql")).parseScript(script, wrappedDatabaseInstance);
    return new LocalResultSetLifecycleDecorator(executeInternal(statements, context));
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... parameters) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    return getQueryEngine(language).query(query, parameters);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> parameters) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    return getQueryEngine(language).query(query, parameters);
  }

  /**
   * Returns true if two databases are the same.
   */
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Database))
      return false;

    final Database other = (Database) o;
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
    readLock();
    try {

      return callable.call();

    } catch (ClosedChannelException e) {
      LogManager.instance().log(this, Level.SEVERE, "Database '%s' has some files that are closed", e, name);
      close();
      throw new DatabaseOperationException("Database '" + name + "' has some files that are closed", e);

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new DatabaseOperationException("Error during read lock", e);

    } finally {
      readUnlock();
    }
  }

  /**
   * Executes a callback in an exclusive lock.
   */
  @Override
  public <RET> RET executeInWriteLock(final Callable<RET> callable) {
    writeLock();
    try {

      return callable.call();

    } catch (ClosedChannelException e) {
      LogManager.instance().log(this, Level.SEVERE, "Database '%s' has some files that are closed", e, name);
      close();
      throw new DatabaseOperationException("Database '" + name + "' has some files that are closed", e);

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new DatabaseOperationException("Error during write lock", e);

    } finally {
      writeUnlock();
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

  @Override
  public WALFileFactory getWALFileFactory() {
    return walFactory;
  }

  @Override
  public int hashCode() {
    return databasePath != null ? databasePath.hashCode() : 0;
  }

  @Override
  public void executeCallbacks(final CALLBACK_EVENT event) throws IOException {
    final List<Callable<Void>> callbacks = this.callbacks.get(event);
    if (callbacks != null && !callbacks.isEmpty()) {
      for (Callable<Void> cb : callbacks) {
        try {
          cb.call();
        } catch (RuntimeException | IOException e) {
          throw e;
        } catch (Exception e) {
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

  public void saveConfiguration() throws IOException {
    FileUtils.writeFile(configurationFile, configuration.toJSON());
  }

  private void lockDatabase() {
    try {
      lockFileIO = new RandomAccessFile(lockFile, "rw");
      lockFileIOChannel = lockFileIO.getChannel();
      lockFileLock = lockFileIOChannel.tryLock();
      if (lockFileLock == null) {
        lockFileIOChannel.close();
        lockFileIO.close();
        throw new LockException("Database '" + name + "' is locked by another process (path=" + new File(databasePath).getAbsolutePath() + ")");
      }

      //LogManager.instance().log(this, Level.INFO, "LOCKED DATABASE FILE '%s' (thread=%s)", null, lockFile, Thread.currentThread().getId());

    } catch (Exception e) {
      try {
        if (lockFileIOChannel != null)
          lockFileIOChannel.close();
        if (lockFileIO != null)
          lockFileIO.close();
      } catch (Exception e2) {
        // IGNORE
      }

      throw new LockException("Database '" + name + "' is locked by another process (path=" + new File(databasePath).getAbsolutePath() + ")", e);
    }
  }

  private void checkDatabaseName() {
    if (name.contains("*") || name.contains(".."))
      throw new IllegalArgumentException("Invalid characters used in database name '" + name + "'");
  }

  private void internalClose(final boolean drop) {
    if (async != null) {
      try {
        // EXECUTE OUTSIDE LOCK
        async.waitCompletion();
        async.close();
      } catch (Throwable e) {
        LogManager.instance().log(this, Level.WARNING, "Error on stopping asynchronous manager during closing operation for database '%s'", e, name);
      }
    }

    executeInWriteLock(() -> {
      if (!open)
        return null;

      open = false;

      try {
        if (async != null)
          async.close();
      } catch (Throwable e) {
        LogManager.instance().log(this, Level.WARNING, "Error on stopping asynchronous manager during closing operation for database '%s'", e, name);
      }

      try {
        final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.removeContext(databasePath);
        if (dbContext != null && !dbContext.transactions.isEmpty()) {
          // ROLLBACK ALL THE TX FROM LAST TO FIRST
          for (int i = dbContext.transactions.size() - 1; i > -1; --i) {
            final TransactionContext tx = dbContext.transactions.get(i);
            if (tx.isActive())
              // ROLLBACK ANY PENDING OPERATION
              tx.rollback();
          }
          dbContext.transactions.clear();
        }
      } catch (Throwable e) {
        LogManager.instance().log(this, Level.WARNING, "Error on clearing transaction status during closing operation for database '%s'", e, name);
      }

      try {
        schema.close();
        pageManager.close();
        fileManager.close();
        transactionManager.close(drop);
        statementCache.clear();
        reusableQueryEngines.clear();
      } catch (Throwable e) {
        LogManager.instance().log(this, Level.WARNING, "Error on closing internal components during closing operation for database '%s'", e, name);
      } finally {
        Profiler.INSTANCE.unregisterDatabase(EmbeddedDatabase.this);
      }

      if (lockFile != null) {
        try {
          if (lockFileLock != null) {
            lockFileLock.release();
            //LogManager.instance().log(this, Level.INFO, "RELEASED DATABASE FILE '%s' (thread=%s)", null, lockFile, Thread.currentThread().getId());
          }
          if (lockFileIOChannel != null)
            lockFileIOChannel.close();
          if (lockFileIO != null)
            lockFileIO.close();
          if (lockFile.exists())
            Files.delete(Paths.get(lockFile.getAbsolutePath()));

          if (lockFile.exists() && !lockFile.delete())
            LogManager.instance().log(this, Level.WARNING, "Error on deleting lock file '%s'", lockFile);

        } catch (IOException e) {
          // IGNORE IT
          LogManager.instance().log(this, Level.WARNING, "Error on deleting lock file '%s'", e, lockFile);
        }
      }

      return null;
    });

    DatabaseFactory.removeActiveDatabaseInstance(databasePath);
  }

  private void checkForRecovery() throws IOException {
    lockFile = new File(databasePath + "/database.lck");

    if (lockFile.exists()) {
      lockDatabase();

      // RECOVERY
      LogManager.instance().log(this, Level.WARNING, "Database '%s' was not closed properly last time", null, name);

      if (mode == PaginatedFile.MODE.READ_ONLY)
        throw new DatabaseMetadataException("Database needs recovery but has been open in read only mode");

      executeCallbacks(CALLBACK_EVENT.DB_NOT_CLOSED);

      transactionManager.checkIntegrity();
    } else {
      if (mode == PaginatedFile.MODE.READ_WRITE) {
        lockFile.createNewFile();
        lockDatabase();
      } else
        lockFile = null;
    }
  }

  private void openInternal() {
    try {
      DatabaseContext.INSTANCE.init(this);

      fileManager = new FileManager(databasePath, mode, SUPPORTED_FILE_EXT);
      transactionManager = new TransactionManager(wrappedDatabaseInstance);
      pageManager = new PageManager(fileManager, transactionManager, configuration);

      open = true;

      try {
        schema = new EmbeddedSchema(wrappedDatabaseInstance, databasePath, security);

        if (fileManager.getFiles().isEmpty())
          schema.create(mode);
        else
          schema.load(mode, true);

        if (mode == PaginatedFile.MODE.READ_WRITE)
          checkForRecovery();

        if (security != null)
          security.updateSchema(this);

        Profiler.INSTANCE.registerDatabase(this);

      } catch (RuntimeException e) {
        open = false;
        pageManager.close();
        throw e;
      } catch (Exception e) {
        open = false;
        pageManager.close();
        throw new DatabaseOperationException("Error on creating new database instance", e);
      }
    } catch (Exception e) {
      open = false;

      if (e instanceof DatabaseOperationException)
        throw (DatabaseOperationException) e;

      throw new DatabaseOperationException("Error on creating new database instance", e);
    }
  }

  protected void checkDatabaseIsOpen() {
    if (!open)
      throw new DatabaseIsClosedException(name);

    if (DatabaseContext.INSTANCE.getContext(databasePath) == null)
      DatabaseContext.INSTANCE.init(this);
  }

  private void setDefaultValues(Record record) {
    if (record instanceof MutableDocument) {
      final MutableDocument doc = (MutableDocument) record;
      final DocumentType type = doc.getType();

      final Set<String> propertiesWithDefaultDefined = type.getPolymorphicPropertiesWithDefaultDefined();

      for (String pName : propertiesWithDefaultDefined) {
        final Object pValue = doc.get(pName);
        if (pValue == null) {
          final Property p = type.getPolymorphicProperty(pName);
          final Object defValue = p.getDefaultValue();
          if (defValue != null)
            doc.set(pName, defValue);
        }
      }
    }
  }

  private ResultSet executeInternal(final List<Statement> statements, final CommandContext scriptContext) {
    ScriptExecutionPlan plan = new ScriptExecutionPlan(scriptContext);

    plan.setStatement(statements.stream().map(Statement::toString).collect(Collectors.joining(";")));

    List<Statement> lastRetryBlock = new ArrayList<>();
    int nestedTxLevel = 0;

    for (Statement stm : statements) {
      if (stm.getOriginalStatement() == null)
        stm.setOriginalStatement(stm.toString());

      if (stm instanceof BeginStatement)
        nestedTxLevel++;

      if (nestedTxLevel <= 0) {
        InternalExecutionPlan sub = stm.createExecutionPlan(scriptContext);
        plan.chain(sub, false);
      } else
        lastRetryBlock.add(stm);

      if (stm instanceof CommitStatement && nestedTxLevel > 0) {
        nestedTxLevel--;
        if (nestedTxLevel == 0) {

          for (Statement statement : lastRetryBlock) {
            InternalExecutionPlan sub = statement.createExecutionPlan(scriptContext);
            plan.chain(sub, false);
          }
          lastRetryBlock = new ArrayList<>();
        }
      }

      if (stm instanceof LetStatement)
        scriptContext.declareScriptVariable(((LetStatement) stm).getName().getStringValue());
    }

    return new LocalResultSet(plan);
  }
}
