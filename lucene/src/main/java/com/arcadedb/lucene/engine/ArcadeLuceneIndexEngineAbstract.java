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

package com.arcadedb.lucene.engine;

import static com.arcadedb.lucene.analyzer.ArcadeLuceneAnalyzerFactory.AnalyzerKind.INDEX;
import static com.arcadedb.lucene.analyzer.ArcadeLuceneAnalyzerFactory.AnalyzerKind.QUERY;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseThreadLocal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.DatabaseEngine; // For storage concepts
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.StorageException;
import com.arcadedb.index.IndexDefinition;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
// import com.arcadedb.index.engine.IndexEngineValuesTransformer; // Transformer might not be used directly
import com.arcadedb.lucene.analyzer.ArcadeLuceneAnalyzerFactory;
import com.arcadedb.lucene.directory.ArcadeLuceneDirectory; // Assuming this will be the new Directory wrapper
import com.arcadedb.lucene.directory.ArcadeLuceneDirectoryFactory; // Assuming this factory
import com.arcadedb.lucene.exception.LuceneIndexException;
import com.arcadedb.lucene.query.LuceneQueryContext;
import com.arcadedb.lucene.tx.LuceneTxChanges;
import com.arcadedb.lucene.tx.LuceneTxChangesMultiRid;
import com.arcadedb.lucene.tx.LuceneTxChangesSingleRid;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;


import java.io.File;
import java.io.IOException;
// import java.nio.file.FileSystem; // Standard Java NIO
// import java.nio.file.FileSystems;
// import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;


public abstract class ArcadeLuceneIndexEngineAbstract implements LuceneIndexEngine {

  public static final String RID = "RID";
  public static final String KEY = "KEY";
  public static final String STORED_ONLY_FIELD_PREFIX = "_stored_";


  private static final Logger logger = Logger.getLogger(ArcadeLuceneIndexEngineAbstract.class.getName());

  private final AtomicLong lastAccess;
  private SearcherManager searcherManager;
  protected IndexDefinition indexDefinition;
  protected String name;
  private ControlledRealTimeReopenThread<IndexSearcher> nrt;
  protected com.arcadedb.database.Document metadata; // ArcadeDB Document
  // Lucene Version is not managed per-instance anymore. It's a library-level concern.

  protected Map<String, Boolean> collectionFields = new HashMap<>();
  private TimerTask commitTask;
  private final AtomicBoolean closed;
  private final DatabaseEngine databaseEngine; // Replaces OStorage context for path resolving etc.
  private volatile long reopenToken;
  private Analyzer indexAnalyzer;
  private Analyzer queryAnalyzer;
  private volatile ArcadeLuceneDirectory directory; // ArcadeDB specific directory wrapper
  private IndexWriter indexWriter;
  private long flushIndexInterval;
  private long closeAfterInterval;
  private long firstFlushAfter;
  private final int fileId; // ArcadeDB File ID for the index definition file
  private final String databasePath;


  public ArcadeLuceneIndexEngineAbstract(DatabaseEngine databaseEngine, String name, int fileId, String databasePath) {
    super();
    this.fileId = fileId; // This is the fileId of the index definition, not Lucene files
    this.databaseEngine = databaseEngine;
    this.name = name;
    this.databasePath = databasePath; // To resolve index path

    lastAccess = new AtomicLong(System.currentTimeMillis());
    closed = new AtomicBoolean(true);
  }

  @Override
  public int getId() {
    return fileId; // Or a specific Lucene engine ID if different from fileId
  }

  protected void updateLastAccess() {
    lastAccess.set(System.currentTimeMillis());
  }

  protected void addDocument(org.apache.lucene.document.Document doc) {
    try {
      reopenToken = indexWriter.addDocument(doc);
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error on adding new document '" + doc + "' to Lucene index " + name, e);
    }
  }

  // init from IndexInternal interface
  @Override
  public void init(IndexInternal index) {
      this.indexDefinition = index.getDefinition();
      this.metadata = new com.arcadedb.database.Document(getDatabase(), index.getTypeName(), null); // ArcadeDB Document
      if (index.getDefinition().getOptions() != null) {
          this.metadata.fromMap(index.getDefinition().getOptions());
      }

      ArcadeLuceneAnalyzerFactory fc = new ArcadeLuceneAnalyzerFactory();
      indexAnalyzer = fc.createAnalyzer(indexDefinition, INDEX, metadata);
      queryAnalyzer = fc.createAnalyzer(indexDefinition, QUERY, metadata);

      checkCollectionIndex(indexDefinition);

      flushIndexInterval = Optional.ofNullable(metadata.<Integer>get("flushIndexInterval")).orElse(10000).longValue();
      closeAfterInterval = Optional.ofNullable(metadata.<Integer>get("closeAfterInterval")).orElse(120000).longValue();
      firstFlushAfter = Optional.ofNullable(metadata.<Integer>get("firstFlushAfter")).orElse(10000).longValue();
  }

  // init for OLuceneIndexEngineAbstract specific initialization, called by ArcadeLuceneFullTextIndexEngine
  public void init(String indexName, String algorithm, IndexDefinition indexDefinition, boolean isAutomatic, com.arcadedb.database.Document metadataDoc) {
      this.name = indexName; // Ensure name is set if not already
      this.indexDefinition = indexDefinition;
      this.metadata = metadataDoc;

      ArcadeLuceneAnalyzerFactory fc = new ArcadeLuceneAnalyzerFactory();
      indexAnalyzer = fc.createAnalyzer(this.indexDefinition, INDEX, this.metadata);
      queryAnalyzer = fc.createAnalyzer(this.indexDefinition, QUERY, this.metadata);

      checkCollectionIndex(this.indexDefinition);

      flushIndexInterval = Optional.ofNullable(this.metadata.<Integer>get("flushIndexInterval")).orElse(10000).longValue();
      closeAfterInterval = Optional.ofNullable(this.metadata.<Integer>get("closeAfterInterval")).orElse(120000).longValue();
      firstFlushAfter = Optional.ofNullable(this.metadata.<Integer>get("firstFlushAfter")).orElse(10000).longValue();
  }


  private void scheduleCommitTask() {
    commitTask = new TimerTask() {
      @Override
      public void run() {
        DatabaseContext.INSTANCE.getContext(databasePath).execute(
            () -> {
              if (shouldClose()) {
                synchronized (ArcadeLuceneIndexEngineAbstract.this) {
                  if (!shouldClose()) return; // Re-check under lock
                  doClose(false);
                }
              }
              if (!closed.get()) {
                logger.log(Level.FINE, "Flushing index: " + indexName());
                flush();
              }
            });
      }
    };
    // Use ArcadeDB's scheduler if available, or a standard Java Timer/ScheduledExecutorService
    // For now, assuming DatabaseContext might provide scheduling, or this needs to be adapted.
     DatabaseContext.INSTANCE.getContext(databasePath).schedule(commitTask, firstFlushAfter, flushIndexInterval);
  }

  private boolean shouldClose() {
    //noinspection resource
    return directory != null && !(directory.getDirectory() instanceof RAMDirectory)
        && System.currentTimeMillis() - lastAccess.get() > closeAfterInterval;
  }

  private void checkCollectionIndex(IndexDefinition indexDefinition) {
    List<String> fields = indexDefinition.getPropertyNames();
    if (fields == null) return;

    DocumentType docType = getDatabase().getSchema().getType(indexDefinition.getTypeName());
    if (docType == null) return;

    for (String field : fields) {
      Property property = docType.getProperty(field);
      if (property != null && property.getType().isCollection() && property.getOfType() != null) { // Simplified check
        collectionFields.put(field, true);
      } else {
        collectionFields.put(field, false);
      }
    }
  }

  private void reOpen() throws IOException {
    //noinspection resource
    if (indexWriter != null
        && indexWriter.isOpen()
        && directory != null && directory.getDirectory() instanceof RAMDirectory) {
      return; // don't waste time reopening an in memory index
    }
    open();
  }

  protected static DatabaseInternal getDatabase() {
    return DatabaseThreadLocal.INSTANCE.get();
  }

  private synchronized void open() throws IOException {
    if (!closed.get()) return;

    ArcadeLuceneDirectoryFactory directoryFactory = new ArcadeLuceneDirectoryFactory();
    // Path needs to be resolved correctly, e.g., databasePath + File.separator + "indexes" + File.separator + name
    String resolvedIndexPath = databasePath + File.separator + "indexes" + File.separator + name;

    directory = directoryFactory.createDirectory(resolvedIndexPath, metadata); // metadata might inform directory type (e.g. RAM vs FS)

    indexWriter = createIndexWriter(directory.getDirectory());
    searcherManager = new SearcherManager(indexWriter, true, true, null);
    reopenToken = 0;
    startNRT();
    closed.set(false);
    flush(); // Initial commit
    scheduleCommitTask();
    addMetadataDocumentIfNotPresent();
  }

  private void addMetadataDocumentIfNotPresent() {
    final IndexSearcher searcher = searcher();
    try {
      final TopDocs topDocs = searcher.search(new TermQuery(new Term("_CLASS", "JSON_METADATA")), 1);
      if (topDocs.totalHits.value == 0) { // Lucene 9+ uses totalHits.value
        String metaAsJson = metadata.toJSON().toString(); // Ensure it's a string
        String defAsJson = indexDefinition.toJSON().toString(); // Ensure it's a string
        org.apache.lucene.document.Document metaDoc = new org.apache.lucene.document.Document();
        metaDoc.add(new StringField("_META_JSON", metaAsJson, Field.Store.YES));
        metaDoc.add(new StringField("_DEF_JSON", defAsJson, Field.Store.YES));
        metaDoc.add(new StringField("_DEF_CLASS_NAME", indexDefinition.getClass().getCanonicalName(), Field.Store.YES));
        metaDoc.add(new StringField("_CLASS", "JSON_METADATA", Field.Store.YES));
        addDocument(metaDoc);
      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error while retrieving index metadata for " + name, e);
    } finally {
      release(searcher);
    }
  }

  private void startNRT() {
    if (indexWriter == null || !indexWriter.isOpen()) {
        logger.warning("IndexWriter is null or closed, cannot start NRT thread for index " + name);
        return;
    }
    if (searcherManager == null) {
        logger.warning("SearcherManager is null, cannot start NRT thread for index " + name);
        return;
    }
    nrt = new ControlledRealTimeReopenThread<>(indexWriter, searcherManager, 60.00, 0.1);
    nrt.setDaemon(true);
    nrt.setName("LuceneNRT-" + name);
    nrt.start();
  }

  private void closeNRT() {
    if (nrt != null) {
      nrt.close(); // This also interrupts
      nrt = null;
    }
  }

  private void cancelCommitTask() {
    if (commitTask != null) {
      commitTask.cancel();
      commitTask = null;
    }
  }

  private void closeSearchManager() throws IOException {
    if (searcherManager != null) {
      searcherManager.close();
      searcherManager = null;
    }
  }

  private void commitAndCloseWriter() throws IOException {
    if (indexWriter != null && indexWriter.isOpen()) {
      indexWriter.commit();
      indexWriter.close();
    }
    indexWriter = null; // Make sure it's null after closing
    closed.set(true);
  }

  protected abstract IndexWriter createIndexWriter(Directory directory) throws IOException;

  @Override
  public synchronized void flush() {
    try {
      if (!closed.get() && indexWriter != null && indexWriter.isOpen()) {
        indexWriter.commit();
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error on flushing Lucene index " + name, e);
    }
  }

  @Override
  public void create(PaginatedFile.MODE mode, Identifiable valueSerializer, boolean isAutomatic, Type[] keyTypes,
                     boolean nullStrategy, int keySize, Map<String, String> engineProperties,
                     com.arcadedb.database.Document metadata) {
    // This 'create' is from IndexEngine interface.
    // OLuceneIndexEngineAbstract's create method was different.
    // This method is more about setting up the index structure if needed.
    // For Lucene, directory creation and initial writer setup happens in open().
    // We can call openIfClosed here to ensure it's initialized.
    updateLastAccess();
    openIfClosed();
  }


  @Override
  public void delete() {
    try {
      updateLastAccess();
      openIfClosed();

      if (indexWriter != null && indexWriter.isOpen()) {
        synchronized (this) {
          doClose(true); // Pass true for onDelete
        }
      }

      // Path to the Lucene index directory for this specific index
      String resolvedIndexPath = databasePath + File.separator + "indexes" + File.separator + name;
      File indexDirFile = new File(resolvedIndexPath);

      if (indexDirFile.exists() && indexDirFile.isDirectory()) {
          FileUtils.deleteRecursively(indexDirFile);
      }

    } catch (IOException e) {
      throw new StorageException("Error during deletion of Lucene index " + name, e);
    }
  }


  private void deleteIndexFolder(File baseStoragePath) throws IOException {
    // This method is complex and potentially dangerous if baseStoragePath is not the DB root.
    // ArcadeDB's Lucene integration should manage its index files within a sub-directory
    // specific to the index name, under a general "indexes" directory for the database.
    // The directory to delete should be `databasePath/indexes/<indexName>`.
    if (directory == null || directory.getDirectory() == null) {
        logger.warning("Lucene directory is null, cannot delete index folder for " + name);
        return;
    }
    String indexPath = directory.getPath(); // Assuming getPath() returns the correct Lucene index dir path
    if (indexPath != null) {
        File indexDir = new File(indexPath);
        if (indexDir.exists() && indexDir.isDirectory()) {
            FileUtils.deleteRecursively(indexDir);
        }
    }
    if (directory.getDirectory() != null) {
        directory.getDirectory().close();
    }
  }


  @Override
  public String indexName() {
    return name;
  }

  @Override
  public abstract void onRecordAddedToResultSet( LuceneQueryContext queryContext, RID recordId, org.apache.lucene.document.Document ret, ScoreDoc score);


  @Override
  public Analyzer indexAnalyzer() {
    openIfClosed(); // Ensure analyzers are initialized
    return indexAnalyzer;
  }

  @Override
  public Analyzer queryAnalyzer() {
    openIfClosed(); // Ensure analyzers are initialized
    return queryAnalyzer;
  }

  @Override
  public boolean remove(Object key, Identifiable value) {
    updateLastAccess();
    openIfClosed();

    Query query = deleteQuery(key, value);
    if (query != null) {
      deleteDocument(query);
      return true; // Assume success, though deleteDocument logs errors
    }
    return false;
  }

  @Override
  public boolean remove(Object key) {
    updateLastAccess();
    openIfClosed();
    try {
      // Assuming key is a query string if it's a single object remove
      final Query query = new QueryParser("", queryAnalyzer()).parse((String) key);
      deleteDocument(query);
      return true;
    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
      logger.log(Level.SEVERE, "Lucene parsing exception during remove for index " + name, e);
    }
    return false;
  }

  void deleteDocument(Query query) {
    try {
      reopenToken = indexWriter.deleteDocuments(query);
      // Note: hasDeletions() might not immediately reflect this for some IndexWriter configs/versions.
      // The effect is visible on the next NRT reader reopen.
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error on deleting document by query '" + query + "' to Lucene index " + name, e);
    }
  }

  private boolean isCollectionDelete() {
    boolean collectionDelete = false;
    for (Boolean aBoolean : collectionFields.values()) {
      collectionDelete = collectionDelete || aBoolean;
    }
    return collectionDelete;
  }

  protected synchronized void openIfClosed() {
    if (closed.get()) {
      try {
        open();
      } catch (final IOException e) {
        logger.log(Level.SEVERE, "Error while opening closed index: " + indexName(), e);
        throw new LuceneIndexException("Error opening Lucene index " + indexName(), e);
      }
    }
  }

  @Override
  public boolean isCollectionIndex() {
    return isCollectionDelete();
  }

  @Override
  public IndexSearcher searcher() {
    try {
      updateLastAccess();
      openIfClosed();
      if (searcherManager == null) { // Should have been initialized by open()
          throw new LuceneIndexException("SearcherManager is not initialized for index " + name);
      }
      if (nrt != null && reopenToken > 0) { // Check if reopenToken is valid
          nrt.waitForGeneration(reopenToken);
      }
      return searcherManager.acquire();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error on get searcher from Lucene index " + name, e);
      throw new LuceneIndexException("Error on get searcher from Lucene index " + name, e);
    }
  }


  @Override
  public long sizeInTx(LuceneTxChanges changes) {
    updateLastAccess();
    openIfClosed();
    IndexSearcher searcher = searcher();
    try {
      @SuppressWarnings("resource") // Reader is managed by searcherManager
      IndexReader reader = searcher.getIndexReader();
      long mainReaderDocs = reader.numDocs();
      if (mainReaderDocs > 0 && metadataDocumentExists(searcher)) {
          mainReaderDocs--; // Adjust for metadata document
      }
      return changes == null ? mainReaderDocs : mainReaderDocs + changes.numDocs();
    } finally {
      release(searcher);
    }
  }

  private boolean metadataDocumentExists(IndexSearcher searcher) {
    try {
        final TopDocs topDocs = searcher.search(new TermQuery(new Term("_CLASS", "JSON_METADATA")), 1);
        return topDocs.totalHits.value > 0;
    } catch (IOException e) {
        logger.log(Level.WARNING, "Could not check for metadata document in index " + name, e);
        return false; // Assume it doesn't exist or is inaccessible on error
    }
  }


  @Override
  public LuceneTxChanges buildTxChanges() throws IOException {
    openIfClosed(); // Ensure writer is available
    // For transactional changes, a RAMDirectory is often used.
    // The deletesExecutor might also be a RAMDirectory or use the main writer with specific markers.
    // This depends on the commit strategy for deletes.
    if (isCollectionDelete()) {
      return new LuceneTxChangesMultiRid(this, createIndexWriter(new RAMDirectory()), createIndexWriter(new RAMDirectory()));
    } else {
      return new LuceneTxChangesSingleRid(this, createIndexWriter(new RAMDirectory()), createIndexWriter(new RAMDirectory()));
    }
  }

  @Override
  public Query deleteQuery(Object key, Identifiable value) {
    updateLastAccess();
    openIfClosed();
    // ArcadeLuceneIndexType.createDeleteQuery needs to be refactored as it used ODocument for metadata
    // For now, creating a simple TermQuery if value is present.
    // This is a placeholder and likely needs more sophisticated logic from the (to be refactored) ArcadeLuceneIndexType.
    if (value != null && value.getIdentity() != null) {
        return new TermQuery(new Term(RID, value.getIdentity().toString()));
    }
    // Fallback or more complex query if only key is provided
    return new TermQuery(new Term(KEY, key.toString()));
  }

  @Override
  public void load(String indexName, IndexDefinition indexDefinition, PaginatedFile.MODE mode) {
    this.name = indexName;
    this.indexDefinition = indexDefinition;
    // metadata should be derived from indexDefinition.getOptions()
    this.metadata = new com.arcadedb.database.Document(getDatabase(), indexDefinition.getTypeName(), null);
     if (indexDefinition.getOptions() != null) {
        this.metadata.fromMap(indexDefinition.getOptions());
    }

    ArcadeLuceneAnalyzerFactory fc = new ArcadeLuceneAnalyzerFactory();
    indexAnalyzer = fc.createAnalyzer(this.indexDefinition, INDEX, this.metadata);
    queryAnalyzer = fc.createAnalyzer(this.indexDefinition, QUERY, this.metadata);

    checkCollectionIndex(this.indexDefinition);

    flushIndexInterval = Optional.ofNullable(this.metadata.<Integer>get("flushIndexInterval")).orElse(10000).longValue();
    closeAfterInterval = Optional.ofNullable(this.metadata.<Integer>get("closeAfterInterval")).orElse(120000).longValue();
    firstFlushAfter = Optional.ofNullable(this.metadata.<Integer>get("firstFlushAfter")).orElse(10000).longValue();

    updateLastAccess();
    openIfClosed(); // This will initialize directory, writer, searcherManager, NRT thread
  }


  @Override
  public void clear(com.arcadedb.database.TransactionContext tx) { // Changed to ArcadeDB TransactionContext
    updateLastAccess();
    openIfClosed();
    try {
      reopenToken = indexWriter.deleteAll();
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error on clearing Lucene index " + name, e);
    }
  }

  @Override
  public synchronized void close() {
    doClose(false);
  }

  private void doClose(boolean onDelete) {
    if (closed.get() && !onDelete) return; // If already closed and not deleting, do nothing. If deleting, proceed to ensure dir is closed.

    try {
      cancelCommitTask();
      closeNRT();
      closeSearchManager(); // This will also release any acquired searchers

      // Only commit and close writer if not part of a delete operation that handles its own dir cleanup
      if (!onDelete) {
          commitAndCloseWriter();
      } else { // If onDelete, just close writer without commit, as files will be deleted
          if (indexWriter != null && indexWriter.isOpen()) {
              indexWriter.rollback(); // Rollback to avoid partial commit if onDelete is true
              indexWriter = null;
          }
      }

      if (directory != null && directory.getDirectory() != null) {
          directory.getDirectory().close();
          directory = null;
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error on closing Lucene index " + name, e);
    } finally {
        closed.set(true); // Mark as closed even if errors occurred during close
        indexWriter = null;
        searcherManager = null;
        nrt = null;
        directory = null;
    }
  }

  @Override
  public Stream<Pair<Object, RID>> descStream(Object valuesTransformer) { // Adjusted to match IndexEngine
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  @Override
  public Stream<Pair<Object, RID>> stream(Object valuesTransformer) { // Adjusted to match IndexEngine
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  @Override
  public Stream<Object> keyStream() {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  public long size(final Object transformer) { // Adjusted to match IndexEngine ValuesTransformer type
    return sizeInTx(null);
  }

  // public long size(final IndexValuesTransformer transformer) { // Adjusted to match IndexEngine ValuesTransformer type
  // return sizeInTx(null);
  // }
  // TODO: Restore when IndexValuesTransformer is available

  @Override
  public void release(IndexSearcher searcher) {
    updateLastAccess();
    // openIfClosed(); // Not strictly needed for release, but good for consistency if searcherManager could be null

    try {
      if (searcherManager != null) {
        searcherManager.release(searcher);
      } else if (searcher != null && searcher.getIndexReader() != null) {
        // Fallback if searcherManager is somehow null but we have a searcher
        searcher.getIndexReader().decRef();
      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error on releasing index searcher of Lucene index " + name, e);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    // Lucene's IndexWriter is thread-safe. Fine-grained locking per key not typical.
    return true;
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    return name; // Lucene engine typically manages one index name
  }

  @Override
  public synchronized void freeze(boolean throwException) {
    try {
      closeNRT();
      cancelCommitTask();
      commitAndCloseWriter(); // Commits and closes the writer
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error on freezing Lucene index: " + indexName(), e);
      if (throwException) {
        throw new LuceneIndexException("Error on freezing Lucene index: " + indexName(), e);
      }
    }
  }

  @Override
  public void release() {
    // This method is for releasing resources after a freeze, typically by reopening.
    // The `closed` flag would have been set by `freeze` via `commitAndCloseWriter`.
    // `openIfClosed` will then re-initialize everything.
    openIfClosed();
  }
}
