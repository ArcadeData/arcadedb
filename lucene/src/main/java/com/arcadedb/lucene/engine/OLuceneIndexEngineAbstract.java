/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.arcadedb.lucene.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseThreadLocal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RecordId;
import com.arcadedb.document.Document;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.Storage;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.IndexException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexDefinition;
import com.arcadedb.index.IndexKeyCursor;
import com.arcadedb.lucene.analyzer.ArcadeLuceneAnalyzerFactory;
import com.arcadedb.lucene.exception.LuceneIndexException;
import com.arcadedb.lucene.index.ArcadeLuceneIndexType;
import com.arcadedb.lucene.query.LuceneQueryContext;
import com.arcadedb.lucene.tx.LuceneTxChanges;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.arcadedb.lucene.analyzer.ArcadeLuceneAnalyzerFactory.AnalyzerK
ind.INDEX;
import static com.arcadedb.lucene.analyzer.ArcadeLuceneAnalyzerFactory.AnalyzerK
ind.QUERY;

public abstract class OLuceneIndexEngineAbstract<V> /* extends OSharedResourceAd
aptiveExternal */ implements OLuceneIndexEngine { // FIXME

  public static final String RID = "RID";
  public static final String KEY = "KEY";
  public static final String STORED = "_STORED";

  public static final String OLUCENE_BASE_DIR = "luceneIndexes";

  protected final AtomicLong lastAccess;
  protected SearcherManager searcherManager;
  protected IndexDefinition index;
  protected String name;
  protected String clusterIndexName;
  protected boolean automatic;
  protected ControlledRealTimeReopenThread nrt;
  protected Document metadata;

  protected Map<String, Boolean> collectionFields = new HashMap<String, Boolean>
();
  protected TimerTask commitTask;
  protected AtomicBoolean closed = new AtomicBoolean(false);
  protected Storage storage;
  private long reopenToken;
  private Analyzer indexAnalyzer;
  private Analyzer queryAnalyzer;
  private Directory directory;
  private TrackingIndexWriter mgrWriter;
  private long flushIndexInterval;
  private long closeAfterInterval;
  private long firstFlushAfter;

  public OLuceneIndexEngineAbstract(Storage storage, String indexName) {
    this.storage = storage;
    this.name = indexName;

    lastAccess = new AtomicLong(System.currentTimeMillis());

    closed = new AtomicBoolean(true);

  }

  // TODO: move to utility class
  public static void sendTotalHits(String indexName, CommandContext context, int
 totalHits) {
    if (context != null) {

      if (context.getVariable("totalHits") == null) {
        context.setVariable("totalHits", totalHits);
      } else {
        context.setVariable("totalHits", null);
      }
      context.setVariable((indexName + ".totalHits").replace(".", "_"), totalHit
s);
    }
  }

  // TODO: move to utility class
  public static void sendLookupTime(String indexName, CommandContext context, fi
nal TopDocs docs, final Integer limit,
      long startFetching) {
    if (context != null) {

      final long finalTime = System.currentTimeMillis() - startFetching;
      context.setVariable((indexName + ".lookupTime").replace(".", "_"), new Has
hMap<String, Object>() {
        {
          put("limit", limit);
          put("totalTime", finalTime);
          put("totalHits", docs.totalHits);
          put("returnedHits", docs.scoreDocs.length);
          if (!Float.isNaN(docs.getMaxScore())) {
            put("maxScore", docs.getMaxScore());
          }

        }
      });
    }
  }

  protected void updateLastAccess() {
    lastAccess.set(System.currentTimeMillis());
  }

  protected abstract IndexWriter openIndexWriter(Directory directory) throws IOE
xception;

  protected void addDocument(Document doc) {
    try {

      reopenToken = mgrWriter.addDocument(doc);
    } catch (IOException e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on adding
 new document '" + doc + "' to Lucene index", e);
    }
  }

  @Override
  public void init(String indexName, String indexType, IndexDefinition indexDefi
nition, boolean isAutomatic, Document metadata) {

    this.index = indexDefinition;
    this.automatic = isAutomatic;
    this.metadata = metadata;

    ArcadeLuceneAnalyzerFactory fc = new ArcadeLuceneAnalyzerFactory();
    indexAnalyzer = fc.createAnalyzer(indexDefinition, INDEX, metadata);
    queryAnalyzer = fc.createAnalyzer(indexDefinition, QUERY, metadata);

    checkCollectionIndex(indexDefinition);

    if (metadata.containsField("flushIndexInterval")) {
      flushIndexInterval = Integer.valueOf(metadata.<Integer>field("flushIndexIn
terval")).longValue();
    } else {
      flushIndexInterval = 10000l;
    }

    if (metadata.containsField("closeAfterInterval")) {
      closeAfterInterval = Integer.valueOf(metadata.<Integer>field("closeAfterIn
terval")).longValue();
    } else {
      closeAfterInterval = 20000l;
    }

    if (metadata.containsField("firstFlushAfter")) {
      firstFlushAfter = Integer.valueOf(metadata.<Integer>field("firstFlushAfter
")).longValue();
    } else {
      firstFlushAfter = 10000l;
    }

  }

  private void scheduleCommitTask() {
    commitTask = new TimerTask() {
      @Override
      public boolean cancel() {
//        Logger.getLogger(getClass().getName()).info(" Cancelling commit task f
or index:: " + indexName());
        return super.cancel();
      }

      @Override
      public void run() {

        if (System.currentTimeMillis() - lastAccess.get() > closeAfterInterval)
{

//          Logger.getLogger(getClass().getName()).info(" Closing index:: " + in
dexName());
          close();
        }
        if (!closed.get()) {

//          Logger.getLogger(getClass().getName()).info(" Flushing index:: " + i
ndexName());
          flush();
        }
      }
    };
    // FIXME
    // Orient.instance().scheduleTask(commitTask, firstFlushAfter, flushIndexInt
erval);
    getDatabase().getSchema().getScheduler().scheduleTask(commitTask, firstFlush
After, flushIndexInterval);
  }

  private void checkCollectionIndex(IndexDefinition indexDefinition) {

    List<String> fields = indexDefinition.getFields();

    DocumentType aClass = getDatabase().getSchema().getType(indexDefinition.getT
ypeName());
    for (String field : fields) {
      Property property = aClass.getProperty(field);

      if (property.getType().isEmbedded() && property.getLinkedType() != null) {
        collectionFields.put(field, true);
      } else {
        collectionFields.put(field, false);
      }
    }
  }

  protected void reOpen() throws IOException {

    if (mgrWriter != null && mgrWriter.getIndexWriter().isOpen() && directory in
stanceof RAMDirectory) {
      // don't waste time reopening an in memory index
      return;
    }
    open();
  }

  protected DatabaseInternal getDatabase() {
    return DatabaseThreadLocal.INSTANCE.get();
  }

  private synchronized void open() throws IOException {

    if (!closed.get())
      return;

    ArcadeLuceneDirectoryFactory directoryFactory = new ArcadeLuceneDirectoryFac
tory(); // FIXME OLuceneDirectoryFactory

    directory = directoryFactory.createDirectory(getDatabase(), name, metadata);

    final IndexWriter indexWriter = createIndexWriter(directory);
    mgrWriter = new TrackingIndexWriter(indexWriter);
    searcherManager = new SearcherManager(indexWriter, true, null);

    reopenToken = 0;

    startNRT();

    closed.set(false);

    flush();

    scheduleCommitTask();

  }

  private void startNRT() {
    nrt = new ControlledRealTimeReopenThread(mgrWriter, searcherManager, 60.00,
0.1);
    nrt.setDaemon(true);
    nrt.start();
  }

  private void closeNRT() {
    if (nrt != null) {
      nrt.interrupt();
      nrt.close();
    }
  }

  private void cancelCommitTask() {
    if (commitTask != null) {
      commitTask.cancel();
    }
  }

  private void closeSearchManager() throws IOException {
    if (searcherManager != null) {
      searcherManager.close();
    }
  }

  private void commitAndCloseWriter() throws IOException {
    if (mgrWriter != null && mgrWriter.getIndexWriter().isOpen()) {
      mgrWriter.getIndexWriter().commit();
      mgrWriter.getIndexWriter().close();
      closed.set(true);
    }
  }

  protected abstract IndexWriter createIndexWriter(Directory directory) throws I
OException;

  @Override
  public void flush() {

    try {
      if (mgrWriter != null && mgrWriter.getIndexWriter().isOpen())
        mgrWriter.getIndexWriter().commit();

    } catch (IOException e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on flushi
ng Lucene index", e);
    } catch (Throwable e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on flushi
ng Lucene index", e);
    }

  }

  @Override
  public void create(com.arcadedb.serializer.BinarySerializer valueSerializer, b
oolean isAutomatic, Type[] keyTypes, boolean nullPointerSupport,
      com.arcadedb.serializer.BinarySerializer keySerializer, int keySize, Set<S
tring> clustersToIndex, Map<String, String> engineProperties,
      Document metadata) {
  }

  @Override
  public void delete() {
    updateLastAccess();
    openIfClosed();

    if (mgrWriter != null && mgrWriter.getIndexWriter() != null) {

      try {
        mgrWriter.getIndexWriter().deleteUnusedFiles();
      } catch (IOException e) {
        e.printStackTrace();
      }
      close();
    }

    final DatabaseInternal database = getDatabase();
    deleteIndexFolder(indexName(), database);
  }

  private void deleteIndexFolder(String indexName, DatabaseInternal database) {
// FIXME OLocalPaginatedStorage
    File f = new File(getIndexPath(database, indexName));
    FileUtils.deleteRecursively(f);
    f = new File(getIndexBasePath(database));
    FileUtils.deleteFolderIfEmpty(f);
  }

  @Override
  public String indexName() {
    return name;
  }

  private String getIndexPath(DatabaseInternal database, String indexName) { // F
IXME OLocalPaginatedStorage
    return database.getDatabasePath() + File.separator + OLUCENE_BASE_DIR + File
.separator + indexName; // FIXME getStoragePath
  }

  protected String getIndexBasePath(DatabaseInternal database) { // FIXME OLocal
PaginatedStorage
    return database.getDatabasePath() + File.separator + OLUCENE_BASE_DIR; // FIX
ME getStoragePath
  }

  public abstract void onRecordAddedToResultSet(LuceneQueryContext queryContext,
 RecordId recordId, Document ret,
      ScoreDoc score);

  @Override
  public Analyzer indexAnalyzer() {
    return indexAnalyzer;
  }

  @Override
  public Analyzer queryAnalyzer() {
    return queryAnalyzer;
  }

  @Override
  public boolean remove(Object key, Identifiable value) {
    updateLastAccess();
    openIfClosed();

    Query query = deleteQuery(key, value);
    if (query != null)
      deleteDocument(query);
    return true;
  }

  protected void deleteDocument(Query query) {
    try {
      reopenToken = mgrWriter.deleteDocuments(query);
      if (!mgrWriter.getIndexWriter().hasDeletions()) {
        Logger.getLogger(getClass().getName())
            .log(Level.SEVERE, "Error on deleting document by query '" + query +
 "' to Lucene index", new IndexException("Error deleting document"));
      }
    } catch (IOException e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on deleti
ng document by query '" + query + "' to Lucene index", e);
    }
  }

  protected boolean isCollectionDelete() {
    boolean collectionDelete = false;
    for (Boolean aBoolean : collectionFields.values()) {
      collectionDelete = collectionDelete || aBoolean;
    }
    return collectionDelete;
  }

  protected void openIfClosed() {
    if (closed.get()) {
//      Logger.getLogger(getClass().getName()).info("open closed index:: " + ind
exName());

      try {
        reOpen();
      } catch (IOException e) {
        Logger.getLogger(getClass().getName()).log(Level.SEVERE, "error while o
pening closed index:: " + indexName(), e);

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
      nrt.waitForGeneration(reopenToken);
      IndexSearcher searcher = searcherManager.acquire();
      return searcher;
    } catch (Exception e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on get se
archer from Lucene index", e);
      throw new LuceneIndexException("Error on get searcher from Lucene index",
e);
    }

  }

  @Override
  public long sizeInTx(LuceneTxChanges changes) {
    IndexSearcher searcher = searcher();
    try {
      IndexReader reader = searcher.getIndexReader();

      return changes == null ? reader.numDocs() : reader.numDocs() + changes.get
NumDocs();
    } finally {

      release(searcher);
    }
  }

  @Override
  public LuceneTxChanges buildTxChanges() throws IOException {
    if (isCollectionDelete()) {
      // FIXME
      // return new OLuceneTxChangesMultiRid(this, createIndexWriter(new RAMDire
ctory()), createIndexWriter(new RAMDirectory()));
      return null;
    } else {
      // FIXME
      // return new OLuceneTxChangesSingleRid(this, createIndexWriter(new RAMDire
ctory()), createIndexWriter(new RAMDirectory()));
      return null;
    }
  }

  @Override
  public Query deleteQuery(Object key, Identifiable value) {
    updateLastAccess();
    openIfClosed();
    if (isCollectionDelete()) {
      return ArcadeLuceneIndexType.createDeleteQuery(value, index.getFields(),
key);
    }
    return ArcadeLuceneIndexType.createQueryId(value);
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    internalDelete(indexName);
  }

  protected void internalDelete(String indexName) {
    if (mgrWriter != null && mgrWriter.getIndexWriter().isOpen()) {
      close();
    }

    final DatabaseInternal database = getDatabase();
    deleteIndexFolder(indexName, database);
  }

  @Override
  public void load(String indexName, com.arcadedb.serializer.BinarySerializer v
alueSerializer, boolean isAutomatic, com.arcadedb.serializer.BinarySerializer k
eySerializer,
      Type[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, Stri
ng> engineProperties) {
    // initIndex(indexName, indexDefinition, isAutomatic, metadata);
  }

  @Override
  public void clear() {
    updateLastAccess();
    openIfClosed();
    try {
      reopenToken = mgrWriter.deleteAll();
    } catch (IOException e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on cleari
ng Lucene index", e);
    }
  }

  @Override
  public synchronized void close() {
    if (closed.get())
      return;

    try {
//      Logger.getLogger(getClass().getName()).info("Closing Lucene index '" + t
his.name + "'...");

      closeNRT();

      closeSearchManager();

      commitAndCloseWriter();

//      Logger.getLogger(getClass().getName()).info("Closed Lucene index '" + th
is.name);
      cancelCommitTask();

    } catch (Throwable e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on closin
g Lucene index", e);
    }
  }

  @Override
  public IndexCursor descCursor(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index"
);
  }

  @Override
  public IndexCursor cursor(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index"
);
  }

  @Override
  public IndexKeyCursor keyCursor() {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index"
);
  }

  public long size(final ValuesTransformer transformer) {
    return sizeInTx(null);
  }

  protected void release(IndexSearcher searcher) {
    updateLastAccess();
    openIfClosed();
    try {
      searcherManager.release(searcher);
    } catch (IOException e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on releas
ing index searcher  of Lucene index", e);
    }
  }

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    return true; // do nothing
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    return name;
  }

  private String getIndexPath(DatabaseInternal database) { // FIXME OLocalPagina
tedStorage
    return getIndexPath(database, name);
  }

  protected Field.Store isToStore(String f) {
    return collectionFields.get(f) ? Field.Store.YES : Field.Store.NO;
  }

  @Override
  public void freeze(boolean throwException) {

    try {
      closeNRT();
      cancelCommitTask();
      commitAndCloseWriter();
    } catch (IOException e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on freezi
ng Lucene index:: " + indexName(), e);
    }

  }

  @Override
  public void release() {
    try {
      close();
      reOpen();
    } catch (IOException e) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error on releas
ing Lucene index:: " + indexName(), e);
    }
  }

  @Override
  public boolean isFrozen() {
    return closed.get();
  }
}
