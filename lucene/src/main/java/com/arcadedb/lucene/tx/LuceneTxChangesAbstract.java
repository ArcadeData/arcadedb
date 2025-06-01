/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  * Copyright 2023 Arcade Data Ltd
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.arcadedb.lucene.tx;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.ArcadeDBException; // Changed
import com.arcadedb.lucene.engine.LuceneIndexEngine; // Changed
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger; // Changed

/** Created by Enrico Risa on 28/09/15. */
public abstract class LuceneTxChangesAbstract implements LuceneTxChanges { // Changed class name and interface
  private static final Logger logger =
      Logger.getLogger(LuceneTxChangesAbstract.class.getName()); // Changed
  public static final String TMP = "_tmp_rid"; // This constant seems unused here, but kept for now.

  protected final LuceneIndexEngine engine; // Changed
  protected final IndexWriter writer; // For new/updated documents
  protected final IndexWriter deletesExecutor; // For pending deletions

  private IndexSearcher txSearcher; // Cached NRT searcher for the current transaction state (adds + main)
  private IndexReader txReader;     // Cached NRT reader for the current transaction state

  public LuceneTxChangesAbstract( // Changed
      final LuceneIndexEngine engine, final IndexWriter writer, final IndexWriter deletesExecutor) {
    this.engine = engine;
    this.writer = writer;
    this.deletesExecutor = deletesExecutor;
  }

  // Method to get a transactional reader, possibly NRT from writer
  protected IndexReader getTxReader() throws IOException {
    if (txReader == null || !txReader.tryIncRef()) { // Check if reader is still valid or can be used
        if (txReader != null) { // was valid, but couldn't incRef, so it's likely closed
            try {
                txReader.decRef(); // ensure it's closed if it was open
            } catch (Exception e) { /* ignore */ }
        }
        // If writer is null or closed, this will throw an exception, which is appropriate.
        txReader = DirectoryReader.open(writer); // Standard NRT reader
    }
    return txReader;
  }

  protected void NRTReaderReopen() throws IOException{
    if (txReader != null) {
        IndexReader newReader = DirectoryReader.openIfChanged((DirectoryReader)txReader, writer);
        if (newReader != null) {
            txReader.decRef();
            txReader = newReader;
            txSearcher = new IndexSearcher(txReader);
        }
    } else {
        txReader = DirectoryReader.open(writer);
        txSearcher = new IndexSearcher(txReader);
    }
  }


  @Override
  public IndexSearcher searcher() {
    try {
      // Return a new NRT searcher reflecting current changes in 'writer'
      // This searcher sees documents added/updated in the current TX but not yet committed.
      // It does not see documents deleted in this TX against the main index.
      // For a searcher that sees deletes as well, getCoordinatingSearcher might be better.
      NRTReaderReopen();
      return txSearcher;
    } catch (IOException e) {
      throw ArcadeDBException.wrapException( // Changed
          new ArcadeDBException("Error creating transactional IndexSearcher from writer"), e); // Changed
    }
  }

  @Override
  public IndexSearcher getCoordinatingSearcher() {
    // This searcher should ideally reflect adds, updates, AND deletes.
    // This typically involves a MultiReader combining the main index (with its own deletions applied)
    // and the in-memory 'writer' index, while filtering out documents marked for deletion by 'deletesExecutor'.
    // For simplicity in this abstract class, could return the same as searcher() and expect
    // query execution layer to use getLiveDocs() or similar.
    // Or, could be more complex here if a combined view is built.
    // For now, let's assume it's similar to searcher() but it's a point for review.
    // The engine's main searcher is `engine.searcher()`
    // FIXME: This needs a proper implementation, probably involving MultiReader and live docs from deletesExecutor
    return searcher();
  }

  @Override
  public IndexReader getReader() throws IOException {
      return getTxReader();
  }

  @Override
  public long countDeletedDocs(Query query) { // Renamed from original deletedDocs
    try {
      // This counts documents matching the query in the 'deletesExecutor' index.
      // These are documents marked for deletion in this transaction.
      if (deletesExecutor.getDocStats().numDocs == 0) return 0; // Optimization
      try (IndexReader reader = DirectoryReader.open(deletesExecutor)) {
        final IndexSearcher indexSearcher = new IndexSearcher(reader);
        final TopDocs search = indexSearcher.search(query, 1); // We only need totalHits
        return search.totalHits.value;
      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error reading pending deletions index", e); // Changed
    }
    return 0;
  }

  @Override
  public Bits deletedDocs(Query query) {
    // This should return a Bits representing documents deleted by this query
    // within the context of the main index reader (from engine.searcher()).
    // This is complex as it needs to check against the 'deletesExecutor' or tracked delete queries.
    // Not typically provided directly by IndexWriter for pending changes.
    // FIXME: This needs a proper implementation, likely involving custom collector or query rewriting.
    logger.warning("deletedDocs(Query) returning Bits is not fully implemented in abstract class.");
    return null; // Placeholder
  }


  @Override
  public void addDocument(Document document) throws IOException {
    writer.addDocument(document);
  }

  @Override
  public void deleteDocument(Query query) throws IOException {
    // Deletes applied to main writer will be visible to its NRT reader.
    // If deletesExecutor is for tracking standalone delete operations before commit to main index:
    // writer.deleteDocuments(query); // This applies to the current TX state
    // deletesExecutor.addDocument(createDeleteMarker(query)); // If deletes are tracked as docs in a separate index
    // For now, assuming deletes are applied to the main writer for NRT visibility.
    // If deletesExecutor is a separate RAMDirectory for _pending full deletes_ against main index,
    // then it should be: deletesExecutor.deleteDocuments(query) or writer.deleteDocuments(query)
    // The original code had separate writer and deletedIdx. Let's assume deletes are applied to writer.
    writer.deleteDocuments(query);
    if(deletesExecutor != writer && deletesExecutor != null) { // If deletes are tracked separately for commit to main index
        deletesExecutor.deleteDocuments(query);
    }
  }

  @Override
  public void updateDocument(Query query, Document document) throws IOException {
    writer.updateDocument(query, document);
     if(deletesExecutor != writer && deletesExecutor != null) {
        // If an update can also affect the "to be deleted from main index" list, handle here.
        // This is complex. Usually an update is a delete then an add.
        // deletesExecutor.updateDocument(query, document); // This might not be how it works.
    }
  }

  @Override
  public void commit() throws IOException {
    writer.commit();
    if (deletesExecutor != null && deletesExecutor != writer) {
      deletesExecutor.commit();
    }
  }

  @Override
  public void rollback() throws IOException {
    writer.rollback();
    if (deletesExecutor != null && deletesExecutor != writer) {
      deletesExecutor.rollback();
    }
  }

  @Override
  public void close() throws IOException {
    try {
        if (txReader != null) {
            txReader.decRef();
            txReader = null;
        }
    } finally {
        txSearcher = null; // Searcher was using txReader
        try {
            writer.close();
        } finally {
            if (deletesExecutor != null && deletesExecutor != writer) {
                deletesExecutor.close();
            }
        }
    }
  }

  @Override
  public int numDocs() {
    // Returns numDocs of the current transactional reader (reflecting adds/updates in this TX)
    try (IndexReader reader = getTxReader()) { // getTxReader handles incRef/decRef
        return reader.numDocs();
    } catch (IOException e) {
        logger.log(Level.SEVERE, "Cannot get numDocs from transactional reader", e);
        return 0;
    }
  }

  @Override
  public int maxDoc() throws IOException {
     try (IndexReader reader = getTxReader()) {
        return reader.maxDoc();
    }
  }

  @Override
  public boolean hasDeletions() {
    // Check deletions in the context of the main writer for NRT changes
    return writer.hasDeletions();
  }

  @Override
  public TopDocs query(Query query, int n) throws IOException {
    NRTReaderReopen(); // Ensure searcher is up-to-date
    return txSearcher.search(query, n);
  }

  @Override
  public Document doc(int docId) throws IOException {
    NRTReaderReopen();
    return txSearcher.storedFields().document(docId);
  }

  @Override
  public Document doc(int docId, Set<String> fieldsToLoad) throws IOException {
    NRTReaderReopen();
    return txSearcher.storedFields().document(docId, fieldsToLoad);
  }

  // Methods requiring more specific state tracking, to be implemented by concrete classes or left as default/abstract.
  // These were not in the original OLuceneTxChangesAbstract.

  @Override
  public abstract void put(Object key, Identifiable value, Document doc);

  @Override
  public abstract void remove(Object key, Identifiable value);

  @Override
  public abstract boolean isDeleted(Document document, Object key, Identifiable value);

  @Override
  public abstract boolean isUpdated(Document document, Object key, Identifiable value);

  @Override
  public boolean isUpdated(Document doc, Analyzer analyzer, Query query) {
    // Default: Not supported or needs concrete implementation
    logger.warning("isUpdated(doc, analyzer, query) not implemented in abstract class.");
    return false;
  }

  @Override
  public boolean isDeleted(Document doc, Analyzer analyzer, Query query) {
    // Default: Not supported or needs concrete implementation
    logger.warning("isDeleted(doc, analyzer, query) not implemented in abstract class.");
    return false;
  }

  @Override
  public int nDoc(Query query) {
    // Number of documents matching query in current TX state
    try {
      TopDocs results = query(query, 1); // Just need total hits
      return (int) results.totalHits.value;
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error executing nDoc query", e);
      return 0;
    }
  }

  // These typically require tracking specific operations, left abstract or default.
  @Override
  public Set<Query> getDeletedDocuments() {
    logger.warning("getDeletedDocuments() not implemented in abstract class, returning empty set.");
    return Collections.emptySet();
  }

  @Override
  public Map<Query, Document> getUpdatedDocuments() {
    logger.warning("getUpdatedDocuments() not implemented in abstract class, returning empty map.");
    return Collections.emptyMap();
  }

  @Override
  public List<Document> getAddedDocuments() {
    logger.warning("getAddedDocuments() not implemented in abstract class, returning empty list.");
    return Collections.emptyList();
  }
}
