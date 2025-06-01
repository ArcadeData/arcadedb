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

import com.arcadedb.database.Identifiable; // Changed
import org.apache.lucene.analysis.Analyzer; // Added for new methods
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader; // Added for new methods
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs; // Added for new methods
import org.apache.lucene.util.Bits; // Added for new methods

import java.io.IOException; // Added for new methods
import java.util.Collections;
import java.util.List; // Added for new methods
import java.util.Map; // Added for new methods
import java.util.Set;

/** Created by Enrico Risa on 15/09/15. */
public interface LuceneTxChanges { // Changed interface name

  // Existing methods adapted
  void put(Object key, Identifiable value, Document doc); // Changed OIdentifiable

  void remove(Object key, Identifiable value); // Changed OIdentifiable

  IndexSearcher searcher(); // Existing method, seems to be the transactional searcher

  // numDocs() from prompt matches existing signature (except return type was long, now int as per Lucene's numDocs())
  int numDocs(); // Changed from long to int

  // getDeletedDocs() from prompt returns Set<Query>, existing returned Set<Document>
  // Renaming existing to getDeletedLuceneDocs for clarity and adding new one
  default Set<Document> getDeletedLuceneDocs() { // Kept original behavior with new name
    return Collections.emptySet();
  }

  // isDeleted(Document, Object, OIdentifiable) adapted
  boolean isDeleted(Document document, Object key, Identifiable value); // Changed OIdentifiable

  // isUpdated(Document, Object, OIdentifiable) adapted
  boolean isUpdated(Document document, Object key, Identifiable value); // Changed OIdentifiable

  // deletedDocs(Query query) from prompt returns Bits, existing returned long
  // Renaming existing to countDeletedDocs for clarity and adding new one
  default long countDeletedDocs(Query query) { // Kept original behavior with new name
      return 0;
  }

  // New methods from prompt
  IndexSearcher getCoordinatingSearcher(); // New: Could be the main index searcher before TX changes overlay

  Bits deletedDocs(Query query); // New: Returns Bits for live docs

  boolean isUpdated(Document doc, Analyzer analyzer, Query query); // New: Overload with Analyzer and Query

  boolean isDeleted(Document doc, Analyzer analyzer, Query query); // New: Overload with Analyzer and Query

  int nDoc(Query query); // New: Number of documents matching query in current TX state

  Set<Query> getDeletedDocuments(); // New: Set of deletion queries

  Map<Query, Document> getUpdatedDocuments(); // New: Map of update queries to new documents

  List<Document> getAddedDocuments(); // New: List of added documents

  IndexReader getReader() throws IOException; // New: Get current transactional reader

  TopDocs query(Query query, int N) throws IOException; // New: Execute query with limit // Changed signature to add N

  Document doc(int doc) throws IOException; // New: Retrieve Lucene document by internal ID

  Document doc(int doc, Set<String> fieldsToLoad) throws IOException; // New: Retrieve specific fields

  void close() throws IOException; // New

  int maxDoc() throws IOException; // New

  boolean hasDeletions(); // New

  void commit() throws IOException; // New

  void rollback() throws IOException; // New

  void addDocument(Document document) throws IOException; // New

  void deleteDocument(Query query) throws IOException; // New

  void updateDocument(Query query, Document document) throws IOException; // New
}
