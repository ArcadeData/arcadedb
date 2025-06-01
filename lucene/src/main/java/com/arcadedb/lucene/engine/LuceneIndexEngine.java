/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  * Copyright 2014 Orient Technologies.
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

package com.arcadedb.lucene.engine;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RecordId;
import com.arcadedb.engine.WALFile; // For Freezeable
import com.arcadedb.index.IndexEngine;
import com.arcadedb.lucene.query.LuceneQueryContext; // Will be refactored
import com.arcadedb.lucene.tx.LuceneTxChanges; // Will be refactored
import java.io.IOException;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

/** Created by Enrico Risa on 04/09/15. */
public interface LuceneIndexEngine extends IndexEngine, WALFile.Freezeable { // Changed interface name and extended interfaces

  String indexName();

  void onRecordAddedToResultSet( // Changed parameter types
      LuceneQueryContext queryContext, RecordId recordId, org.apache.lucene.document.Document ret, ScoreDoc score);

  org.apache.lucene.document.Document buildDocument(Object key, Identifiable value); // Changed parameter type

  Query buildQuery(Object query);

  Analyzer indexAnalyzer();

  Analyzer queryAnalyzer();

  boolean remove(Object key, Identifiable value); // Changed parameter type

  boolean remove(Object key);

  IndexSearcher searcher();

  void release(IndexSearcher searcher);

  Set<Identifiable> getInTx(Object key, LuceneTxChanges changes); // Changed parameter and return types

  long sizeInTx(LuceneTxChanges changes); // Changed parameter type

  LuceneTxChanges buildTxChanges() throws IOException; // Changed return type

  Query deleteQuery(Object key, Identifiable value); // Changed parameter type

  boolean isCollectionIndex();
}
