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

package com.arcadedb.lucene.collections;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.lucene.engine.ArcadeLuceneIndexEngine;
import com.arcadedb.lucene.engine.ArcadeLuceneIndexEngineAbstract;
import com.arcadedb.lucene.engine.ArcadeLuceneEngineUtils;
import com.arcadedb.lucene.exception.LuceneIndexException;
import com.arcadedb.lucene.query.LuceneQueryContext;
import com.arcadedb.lucene.tx.ArcadeLuceneTxChangesAbstract;
import com.arcadedb.query.sql.executor.CommandContext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ArcadeLuceneResultSet implements Set<Identifiable> {

  private static final Logger LOGGER = Logger.getLogger(ArcadeLuceneResultSet.class.getName());
  public static final  int    PAGE_SIZE = 10000;

  private final ArcadeLuceneIndexEngine             engine;
  private final LuceneQueryContext                  queryContext;
  private       IndexSearcher                       searcher;
  private final Query                               query;
  private       TopDocs                             topDocs;
  private       ScoreDoc[]                          scoreDocs;
  private       int                                 idx;
  private       String                              indexName;
  private       Map<String, Object>                 metadata;
  private       Analyzer                            analyzer;
  private       boolean                             txChanges;

  public ArcadeLuceneResultSet(ArcadeLuceneIndexEngine engine, LuceneQueryContext queryContext) {
    this.engine = engine;
    this.indexName = engine.indexName();
    this.queryContext = queryContext;
    this.analyzer = engine.getAnalyzer();

    try {
      this.searcher = queryContext.getSearcher();
      this.query = queryContext.getQuery();

      if (query == null) {
        // Query could be null if the query is '*' and the query shape is null
        // then the query is re-written to MatchAllDocsQuery
        // if the query is rewritten to null then the next queryContext.deletedDocs(query)
        // fails with NPE
        // SEE OLuceneQueryContext: query =空气污染指数
        // this.query = new MatchAllDocsQuery();
        // TODO: check this case better
        scoreDocs = new ScoreDoc[0];
        return;
      }

      Sort sort = queryContext.getSort();
      if (sort == null) {
        final IndexReader reader = searcher.getIndexReader();
        int N = Math.max(1, reader.numDocs());
        if (queryContext.isDisableScores()) {
          topDocs = searcher.search(query, N, Sort.INDEXORDER);
        } else {
          topDocs = searcher.search(query, N);
        }

      } else {
        final IndexReader reader = searcher.getIndexReader();
        int N = Math.max(1, reader.numDocs());
        topDocs = searcher.search(query, N, sort);
      }
      scoreDocs = topDocs.scoreDocs;
      this.idx = 0;

      final Bits liveDocs = MultiReader.getLiveDocs(searcher.getIndexReader());
      queryContext.deletedDocs(query, liveDocs);

    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Error while searching in Lucene index", e);
      throw new LuceneIndexException("Error while searching in Lucene index", e);
    }
  }

  @Override
  public int size() {
    return scoreDocs.length;
  }

  @Override
  public boolean isEmpty() {
    return scoreDocs.length == 0;
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return new ArcadeLuceneResultSetIteratorTx(this);
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(Identifiable identifiable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends Identifiable> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  protected ScoreDoc nextDoc() {
    if (idx >= scoreDocs.length) {
      return null;
    }
    return scoreDocs[idx++];
  }

  private class ArcadeLuceneResultSetIteratorTx implements Iterator<Identifiable> {
    private final ArcadeLuceneResultSet resultSet;
    private       Identifiable          nextElement;
    private       ScoreDoc              scoreDoc;

    public ArcadeLuceneResultSetIteratorTx(ArcadeLuceneResultSet resultSet) {
      this.resultSet = resultSet;
      fetchNext();
    }

    @Override
    public boolean hasNext() {
      return nextElement != null;
    }

    @Override
    public Identifiable next() {
      if (nextElement == null) {
        throw new NoSuchElementException();
      }
      Identifiable current = nextElement;
      fetchNext();
      return current;
    }

    private void fetchNext() {
      nextElement = null;
      while (true) {
        scoreDoc = resultSet.nextDoc();
        if (scoreDoc == null) {
          break;
        }
        try {
          org.apache.lucene.document.Document doc = searcher.doc(scoreDoc.doc);
          Object metadata = resultSet.metadata; // Assuming metadata is a field in ArcadeLuceneResultSet
          if (metadata != null && metadata instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) metadata;
            if (Boolean.TRUE.equals(m.get("highlight"))) {
              engine.onRecordAddedToResultSet(queryContext, doc, scoreDoc.score, (CommandContext) null); //TODO: check context casting
            }
          }

          String rId = doc.get(ArcadeLuceneIndexEngineAbstract.RID);
          if (queryContext.isDeleted(rId, doc)) {
            continue;
          }
          if (queryContext.isUpdated(rId, doc)) {
            // GET THE REAL DOCUMENT FROM TX
            Document updatedDoc = queryContext.getUpdated(rId, doc);
            if (updatedDoc == null) {
              // DELETED IN TX
              continue;
            }
            nextElement = updatedDoc;
          } else {
            nextElement = new RID(rId);
          }
          break;
        } catch (IOException e) {
          LOGGER.log(Level.SEVERE, "Error during Lucene index iteration", e);
          throw new LuceneIndexException("Error during Lucene index iteration", e);
        }
      }
    }
  }
}
