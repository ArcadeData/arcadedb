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

package com.arcadedb.index.lsm;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.*;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.Type;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Full Text index implementation based on LSM-Tree index.
 * In order to support a full-text index, we leverage on the Lucene ecosystem in terms of Analyzer, Tokenizers, and stemmers, but leaving the current efficient
 * LSM-Tree implementation with the management for ACID(ity), bg compaction, wal, replication, ha, etc.
 * <p>
 * The idea to index a text is:
 * <p>
 * parse the text with the configured analyzer. The analyzer uses a tokenizer that splits the text into words, then the stemmer extracts the stem of each word.
 * In the end, the stop words are removed. The output of this phase is an array of strings to be indexed.
 * Put all the strings from the resulting array in the underlying LSM index with the RID as value (as with default LSM-Tree index implementation)
 * For the search, the process is similar, with the computation of the score:
 * <p>
 * parse the text with the configured analyzer, extract the array of strings (see above)
 * search for all the strings in the array, by storing the multiple results in a `Map<String,List<RID>>` (as `Map<keyword,results>)
 * browse all the results in the maps, by adding all of them to a final `TreeMap<RID, AtomicInteger>` that represents the score, where the key is the record id
 * and the value is a counter that stores the score. At the beginning the score is 1. Every time a RID is already present in the score TreeMap, then the value
 * is incremented. In this way, the records that match a higher number of keywords will have a higher score. The score can start from 1 to Integer.MAX_INT.
 * the query result will be the TreeMap ordered by score, so if the query has a limit, only the first X items will be returned ordered by score desc
 */
public class LSMTreeFullTextIndex implements Index, IndexInternal {
  private LSMTreeIndex underlyingIndex;
  private Analyzer     analyzer;

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final DatabaseInternal database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final byte[] keyTypes, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final BuildIndexCallback callback)
        throws IOException {
      return new LSMTreeFullTextIndex(database, name, filePath, mode, pageSize, callback);
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final PaginatedFile.MODE mode, final int pageSize) throws IOException {
      final LSMTreeFullTextIndex mainIndex = new LSMTreeFullTextIndex(database, name, filePath, id, mode, pageSize);
      return mainIndex.underlyingIndex.mutable;
    }
  }

  /**
   * Creation time.
   */
  public LSMTreeFullTextIndex(final DatabaseInternal database, final String name, final String filePath, final PaginatedFile.MODE mode, final int pageSize,
      final BuildIndexCallback callback) {
    try {
      analyzer = new StandardAnalyzer();
      underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode, new byte[] { Type.STRING.getBinaryType() }, pageSize,
          LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
    } catch (IOException e) {
      throw new IndexException("Cannot create search engine (error=" + e + ")", e);
    }
  }

  /**
   * Loading time.
   */
  public LSMTreeFullTextIndex(final DatabaseInternal database, final String name, final String filePath, final int fileId, final PaginatedFile.MODE mode,
      final int pageSize) {
    try {
      underlyingIndex = new LSMTreeIndex(database, name, false, filePath, fileId, mode, pageSize);
    } catch (IOException e) {
      throw new IndexException("Cannot create search engine (error=" + e + ")", e);
    }
    analyzer = new StandardAnalyzer();
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    final List<String> keywords = analyzeText(analyzer, keys);

    final HashMap<RID, AtomicInteger> scoreMap = new HashMap<>();

    for (String k : keywords) {
      final IndexCursor rids = underlyingIndex.get(new String[] { k });

      while (rids.hasNext()) {
        final RID rid = rids.next().getIdentity();

        AtomicInteger score = scoreMap.get(rid);
        if (score == null)
          scoreMap.put(rid, new AtomicInteger(1));
        else
          score.incrementAndGet();
      }
    }

    final int maxElements = limit > -1 ? limit : scoreMap.size();

    final ArrayList<IndexCursorEntry> list = new ArrayList<>(maxElements);
    for (Map.Entry<RID, AtomicInteger> entry : scoreMap.entrySet())
      list.add(new IndexCursorEntry(keys, entry.getKey(), entry.getValue().get()));

    if (list.size() > 1)
      Collections.sort(list, new Comparator<IndexCursorEntry>() {
        @Override
        public int compare(final IndexCursorEntry o1, final IndexCursorEntry o2) {
          if (o1.score == o2.score)
            return 0;
          return o1.score < o2.score ? -1 : 1;
        }
      });

    return new TempIndexCursor(list);
  }

  @Override
  public void put(final Object[] keys, final RID[] rids) {
    final List<String> keywords = analyzeText(analyzer, keys);
    for (String k : keywords)
      underlyingIndex.put(new String[] { k }, rids);
  }

  @Override
  public void remove(final Object[] keys) {
    final List<String> keywords = analyzeText(analyzer, keys);
    for (String k : keywords)
      underlyingIndex.remove(new String[] { k });
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    final List<String> keywords = analyzeText(analyzer, keys);
    for (String k : keywords)
      underlyingIndex.remove(new String[] { k }, rid);
  }

  @Override
  public long countEntries() {
    return underlyingIndex.countEntries();
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    return underlyingIndex.compact();
  }

  @Override
  public boolean isCompacting() {
    return underlyingIndex.isCompacting();
  }

  @Override
  public boolean scheduleCompaction() {
    return underlyingIndex.scheduleCompaction();
  }

  @Override
  public void setMetadata(final String name, final String[] propertyNames, final int associatedBucketId) {
    underlyingIndex.setMetadata(name, propertyNames, associatedBucketId);
  }

  @Override
  public String getTypeName() {
    return underlyingIndex.getTypeName();
  }

  @Override
  public String[] getPropertyNames() {
    return underlyingIndex.getPropertyNames();
  }

  @Override
  public void close() {
    underlyingIndex.close();
  }

  @Override
  public void drop() {
    underlyingIndex.drop();
  }

  @Override
  public String getName() {
    return underlyingIndex.getName();
  }

  @Override
  public Map<String, Long> getStats() {
    return underlyingIndex.getStats();
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return LSMTreeIndexAbstract.NULL_STRATEGY.ERROR;
  }

  @Override
  public void setNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    if (nullStrategy != LSMTreeIndexAbstract.NULL_STRATEGY.ERROR)
      throw new IllegalArgumentException("Unsupported null strategy '" + nullStrategy + "'");
  }

  @Override
  public int getFileId() {
    return underlyingIndex.getFileId();
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  public PaginatedComponent getPaginatedComponent() {
    return underlyingIndex.getPaginatedComponent();
  }

  @Override
  public int getAssociatedBucketId() {
    return underlyingIndex.getAssociatedBucketId();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return underlyingIndex.propertyNames != null;
  }

  @Override
  public long build(BuildIndexCallback callback) {
    return underlyingIndex.build(callback);
  }

  @Override
  public EmbeddedSchema.INDEX_TYPE getType() {
    return EmbeddedSchema.INDEX_TYPE.FULL_TEXT;
  }

  public Analyzer getAnalyzer() {
    return analyzer;
  }

  public List<String> analyzeText(final Analyzer analyzer, final Object[] text) {
    final List<String> tokens = new ArrayList<>();

    for (Object t : text) {
      final TokenStream tokenizer = analyzer.tokenStream("contents", t.toString());
      try {
        tokenizer.reset();
        final CharTermAttribute termAttribute = tokenizer.getAttribute(CharTermAttribute.class);

        try {
          while (tokenizer.incrementToken()) {
            String token = termAttribute.toString();
            tokens.add(token);
          }

        } catch (IOException e) {
          throw new IndexException("Error on analyzing text", e);
        }
      } catch (IOException e) {
        throw new IndexException("Error on tokenizer", e);
      } finally {
        try {
          tokenizer.close();
        } catch (IOException e) {
          // IGNORE IT
        }
      }
    }
    return tokens;
  }
}
