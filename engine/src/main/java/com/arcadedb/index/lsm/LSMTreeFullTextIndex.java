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
package com.arcadedb.index.lsm;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.*;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.Schema;
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
 * <br>
 * The idea to index a text is:
 * <br>
 * parse the text with the configured analyzer. The analyzer uses a tokenizer that splits the text into words, then the stemmer extracts the stem of each word.
 * In the end, the stop words are removed. The output of this phase is an array of strings to be indexed.
 * Put all the strings from the resulting array in the underlying LSM index with the RID as value (as with default LSM-Tree index implementation)
 * For the search, the process is similar, with the computation of the score:
 * <br>
 * parse the text with the configured analyzer, extract the array of strings (see above)
 * search for all the strings in the array, by storing the multiple results in a {@literal Map<String,List<RID>>} (as {@literal Map<keyword,results>})
 * browse all the results in the maps, by adding all of them to a final {@literal TreeMap<RID, AtomicInteger>} that represents the score, where the key is the record id
 * and the value is a counter that stores the score. At the beginning the score is 1. Every time a RID is already present in the score TreeMap, then the value
 * is incremented. In this way, the records that match a higher number of keywords will have a higher score. The score can start from 1 to Integer.MAX_INT.
 * the query result will be the TreeMap ordered by score, so if the query has a limit, only the first X items will be returned ordered by score desc
 */
public class LSMTreeFullTextIndex implements Index, IndexInternal {
  private final LSMTreeIndex underlyingIndex;
  private final Analyzer     analyzer;
  private       TypeIndex    typeIndex;

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final DatabaseInternal database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final Type[] keyTypes, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final BuildIndexCallback callback) {
      return new LSMTreeFullTextIndex(database, name, filePath, mode, pageSize, callback);
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final PaginatedFile.MODE mode, final int pageSize, int version) throws IOException {
      final LSMTreeFullTextIndex mainIndex = new LSMTreeFullTextIndex(database, name, filePath, id, mode, pageSize, version);
      return mainIndex.underlyingIndex.mutable;
    }
  }

  /**
   * Creation time.
   */
  public LSMTreeFullTextIndex(final DatabaseInternal database, final String name, final String filePath, final PaginatedFile.MODE mode, final int pageSize,
      final BuildIndexCallback callback) {
    analyzer = new StandardAnalyzer();
    underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode, new Type[] { Type.STRING }, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
  }

  /**
   * Loading time.
   */
  public LSMTreeFullTextIndex(final DatabaseInternal database, final String name, final String filePath, final int fileId, final PaginatedFile.MODE mode,
      final int pageSize, final int version) {
    try {
      underlyingIndex = new LSMTreeIndex(database, name, false, filePath, fileId, mode, pageSize, version);
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
      list.sort((o1, o2) -> {
          if (o1.score == o2.score)
              return 0;
          return o1.score < o2.score ? -1 : 1;
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
  public List<String> getPropertyNames() {
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
  public Type[] getKeyTypes() {
    return underlyingIndex.getKeyTypes();
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return underlyingIndex.getBinaryKeyTypes();
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
  public int getPageSize() {
    return underlyingIndex.getPageSize();
  }

  @Override
  public List<Integer> getFileIds() {
    return underlyingIndex.getFileIds();
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    this.typeIndex = typeIndex;
  }

  @Override
  public TypeIndex getTypeIndex() {
    return typeIndex;
  }

  @Override
  public long build(BuildIndexCallback callback) {
    return underlyingIndex.build(callback);
  }

  @Override
  public EmbeddedSchema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.FULL_TEXT;
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
