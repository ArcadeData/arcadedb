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
package com.arcadedb.index.fulltext;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

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
  private final Analyzer     indexAnalyzer;
  private final Analyzer                queryAnalyzer;
  private final FullTextIndexMetadata   ftMetadata;
  private       TypeIndex               typeIndex;

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
      if (builder.isUnique())
        throw new IllegalArgumentException("Full text index cannot be unique");

      // Allow multiple STRING properties for multi-property indexes
      for (final Type keyType : builder.getKeyTypes()) {
        if (keyType != Type.STRING)
          throw new IllegalArgumentException(
              "Full text index can only be defined on STRING properties, found: " + keyType);
      }

      // Get metadata if available
      FullTextIndexMetadata ftMetadata = null;
      if (builder.getMetadata() instanceof FullTextIndexMetadata) {
        ftMetadata = (FullTextIndexMetadata) builder.getMetadata();
      }

      return new LSMTreeFullTextIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE, builder.getPageSize(), builder.getNullStrategy(), ftMetadata);
    }
  }

  /**
   * Called at load time. The Full Text index is just a wrapper of an LSMTree Index.
   */
  public LSMTreeFullTextIndex(final LSMTreeIndex index) {
    this(index, null);
  }

  /**
   * Called at load time with optional metadata. The Full Text index is just a wrapper of an LSMTree Index.
   */
  public LSMTreeFullTextIndex(final LSMTreeIndex index, final FullTextIndexMetadata metadata) {
    this.underlyingIndex = index;
    this.ftMetadata = metadata;
    this.indexAnalyzer = createAnalyzer(metadata, true);
    this.queryAnalyzer = createAnalyzer(metadata, false);
  }

  /**
   * Creation time with metadata.
   */
  public LSMTreeFullTextIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final FullTextIndexMetadata metadata) {
    this.ftMetadata = metadata;
    this.indexAnalyzer = createAnalyzer(metadata, true);
    this.queryAnalyzer = createAnalyzer(metadata, false);
    underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode, new Type[] { Type.STRING }, pageSize, nullStrategy);
  }

  /**
   * Loading time from file.
   */
  public LSMTreeFullTextIndex(final DatabaseInternal database, final String name, final String filePath, final int fileId,
      final ComponentFile.MODE mode, final int pageSize, final int version) {
    try {
      underlyingIndex = new LSMTreeIndex(database, name, false, filePath, fileId, mode, pageSize, version);
    } catch (final IOException e) {
      throw new IndexException("Cannot create search engine (error=" + e + ")", e);
    }
    // When loading from file, metadata will be set later via setMetadata()
    this.ftMetadata = null;
    this.indexAnalyzer = new StandardAnalyzer();
    this.queryAnalyzer = new StandardAnalyzer();
  }

  @Override
  public void updateTypeName(final String newTypeName) {
    underlyingIndex.updateTypeName(newTypeName);
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    final HashMap<RID, AtomicInteger> scoreMap = new HashMap<>();

    // Parse query text to handle field-specific terms (field:term)
    final String queryText = keys.length > 0 && keys[0] != null ? keys[0].toString() : "";
    final List<QueryTerm> queryTerms = parseQueryTerms(queryText);

    for (final QueryTerm term : queryTerms) {
      // Analyze the term value
      final List<String> keywords = analyzeText(queryAnalyzer, new Object[] { term.value });

      for (final String k : keywords) {
        final IndexCursor rids;
        if (term.fieldName != null) {
          // Field-specific search - look up the prefixed token
          rids = underlyingIndex.get(new String[] { term.fieldName + ":" + k });
        } else {
          // Unqualified search - search without prefix
          rids = underlyingIndex.get(new String[] { k });
        }

        while (rids.hasNext()) {
          final RID rid = rids.next().getIdentity();

          final AtomicInteger score = scoreMap.get(rid);
          if (score == null)
            scoreMap.put(rid, new AtomicInteger(1));
          else
            score.incrementAndGet();
        }
      }
    }

    final int maxElements = limit > -1 ? limit : scoreMap.size();

    final ArrayList<IndexCursorEntry> list = new ArrayList<>(maxElements);
    for (final Map.Entry<RID, AtomicInteger> entry : scoreMap.entrySet())
      list.add(new IndexCursorEntry(keys, entry.getKey(), entry.getValue().get()));

    if (list.size() > 1)
      list.sort((o1, o2) -> {
        if (o1.score == o2.score)
          return 0;
        return o1.score < o2.score ? -1 : 1;
      });

    return new TempIndexCursor(list);
  }

  /**
   * Represents a parsed query term with optional field name.
   */
  private static class QueryTerm {
    final String fieldName; // null for unqualified terms
    final String value;

    QueryTerm(final String fieldName, final String value) {
      this.fieldName = fieldName;
      this.value = value;
    }
  }

  /**
   * Parse query text into terms, identifying field-prefixed terms (field:value).
   * For example, "title:java programming" returns:
   *   - QueryTerm(fieldName="title", value="java")
   *   - QueryTerm(fieldName=null, value="programming")
   */
  private List<QueryTerm> parseQueryTerms(final String queryText) {
    final List<QueryTerm> terms = new ArrayList<>();
    if (queryText == null || queryText.isEmpty())
      return terms;

    // Split by whitespace to get individual terms
    final String[] parts = queryText.split("\\s+");
    for (final String part : parts) {
      if (part.isEmpty())
        continue;

      // Check for field:value pattern
      final int colonIdx = part.indexOf(':');
      if (colonIdx > 0 && colonIdx < part.length() - 1) {
        // Field-prefixed term
        final String fieldName = part.substring(0, colonIdx);
        final String value = part.substring(colonIdx + 1);
        terms.add(new QueryTerm(fieldName, value));
      } else {
        // Unqualified term
        terms.add(new QueryTerm(null, part));
      }
    }
    return terms;
  }

  /**
   * Returns the number of properties in this index.
   * This is derived dynamically from the property names list to ensure
   * correct behavior after database restart (when loaded from disk).
   *
   * @return the number of properties in the index
   */
  private int getPropertyCount() {
    final List<String> props = getPropertyNames();
    return props != null ? props.size() : 1;
  }

  @Override
  public void put(final Object[] keys, final RID[] rids) {
    // If keys.length doesn't match propertyCount, this is a tokenized value from commit replay
    // (TransactionIndexContext stores tokens during transaction and replays them at commit time)
    // In that case, pass through directly to the underlying index without re-tokenizing
    if (keys.length != getPropertyCount()) {
      // Already tokenized - pass through directly
      underlyingIndex.put(keys, rids);
      return;
    }

    if (getPropertyCount() == 1) {
      // Single property - existing behavior
      final List<String> keywords = analyzeText(indexAnalyzer, keys);
      for (final String k : keywords)
        underlyingIndex.put(new String[] { k }, rids);
    } else {
      // Multi-property - prefix tokens with field name
      final List<String> propertyNames = getPropertyNames();
      for (int i = 0; i < keys.length && i < propertyNames.size(); i++) {
        if (keys[i] == null)
          continue;
        final String fieldName = propertyNames.get(i);
        final List<String> keywords = analyzeText(indexAnalyzer, new Object[] { keys[i] });
        for (final String k : keywords) {
          // Store with field prefix for field-specific queries
          underlyingIndex.put(new String[] { fieldName + ":" + k }, rids);
          // Also store without prefix for unqualified queries
          underlyingIndex.put(new String[] { k }, rids);
        }
      }
    }
  }

  @Override
  public void remove(final Object[] keys) {
    // If keys.length doesn't match propertyCount, this is a tokenized value from commit replay
    if (keys.length != getPropertyCount()) {
      // Already tokenized - pass through directly
      underlyingIndex.remove(keys);
      return;
    }

    if (getPropertyCount() == 1) {
      // Single property - existing behavior
      final List<String> keywords = analyzeText(indexAnalyzer, keys);
      for (final String k : keywords)
        underlyingIndex.remove(new String[] { k });
    } else {
      // Multi-property - remove both prefixed and unprefixed tokens
      final List<String> propertyNames = getPropertyNames();
      for (int i = 0; i < keys.length && i < propertyNames.size(); i++) {
        if (keys[i] == null)
          continue;
        final String fieldName = propertyNames.get(i);
        final List<String> keywords = analyzeText(indexAnalyzer, new Object[] { keys[i] });
        for (final String k : keywords) {
          underlyingIndex.remove(new String[] { fieldName + ":" + k });
          underlyingIndex.remove(new String[] { k });
        }
      }
    }
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    // If keys.length doesn't match propertyCount, this is a tokenized value from commit replay
    if (keys.length != getPropertyCount()) {
      // Already tokenized - pass through directly
      underlyingIndex.remove(keys, rid);
      return;
    }

    if (getPropertyCount() == 1) {
      // Single property - existing behavior
      final List<String> keywords = analyzeText(indexAnalyzer, keys);
      for (final String k : keywords)
        underlyingIndex.remove(new String[] { k }, rid);
    } else {
      // Multi-property - remove both prefixed and unprefixed tokens
      final List<String> propertyNames = getPropertyNames();
      for (int i = 0; i < keys.length && i < propertyNames.size(); i++) {
        if (keys[i] == null)
          continue;
        final String fieldName = propertyNames.get(i);
        final List<String> keywords = analyzeText(indexAnalyzer, new Object[] { keys[i] });
        for (final String k : keywords) {
          underlyingIndex.remove(new String[] { fieldName + ":" + k }, rid);
          underlyingIndex.remove(new String[] { k }, rid);
        }
      }
    }
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("type", getType());

    json.put("bucket", underlyingIndex.mutable.getDatabase().getSchema().getBucketById(getAssociatedBucketId()).getName());
    json.put("properties", getPropertyNames());
    json.put("nullStrategy", getNullStrategy());
    json.put("unique", isUnique());
    return json;
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return null;
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
  public IndexMetadata getMetadata() {
    return underlyingIndex.getMetadata();
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
  public String getMostRecentFileName() {
    return underlyingIndex.getMostRecentFileName();
  }

  @Override
  public void setMetadata(final IndexMetadata metadata) {
    underlyingIndex.setMetadata(metadata);
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    return underlyingIndex.setStatus(expectedStatuses, newStatus);
  }

  @Override
  public void setMetadata(final JSONObject indexJSON) {
    underlyingIndex.setMetadata(indexJSON);
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
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
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
  public PaginatedComponent getComponent() {
    return underlyingIndex.getComponent();
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
    return underlyingIndex.getPropertyNames() != null;
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
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    return underlyingIndex.build(buildIndexBatchSize, callback);
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.FULL_TEXT;
  }

  /**
   * Returns the query analyzer.
   *
   * @return the query analyzer
   */
  public Analyzer getAnalyzer() {
    return queryAnalyzer;
  }

  /**
   * Returns the index analyzer.
   *
   * @return the index analyzer
   */
  public Analyzer getIndexAnalyzer() {
    return indexAnalyzer;
  }

  /**
   * Returns the full-text index metadata.
   *
   * @return the metadata, or null if using defaults
   */
  public FullTextIndexMetadata getFullTextMetadata() {
    return ftMetadata;
  }

  @Override
  public boolean isValid() {
    return underlyingIndex.isValid();
  }

  /**
   * Creates an analyzer from the metadata configuration.
   *
   * @param metadata     the full-text index metadata (may be null)
   * @param forIndexing  true for indexing analyzer, false for query analyzer
   * @return the configured analyzer, or StandardAnalyzer if metadata is null
   */
  private static Analyzer createAnalyzer(final FullTextIndexMetadata metadata, final boolean forIndexing) {
    if (metadata == null)
      return new StandardAnalyzer();

    final String analyzerClass = forIndexing ? metadata.getIndexAnalyzerClass() : metadata.getQueryAnalyzerClass();

    try {
      final Class<?> clazz = Class.forName(analyzerClass);
      return (Analyzer) clazz.getDeclaredConstructor().newInstance();
    } catch (final Exception e) {
      throw new IndexException("Cannot instantiate analyzer: " + analyzerClass, e);
    }
  }

  public List<String> analyzeText(final Analyzer analyzer, final Object[] text) {
    final List<String> tokens = new ArrayList<>();

    for (final Object t : text) {
      if (t == null)
        tokens.add(null);
      else {
        final TokenStream tokenizer = analyzer.tokenStream("contents", t.toString());
        try {
          tokenizer.reset();
          final CharTermAttribute termAttribute = tokenizer.getAttribute(CharTermAttribute.class);

          try {
            while (tokenizer.incrementToken()) {
              final String token = termAttribute.toString();
              tokens.add(token);
            }

          } catch (final IOException e) {
            throw new IndexException("Error on analyzing text", e);
          }
        } catch (final IOException e) {
          throw new IndexException("Error on tokenizer", e);
        } finally {
          try {
            tokenizer.close();
          } catch (final IOException e) {
            // IGNORE IT
          }
        }
      }
    }
    return tokens;
  }

  /**
   * Search for documents similar to the source document(s) using More Like This algorithm.
   * <p>
   * The algorithm works as follows:
   * 1. Extract terms from source documents using the configured analyzer
   * 2. Calculate document frequencies for each term
   * 3. Select top terms using TF-IDF scoring and configured filters
   * 4. Execute an OR query across selected terms, accumulating scores
   * 5. Optionally exclude source documents from results
   * 6. Return results sorted by score in descending order
   *
   * @param sourceRids the RIDs of source documents to find similar documents for
   * @param config     the More Like This configuration parameters
   * @return cursor over matching documents, sorted by similarity score descending
   * @throws IllegalArgumentException if sourceRids is null, empty, or exceeds maxSourceDocs
   */
  public IndexCursor searchMoreLikeThis(final Set<RID> sourceRids, final MoreLikeThisConfig config) {
    // Validate inputs
    if (sourceRids == null) {
      throw new IllegalArgumentException("sourceRids cannot be null");
    }
    if (sourceRids.isEmpty()) {
      throw new IllegalArgumentException("sourceRids cannot be empty");
    }
    if (sourceRids.size() > config.getMaxSourceDocs()) {
      throw new IllegalArgumentException(
          "Number of source documents (" + sourceRids.size() + ") exceeds maxSourceDocs (" + config.getMaxSourceDocs() + ")");
    }

    // Step 1 & 2: Extract terms from source documents and count term frequencies
    final Map<String, Integer> termFreqs = new HashMap<>();

    for (final RID sourceRid : sourceRids) {
      // Load the document
      final Identifiable identifiable = sourceRid.getRecord();
      if (identifiable == null) {
        continue;
      }

      // Extract text from indexed properties
      final List<String> propertyNames = getPropertyNames();
      if (propertyNames != null && !propertyNames.isEmpty()) {
        final com.arcadedb.database.Document doc = (com.arcadedb.database.Document) identifiable;
        for (final String propName : propertyNames) {
          final Object value = doc.get(propName);
          if (value != null) {
            // Analyze the text to get tokens
            final List<String> tokens = analyzeText(indexAnalyzer, new Object[] { value });
            for (final String token : tokens) {
              if (token != null) {
                termFreqs.merge(token, 1, Integer::sum);
              }
            }
          }
        }
      }
    }

    // If no terms extracted, return empty cursor
    if (termFreqs.isEmpty()) {
      return new TempIndexCursor(Collections.emptyList());
    }

    // Step 3: Get document frequencies for each term
    final Map<String, Integer> docFreqs = new HashMap<>();
    for (final String term : termFreqs.keySet()) {
      final IndexCursor termCursor = underlyingIndex.get(new String[] { term });
      int docCount = 0;
      while (termCursor.hasNext()) {
        termCursor.next();
        docCount++;
      }
      docFreqs.put(term, docCount);
    }

    // Estimate total documents (use the max doc frequency as approximation)
    final int totalDocs = docFreqs.values().stream().mapToInt(Integer::intValue).max().orElse(1);

    // Step 4: Select top terms using MoreLikeThisQueryBuilder
    final MoreLikeThisQueryBuilder queryBuilder = new MoreLikeThisQueryBuilder(config);
    final List<String> topTerms = queryBuilder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    // If no terms selected, return empty cursor
    if (topTerms.isEmpty()) {
      return new TempIndexCursor(Collections.emptyList());
    }

    // Step 5: Execute OR query and accumulate scores
    final Map<RID, AtomicInteger> scoreMap = new HashMap<>();
    for (final String term : topTerms) {
      final IndexCursor termCursor = underlyingIndex.get(new String[] { term });
      while (termCursor.hasNext()) {
        final RID rid = termCursor.next().getIdentity();
        final AtomicInteger score = scoreMap.get(rid);
        if (score == null) {
          scoreMap.put(rid, new AtomicInteger(1));
        } else {
          score.incrementAndGet();
        }
      }
    }

    // Step 6: Exclude source documents if configured
    if (config.isExcludeSource()) {
      for (final RID sourceRid : sourceRids) {
        scoreMap.remove(sourceRid);
      }
    }

    // Step 7: Build result list sorted by score descending
    final List<IndexCursorEntry> results = new ArrayList<>(scoreMap.size());
    for (final Map.Entry<RID, AtomicInteger> entry : scoreMap.entrySet()) {
      results.add(new IndexCursorEntry(null, entry.getKey(), entry.getValue().get()));
    }

    // Sort by score descending
    if (results.size() > 1) {
      results.sort((o1, o2) -> {
        if (o1.score == o2.score) {
          return 0;
        }
        return o1.score < o2.score ? 1 : -1; // Descending order
      });
    }

    return new TempIndexCursor(results);
  }
}
