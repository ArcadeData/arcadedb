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
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.FullTextPostingRID;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final LSMTreeIndex          underlyingIndex;
  private final Analyzer              indexAnalyzer;
  private final Analyzer              queryAnalyzer;
  private final FullTextIndexMetadata ftMetadata;
  private       TypeIndex             typeIndex;

  /**
   * Factory handler for creating LSMTreeFullTextIndex instances.
   * Validates that the index is not unique and is defined on STRING properties.
   */
  public static class LSMTreeFullTextIndexFactoryHandler implements IndexFactoryHandler {
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

      // Get metadata if available. New full-text indexes default to BM25 similarity (issue #4687): when the user did not supply a
      // FullTextIndexMetadata, synthesize one carrying the BM25 defaults so freshly created indexes rank with BM25 out of the box.
      final FullTextIndexMetadata ftMetadata;
      if (builder.getMetadata() instanceof FullTextIndexMetadata m)
        ftMetadata = m;
      else {
        final IndexMetadata base = builder.getMetadata();
        ftMetadata = new FullTextIndexMetadata(base.typeName, base.propertyNames.toArray(new String[0]),
            base.associatedBucketId);
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
    if (metadata != null && metadata.isBM25())
      index.setStoreTermFrequency(true);
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
    if (metadata != null && metadata.isBM25()) {
      underlyingIndex.setStoreTermFrequency(true);
      // A brand-new index starts empty: the corpus counters are trivially valid (0 docs, 0 tokens).
      if (!metadata.isCountersValid())
        metadata.setCounters(0L, 0L);
    }
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

  /**
   * Searches the index for the given query text.
   * The query text is parsed into terms, analyzed, and then matched against the index.
   * Results are scored based on the number of matching terms (coordination factor).
   *
   * @param keys  The query arguments. keys[0] is expected to be the query string.
   * @param limit The maximum number of results to return. -1 for no limit.
   * @return An IndexCursor containing the matching results, sorted by score descending.
   */
  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    if (underlyingIndex.isStoreTermFrequency())
      return getBM25(keys, limit);

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

          // Accumulate score for this RID based on term frequency in the query
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
   * Shared BM25 scoring core used by both the direct ({@link #get}) and the Lucene-syntax ({@link FullTextQueryExecutor}) paths,
   * so the formula, parameters and corpus statistics can never diverge between them. For each scoring token it derives the
   * document frequency in a first streaming pass (counting only, never materializing the posting list) and accumulates the BM25
   * contribution in a second pass. When {@code candidates} is non-null only those documents are scored (the map is pre-seeded
   * with them); otherwise every matching document is scored. Memory is therefore bounded by the result/candidate set, not by the
   * (possibly huge) posting list of a common term.
   *
   * @param tokenBoosts scoring tokens (stored-key form) mapped to their effective boost
   * @param candidates  documents to score, or {@code null} to score every matching document
   */
  private Map<RID, Float> computeBM25Scores(final Map<String, Float> tokenBoosts, final Set<RID> candidates) {
    ensureCounters();
    final long totalDocs = resolveTotalDocs();
    final double avgdl = resolveAvgDocLength();
    final double k1 = ftMetadata.getBm25K1();
    final double b = ftMetadata.getBm25B();

    final Map<RID, Float> scoreMap = candidates != null ? new HashMap<>(candidates.size()) : new HashMap<>();
    if (candidates != null)
      for (final RID c : candidates)
        scoreMap.put(c, 0.0f);

    for (final Map.Entry<String, Float> e : tokenBoosts.entrySet()) {
      final String[] storedKey = new String[] { e.getKey() };
      final float boost = e.getValue();

      // Pass 1: document frequency - stream-count the postings without retaining them.
      long df = 0L;
      final IndexCursor dfCursor = underlyingIndex.get(storedKey);
      while (dfCursor.hasNext()) {
        dfCursor.next();
        ++df;
      }
      if (df == 0L)
        continue;
      final double idf = BM25Scorer.idf(totalDocs, df);

      // Pass 2: accumulate contributions, scoring only candidates when a candidate set was given.
      final IndexCursor scoreCursor = underlyingIndex.get(storedKey);
      while (scoreCursor.hasNext()) {
        final Identifiable id = scoreCursor.next();
        if (!(id instanceof FullTextPostingRID s))
          continue;
        final RID rid = s.getIdentity();
        if (candidates != null && !scoreMap.containsKey(rid))
          continue;
        scoreMap.merge(rid, (float) (BM25Scorer.termScore(idf, s.tf, s.docLength, avgdl, k1, b) * boost), Float::sum);
      }
    }
    return scoreMap;
  }

  /**
   * BM25 scoring path for the direct index lookup. Builds the scoring tokens from the query and delegates to
   * {@link #computeBM25Scores}.
   */
  private IndexCursor getBM25(final Object[] keys, final int limit) {
    final String queryText = keys.length > 0 && keys[0] != null ? keys[0].toString() : "";
    final List<QueryTerm> queryTerms = parseQueryTerms(queryText);

    // Build the scoring tokens (stored-key form) with their field boost, then delegate to the shared scorer. No candidate set:
    // every matching document is scored.
    final Map<String, Float> tokenBoosts = new HashMap<>();
    for (final QueryTerm term : queryTerms) {
      final float boost = term.fieldName != null ? ftMetadata.getFieldBoost(term.fieldName) : 1.0f;
      for (final String k : analyzeText(queryAnalyzer, new Object[] { term.value })) {
        final String storedKey = term.fieldName != null ? term.fieldName + ":" + k : k;
        tokenBoosts.merge(storedKey, boost, Math::max);
      }
    }

    final Map<RID, Float> scoreMap = computeBM25Scores(tokenBoosts, null);

    final ArrayList<IndexCursorEntry> list = new ArrayList<>(scoreMap.size());
    for (final Map.Entry<RID, Float> entry : scoreMap.entrySet())
      list.add(new IndexCursorEntry(keys, entry.getKey(), entry.getValue().floatValue()));

    if (list.size() > 1)
      list.sort((o1, o2) -> Float.compare(o2.floatScore, o1.floatScore));

    if (limit > -1 && list.size() > limit)
      return new TempIndexCursor(list.subList(0, limit));

    return new TempIndexCursor(list);
  }

  /**
   * Returns true when this index ranks with BM25 (i.e. it stores per-posting term frequency).
   */
  public boolean isBM25() {
    return underlyingIndex.isStoreTermFrequency();
  }

  /**
   * Scores a set of candidate documents (already matched by the boolean/structural part of a query) with BM25, summing the
   * contribution of every scoring token. Each token is looked up once to derive its document frequency (and therefore IDF); only
   * candidates are scored. Used by {@link FullTextQueryExecutor} so the SQL {@code SEARCH_INDEX} path ranks with BM25 while
   * preserving Lucene boolean/phrase/wildcard matching semantics.
   *
   * @param candidates  the documents that satisfy the query's matching logic
   * @param tokenBoosts the scoring tokens (stored-key form) mapped to their field boost
   * @param keys        the original query keys (carried into the result entries)
   * @param limit       maximum number of results (-1 for unlimited)
   */
  public IndexCursor scoreCandidatesBM25(final Set<RID> candidates, final Map<String, Float> tokenBoosts, final Object[] keys,
      final int limit) {
    final Map<RID, Float> scoreMap = computeBM25Scores(tokenBoosts, candidates);

    final ArrayList<IndexCursorEntry> list = new ArrayList<>(scoreMap.size());
    for (final Map.Entry<RID, Float> entry : scoreMap.entrySet())
      list.add(new IndexCursorEntry(keys, entry.getKey(), entry.getValue().floatValue()));

    list.sort((o1, o2) -> {
      final int cmp = Float.compare(o2.floatScore, o1.floatScore);
      if (cmp != 0)
        return cmp;
      return o1.record.getIdentity().compareTo(o2.record.getIdentity());
    });

    if (limit > 0 && list.size() > limit)
      return new TempIndexCursor(list.subList(0, limit));
    return new TempIndexCursor(list);
  }

  /**
   * Builds the query-level BM25 scoring explanation surfaced by {@code EXPLAIN}/{@code PROFILE}: the similarity mode, the BM25
   * parameters (k1, b), the corpus statistics (N, avgdl) and, for each scoring token, its document frequency, IDF and applied
   * boost. This is the "why are the scores what they are" view; per-document contributions are intentionally not included since a
   * plan describes the query, not individual rows.
   *
   * @param tokenBoosts the scoring tokens (stored-key form) mapped to their field boost
   */
  public JSONObject explainScoring(final Map<String, Float> tokenBoosts) {
    final JSONObject json = new JSONObject();
    if (!isBM25()) {
      json.put("similarity", FullTextIndexMetadata.SIMILARITY_CLASSIC);
      return json;
    }

    ensureCounters();
    final long totalDocs = resolveTotalDocs();
    final double avgdl = resolveAvgDocLength();
    json.put("similarity", FullTextIndexMetadata.SIMILARITY_BM25);
    json.put("k1", ftMetadata.getBm25K1());
    json.put("b", ftMetadata.getBm25B());
    json.put("totalDocs", totalDocs);
    json.put("avgDocLength", avgdl);

    final JSONArray terms = new JSONArray();
    for (final Map.Entry<String, Float> e : tokenBoosts.entrySet()) {
      final IndexCursor postings = underlyingIndex.get(new String[] { e.getKey() });
      long df = 0;
      while (postings.hasNext()) {
        postings.next();
        ++df;
      }
      terms.put(new JSONObject().put("term", e.getKey()).put("df", df).put("idf", BM25Scorer.idf(totalDocs, df))
          .put("boost", e.getValue()));
    }
    json.put("terms", terms);
    return json;
  }

  /**
   * BM25 is scored per bucket (per-shard, like Elasticsearch/Lucene): each bucket-level full-text index ranks its own documents
   * using the document frequency read from its own postings. To keep the IDF unbiased, N must be measured at the same scope as
   * df, i.e. this bucket - so N is the bucket's live record count. (The corpus counters in {@link FullTextIndexMetadata} are
   * SHARED type-wide across all of the type's bucket indexes - the same metadata object is passed to each - so they are used only
   * for the average document length, a length normalizer for which a type-wide value is an appropriate estimate.)
   */
  private long resolveTotalDocs() {
    final long count = associatedBucketCount();
    return count > 0 ? count : 1L;
  }

  /**
   * Returns the (type-wide) average document length used as the BM25 length normalizer, or 1.0 when no statistics are available.
   */
  private double resolveAvgDocLength() {
    if (ftMetadata != null && ftMetadata.getTotalDocs() > 0)
      return ftMetadata.avgDocLength();
    return 1.0;
  }

  /**
   * Returns the live record count of the bucket this index is associated with, or 0 when it cannot be resolved.
   */
  private long associatedBucketCount() {
    final Bucket bucket = associatedBucket();
    return bucket != null ? bucket.count() : 0L;
  }

  private Bucket associatedBucket() {
    try {
      return underlyingIndex.getMutableIndex().getDatabase().getSchema().getBucketById(getAssociatedBucketId());
    } catch (final Exception e) {
      return null;
    }
  }

  /**
   * Lazily rebuilds the (type-wide) corpus counters when they are not trustworthy (e.g. a BM25 index reopened before the schema
   * carrying the counters was persisted, or an index that predates BM25 support). The {@code countersValid} flag is the
   * authority: when set the counters are used as-is. Recompute is done in memory only; persistence is deferred to the next schema
   * save or an explicit {@link #recomputeBM25Counters()}, so the read/query path never calls {@code saveConfiguration}.
   */
  private void ensureCounters() {
    if (ftMetadata == null || ftMetadata.isCountersValid())
      return;
    computeCorpusCounters(false);
  }

  /**
   * Rescans the type and rebuilds the type-wide BM25 corpus counters (document count and total document length), then persists
   * them. Use it after a bulk import or to repair drifted counters.
   */
  public void recomputeBM25Counters() {
    computeCorpusCounters(true);
  }

  /**
   * Scans the whole type and rebuilds the shared corpus counters used for the average document length. The counters are type-wide
   * because the {@link FullTextIndexMetadata} instance is shared by all of the type's bucket indexes; scanning a single bucket
   * would corrupt them. When {@code persist} is true the schema is saved so the counters survive a restart; the lazy read-path
   * caller passes false to avoid saving the schema during a query.
   */
  private void computeCorpusCounters(final boolean persist) {
    if (ftMetadata == null)
      return;
    final DatabaseInternal db = underlyingIndex.getMutableIndex().getDatabase();
    final String typeName = getTypeName();
    if (typeName == null)
      return;

    final List<String> props = getPropertyNames();
    long docs = 0L;
    long sumLen = 0L;
    final Iterator<com.arcadedb.database.Record> it = db.iterateType(typeName, true);
    while (it.hasNext()) {
      final com.arcadedb.database.Record record = it.next();
      if (!(record instanceof Document doc))
        continue;
      int len = 0;
      for (final String p : props) {
        final Object v = doc.get(p);
        if (v != null)
          len += analyzeText(indexAnalyzer, new Object[] { v }).size();
      }
      ++docs;
      sumLen += len;
    }

    ftMetadata.setCounters(docs, sumLen);
    if (persist)
      underlyingIndex.getMutableIndex().getDatabase().getSchema().getEmbedded().saveConfiguration();
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
   * - QueryTerm(fieldName="title", value="java")
   * - QueryTerm(fieldName=null, value="programming")
   *
   * @param queryText The raw query string.
   * @return A list of parsed QueryTerms.
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

  /**
   * Indexes a document.
   * <p>
   * Always treats {@code keys} as the raw property values from {@code DocumentIndexer} and
   * analyzes them into one storage token per non-stop term. The transaction commit replay
   * uses {@link #putReplay}, which forwards already-analyzed tokens to the underlying
   * LSM-Tree unchanged (issue #4073).
   *
   * @param keys The values of the indexed properties for the document.
   * @param rids The RIDs associated with these keys (usually just one).
   */
  @Override
  public void put(final Object[] keys, final RID[] rids) {
    if (underlyingIndex.isStoreTermFrequency()) {
      putWithStats(keys, rids);
      return;
    }

    // CLASSIC similarity: store one posting per token occurrence (RID-only), unchanged behavior.
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

  /**
   * BM25 indexing path: aggregates the per-token term frequency and the document length, then stores one posting per distinct
   * token carrying {@link FullTextPostingRID} so scoring can read tf and document length back without re-reading the document. Also
   * maintains the persisted corpus counters (document count and total length) used to compute the average document length.
   */
  private void putWithStats(final Object[] keys, final RID[] rids) {
    final DatabaseInternal db = underlyingIndex.getMutableIndex().getDatabase();

    if (getPropertyCount() == 1) {
      final List<String> keywords = analyzeText(indexAnalyzer, keys);
      final int docLen = keywords.size();
      final Map<String, Integer> tfs = new HashMap<>();
      for (final String k : keywords)
        tfs.merge(k, 1, Integer::sum);
      for (final Map.Entry<String, Integer> e : tfs.entrySet())
        underlyingIndex.put(new String[] { e.getKey() }, withStats(db, rids, e.getValue(), docLen));
      countDocuments(rids.length, docLen);
    } else {
      final List<String> propertyNames = getPropertyNames();

      // PASS 1: analyze every field, accumulate document length and per-field + global term frequencies.
      final Map<String, Integer> globalTf = new HashMap<>();
      final List<String> fieldNames = new ArrayList<>();
      final List<Map<String, Integer>> fieldTfs = new ArrayList<>();
      int docLen = 0;
      for (int i = 0; i < keys.length && i < propertyNames.size(); i++) {
        if (keys[i] == null)
          continue;
        final List<String> keywords = analyzeText(indexAnalyzer, new Object[] { keys[i] });
        docLen += keywords.size();
        final Map<String, Integer> fieldTf = new HashMap<>();
        for (final String k : keywords) {
          fieldTf.merge(k, 1, Integer::sum);
          globalTf.merge(k, 1, Integer::sum);
        }
        fieldNames.add(propertyNames.get(i));
        fieldTfs.add(fieldTf);
      }

      // PASS 2: store field-prefixed postings (per-field tf) and unprefixed postings (global tf) now that docLen is known.
      for (int f = 0; f < fieldNames.size(); f++) {
        final String fieldName = fieldNames.get(f);
        for (final Map.Entry<String, Integer> e : fieldTfs.get(f).entrySet())
          underlyingIndex.put(new String[] { fieldName + ":" + e.getKey() }, withStats(db, rids, e.getValue(), docLen));
      }
      for (final Map.Entry<String, Integer> e : globalTf.entrySet())
        underlyingIndex.put(new String[] { e.getKey() }, withStats(db, rids, e.getValue(), docLen));

      countDocuments(rids.length, docLen);
    }
  }

  /**
   * Wraps the given RIDs into {@link FullTextPostingRID} carrying the term frequency and document length for a posting.
   */
  private static RID[] withStats(final DatabaseInternal db, final RID[] rids, final int tf, final int docLen) {
    final RID[] out = new RID[rids.length];
    for (int i = 0; i < rids.length; i++)
      out[i] = new FullTextPostingRID(db, rids[i].getBucketId(), rids[i].getPosition(), tf, docLen);
    return out;
  }

  /**
   * Updates the persisted corpus counters when a document is indexed.
   */
  private void countDocuments(final int numDocs, final int docLen) {
    if (ftMetadata != null)
      for (int i = 0; i < numDocs; i++)
        ftMetadata.addDocument(docLen);
  }

  /**
   * Removes a document from the index.
   * Tokenizes the input values and removes the corresponding entries from the underlying LSM tree.
   *
   * @param keys The values of the indexed properties to remove.
   */
  @Override
  public void remove(final Object[] keys) {
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

  /**
   * Removes a specific RID associated with the given keys from the index.
   *
   * @param keys The values of the indexed properties.
   * @param rid  The specific RID to remove.
   */
  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    int docLen = 0;
    if (getPropertyCount() == 1) {
      // Single property - existing behavior
      final List<String> keywords = analyzeText(indexAnalyzer, keys);
      docLen = keywords.size();
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
        docLen += keywords.size();
        for (final String k : keywords) {
          underlyingIndex.remove(new String[] { fieldName + ":" + k }, rid);
          underlyingIndex.remove(new String[] { k }, rid);
        }
      }
    }

    // Keep the BM25 corpus counters in sync when a document is removed.
    if (underlyingIndex.isStoreTermFrequency() && ftMetadata != null)
      ftMetadata.removeDocument(docLen);
  }

  /**
   * Replay entry point invoked by {@code TransactionIndexContext.applyChanges} at commit time
   * (issue #4073). The {@code keys} are already analyzed storage tokens (queued by the
   * underlying LSM-Tree at original-call time), so the wrapper must NOT re-analyze them.
   * Forwards directly to the underlying index, which fixes the latent single-property case
   * where the prior shape-sniff heuristic ({@code keys.length != getPropertyCount()}) would
   * silently re-tokenize already-analyzed tokens (a soundness hazard for non-idempotent
   * analyzers such as stemmers or stop-word filters).
   */
  @Override
  public void putReplay(final Object[] keys, final RID[] rids) {
    underlyingIndex.put(keys, rids);
  }

  @Override
  public void removeReplay(final Object[] keys, final Identifiable rid) {
    underlyingIndex.remove(keys, rid);
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("type", getType());

    json.put("bucket",
        underlyingIndex.getMutableIndex().getDatabase().getSchema().getBucketById(getAssociatedBucketId()).getName());
    json.put("properties", getPropertyNames());
    json.put("nullStrategy", getNullStrategy());
    json.put("unique", isUnique());
    // Persist analyzer configuration and BM25 settings + corpus counters so they survive a restart (issue #4687). Historically
    // this method dropped the metadata, silently reverting custom analyzers to StandardAnalyzer on reload.
    if (ftMetadata != null)
      ftMetadata.writeToJSON(json);
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
    return underlyingIndex.getNullStrategy();
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    underlyingIndex.setNullStrategy(nullStrategy);
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

  /**
   * Iterates the underlying LSM-Tree starting at the given key.
   * Used by full-text query executors to support prefix and wildcard scans.
   *
   * @param ascendingOrder true for ascending iteration, false for descending
   * @param fromKeys       the starting key (single-element String array). May be null for full scan.
   * @param inclusive      whether the start key is inclusive
   *
   * @return cursor over the underlying index entries
   */
  public IndexCursor iterateUnderlying(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    if (fromKeys == null)
      return underlyingIndex.iterator(ascendingOrder);
    return underlyingIndex.iterator(ascendingOrder, fromKeys, inclusive);
  }

  @Override
  public boolean isValid() {
    return underlyingIndex.isValid();
  }

  /**
   * Creates an analyzer from the metadata configuration.
   *
   * @param metadata    the full-text index metadata (may be null)
   * @param forIndexing true for indexing analyzer, false for query analyzer
   *
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

  /**
   * Analyzes the input text using the provided Lucene Analyzer.
   * Tokenizes the text and returns a list of tokens (strings).
   *
   * @param analyzer The Lucene Analyzer to use.
   * @param text     The input text objects to analyze.
   * @return A list of tokens extracted from the text.
   */
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
   *
   * @return cursor over matching documents, sorted by similarity score descending
   *
   * @throws IllegalArgumentException if sourceRids is null, empty, or exceeds maxSourceDocs
   */
  public IndexCursor searchMoreLikeThis(final Set<RID> sourceRids, final MoreLikeThisConfig config) {
    if (sourceRids == null)
      throw new IllegalArgumentException("sourceRids cannot be null");
    if (sourceRids.isEmpty())
      throw new IllegalArgumentException("sourceRids cannot be empty");
    if (sourceRids.size() > config.getMaxSourceDocs())
      throw new IllegalArgumentException(
          "Number of source documents (" + sourceRids.size() + ") exceeds maxSourceDocs (" + config.getMaxSourceDocs() + ")");

    // Step 1 & 2: Extract terms from source documents and count term frequencies
    final Map<String, Integer> termFreqs = new HashMap<>();
    final List<String> propertyNames = getPropertyNames();

    if (propertyNames != null && !propertyNames.isEmpty()) {
      final DatabaseInternal db = underlyingIndex.getComponent().getDatabase();
      for (final RID sourceRid : sourceRids) {
        final Identifiable identifiable = db.lookupByRID(sourceRid, true);
        if (identifiable == null)
          continue;

        final Document doc = (Document) identifiable;
        for (final String propName : propertyNames) {
          final Object value = doc.get(propName);
          if (value == null)
            continue;

          final List<String> tokens = analyzeText(indexAnalyzer, new Object[] { value });
          for (final String token : tokens) {
            if (token != null)
              termFreqs.merge(token, 1, Integer::sum);
          }
        }
      }
    }

    if (termFreqs.isEmpty())
      return new TempIndexCursor(Collections.emptyList());

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

    if (topTerms.isEmpty())
      return new TempIndexCursor(Collections.emptyList());

    // Step 5: Execute OR query and accumulate scores
    final Map<RID, Integer> scoreMap = new HashMap<>();
    for (final String term : topTerms) {
      final IndexCursor termCursor = underlyingIndex.get(new String[] { term });
      while (termCursor.hasNext()) {
        final RID rid = termCursor.next().getIdentity();
        scoreMap.merge(rid, 1, Integer::sum);
      }
    }

    // Step 6: Exclude source documents if configured
    if (config.isExcludeSource()) {
      for (final RID sourceRid : sourceRids)
        scoreMap.remove(sourceRid);
    }

    // Step 7: Build result list sorted by score descending
    final List<IndexCursorEntry> results = new ArrayList<>(scoreMap.size());
    for (final Map.Entry<RID, Integer> entry : scoreMap.entrySet())
      results.add(new IndexCursorEntry(null, entry.getKey(), entry.getValue()));

    if (results.size() > 1)
      results.sort(Comparator.comparingInt((IndexCursorEntry e) -> e.score).reversed());

    return new TempIndexCursor(results);
  }
}
