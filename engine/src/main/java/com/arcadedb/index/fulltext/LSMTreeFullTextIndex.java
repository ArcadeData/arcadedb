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
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.NeedRetryException;
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
import com.arcadedb.log.LogManager;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

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
  /** Max query terms detailed in an EXPLAIN/PROFILE scoring breakdown (each costs one posting scan); beyond it the output is truncated. */
  private static final int MAX_EXPLAIN_TERMS = 64;

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

      // Reject a property whose name collides with the query parser's reserved default-field sentinel: on a multi-property index
      // such a field could not be distinguished from an unqualified term. Fail fast at creation rather than mis-scoring silently.
      if (builder.getMetadata() != null)
        checkReservedPropertyNames(builder.getMetadata().propertyNames);

      // Get metadata if available. New full-text indexes default to BM25 similarity (issue #4687): when the user did not supply a
      // FullTextIndexMetadata, synthesize one carrying the BM25 defaults so freshly created indexes rank with BM25 out of the box.
      final FullTextIndexMetadata ftMetadata;
      if (builder.getMetadata() instanceof FullTextIndexMetadata m)
        ftMetadata = m;
      else {
        final IndexMetadata base = builder.getMetadata();
        ftMetadata = FullTextIndexMetadata.defaultBM25(base.typeName, base.propertyNames.toArray(new String[0]),
            base.associatedBucketId);
      }

      return new LSMTreeFullTextIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE, builder.getPageSize(), builder.getNullStrategy(), ftMetadata);
    }
  }

  /**
   * Rejects any property whose name equals the query parser's reserved default-field sentinel: on a multi-property index such a
   * field could not be distinguished from an unqualified term and would mis-score silently. Enforced both at index creation and on
   * schema reload (a hand-edited/restored schema could otherwise reintroduce the collision).
   */
  public static void checkReservedPropertyNames(final List<String> propertyNames) {
    if (propertyNames != null)
      for (final String property : propertyNames)
        if (FullTextQueryExecutor.DEFAULT_FIELD.equals(property))
          throw new IllegalArgumentException(
              "Property name '" + property + "' is reserved for full-text indexes; please rename the property");
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
    if (metadata != null && metadata.isBM25()) {
      index.setStoreTermFrequency(true);
      // Warn at open time (not only when the first query triggers it) so operators can pre-warm before serving traffic: a BM25
      // index whose persisted counters are not trustworthy will do a full type scan (under a short lock) on its first query.
      if (!metadata.isCountersValid())
        LogManager.instance().log(this, Level.WARNING,
            "BM25 full-text index '%s' opened with no valid corpus counters; the first query will run a full type scan. "
                + "Pre-warm with REBUILD INDEX <name> WITH statsOnly = true before serving traffic to avoid a first-query stall.",
            null, index.getName());
    }
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

    // Most-relevant first (descending coordination score), RID as a stable tiebreaker so the order is deterministic. Matches the
    // BM25 path; the previous ascending sort returned the least-relevant documents first.
    if (list.size() > 1)
      list.sort((o1, o2) -> {
        final int cmp = Integer.compare(o2.score, o1.score);
        if (cmp != 0)
          return cmp;
        return o1.record.getIdentity().compareTo(o2.record.getIdentity());
      });

    if (limit > -1 && list.size() > limit)
      return new TempIndexCursor(list.subList(0, limit));

    return new TempIndexCursor(list);
  }

  /**
   * Shared BM25 scoring core used by both the direct ({@link #get}) and the Lucene-syntax ({@link FullTextQueryExecutor}) paths,
   * so the formula, parameters and corpus statistics can never diverge between them.
   * <p>
   * When {@code candidates} is non-null only those documents are scored: a single streaming pass counts the document frequency
   * and collects just the candidate postings (bounded by the candidate set), then the BM25 contribution is applied once the IDF
   * is known. When {@code candidates} is null every matching document is scored, which needs two passes (count df, then
   * accumulate) to avoid materializing the whole posting list.
   * <p>
   * DESIGN (deliberate, not a TODO): the no-candidate path streams each token's posting list twice (count df, then accumulate)
   * rather than materializing it once and deriving df from the list. This is a memory-vs-I/O trade-off resolved in favour of
   * bounded peak memory, per the project's low-GC priority: a single-pass-materialize would hold one common term's entire posting
   * list (df {@link FullTextPostingRID} objects) <i>simultaneously</i> with the growing score map - roughly doubling peak memory
   * for a high-frequency term - whereas the streaming df pass keeps only the score map live. The cost is 2*T posting scans for a
   * T-term query. This path is the direct {@code index.get(query)} API only; the primary SQL {@code SEARCH_INDEX} entry point is
   * candidate-based and already single-pass, so prefer it for large unconstrained queries. The clean way to get single-pass
   * <i>without</i> the memory hit is a per-key entry count at the LSM layer (the compacted index already keeps page-level counts
   * in its root); that is a larger storage-layer change tracked separately.
   *
   * @param tokenBoosts scoring tokens (stored-key form) mapped to their effective boost
   * @param candidates  documents to score, or {@code null} to score every matching document
   */
  private Map<RID, double[]> computeBM25Scores(final Map<String, Float> tokenBoosts, final Set<RID> candidates) {
    return computeBM25Scores(tokenBoosts, candidates, null);
  }

  /**
   * Scores this bucket's candidates with either bucket-local statistics ({@code scoringContext == null}) or one type-wide
   * context supplied by {@link FullTextSearch}. The latter keeps scores comparable when a logical type spans several buckets.
   */
  private Map<RID, double[]> computeBM25Scores(final Map<String, Float> tokenBoosts, final Set<RID> candidates,
      final BM25ScoringContext scoringContext) {
    ensureCounters();
    final long totalDocs = scoringContext != null ? scoringContext.totalDocs() : resolveTotalDocs();
    final double avgdl = scoringContext != null ? scoringContext.avgDocLength() : resolveAvgDocLength();
    final double k1 = ftMetadata.getBm25K1();
    final double b = ftMetadata.getBm25B();

    // Accumulate in a per-RID double[1] cell rather than a boxed Double: a document matched by T query terms is updated T times,
    // and Map.merge(..., Double::sum) would box on every one of those updates. The cell is allocated once per unique document
    // (same as a Double key would be) and then accumulated as a primitive. The score is narrowed to float when the cursor entry
    // is built. Reading is via getScore() below.
    final Map<RID, double[]> scoreMap = candidates != null ? new HashMap<>(candidates.size()) : new HashMap<>();
    if (candidates != null)
      for (final RID c : candidates)
        scoreMap.put(c, new double[1]);

    // Reused across tokens to avoid per-token allocations (candidate path only).
    final List<FullTextPostingRID> hits = new ArrayList<>();

    for (final Map.Entry<String, Float> e : tokenBoosts.entrySet()) {
      // Fresh single-element lookup key per token: it is handed to underlyingIndex.get(), and allocating it here (rather than
      // reusing one array) keeps this correct even if that lookup ever became lazy/async and retained the array - the few small
      // arrays per query are negligible GC.
      final String[] storedKey = new String[] { e.getKey() };
      final float boost = e.getValue();

      if (candidates != null) {
        // Single pass over this bucket: collect only candidate postings. For a leaf-index query, count df locally; for a
        // type-wide query, FullTextSearch already counted df across every bucket and supplied it in the scoring context.
        long df = scoringContext != null ? scoringContext.documentFrequency(e.getKey()) : 0L;
        hits.clear();
        final IndexCursor cursor = underlyingIndex.get(storedKey);
        while (cursor.hasNext()) {
          final Identifiable id = cursor.next();
          // Skip deletion markers (negative bucket id): they are not live documents and must not inflate the document frequency.
          if (id.getIdentity().getBucketId() < 0)
            continue;
          if (scoringContext == null)
            ++df;
          if (id instanceof FullTextPostingRID s && scoreMap.containsKey(s.getIdentity()))
            hits.add(s);
        }
        if (df == 0L)
          continue;
        final double idf = BM25Scorer.idf(totalDocs, df);
        for (final FullTextPostingRID s : hits)
          scoreMap.get(s.getIdentity())[0] += BM25Scorer.termScore(idf, s.getTf(), s.getDocLength(), avgdl, k1, b) * boost;
      } else {
        // No candidate filter: a leaf-index query counts df locally with a streaming first pass. A type-wide coordinator has
        // already counted global df, so it goes straight to the scoring pass over this bucket.
        long df = scoringContext != null ? scoringContext.documentFrequency(e.getKey()) : 0L;
        if (scoringContext == null) {
          final IndexCursor dfCursor = underlyingIndex.get(storedKey);
          while (dfCursor.hasNext()) {
            if (dfCursor.next().getIdentity().getBucketId() >= 0) // skip deletion markers
              ++df;
          }
        }
        if (df == 0L)
          continue;
        final double idf = BM25Scorer.idf(totalDocs, df);
        final IndexCursor scoreCursor = underlyingIndex.get(storedKey);
        while (scoreCursor.hasNext()) {
          final Identifiable id = scoreCursor.next();
          if (id instanceof FullTextPostingRID s)
            scoreMap.computeIfAbsent(s.getIdentity(), k -> new double[1])[0] +=
                BM25Scorer.termScore(idf, s.getTf(), s.getDocLength(), avgdl, k1, b) * boost;
        }
      }
    }
    return scoreMap;
  }

  /**
   * BM25 scoring path for the direct {@link #get} lookup. Builds the scoring tokens from the query and delegates to
   * {@link #computeBM25Scores}.
   * <p>
   * Query syntax here is limited: {@link #parseQueryTerms} only understands whitespace-separated terms and {@code field:value}.
   * It does NOT support the Lucene query syntax - caret boosts ({@code term^3}), boolean operators, phrases, or wildcards - which
   * the SQL {@code SEARCH_INDEX(...)} entry point handles via the full Lucene parser. A {@code term^3} reaching here is passed to
   * the analyzer as literal text: the StandardAnalyzer tokenizes around the caret into {@code [term, 3]}, so it matches on
   * {@code term} but the {@code ^3} is silently ignored rather than applied as a boost. Use {@code SEARCH_INDEX} for the full
   * query syntax; this direct path is for simple term lookups.
   * <p>
   * This simple-token semantics is INTENTIONAL, not a deficiency: the direct {@code get()} path also backs the SQL
   * {@code CONTAINSTEXT} operator, whose argument is literal text to find - not a query language. Routing {@code get()} through the
   * Lucene parser would make {@code label CONTAINSTEXT 'a-b'} or {@code 'foo AND bar'} interpret {@code -}/{@code AND} as
   * operators (wrong); throwing on such characters would reject legitimate literal text. So {@code get()} stays token-based and
   * {@code SEARCH_INDEX} is the deliberate home for Lucene syntax.
   * <p>
   * PERFORMANCE: this path scores every matching document with no candidate set, so {@link #computeBM25Scores} streams each
   * token's posting list twice (count df, then accumulate) - {@code 2*T} posting scans for a {@code T}-term query. The SQL
   * {@code SEARCH_INDEX} path is single-pass; prefer it for large or many-term queries.
   */
  private IndexCursor getBM25(final Object[] keys, final int limit) {
    final Map<String, Float> tokenBoosts = getSimpleQueryTokenBoosts(keys);

    // The no-candidate path streams each token's postings twice (df + accumulate). Log at FINE for a multi-term query so an
    // operator debugging a slow direct get() can see it doing 2*T scans and switch to SEARCH_INDEX. FINE keeps normal runs quiet.
    if (tokenBoosts.size() > 1)
      LogManager.instance().log(this, Level.FINE,
          "Full-text get() scoring %d terms on index '%s' with two posting scans each; use SEARCH_INDEX(...) for a single-pass scan.",
          null, tokenBoosts.size(), getName());

    return buildScoredCursor(computeBM25Scores(tokenBoosts, null), keys, limit);
  }

  /**
   * Analyzes the direct {@link #get} query into stored-key tokens without applying Lucene query syntax. Package-private so the
   * logical {@link TypeIndex} can score the same literal query with type-wide statistics across all bucket indexes.
   */
  Map<String, Float> getSimpleQueryTokenBoosts(final Object[] keys) {
    final String queryText = keys.length > 0 && keys[0] != null ? keys[0].toString() : "";
    // The direct path does not parse Lucene syntax; a caret boost here is silently treated as part of the token and matches
    // nothing. Log at FINE so this surfaces when debugging an unexpected empty result, without spamming normal queries.
    if (queryText.indexOf('^') >= 0)
      LogManager.instance().log(this, Level.FINE,
          "Full-text get() query '%s' contains a caret; the direct lookup path does not support Lucene syntax (caret/boolean/"
              + "phrase/wildcard) - use SEARCH_INDEX(...) for that.", null, queryText);
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
    return tokenBoosts;
  }

  /**
   * Strips the internal {@link FullTextPostingRID} subtype off a posting RID so the public cursor - and therefore the {@code @rid}
   * that reaches query results - exposes the canonical record identity ({@code #bucket:offset}). The tf/docLength carried by the
   * subtype are an internal BM25 scoring detail, already consumed to compute the score by the time a result cursor is built, and
   * its {@code toString()} would otherwise leak {@code {tf=..,docLength=..}} into the externally visible RID (issue #4731). A RID
   * that is not a posting subtype is returned unchanged. Matches the class's "only the full-text index interprets them" intent.
   */
  private static RID canonicalRID(final RID rid) {
    return rid instanceof final FullTextPostingRID posting ? new DatabaseRID(posting.getBoundDatabase(), posting) : rid;
  }

  /**
   * Ranks the scored documents most-relevant-first (descending score, RID as a stable tiebreaker so equal-scored documents have a
   * deterministic order) and returns a cursor over the (optionally limited) result. When a {@code limit} smaller than the result
   * set is requested it selects the top-K with a bounded min-heap (O(N log K)) instead of fully sorting the whole set
   * (O(N log N)) - a meaningful win for the common {@code ORDER BY $score DESC LIMIT k} shape over a large candidate set.
   */
  private IndexCursor buildScoredCursor(final Map<RID, double[]> scoreMap, final Object[] keys, final int limit) {
    final Comparator<IndexCursorEntry> bestFirst = (o1, o2) -> {
      final int cmp = Float.compare(o2.floatScore, o1.floatScore);
      if (cmp != 0)
        return cmp;
      return o1.record.getIdentity().compareTo(o2.record.getIdentity());
    };

    final int size = scoreMap.size();
    if (limit > -1 && limit < size) {
      // Bounded min-heap whose head is the WORST kept entry (reversed comparator), so we can evict it when a better one arrives.
      final PriorityQueue<IndexCursorEntry> heap = new PriorityQueue<>(limit, bestFirst.reversed());
      for (final Map.Entry<RID, double[]> e : scoreMap.entrySet()) {
        final IndexCursorEntry entry = new IndexCursorEntry(keys, canonicalRID(e.getKey()), (float) e.getValue()[0]);
        if (heap.size() < limit)
          heap.add(entry);
        else if (bestFirst.compare(entry, heap.peek()) < 0) {
          heap.poll();
          heap.add(entry);
        }
      }
      final IndexCursorEntry[] top = heap.toArray(new IndexCursorEntry[0]);
      Arrays.sort(top, bestFirst);
      return new TempIndexCursor(Arrays.asList(top));
    }

    final ArrayList<IndexCursorEntry> list = new ArrayList<>(size);
    for (final Map.Entry<RID, double[]> entry : scoreMap.entrySet())
      list.add(new IndexCursorEntry(keys, canonicalRID(entry.getKey()), (float) entry.getValue()[0]));
    if (list.size() > 1)
      list.sort(bestFirst);
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
    return buildScoredCursor(computeBM25Scores(tokenBoosts, candidates), keys, limit);
  }

  /**
   * Scores this bucket with corpus statistics already aggregated by {@link FullTextSearch}. A null candidate set means every
   * posting matching the scoring tokens is eligible, which is the direct TypeIndex.get() path.
   */
  IndexCursor scoreBM25WithContext(final Set<RID> candidates, final Map<String, Float> tokenBoosts, final Object[] keys,
      final int limit, final BM25ScoringContext scoringContext) {
    return buildScoredCursor(computeBM25Scores(tokenBoosts, candidates, scoringContext), keys, limit);
  }

  /**
   * Returns the raw postings for an already-resolved stored key (RID-only, no BM25 scoring and no re-analysis), used by
   * {@link FullTextQueryExecutor} to collect candidate documents cheaply before scoring them once via
   * {@link #scoreCandidatesBM25}. Going through {@link #get} here would run the full scoring pipeline only to discard the scores.
   * <p>
   * Package-private: it takes a pre-analyzed stored key and bypasses the analysis pipeline, so an unanalyzed input (e.g. mixed
   * case) would silently match nothing. Only {@link FullTextQueryExecutor} (same package) should call it.
   */
  IndexCursor getPostings(final String storedKey) {
    return underlyingIndex.get(new String[] { storedKey });
  }

  /**
   * Makes the shared type-wide counters trustworthy before a coordinator reads them.
   */
  void ensureTypeWideBM25Counters() {
    ensureCounters();
  }

  long getTypeWideDocumentCount() {
    return ftMetadata != null ? ftMetadata.getTotalDocs() : 0L;
  }

  double getTypeWideAverageDocumentLength() {
    return resolveAvgDocLength();
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
    // This leaf-index explanation is bucket-local. SQL EXPLAIN/PROFILE routes through FullTextSearch and reports type-wide
    // statistics; retain the local scope here for callers deliberately addressing a bucket index directly.
    final Bucket bucket = associatedBucket();
    if (bucket != null)
      json.put("bucket", bucket.getName());
    json.put("note", "bucket-level index statistics; use the logical TypeIndex for globally comparable BM25 scores");

    // One posting scan per scoring token to derive df. EXPLAIN/PROFILE is an interactive diagnostic, not a hot path, but cap the
    // number of explained terms so a pathological wildcard/fuzzy expansion (which can explode into thousands of tokens) cannot
    // turn an EXPLAIN into a very long scan; beyond the cap the explain reports "termsTruncated": true.
    final JSONArray terms = new JSONArray();
    int explained = 0;
    for (final Map.Entry<String, Float> e : tokenBoosts.entrySet()) {
      if (explained >= MAX_EXPLAIN_TERMS) {
        // Report both the flag and how many terms were omitted so a user debugging a complex query knows the breakdown is partial.
        json.put("termsTruncated", true);
        json.put("termsOmitted", tokenBoosts.size() - MAX_EXPLAIN_TERMS);
        json.put("termsShown", MAX_EXPLAIN_TERMS);
        break;
      }
      explained++;
      final IndexCursor postings = underlyingIndex.get(new String[] { e.getKey() });
      long df = 0;
      while (postings.hasNext()) {
        // Skip deletion markers (negative bucket id) so the explained df/idf matches what computeBM25Scores actually uses.
        if (postings.next().getIdentity().getBucketId() >= 0)
          ++df;
      }
      terms.put(new JSONObject().put("term", e.getKey()).put("df", df).put("idf", BM25Scorer.idf(totalDocs, df))
          .put("boost", e.getValue()));
    }
    json.put("terms", terms);
    return json;
  }

  /**
   * Returns N for a bucket-level lookup. The logical {@link TypeIndex}/{@link FullTextSearch} path supplies type-wide N and df
   * through {@link BM25ScoringContext}; a caller deliberately addressing this leaf index instead keeps both values scoped to this
   * bucket. The shared type-wide counters still provide the average document length.
   */
  private long resolveTotalDocs() {
    final long count = associatedBucketCount();
    return count > 0 ? count : 1L;
  }

  /**
   * Returns the (type-wide) average document length used as the BM25 length normalizer, or 1.0 when no statistics are available
   * or the corpus is empty (no documents means no meaningful average; 1.0 makes the length-normalization term a no-op).
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
      // Should not happen for a bucket-level index; surface it (IDF then falls back to N=1) instead of failing silently.
      LogManager.instance().log(this, Level.WARNING, "Cannot resolve bucket %d for full-text index '%s'; BM25 will use N=1",
          e, getAssociatedBucketId(), getName());
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
    if (ftMetadata == null)
      return;
    if (!ftMetadata.isCountersValid()) {
      // Cold counters (pre-feature/unsaved index): rebuild so the caller scores with valid statistics. This is the ONLY place
      // that needs a lock - the counters themselves are AtomicLong (lock-free reads/updates everywhere else); the synchronized
      // block exists solely so concurrent first-queries (across the type's bucket indexes, which share this metadata) do not all
      // run the full type scan at once. Double-checked: the first thread rebuilds and marks the counters valid; the rest observe
      // that inside the lock and skip.
      synchronized (ftMetadata) {
        if (!ftMetadata.isCountersValid())
          computeCorpusCounters(false);
      }
      return;
    }
    // Persisted counters can lag the on-disk data (documents indexed after the last schema save). Once per session, validate them
    // with a cheap live document count and rebuild only if they actually disagree - so a clean restart with fresh counters pays
    // nothing, while a stale one self-heals on the first query. The CAS ensures only one thread runs this even though the
    // metadata is shared across the type's bucket indexes.
    if (ftMetadata.claimStaleCheck()) {
      final String typeName = getTypeName();
      if (typeName != null) {
        // Both sides are TYPE-WIDE: getTotalDocs() is the shared counter accumulated across every bucket index (they share this
        // metadata instance), and countType() is the type's live record count. The logical TypeIndex search uses this count for
        // global IDF as well as avgdl; a direct bucket-level lookup uses resolveTotalDocs() instead.
        final long liveCount = underlyingIndex.getMutableIndex().getDatabase().countType(typeName, false);
        final long persisted = ftMetadata.getTotalDocs();
        if (liveCount != persisted) {
          // Surface the drift so operators can notice it (e.g. counters inflated by rolled-back inserts) without reading EXPLAIN.
          // Escalate to WARNING when the divergence is large (> 10% of the live count): small drift self-heals quietly here, but a
          // big gap usually signals a heavy-rollback workload worth a scheduled REBUILD INDEX ... WITH statsOnly = true.
          final boolean large = liveCount > 0 && Math.abs(persisted - liveCount) * 10 > liveCount;
          LogManager.instance().log(this, large ? Level.WARNING : Level.INFO,
              "BM25 corpus counters for type '%s' diverged from live data (persisted=%d, live=%d); recomputing.%s", null, typeName,
              persisted, liveCount, large ? " Consider REBUILD INDEX ... WITH statsOnly = true if this recurs." : "");
          computeCorpusCounters(false);
        }
      }
    }
  }

  /**
   * Rescans the type and rebuilds the type-wide BM25 corpus counters (document count and total document length), then persists
   * them. Use it after a bulk import or to repair drifted counters.
   */
  public void recomputeBM25Counters() {
    computeCorpusCounters(true);
  }

  /**
   * Recomputes the BM25 corpus counters when this index ranks with BM25 (CLASSIC indexes keep no such statistics). Drives the
   * SQL {@code REBUILD INDEX <name> {statsOnly: true}} drift-repair path.
   */
  @Override
  public boolean recomputeStatistics() {
    if (!isBM25())
      return false;
    recomputeBM25Counters();
    return true;
  }

  /**
   * Scans the whole type and rebuilds the shared corpus counters used for global N and average document length. The counters are
   * type-wide because the {@link FullTextIndexMetadata} instance is shared by all bucket indexes; scanning a single bucket would
   * corrupt them. When {@code persist} is true the schema is saved so the counters survive a restart; the lazy read-path caller
   * passes false to avoid saving the schema during a query.
   */
  private void computeCorpusCounters(final boolean persist) {
    if (ftMetadata == null)
      return;
    final DatabaseInternal db = underlyingIndex.getMutableIndex().getDatabase();
    final String typeName = getTypeName();
    if (typeName == null)
      return;

    // Cold-start scan: only reached when the counters are not trustworthy (unsaved/pre-feature index). Logged because on a large
    // collection the first BM25 query that triggers it can be slow.
    LogManager.instance().log(this, Level.INFO, "Recomputing BM25 corpus statistics for type '%s' (full scan)", null, typeName);

    final List<String> props = getPropertyNames();
    long docs = 0L;
    long sumLen = 0L;
    final Iterator<Record> it = db.iterateType(typeName, true);
    while (it.hasNext()) {
      final Record record = it.next();
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
    // Report the result so operators can correlate the cold-start latency above with the corpus size that drove it.
    LogManager.instance().log(this, Level.INFO,
        "Recomputed BM25 corpus statistics for type '%s': %d documents, %d total tokens (avgdl=%.2f)", null, typeName, docs,
        sumLen, docs > 0 ? (double) sumLen / docs : 1.0);
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
      final Map<String, Integer> tfs = new HashMap<>();
      int docLen = 0;
      boolean hasNull = false;
      for (final String k : keywords) {
        // A null token (null indexed value) carries no meaningful tf/docLength: keep it out of the stats so it never becomes a
        // FullTextPostingRID with a bogus tf, but remember it so it can still be put as a plain posting below (letting the
        // configured NULL_STRATEGY decide whether to skip, error, or index it).
        if (k == null) {
          hasNull = true;
          continue;
        }
        tfs.merge(k, 1, Integer::sum);
        ++docLen;
      }
      final int len = docLen;
      for (final Map.Entry<String, Integer> e : tfs.entrySet())
        underlyingIndex.put(new String[] { e.getKey() }, withStats(db, rids, e.getValue(), len));
      if (hasNull)
        // Plain RID (no stats): if NULL_STRATEGY indexes it, the posting carries tf=0/docLength=0 rather than a stale tf.
        underlyingIndex.put(new String[] { null }, rids);
      countDocuments(rids.length, docLen);
    } else {
      final List<String> propertyNames = getPropertyNames();

      // PASS 1: analyze every field, accumulate document length and per-field + global term frequencies.
      // Design choice: the unprefixed (field-agnostic) posting carries the GLOBAL tf - the term's total occurrences across all
      // indexed fields of the document - while each field-prefixed posting carries that field's tf. So an unqualified query
      // (`java`) treats a term appearing in both title and body as tf=2, whereas a field-qualified query (`title:java`) sees
      // tf=1. This differs from Lucene's per-field independence and means cross-field repetition boosts unqualified relevance;
      // it is intentional ("how often does the term occur anywhere in the document").
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
   * Updates the persisted corpus counters when a document is indexed. {@code numDocs} is {@code rids.length}: the standard
   * indexing path passes exactly one RID per document (one document being indexed), so each RID is counted as a distinct
   * document of length {@code docLen}.
   * <p>
   * This runs at index time, before the transaction commits, and is NOT reversed on rollback, so a rolled-back insert leaves the
   * counters slightly inflated. The counters feed only {@code avgDocLength} (a robust normalizer); {@link #recomputeBM25Counters}
   * (also reachable via {@code REBUILD INDEX <name> WITH statsOnly = true}) repairs any drift exactly.
   * <p>
   * Because the increment is pre-commit, a BM25 {@code get()} issued LATER in the SAME open transaction sees the bumped
   * {@code totalDocs}/{@code sumDocLength}, so a just-inserted (not yet committed) document is reflected in {@code avgDocLength}
   * for that transaction. This only shifts the length normalizer marginally and resolves at commit; it is not a correctness issue.
   * <p>
   * FOLLOW-UP (not done deliberately): the drift could be avoided by deferring this update to an after-commit callback
   * ({@code TransactionContext.addAfterCommitCallback}) so a rolled-back transaction never bumps the counters. That is left for a
   * separate change because the counters must stay consistent across an HA cluster: the current update runs inside the index
   * write that every node replays, whereas an after-commit callback fires only on the committing node and would diverge replica
   * counters. Reversing on rollback (rather than deferring) would need a rollback hook that does not exist today.
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

    // Keep the BM25 corpus counters in sync when a document is removed. remove() carries a SINGLE rid (one document), so exactly
    // one removeDocument() call balances the single addDocument() that put() made for that document - the indexing convention is
    // one rid per document. (put() loops addDocument() over its rid array; were remove() ever called with N documents sharing a
    // key it would need the same N decrements, but no caller does that.) docLen is recomputed from the keys supplied at remove
    // time: if a field is null now but was set at index time (or the analyzer changed), this under-decrements sumDocLength and
    // avgDocLength drifts. Since avgDocLength is only a length normalizer this is tolerable; recomputeBM25Counters() repairs it.
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

  /**
   * Builds the full-text index over the records already present in the type (issue #4733). Unlike a plain LSM-Tree index, the
   * full-text index must route every record through THIS wrapper's {@link #put} so the property values are analyzed/tokenized
   * (and, for BM25, carry tf/docLength). Delegating to {@code underlyingIndex.build()} would instead feed the raw, per-property
   * value array straight to the single-key underlying LSM-Tree: a multi-property index then crashes with
   * "Index N out of bounds for length 1" (the underlying index has one key type but receives N property values), and a
   * single-property index would store untokenized raw values. So the scan is performed here, indexing through {@code this}.
   */
  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    final DatabaseInternal db = underlyingIndex.getMutableIndex().getDatabase();
    final String typeName = getTypeName();
    if (typeName == null)
      throw new IndexException("Cannot build index '" + getName() + "' because metadata information are missing");

    if (!setStatus(new INDEX_STATUS[] { INDEX_STATUS.AVAILABLE }, INDEX_STATUS.UNAVAILABLE))
      throw new NeedRetryException("Error on building index '" + getName() + "' because not available");

    final AtomicLong total = new AtomicLong();
    try {
      final String bucketName = db.getSchema().getBucketById(getAssociatedBucketId()).getName();
      LogManager.instance().log(this, Level.INFO, "Building full-text index '%s' on %s...", getName(),
          typeName + getPropertyNames());

      db.scanBucket(bucketName, record -> {
        db.getIndexer().addToIndex(LSMTreeFullTextIndex.this, record.getIdentity(), (Document) record);
        total.incrementAndGet();

        if (total.get() % buildIndexBatchSize == 0) {
          db.getWrappedDatabaseInstance().commit();
          db.getWrappedDatabaseInstance().begin();
        }

        if (callback != null)
          callback.onDocumentIndexed((Document) record, total.get());

        return true;
      }, (rid, exception) -> {
        if (exception instanceof RuntimeException re)
          throw re;
        throw new IndexException("Error on building index '" + getName() + "' at record " + rid, exception);
      });
    } finally {
      setStatus(new INDEX_STATUS[] { INDEX_STATUS.UNAVAILABLE }, INDEX_STATUS.AVAILABLE);
    }

    return total.get();
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
        // Emit a null token so a null value still reaches the underlying index and its configured NULL_STRATEGY (SKIP/ERROR) is
        // enforced. Callers that compute document length exclude these null tokens (see putWithStats).
        tokens.add(null);
      else if (t instanceof Collection<?> collection) {
        // A LIST BY ITEM property value: tokenize each element under the same field (union of the list's terms), issue #5181.
        // Null elements are skipped (an absent list item is not a null field, so it must not trigger NULL_STRATEGY here).
        for (final Object element : collection)
          if (element != null)
            tokenize(analyzer, element, tokens);
      } else if (t instanceof Object[] array) {
        for (final Object element : array)
          if (element != null)
            tokenize(analyzer, element, tokens);
      } else
        tokenize(analyzer, t, tokens);
    }
    return tokens;
  }

  /**
   * Tokenizes a single non-null value with the analyzer and appends the resulting tokens to {@code tokens}.
   */
  private static void tokenize(final Analyzer analyzer, final Object value, final List<String> tokens) {
    final TokenStream tokenizer = analyzer.tokenStream("contents", value.toString());
    try {
      tokenizer.reset();
      final CharTermAttribute termAttribute = tokenizer.getAttribute(CharTermAttribute.class);

      try {
        while (tokenizer.incrementToken())
          tokens.add(termAttribute.toString());

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

    // Total documents in this bucket, consistent with the per-bucket document frequencies above. The live bucket record count is
    // exact and always >= any term's df, unlike the previous max-df proxy which understated IDF for terms shared by many
    // documents (the most common term defined totalDocs, forcing its IDF toward zero and skewing term selection).
    final int totalDocs = (int) Math.min(Integer.MAX_VALUE, resolveTotalDocs());

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
      results.add(new IndexCursorEntry(null, canonicalRID(entry.getKey()), entry.getValue()));

    if (results.size() > 1)
      results.sort(Comparator.comparingInt((IndexCursorEntry e) -> e.score).reversed());

    return new TempIndexCursor(results);
  }
}
