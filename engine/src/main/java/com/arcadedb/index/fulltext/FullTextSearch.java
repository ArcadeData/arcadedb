/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.FullTextPostingRID;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Shared entry point for full-text search (BM25 or CLASSIC similarity) over a type's full-text index. Used by the
 * SEARCH_INDEX SQL function and by callers that need scored results without going through SQL.
 */
public class FullTextSearch {
  private static final int      MAX_EXPLAIN_TERMS = 64;
  // The map-returning search paths expose scores by RID and never read the cursor entry keys, so one shared empty array
  // stands in for them rather than allocating a throwaway per bucket.
  private static final Object[] NO_KEYS           = new Object[0];

  private FullTextSearch() {
  }

  /**
   * Resolves a full-text index by name and validates it is a full-text type index. The TypeIndex found while validating
   * is returned directly, so a caller that already needs to search or inspect it (e.g. {@link #search(TypeIndex, String, int)})
   * does not repeat the schema lookup by name. What is saved is that one lookup, not the bucket array itself: this
   * method's own validation and {@code search}'s iteration each call {@link TypeIndex#getIndexesOnBuckets()}
   * independently, so the array is still materialized once per call.
   *
   * @throws com.arcadedb.exception.SchemaException if no index carries that name
   * @throws CommandExecutionException              if the index exists but is not a full-text type index
   */
  public static TypeIndex resolveFullTextIndex(final Database database, final String indexName) {
    final Index index = database.getSchema().getIndexByName(indexName);

    if (!(index instanceof final TypeIndex typeIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

    final IndexInternal[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

    return typeIndex;
  }

  /**
   * Resolves the named full-text index and searches it for every match, unbounded. Kept for callers that only have an
   * index name on hand, such as the SEARCH_INDEX SQL function, whose per-row boolean semantics need the full match set.
   *
   * @throws com.arcadedb.exception.SchemaException if no index carries that name
   * @throws CommandExecutionException              if the index exists but is not a full-text type index
   */
  public static Map<RID, Float> search(final Database database, final String indexName, final String queryText) {
    return search(resolveFullTextIndex(database, indexName), queryText, -1);
  }

  /**
   * Searches every bucket index behind an already-resolved full-text index and returns the matching RIDs with their
   * scores. Any {@code limit} less than 1 (including {@code 0} and any negative value other than {@code -1}) is
   * normalized to {@code -1}, meaning unbounded; the SQL {@code SEARCH_INDEX} function depends on {@code -1}
   * continuing to mean unbounded.
   * <p>
   * BM25 searches spanning several buckets match every bucket first, counting each scoring token's document frequency in the
   * same posting walk, and then score every bucket against those pooled type-wide statistics. This makes scores globally
   * comparable without adding shared counters to the write path and without a dedicated document-frequency scan. The limit is
   * still pushed down after each bucket's candidates are scored; a global top-K is contained in the union of those
   * now-comparable per-bucket top-K sets, so callers still sort and truncate a union of at most {@code buckets * limit}
   * entries.
   */
  public static Map<RID, Float> search(final TypeIndex typeIndex, final String queryText, final int limit) {
    final int effectiveLimit = limit < 1 ? -1 : limit;
    final List<LSMTreeFullTextIndex> bucketIndexes = getBucketIndexes(typeIndex);
    if (bucketIndexes.isEmpty())
      return Map.of();

    if (bucketIndexes.size() > 1 && bucketIndexes.getFirst().isBM25())
      return searchBM25(bucketIndexes, queryText, effectiveLimit);

    final Map<RID, Float> allResults = new HashMap<>();

    for (final LSMTreeFullTextIndex ftIndex : bucketIndexes) {
      final IndexCursor cursor = new FullTextQueryExecutor(ftIndex).search(queryText, effectiveLimit);
      mergeCursor(allResults, cursor);
    }

    return allResults;
  }

  private static Map<RID, Float> searchBM25(final List<LSMTreeFullTextIndex> bucketIndexes, final String queryText,
      final int limit) {
    // Pass one: match each bucket and count every scoring token's document frequency during the same posting walk, so global
    // statistics cost no extra scan. Matching stays bucket-local, preserving Lucene boolean/phrase/wildcard semantics; only the
    // corpus statistics are pooled. Both passes are needed because no bucket can be scored until every bucket has reported its
    // share of each token's document frequency.
    final List<FullTextQueryExecutor.BM25QueryMatch> matches = new ArrayList<>(bucketIndexes.size());
    final LinkedHashMap<String, Float> scoringTokens = new LinkedHashMap<>();
    final Map<String, Long> documentFrequencies = new HashMap<>();

    for (final LSMTreeFullTextIndex ftIndex : bucketIndexes) {
      final FullTextQueryExecutor.BM25QueryMatch match = new FullTextQueryExecutor(ftIndex).collectBM25Matches(queryText);
      matches.add(match);
      for (final Map.Entry<String, Float> token : match.scoringTokens().entrySet())
        scoringTokens.merge(token.getKey(), token.getValue(), Math::max);
      // Each bucket reports only the tokens it holds postings for, and a wildcard is expanded against every bucket's own key
      // range, so summing the per-bucket counts yields the type-wide document frequency.
      for (final Map.Entry<String, Long> frequency : match.documentFrequencies().entrySet())
        documentFrequencies.merge(frequency.getKey(), frequency.getValue(), Long::sum);
    }

    capScoringTokens(scoringTokens, documentFrequencies, FullTextQueryExecutor.MAX_EXPANDED_SCORING_TERMS);

    // Pass two: every bucket scores against the same corpus statistics, which is what makes the scores globally comparable.
    final BM25ScoringContext scoringContext = createScoringContext(bucketIndexes, documentFrequencies);
    final Map<RID, Float> allResults = new HashMap<>();
    for (int i = 0; i < bucketIndexes.size(); i++)
      mergeCursor(allResults, bucketIndexes.get(i)
          .scoreBM25WithContext(matches.get(i).candidates(), scoringTokens, NO_KEYS, limit, scoringContext));

    return allResults;
  }

  /**
   * Bounds the union of the per-bucket scoring tokens exactly as a single bucket's own cap does: keep the first
   * {@code maxTerms} discovered and drop the rest, so a pathological wildcard expansion cannot grow the scoring pass without
   * limit. Matching already happened, so the result set is unaffected - dropped terms simply stop contributing score. Their
   * document frequencies are dropped too, keeping the context free of statistics it will never consult.
   */
  private static void capScoringTokens(final LinkedHashMap<String, Float> scoringTokens,
      final Map<String, Long> documentFrequencies, final int maxTerms) {
    if (scoringTokens.size() <= maxTerms)
      return;

    final Iterator<Map.Entry<String, Float>> tokens = scoringTokens.entrySet().iterator();
    for (int kept = 0; tokens.hasNext(); kept++) {
      final Map.Entry<String, Float> token = tokens.next();
      if (kept >= maxTerms) {
        documentFrequencies.remove(token.getKey());
        tokens.remove();
      }
    }
  }

  /**
   * Executes the literal-token lookup used by {@link TypeIndex#get(Object[])} while keeping BM25 scores comparable across bucket
   * indexes. Lucene boolean/wildcard syntax is intentionally not interpreted on this path.
   */
  public static IndexCursor searchSimple(final TypeIndex typeIndex, final Object[] keys, final int limit) {
    final List<LSMTreeFullTextIndex> bucketIndexes = getBucketIndexes(typeIndex);
    if (bucketIndexes.isEmpty())
      return new TempIndexCursor(List.of());
    if (!bucketIndexes.getFirst().isBM25())
      throw new IllegalArgumentException("searchSimple requires a BM25 full-text index");

    final int effectiveLimit = limit < 1 ? -1 : limit;
    final Map<String, Float> scoringTokens = bucketIndexes.getFirst().getSimpleQueryTokenBoosts(keys);
    final BM25ScoringContext scoringContext = createScoringContext(bucketIndexes,
        scanDocumentFrequencies(bucketIndexes, scoringTokens));
    final Map<RID, Float> allResults = new HashMap<>();
    for (final LSMTreeFullTextIndex ftIndex : bucketIndexes)
      mergeCursor(allResults,
          ftIndex.scoreBM25WithContext(null, scoringTokens, keys, effectiveLimit, scoringContext));

    return rankedCursor(allResults, keys, effectiveLimit);
  }

  /**
   * Returns a type-wide BM25 scoring explanation for SQL EXPLAIN/PROFILE. Wildcard expansions are discovered across every bucket,
   * but at most 64 terms are scanned for the detailed breakdown.
   */
  public static JSONObject explainScoring(final TypeIndex typeIndex, final String queryText) {
    final List<LSMTreeFullTextIndex> bucketIndexes = getBucketIndexes(typeIndex);
    if (bucketIndexes.isEmpty() || !bucketIndexes.getFirst().isBM25())
      return null;

    final Map<String, Float> allTokens = collectScoringTokens(bucketIndexes, queryText,
        FullTextQueryExecutor.MAX_EXPANDED_SCORING_TERMS);
    final Map<String, Float> shownTokens = firstTokens(allTokens, MAX_EXPLAIN_TERMS);
    final BM25ScoringContext context = createScoringContext(bucketIndexes,
        scanDocumentFrequencies(bucketIndexes, shownTokens));
    final FullTextIndexMetadata metadata = bucketIndexes.getFirst().getFullTextMetadata();

    final JSONObject explain = new JSONObject()
        .put("similarity", FullTextIndexMetadata.SIMILARITY_BM25)
        .put("k1", metadata.getBm25K1())
        .put("b", metadata.getBm25B())
        .put("totalDocs", context.totalDocs())
        .put("avgDocLength", context.avgDocLength())
        .put("bucketCount", bucketIndexes.size())
        .put("corpusScope", "type")
        .put("note", "statistics are type-wide; BM25 scores are comparable across all bucket indexes");

    if (allTokens.size() > shownTokens.size()) {
      explain.put("termsTruncated", true);
      explain.put("termsOmitted", allTokens.size() - shownTokens.size());
      explain.put("termsShown", shownTokens.size());
    }

    final JSONArray terms = new JSONArray();
    for (final Map.Entry<String, Float> token : shownTokens.entrySet()) {
      final long df = context.documentFrequency(token.getKey());
      terms.put(new JSONObject()
          .put("term", token.getKey())
          .put("df", df)
          .put("idf", BM25Scorer.idf(context.totalDocs(), df))
          .put("boost", token.getValue()));
    }
    explain.put("terms", terms);
    return explain;
  }

  private static List<LSMTreeFullTextIndex> getBucketIndexes(final TypeIndex typeIndex) {
    final List<LSMTreeFullTextIndex> indexes = new ArrayList<>();
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex)
        indexes.add(ftIndex);
    return indexes;
  }

  private static Map<String, Float> collectScoringTokens(final List<LSMTreeFullTextIndex> bucketIndexes,
      final String queryText, final int maxTerms) {
    final TreeMap<String, Float> tokens = new TreeMap<>();
    for (final LSMTreeFullTextIndex ftIndex : bucketIndexes)
      for (final Map.Entry<String, Float> token :
          new FullTextQueryExecutor(ftIndex).collectScoringTokens(queryText).entrySet())
        tokens.merge(token.getKey(), token.getValue(), Math::max);

    while (tokens.size() > maxTerms)
      tokens.pollLastEntry();
    return tokens;
  }

  private static Map<String, Float> firstTokens(final Map<String, Float> tokens, final int limit) {
    if (tokens.size() <= limit)
      return tokens;

    final TreeMap<String, Float> first = new TreeMap<>();
    for (final Map.Entry<String, Float> token : tokens.entrySet()) {
      if (first.size() >= limit)
        break;
      first.put(token.getKey(), token.getValue());
    }
    return first;
  }

  private static BM25ScoringContext createScoringContext(final List<LSMTreeFullTextIndex> bucketIndexes,
      final Map<String, Long> documentFrequencies) {
    final LSMTreeFullTextIndex first = bucketIndexes.getFirst();
    first.ensureTypeWideBM25Counters();

    // Both counters are clamped to the neutral values BM25 uses when no statistics are available. A type whose indexed
    // properties analyze to no tokens counts every document but sums a zero length, leaving a zero average that would
    // otherwise be rejected by BM25ScoringContext; length normalization is meaningless for such a corpus anyway.
    return new BM25ScoringContext(Math.max(1L, first.getTypeWideDocumentCount()),
        Math.max(1.0, first.getTypeWideAverageDocumentLength()), documentFrequencies);
  }

  /**
   * Counts type-wide document frequencies with a dedicated posting scan, one scan per token per bucket. Used by the entry
   * points that have no matching pass to fold the counting into: the literal {@link #searchSimple} lookup, whose scoring pass
   * visits only the tokens it was handed, and {@link #explainScoring}.
   */
  private static Map<String, Long> scanDocumentFrequencies(final List<LSMTreeFullTextIndex> bucketIndexes,
      final Map<String, Float> scoringTokens) {
    final Map<String, Long> documentFrequencies = new HashMap<>();
    for (final String token : scoringTokens.keySet()) {
      long df = 0L;
      for (final LSMTreeFullTextIndex ftIndex : bucketIndexes) {
        final IndexCursor postings = ftIndex.getPostings(token);
        while (postings.hasNext()) {
          final Identifiable posting = postings.next();
          if (posting != null && posting.getIdentity().getBucketId() >= 0)
            ++df;
        }
      }
      documentFrequencies.put(token, df);
    }
    return documentFrequencies;
  }

  private static void mergeCursor(final Map<RID, Float> target, final IndexCursor cursor) {
    while (cursor.hasNext()) {
      final Identifiable match = cursor.next();
      if (match != null)
        target.put(canonicalRID(match.getIdentity()), cursor.getFloatScore());
    }
  }

  private static IndexCursor rankedCursor(final Map<RID, Float> scores, final Object[] keys, final int limit) {
    final List<IndexCursorEntry> entries = new ArrayList<>(scores.size());
    for (final Map.Entry<RID, Float> score : scores.entrySet())
      entries.add(new IndexCursorEntry(keys, score.getKey(), score.getValue()));

    entries.sort((left, right) -> {
      final int scoreComparison = Float.compare(right.floatScore, left.floatScore);
      return scoreComparison != 0 ? scoreComparison :
          left.record.getIdentity().compareTo(right.record.getIdentity());
    });
    if (limit > -1 && entries.size() > limit)
      return new TempIndexCursor(entries.subList(0, limit));
    return new TempIndexCursor(entries);
  }

  private static RID canonicalRID(final RID rid) {
    return rid instanceof final FullTextPostingRID posting ?
        new DatabaseRID(posting.getBoundDatabase(), posting) : rid;
  }

  /**
   * Returns the similarity the index scores with: {@link FullTextIndexMetadata#SIMILARITY_BM25} or
   * {@link FullTextIndexMetadata#SIMILARITY_CLASSIC}. Indexes persisted before BM25 support load as CLASSIC.
   */
  public static String getSimilarity(final TypeIndex typeIndex) {
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex)
        return ftIndex.isBM25() ? FullTextIndexMetadata.SIMILARITY_BM25 : FullTextIndexMetadata.SIMILARITY_CLASSIC;

    return FullTextIndexMetadata.SIMILARITY_CLASSIC;
  }

  /**
   * Returns the sorted names of every full-text index in the database.
   */
  public static List<String> listFullTextIndexes(final Database database) {
    final List<String> names = new ArrayList<>();

    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof final TypeIndex typeIndex && typeIndex.getType() == Schema.INDEX_TYPE.FULL_TEXT)
        names.add(typeIndex.getName());

    Collections.sort(names);
    return names;
  }
}
