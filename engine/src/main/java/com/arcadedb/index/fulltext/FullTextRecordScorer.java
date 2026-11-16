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
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.FullTextPostingRID;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.FullTextIndexMetadata;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The BM25 search of a full-text index answered for one record at a time, from the record itself (issue #11683): its indexed
 * values go through the postings the index writes for them ({@link LSMTreeFullTextIndex#forEachBulkEntry}), so with the analyzer
 * of the index, the term frequencies and the document length a posting stores, and the query is decided on the keys of those
 * postings as {@link FullTextQueryExecutor} decides it on the index. The score is the one {@link FullTextSearch#search} gives the
 * record: the same statistics (the type-wide or the bucket's document count, average length and each token's document frequency),
 * the same formula, and the same sum, over the same tokens in the same order, narrowed to a float at the end.
 * <p>
 * Its cost is the document frequencies, counted once on the postings without collecting them, and then the analysis of each
 * record asked about, rather than every match of the query collected, scored and ordered.
 */
public final class FullTextRecordScorer {
  /** {@link #score} of a record the search does not match. */
  public static final float NO_MATCH  = Float.NEGATIVE_INFINITY;
  /** {@link #score} of a record whose postings cannot be told from its values alone: the index has to answer for it. */
  public static final float UNDECIDED = Float.NaN;

  private final Map<Integer, LSMTreeFullTextIndex>    bucketIndexes;
  private final FullTextQueryExecutor.RecordMatcher matcher;
  private final Map<String, Float>                    scoringTokens;
  private final BM25ScoringContext                    statistics;
  private final long                                  documentFrequencies;

  private FullTextRecordScorer(final Map<Integer, LSMTreeFullTextIndex> bucketIndexes,
      final FullTextQueryExecutor.RecordMatcher matcher, final Map<String, Float> scoringTokens,
      final BM25ScoringContext statistics) {
    this.bucketIndexes = bucketIndexes;
    this.matcher = matcher;
    this.scoringTokens = scoringTokens;
    this.statistics = statistics;
    long sum = 0L;
    if (statistics != null)
      for (final String token : scoringTokens.keySet())
        sum += statistics.documentFrequency(token);
    this.documentFrequencies = sum;
  }

  /**
   * The search {@link FullTextSearch#search(TypeIndex, String, int, WorkGuard, List, com.arcadedb.query.sql.executor.RIDFilter)}
   * runs without a limit or a filter, prepared to answer one record at a time, or null when that answer could differ from the
   * search's for some record: a CLASSIC index, a query with a clause the record alone does not decide (see
   * {@link FullTextQueryExecutor#recordQuery}), a property indexed by item, key or value (an index built before issue #10209 kept
   * per-item statistics in the postings of a record not written since), an index still being built, or one this transaction has
   * pending changes for. Counts the document frequency of every scoring token.
   */
  public static FullTextRecordScorer prepare(final TypeIndex typeIndex, final String queryText, final List<String> restrictedFields,
      final WorkGuard guard) {
    final List<LSMTreeFullTextIndex> indexes = FullTextSearch.getBucketIndexes(typeIndex);
    if (indexes.isEmpty())
      // the search answers nothing
      return new FullTextRecordScorer(Map.of(), (keys, excluded) -> false, Map.of(), null);

    final LSMTreeFullTextIndex first = indexes.getFirst();
    if (!first.isBM25() || first.getFullTextMetadata() == null)
      return null;
    for (final String property : first.getPropertyNames())
      if (!Index.basePropertyName(property).equals(property))
        return null;

    final Map<Integer, LSMTreeFullTextIndex> byBucket = new HashMap<>();
    for (final LSMTreeFullTextIndex index : indexes) {
      if (!index.isValid() || index.isAwaitingFill() || hasPendingChanges(index))
        return null;
      byBucket.put(index.getAssociatedBucketId(), index);
    }

    final FullTextQueryExecutor.RecordQuery query = new FullTextQueryExecutor(first, guard, restrictedFields, null).recordQuery(
        queryText);
    if (query == null)
      return null;

    if (indexes.size() == 1) {
      // FullTextSearch.search scores one bucket with its own statistics, summing the tokens in the order of its executor's map
      final Map<String, Float> tokens = query.scoringTokens();
      return new FullTextRecordScorer(byBucket, query.matcher(), tokens,
          first.bucketScoringContext(FullTextSearch.scanDocumentFrequencies(indexes, tokens, guard)));
    }

    // FullTextSearch.searchBM25 merges the immutable copy of each bucket's tokens in that copy's order, then caps them. The tokens
    // of a query decided on records do not depend on what a bucket holds, so every bucket brings the same copy, and the first one
    // gives the order
    final LinkedHashMap<String, Float> tokens = new LinkedHashMap<>();
    for (final Map.Entry<String, Float> token : Map.copyOf(query.scoringTokens()).entrySet())
      tokens.merge(token.getKey(), token.getValue(), Math::max);
    final Map<String, Long> documentFrequencies = FullTextSearch.scanDocumentFrequencies(indexes, tokens, guard);
    FullTextSearch.capScoringTokens(tokens, documentFrequencies, FullTextQueryExecutor.MAX_EXPANDED_SCORING_TERMS);
    return new FullTextRecordScorer(byBucket, query.matcher(), tokens,
        FullTextSearch.createScoringContext(indexes, documentFrequencies));
  }

  /**
   * A transaction's own changes to the index are part of what the index answers inside it, and the counters leave them out: a
   * record whose indexed values changed in it is left to the index.
   */
  private static boolean hasPendingChanges(final LSMTreeFullTextIndex index) {
    final DatabaseInternal database = index.getDatabase();
    if (!database.isTransactionActive())
      return false;
    final TransactionContext transaction = database.getTransaction();
    return transaction.getIndexChanges().getTotalEntriesByIndex(index.getName()) > 0;
  }

  /** The sum of the document frequencies of the scoring tokens: how many postings the search itself reads at least twice. */
  public long getDocumentFrequencies() {
    return documentFrequencies;
  }

  /** Whether the search reads the bucket of {@code rid}: a record of any other bucket is not among its matches. */
  public boolean covers(final RID rid) {
    return bucketIndexes.containsKey(rid.getBucketId());
  }

  /**
   * The score the search gives {@code record}, {@link #NO_MATCH} when it does not match, or {@link #UNDECIDED}. The record has to
   * be of a bucket the search {@link #covers}, and hold the values its postings were written from.
   *
   * @param analyzed its first cell is increased by the length of the record and the number of its postings
   */
  public float score(final Document record, final long[] analyzed) {
    final RID rid = record.getIdentity();
    final LSMTreeFullTextIndex index = bucketIndexes.get(rid.getBucketId());
    final DatabaseInternal database = index.getDatabase();

    // The postings the index holds for the record, keyed by their stored key
    final Map<String, FullTextPostingRID> postings = new HashMap<>();
    final boolean[] undecided = new boolean[1];
    database.getIndexer().forEachIndexKey(index, record, keys -> index.forEachBulkEntry(keys, rid, (key, value) -> {
      if (key[0] == null)
        // the posting of a null value, which no query reaches
        return;
      // Two postings under one key (a field-prefixed token equal to an unprefixed one) are each summed by the search, in the order
      // the index returns them
      if (!(value instanceof final FullTextPostingRID posting) || postings.put(key[0].toString(), posting) != null)
        undecided[0] = true;
    }));
    // every posting of a record carries the record's length
    analyzed[0] += postings.size() + (postings.isEmpty() ? 0 : postings.values().iterator().next().getDocLength());
    if (undecided[0])
      return UNDECIDED;

    final boolean[] excluded = new boolean[1];
    if (!matcher.matches(postings.keySet(), excluded) || excluded[0])
      return NO_MATCH;

    // computeBM25Scores, for one record: the same operands in the same order
    final FullTextIndexMetadata metadata = index.getFullTextMetadata();
    final double k1 = metadata.getBm25K1();
    final double b = metadata.getBm25B();
    final long totalDocs = statistics.totalDocs();
    final double avgdl = statistics.avgDocLength();
    double score = 0.0;
    for (final Map.Entry<String, Float> token : scoringTokens.entrySet()) {
      final FullTextPostingRID posting = postings.get(token.getKey());
      if (posting == null)
        continue;
      final long df = statistics.documentFrequency(token.getKey());
      if (df == 0L)
        continue;
      final float boost = token.getValue();
      score += BM25Scorer.termScore(BM25Scorer.idf(totalDocs, df), posting.getTf(), posting.getDocLength(), avgdl, k1, b) * boost;
    }
    return (float) score;
  }
}
