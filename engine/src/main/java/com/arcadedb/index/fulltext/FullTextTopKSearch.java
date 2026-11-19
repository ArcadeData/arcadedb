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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndexPostings;
import com.arcadedb.query.sql.executor.RIDFilter;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.FullTextIndexMetadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * BM25 search of a query that is a disjunction of terms, from primitive posting arrays, with MaxScore pruning against one top-k
 * threshold shared by every bucket (issue #12458).
 * <p>
 * The general path ({@link FullTextQueryExecutor}, {@link FullTextSearch#search}) matches first and scores after: it reads every
 * posting of every query term twice, once into a candidate set and once to score the candidates, with a hash-map entry per
 * candidate each time, and each bucket keeps its own top k. A disjunction of terms needs none of that, since a document matches
 * exactly when it holds one of the tokens and scores the sum of the contributions of the tokens it holds:
 * <ol>
 *   <li>each token's postings are read once per bucket, decoded straight into arrays sorted by RID
 *   ({@link com.arcadedb.index.lsm.LSMTreeIndex#getPostings}). Their sizes are the document frequencies, summed over the buckets
 *   before any bucket is scored, exactly as the general path counts them;</li>
 *   <li>each posting's contribution is computed with the same expression the general path uses ({@link BM25Scorer#termScore} times
 *   the token's boost), and a document's score sums its contributions in the same token order, so the scores are the same to the
 *   last bit;</li>
 *   <li>the buckets are walked with MaxScore (see {@link Scorer}): the lists are ordered by the largest contribution they hold, and
 *   the lists whose maxima together cannot reach the current k-th score are not walked, only probed for the documents the other
 *   lists bring. The k-th score is the one of a single heap the buckets share, so a bucket starts from the threshold the buckets
 *   before it reached.</li>
 * </ol>
 * The postings are still all read: the document frequencies need every one of them, and the index keeps a token's postings in
 * the order they were written, not in RID order with the bounds a reader could skip blocks by. What MaxScore saves is the scoring,
 * which used to be the hash maps' work on every match.
 * The answer is the best k matches by score and then RID, which is what the general path's union of per-bucket best k holds at
 * its top; without a limit it is every match with its score.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class FullTextTopKSearch {
  /**
   * Relative slack added to an upper bound before it is compared with the threshold: the bound and the exact score sum the same
   * contributions in different orders, so the computed bound may round a few ulps below a score it bounds.
   */
  private static final double BOUND_SLACK = 1e-9;

  private FullTextTopKSearch() {
  }

  /**
   * The best {@code limit} matches of the query with their scores, or every match without a limit ({@code limit < 1}); null when
   * the query is not a disjunction of terms or the filter has a predicate on the records, for the caller to take the general path.
   */
  static Map<RID, Float> search(final List<LSMTreeFullTextIndex> bucketIndexes, final String queryText, final int limit,
      final WorkGuard guard, final List<String> restrictedFields, final RIDFilter ridFilter) {
    return search(bucketIndexes, queryText, limit, guard, restrictedFields, ridFilter, null);
  }

  /**
   * {@link #search(List, String, int, WorkGuard, List, RIDFilter)} counting, in the first cell of {@code exactScores} when it is not
   * null, the documents whose exact score was computed: MaxScore keeps them far below the matches, which a test asserts.
   */
  static Map<RID, Float> search(final List<LSMTreeFullTextIndex> bucketIndexes, final String queryText, final int limit,
      final WorkGuard guard, final List<String> restrictedFields, final RIDFilter ridFilter, final long[] exactScores) {
    // a predicate on the records decides some buckets only once their records are loaded, which no limit here can wait for
    if (ridFilter != null && ridFilter.hasRecordPredicate())
      return null;
    final LSMTreeFullTextIndex first = bucketIndexes.getFirst();
    if (!first.isBM25())
      return null;

    final LinkedHashMap<String, Float> tokens = new FullTextQueryExecutor(first, guard, restrictedFields, null).termDisjunction(
        queryText);
    if (tokens == null)
      return null;

    final int tokenCount = tokens.size();
    final String[] tokenKeys = tokens.keySet().toArray(new String[0]);

    // Pass one: every posting of every token in every bucket, read once. A bucket the filter rejects is read too: its share of the
    // document frequencies is what keeps the scores of the others those of the unfiltered search
    final LSMTreeIndexPostings[][] postings = new LSMTreeIndexPostings[bucketIndexes.size()][tokenCount];
    final long[] documentFrequencies = new long[tokenCount];
    for (int b = 0; b < bucketIndexes.size(); ++b) {
      final LSMTreeFullTextIndex ftIndex = bucketIndexes.get(b);
      final int bucketId = ftIndex.getAssociatedBucketId();
      for (int t = 0; t < tokenCount; ++t) {
        guard.check();
        final LSMTreeIndexPostings read = ftIndex.readPostings(tokenKeys[t]);
        // the walk orders a bucket's documents by position alone: postings of another bucket (sorted first by bucket, so they show
        // at an end) are left to the general path
        if (read.size() > 0 && (read.bucketIds()[0] != bucketId || read.bucketIds()[read.size() - 1] != bucketId))
          return null;
        postings[b][t] = read;
        documentFrequencies[t] += read.size();
      }
    }

    final Map<String, Long> frequencies = new HashMap<>(tokenCount);
    for (int t = 0; t < tokenCount; ++t)
      if (documentFrequencies[t] > 0)
        frequencies.put(tokenKeys[t], documentFrequencies[t]);
    // One bucket is searched with its own statistics, as the general path searches it; several share the type-wide ones
    final BM25ScoringContext context = bucketIndexes.size() == 1 ?
        first.bucketScoringContext(frequencies) :
        FullTextSearch.createScoringContext(bucketIndexes, frequencies);

    final double[] idf = new double[tokenCount];
    final double[] boosts = new double[tokenCount];
    for (int t = 0; t < tokenCount; ++t) {
      idf[t] = BM25Scorer.idf(context.totalDocs(), documentFrequencies[t]);
      boosts[t] = tokens.get(tokenKeys[t]);
    }

    // Pass two: every bucket against the one heap
    final TopK top = new TopK(limit, first.getDatabase());
    final FullTextIndexMetadata metadata = first.getFullTextMetadata();
    final Scorer scorer = new Scorer(top, guard, ridFilter, idf, boosts, context.avgDocLength(), metadata.getBm25K1(),
        metadata.getBm25B());
    for (int bucket = 0; bucket < bucketIndexes.size(); ++bucket) {
      final LSMTreeFullTextIndex ftIndex = bucketIndexes.get(bucket);
      final int bucketId = ftIndex.getAssociatedBucketId();
      if (ridFilter != null && !ridFilter.acceptsBucket(bucketId))
        continue;
      scorer.scoreBucket(ftIndex, bucketId, postings[bucket]);
    }
    if (exactScores != null)
      exactScores[0] += scorer.exactScores;
    return top.toMap();
  }

  /**
   * MaxScore over one bucket at a time, in windows of positions as Lucene's {@code MaxScoreBulkScorer} walks doc ids. The lists
   * whose maxima together still reach the threshold (the essential ones) are accumulated into a score per position of the window,
   * one pass over their postings; each document they bring is then completed from the other lists, probed by galloping and
   * abandoned as soon as what they can still add cannot reach the threshold. A document that may enter the heap is scored
   * exactly, by looking each token up in token order, which is the order the general path sums in.
   */
  private static final class Scorer {
    /** Positions per window: the window's arrays stay in the first cache levels. */
    private static final int WINDOW = 4096;

    private final int       tokenCount;
    private final TopK      top;
    private final WorkGuard guard;
    private final RIDFilter ridFilter;
    // the BM25 inputs of the whole search, the same for every bucket
    private final double[]  idf;
    private final double[]  boosts;
    private final double    avgdl;
    private final double    k1;
    private final double    b;
    // the lists of the bucket being scored: per token its list or -1, per list its postings
    private       int[]     listOfToken;
    private       long[][]  positions;
    private       int[][]   tfs;
    private       int[][]   docLengths;
    private       int[]     sizes;
    // the window: the partial score of each position, and which positions an essential list holds
    private final double[]  windowScores        = new double[WINDOW];
    private final long[]    windowDocuments     = new long[WINDOW >>> 6];
    // per term frequency below its length, the shortest document of a list holding the term that often
    private final int[]     shortestByFrequency = new int[64];
    private       int       ticks;
    private       long      exactScores;

    private Scorer(final TopK top, final WorkGuard guard, final RIDFilter ridFilter, final double[] idf, final double[] boosts,
        final double avgdl, final double k1, final double b) {
      this.tokenCount = idf.length;
      this.top = top;
      this.guard = guard;
      this.ridFilter = ridFilter;
      this.idf = idf;
      this.boosts = boosts;
      this.avgdl = avgdl;
      this.k1 = k1;
      this.b = b;
    }

    private void scoreBucket(final LSMTreeFullTextIndex ftIndex, final int bucketId, final LSMTreeIndexPostings[] postings) {
      // The lists of this bucket that hold a posting, and the largest contribution of each
      int lists = 0;
      for (final LSMTreeIndexPostings p : postings)
        if (p.size() > 0)
          ++lists;
      if (lists == 0)
        return;

      final int[] token = new int[lists];
      listOfToken = new int[tokenCount];
      positions = new long[lists][];
      tfs = new int[lists][];
      docLengths = new int[lists][];
      sizes = new int[lists];
      final double[] maxima = new double[lists];
      int l = 0;
      for (int t = 0; t < tokenCount; ++t) {
        final LSMTreeIndexPostings p = postings[t];
        if (p.size() == 0) {
          listOfToken[t] = -1;
          continue;
        }
        listOfToken[t] = l;
        token[l] = t;
        positions[l] = p.positions();
        tfs[l] = p.termFrequencies();
        docLengths[l] = p.documentLengths();
        sizes[l] = p.size();
        maxima[l] = largestContribution(l, t);
        ++l;
      }

      // Ascending by maximum: the lists that cannot reach the threshold together are a prefix of this order
      final int[] order = new int[lists];
      for (int i = 0; i < lists; ++i)
        order[i] = i;
      for (int i = 1; i < lists; ++i) {
        final int current = order[i];
        int j = i - 1;
        while (j >= 0 && maxima[order[j]] > maxima[current]) {
          order[j + 1] = order[j];
          --j;
        }
        order[j + 1] = current;
      }
      // boundBelow[i] bounds what the lists order[0..i-1] can add to a document
      final double[] boundBelow = new double[lists + 1];
      for (int i = 0; i < lists; ++i)
        boundBelow[i + 1] = boundBelow[i] + Math.max(0.0, maxima[order[i]]);

      // the place of each list in that order: a list is walked when its place is not below the count of probed lists
      final int[] rank = new int[lists];
      for (int i = 0; i < lists; ++i)
        rank[order[i]] = i;

      final int[] cursors = new int[lists];
      int nonEssential = nonEssentialLists(boundBelow, lists);

      while (true) {
        // the window starts at the next document of the essential lists: one only the other lists hold cannot reach the threshold
        long start = Long.MAX_VALUE;
        for (int i = nonEssential; i < lists; ++i) {
          final int list = order[i];
          if (cursors[list] < sizes[list] && positions[list][cursors[list]] < start)
            start = positions[list][cursors[list]];
        }
        if (start == Long.MAX_VALUE)
          break;
        final long end = start + WINDOW;

        // The walked lists in token order (lists are numbered in token order), so a position's sum adds the contributions in the
        // order the general path adds them: with no probed list, which is always the case without a limit, it is the exact score
        for (int list = 0; list < lists; ++list) {
          if (rank[list] < nonEssential)
            continue;
          final int t = token[list];
          final long[] listPositions = positions[list];
          final int size = sizes[list];
          int cursor = cursors[list];
          for (; cursor < size && listPositions[cursor] < end; ++cursor) {
            guard.checkPeriodically(++ticks);
            final int slot = (int) (listPositions[cursor] - start);
            final double contribution = contribution(t, tfs[list][cursor], docLengths[list][cursor]);
            final int word = slot >>> 6;
            final long bit = 1L << slot;
            if ((windowDocuments[word] & bit) == 0) {
              windowDocuments[word] |= bit;
              // 0.0 + c rather than c, as the general path starts each document's sum from 0.0: the two differ for -0.0 only
              windowScores[slot] = 0.0 + contribution;
            } else
              windowScores[slot] += contribution;
          }
          cursors[list] = cursor;
        }

        // the window's documents in position order, so the probed lists are walked forward only
        for (int word = 0; word < windowDocuments.length; ++word) {
          long bits = windowDocuments[word];
          if (bits == 0)
            continue;
          windowDocuments[word] = 0;
          while (bits != 0) {
            final int slot = (word << 6) + Long.numberOfTrailingZeros(bits);
            bits &= bits - 1;
            final long document = start + slot;

            double partial = windowScores[slot];
            boolean reachable = top.mayEnter(partial + boundBelow[nonEssential]);
            for (int i = nonEssential - 1; reachable && i >= 0; --i) {
              final int list = order[i];
              final int cursor = advance(positions[list], cursors[list], sizes[list], document);
              cursors[list] = cursor;
              if (cursor < sizes[list] && positions[list][cursor] == document)
                partial += contribution(token[list], tfs[list][cursor], docLengths[list][cursor]);
              reachable = top.mayEnter(partial + boundBelow[i]);
            }
            if (!reachable)
              continue;

            // probed lists add their contributions out of token order: such a document is scored again, in order
            final float score = (float) (nonEssential == 0 ? partial : exactScore(document));
            if (!top.beats(score, bucketId, document))
              continue;
            if (ridFilter != null && !ridFilter.accepts(new DatabaseRID(ftIndex.getDatabase(), bucketId, document)))
              continue;
            top.add(score, bucketId, document);
          }
        }

        // a higher threshold turns more lists into probed ones; the window just walked used the lists it started with
        nonEssential = nonEssentialLists(boundBelow, lists);
      }
    }

    /**
     * The score of a document as the general path computes it: the contributions of the tokens it holds, summed in token order.
     * Only for the few documents that probed lists completed and that may still enter the heap: it looks every token up.
     */
    private double exactScore(final long document) {
      ++exactScores;
      double score = 0.0;
      for (int t = 0; t < tokenCount; ++t) {
        final int list = listOfToken[t];
        if (list < 0)
          continue;
        final int i = Arrays.binarySearch(positions[list], 0, sizes[list], document);
        if (i >= 0)
          score += contribution(t, tfs[list][i], docLengths[list][i]);
      }
      return score;
    }

    /** The expression of {@code LSMTreeFullTextIndex.computeBM25Scores}, so that a score is the general path's to the last bit. */
    private double contribution(final int t, final int tf, final int docLength) {
      return BM25Scorer.termScore(idf[t], tf, docLength, avgdl, k1, b) * boosts[t];
    }

    /**
     * The largest contribution a posting of the list makes. A contribution grows with the term frequency and shrinks with the
     * document length, so it is the largest of those of each frequency's shortest document: one integer pass over the postings,
     * and a contribution per distinct frequency rather than per posting.
     */
    private double largestContribution(final int list, final int t) {
      final int[] listTfs = tfs[list];
      final int[] listDocLengths = docLengths[list];
      final int size = sizes[list];
      final double tokenIdf = idf[t];
      final double boost = boosts[t];
      double max = Double.NEGATIVE_INFINITY;
      if (tokenIdf < 0 || boost < 0 || b < 0 || b > 1 || k1 < 0) {
        // the monotony does not hold: every posting
        for (int i = 0; i < size; ++i) {
          guard.checkPeriodically(++ticks);
          max = Math.max(max, contribution(t, listTfs[i], listDocLengths[i]));
        }
        return max;
      }
      Arrays.fill(shortestByFrequency, Integer.MAX_VALUE);
      for (int i = 0; i < size; ++i) {
        guard.checkPeriodically(++ticks);
        final int tf = listTfs[i];
        if (tf < shortestByFrequency.length) {
          if (listDocLengths[i] < shortestByFrequency[tf])
            shortestByFrequency[tf] = listDocLengths[i];
        } else
          max = Math.max(max, contribution(t, tf, listDocLengths[i]));
      }
      for (int tf = 0; tf < shortestByFrequency.length; ++tf)
        if (shortestByFrequency[tf] != Integer.MAX_VALUE)
          max = Math.max(max, contribution(t, tf, shortestByFrequency[tf]));
      return max;
    }

    /** How many of the lists, in ascending order of maximum, cannot reach the threshold together. */
    private int nonEssentialLists(final double[] boundBelow, final int lists) {
      int count = 0;
      while (count < lists && !top.mayEnter(boundBelow[count + 1]))
        ++count;
      return count;
    }

    /** The first index at or after {@code from} whose position is at least {@code target}, by galloping then bisecting. */
    private static int advance(final long[] positions, final int from, final int size, final long target) {
      if (from >= size || positions[from] >= target)
        return from;
      int step = 1;
      int low = from;
      int high = from + 1;
      while (high < size && positions[high] < target) {
        low = high;
        step <<= 1;
        high = from + step;
      }
      if (high > size)
        high = size;
      // positions[low] < target, and positions[high] >= target or high == size
      while (low + 1 < high) {
        final int mid = (low + high) >>> 1;
        if (positions[mid] < target)
          low = mid;
        else
          high = mid;
      }
      return high;
    }
  }

  /**
   * The best matches by score and then RID, in primitive arrays: a binary heap whose root is the worst match kept. Without a limit
   * it keeps every match and never orders them.
   */
  private static final class TopK {
    private static final int INITIAL_CAPACITY = 64;

    private final int                    limit;
    private       float[]                scores;
    private       int[]                  bucketIds;
    private       long[]                 positions;
    private final DatabaseInternal       database;
    private       int                    size;

    private TopK(final int limit, final DatabaseInternal database) {
      this.limit = limit;
      this.database = database;
      // a limit is what a caller asks, not what the matches hold: the arrays grow towards it as the matches come
      final int capacity = limit > 0 ? Math.min(limit, INITIAL_CAPACITY) : INITIAL_CAPACITY;
      scores = new float[capacity];
      bucketIds = new int[capacity];
      positions = new long[capacity];
    }

    private boolean full() {
      return limit > 0 && size == limit;
    }

    /** Whether a match scoring at most {@code bound} could still enter: it ties the threshold at best and wins on its RID. */
    private boolean mayEnter(final double bound) {
      if (!full())
        return true;
      return (float) (bound + Math.abs(bound) * BOUND_SLACK) >= scores[0];
    }

    private boolean beats(final float score, final int bucketId, final long position) {
      if (!full())
        return true;
      return worse(scores[0], bucketIds[0], positions[0], score, bucketId, position);
    }

    private void add(final float score, final int bucketId, final long position) {
      if (limit < 1) {
        if (size == scores.length)
          grow();
        set(size++, score, bucketId, position);
        return;
      }
      if (size < limit) {
        if (size == scores.length)
          grow();
        set(size, score, bucketId, position);
        siftUp(size++);
        return;
      }
      set(0, score, bucketId, position);
      siftDown(0);
    }

    /** Whether the first match ranks below the second: a lower score, or the same score and a higher RID. */
    private static boolean worse(final float scoreA, final int bucketA, final long positionA, final float scoreB, final int bucketB,
        final long positionB) {
      final int cmp = Float.compare(scoreA, scoreB);
      if (cmp != 0)
        return cmp < 0;
      final int bucketCmp = Integer.compare(bucketA, bucketB);
      if (bucketCmp != 0)
        return bucketCmp > 0;
      return positionA > positionB;
    }

    private boolean worse(final int i, final int j) {
      return worse(scores[i], bucketIds[i], positions[i], scores[j], bucketIds[j], positions[j]);
    }

    private void siftUp(int i) {
      while (i > 0) {
        final int parent = (i - 1) >>> 1;
        if (!worse(i, parent))
          break;
        swap(i, parent);
        i = parent;
      }
    }

    private void siftDown(int i) {
      while (true) {
        final int left = 2 * i + 1;
        if (left >= size)
          break;
        final int right = left + 1;
        final int worst = right < size && worse(right, left) ? right : left;
        if (!worse(worst, i))
          break;
        swap(i, worst);
        i = worst;
      }
    }

    private void set(final int i, final float score, final int bucketId, final long position) {
      scores[i] = score;
      bucketIds[i] = bucketId;
      positions[i] = position;
    }

    private void swap(final int i, final int j) {
      final float s = scores[i];
      scores[i] = scores[j];
      scores[j] = s;
      final int b = bucketIds[i];
      bucketIds[i] = bucketIds[j];
      bucketIds[j] = b;
      final long p = positions[i];
      positions[i] = positions[j];
      positions[j] = p;
    }

    private void grow() {
      final int capacity = limit > 0 ? (int) Math.min(limit, (long) scores.length << 1) : scores.length << 1;
      scores = Arrays.copyOf(scores, capacity);
      bucketIds = Arrays.copyOf(bucketIds, capacity);
      positions = Arrays.copyOf(positions, capacity);
    }

    private Map<RID, Float> toMap() {
      final Map<RID, Float> result = new HashMap<>(Math.max(16, (int) (size / 0.75f) + 1));
      for (int i = 0; i < size; ++i)
        result.put(new DatabaseRID(database, bucketIds[i], positions[i]), scores[i]);
      return result;
    }
  }
}
