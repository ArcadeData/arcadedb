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

/**
 * Pure (database-free) implementation of the Okapi BM25 ranking function used by the full-text index when the index is configured
 * with {@code similarity = BM25}. Keeping the math isolated here makes it trivially unit-testable against hand-computed values and
 * keeps {@link LSMTreeFullTextIndex} lean.
 * <p>
 * The IDF formula matches the one used by the sparse-vector index ({@code LSMSparseVectorIndex.idf}) for consistency across the
 * two scoring engines:
 *
 * <pre>
 * idf(N, df)                      = ln( (N - df + 0.5) / (df + 0.5) + 1 )
 * termScore(idf, tf, dl, avgdl)   = idf * ( tf * (k1 + 1) ) / ( tf + k1 * (1 - b + b * dl / avgdl) )
 * </pre>
 *
 * The {@code +1} inside the log keeps the IDF non-negative even for terms appearing in more than half of the collection, which is
 * the Lucene-compatible variant of the Robertson-Sparck-Jones weight.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class BM25Scorer {
  /**
   * Default term-frequency saturation parameter. Higher values make additional occurrences of a term count for more.
   */
  public static final float DEFAULT_K1 = 1.2f;

  /**
   * Default document-length normalization parameter in the range [0,1]. 0 disables length normalization, 1 applies it fully.
   */
  public static final float DEFAULT_B = 0.75f;

  private BM25Scorer() {
    // STATIC-ONLY HELPER
  }

  /**
   * Computes the inverse document frequency for a term.
   * <p>
   * The log argument is {@code (N - df + 0.5)/(df + 0.5) + 1 = (N + 1)/(df + 0.5)}, which is strictly positive for any
   * {@code N >= 0, df >= 0}, so {@link Math#log} is always defined (never {@code NaN}/{@code -Infinity}). IDF is {@code >= 0}
   * whenever {@code df <= N}, which always holds in normal operation (df is counted at the same scope as N). A transiently stale
   * {@code df > N} (e.g. drifted counters or deletion markers not yet compacted) yields a small negative IDF, which merely demotes
   * an over-common term - harmless, and self-corrected once counters are recomputed.
   *
   * @param totalDocs total number of documents in the collection (N)
   * @param docFreq   number of documents containing the term (df)
   *
   * @return the IDF weight ({@code >= 0} when {@code df <= N}; slightly negative only for a stale {@code df > N})
   */
  public static double idf(final long totalDocs, final long docFreq) {
    final double numerator = (double) (totalDocs - docFreq) + 0.5;
    final double denominator = (double) docFreq + 0.5;
    return Math.log((numerator / denominator) + 1.0);
  }

  /**
   * Computes the BM25 contribution of a single term to a document's score.
   *
   * @param idf    inverse document frequency of the term (see {@link #idf(long, long)})
   * @param tf     term frequency: number of occurrences of the term in the document
   * @param docLen length of the document (number of analyzed tokens)
   * @param avgdl  average document length across the collection (must be > 0)
   * @param k1     term-frequency saturation parameter
   * @param b      document-length normalization parameter
   *
   * @return the BM25 score contribution of this term
   */
  public static double termScore(final double idf, final int tf, final int docLen, final double avgdl, final double k1,
      final double b) {
    // tf <= 0 contributes nothing. This also defends the (architecturally impossible) case of a non-posting RID reaching here:
    // compacted root-page pointers deserialize as FullTextPostingRID(tf=0, docLength=0), and although they never reach scoring
    // (only leaf postings are returned for a token lookup), their tf=0 would zero out any contribution anyway.
    if (tf <= 0)
      return 0.0;
    final double safeAvgdl = avgdl > 0 ? avgdl : 1.0;
    final double norm = 1.0 - b + b * (docLen / safeAvgdl);
    return idf * (tf * (k1 + 1.0)) / (tf + k1 * norm);
  }
}
