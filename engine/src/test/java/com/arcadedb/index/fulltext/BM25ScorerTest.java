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

import com.arcadedb.schema.FullTextIndexMetadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for the pure BM25 math in {@link BM25Scorer}. No database is involved so the expected values can be computed by hand.
 */
class BM25ScorerTest {
  private static final double K1 = BM25Scorer.DEFAULT_K1;
  private static final double B  = BM25Scorer.DEFAULT_B;

  @Test
  void idfMatchesRobertsonSparckJonesFormula() {
    // idf = ln((N - df + 0.5)/(df + 0.5) + 1). Use asymmetric df != N/2 cases so the numerator's N - df term is actually
    // exercised (at df = N/2, N - df == df and a formula that dropped N would still pass).
    assertThat(BM25Scorer.idf(100, 1)).isCloseTo(Math.log((99 + 0.5) / (1 + 0.5) + 1.0), within(1e-9));
    assertThat(BM25Scorer.idf(100, 10)).isCloseTo(Math.log((90 + 0.5) / (10 + 0.5) + 1.0), within(1e-9));
    assertThat(BM25Scorer.idf(100, 80)).isCloseTo(Math.log((20 + 0.5) / (80 + 0.5) + 1.0), within(1e-9));
    // df = 0 (term not in the corpus): the highest possible idf, finite and positive.
    assertThat(BM25Scorer.idf(100, 0)).isCloseTo(Math.log((100 + 0.5) / 0.5 + 1.0), within(1e-9));
    assertThat(BM25Scorer.idf(100, 0)).isGreaterThan(BM25Scorer.idf(100, 1));
    // a term in every document (df = N) still yields a non-negative idf thanks to the +1
    assertThat(BM25Scorer.idf(100, 100)).isGreaterThanOrEqualTo(0.0);
  }

  @Test
  void rarerTermHasHigherIdf() {
    final double rare = BM25Scorer.idf(1000, 2);
    final double common = BM25Scorer.idf(1000, 500);
    assertThat(rare).isGreaterThan(common);
  }

  @Test
  void termScoreMatchesHandComputedValue() {
    final double idf = BM25Scorer.idf(1000, 10);
    final int tf = 3;
    final int dl = 90;
    final double avgdl = 120.0;

    final double norm = 1.0 - B + B * (dl / avgdl);
    final double expected = idf * (tf * (K1 + 1.0)) / (tf + K1 * norm);

    assertThat(BM25Scorer.termScore(idf, tf, dl, avgdl, K1, B)).isCloseTo(expected, within(1e-9));
  }

  @Test
  void zeroTermFrequencyContributesNothing() {
    assertThat(BM25Scorer.termScore(2.0, 0, 100, 100, K1, B)).isEqualTo(0.0);
  }

  @Test
  void higherTermFrequencyIncreasesScore() {
    final double idf = BM25Scorer.idf(1000, 10);
    final double s1 = BM25Scorer.termScore(idf, 1, 100, 100, K1, B);
    final double s3 = BM25Scorer.termScore(idf, 3, 100, 100, K1, B);
    assertThat(s3).isGreaterThan(s1);
  }

  @Test
  void termFrequencySaturates() {
    // doubling tf must increase the score by LESS than double (saturation via k1)
    final double idf = BM25Scorer.idf(1000, 10);
    final double s5 = BM25Scorer.termScore(idf, 5, 100, 100, K1, B);
    final double s10 = BM25Scorer.termScore(idf, 10, 100, 100, K1, B);
    assertThat(s10).isLessThan(2 * s5);
  }

  @Test
  void longerDocumentScoresLowerForSameTermFrequency() {
    final double idf = BM25Scorer.idf(1000, 10);
    final double shortDoc = BM25Scorer.termScore(idf, 2, 50, 100, K1, B);
    final double longDoc = BM25Scorer.termScore(idf, 2, 300, 100, K1, B);
    assertThat(shortDoc).isGreaterThan(longDoc);
  }

  @Test
  void noLengthNormalizationWhenBisZero() {
    final double idf = BM25Scorer.idf(1000, 10);
    final double shortDoc = BM25Scorer.termScore(idf, 2, 50, 100, K1, 0.0);
    final double longDoc = BM25Scorer.termScore(idf, 2, 300, 100, K1, 0.0);
    assertThat(shortDoc).isCloseTo(longDoc, within(1e-9));
  }

  @Test
  void defaultConstantsStayInSyncWithMetadata() {
    // FullTextIndexMetadata deliberately duplicates these defaults (it cannot reference BM25Scorer without inverting the package
    // dependency). This assertion makes the "keep in sync" requirement self-enforcing at test time.
    assertThat(FullTextIndexMetadata.DEFAULT_BM25_K1).isEqualTo(BM25Scorer.DEFAULT_K1);
    assertThat(FullTextIndexMetadata.DEFAULT_BM25_B).isEqualTo(BM25Scorer.DEFAULT_B);
  }

  @Test
  void zeroAverageDocLengthIsGuardedAndStaysFinite() {
    // avgdl == 0 (empty/uninitialized corpus) must not divide by zero: the scorer substitutes 1.0 and returns a finite score.
    final double idf = BM25Scorer.idf(1000, 10);
    final double score = BM25Scorer.termScore(idf, 2, 100, 0.0, K1, B);
    assertThat(Double.isNaN(score)).isFalse();
    assertThat(Double.isInfinite(score)).isFalse();
    // It equals the same call with avgdl substituted by 1.0.
    assertThat(score).isCloseTo(BM25Scorer.termScore(idf, 2, 100, 1.0, K1, B), within(1e-12));
  }

  @Test
  void staleDocFrequencyAboveCorpusSizeYieldsSmallNegativeIdfThatOnlyDampens() {
    // df > N can happen transiently with drifted counters / deletion markers. The log argument (N+1)/(df+0.5) stays > 0, so idf
    // is defined; it goes slightly negative rather than NaN/Infinity.
    final double idf = BM25Scorer.idf(10, 20);
    assertThat(Double.isNaN(idf)).isFalse();
    assertThat(Double.isInfinite(idf)).isFalse();
    assertThat(idf).isLessThan(0.0);
    // A rarer term (df <= N) still outranks the over-frequent one, so the negative idf dampens but does not invert ranking.
    final double rareIdf = BM25Scorer.idf(10, 2);
    assertThat(BM25Scorer.termScore(rareIdf, 2, 100, 100, K1, B))
        .isGreaterThan(BM25Scorer.termScore(idf, 2, 100, 100, K1, B));
  }

}
