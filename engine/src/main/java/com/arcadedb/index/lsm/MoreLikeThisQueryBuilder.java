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
 */
package com.arcadedb.index.lsm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Builds More-Like-This queries by selecting top terms from source documents
 * using TF-IDF scoring and configurable filtering thresholds.
 *
 * @author Frank Reale
 */
public class MoreLikeThisQueryBuilder {

  private final MoreLikeThisConfig config;

  /**
   * Creates a new query builder with the specified configuration.
   *
   * @param config the MLT configuration parameters
   */
  public MoreLikeThisQueryBuilder(final MoreLikeThisConfig config) {
    this.config = config;
  }

  /**
   * Selects the top terms from source documents based on TF-IDF scores
   * and configured filtering thresholds.
   *
   * @param termFreqs  term frequencies from source documents
   * @param docFreqs   document frequencies for each term across the index
   * @param totalDocs  total number of documents in the index
   * @return list of selected terms, ordered by score if boostByScore is enabled
   */
  public List<String> selectTopTerms(
      final Map<String, Integer> termFreqs,
      final Map<String, Integer> docFreqs,
      final int totalDocs) {

    final List<TermScore> candidates = new ArrayList<>();

    // Filter and score each term
    for (final Map.Entry<String, Integer> entry : termFreqs.entrySet()) {
      final String term = entry.getKey();
      final int termFreq = entry.getValue();

      // Apply filtering thresholds
      if (!passesFilters(term, termFreq, docFreqs, totalDocs)) {
        continue;
      }

      // Calculate TF-IDF score if enabled
      final double score = config.isBoostByScore()
          ? calculateTfIdfScore(termFreq, docFreqs.get(term), totalDocs)
          : 1.0;

      candidates.add(new TermScore(term, score));
    }

    // Sort by score descending if boosting is enabled
    if (config.isBoostByScore()) {
      candidates.sort(Comparator.comparingDouble(TermScore::score).reversed());
    }

    // Limit to maxQueryTerms
    final int limit = Math.min(candidates.size(), config.getMaxQueryTerms());
    final List<String> result = new ArrayList<>(limit);
    for (int i = 0; i < limit; i++) {
      result.add(candidates.get(i).term());
    }

    return result;
  }

  /**
   * Applies all configured filters to determine if a term should be included.
   */
  private boolean passesFilters(
      final String term,
      final int termFreq,
      final Map<String, Integer> docFreqs,
      final int totalDocs) {

    // Filter by minimum term frequency
    if (termFreq < config.getMinTermFreq()) {
      return false;
    }

    // Filter by word length
    final int wordLen = term.length();
    final int minWordLen = config.getMinWordLen();
    final int maxWordLen = config.getMaxWordLen();

    if (minWordLen > 0 && wordLen < minWordLen) {
      return false;
    }

    if (maxWordLen > 0 && wordLen > maxWordLen) {
      return false;
    }

    // Filter by document frequency
    final Integer docFreq = docFreqs.get(term);
    if (docFreq == null) {
      return false;
    }

    if (docFreq < config.getMinDocFreq()) {
      return false;
    }

    // Filter by maximum document frequency percentage
    final Float maxDocFreqPercent = config.getMaxDocFreqPercent();
    if (maxDocFreqPercent != null) {
      final double docFreqPercent = (docFreq * 100.0) / totalDocs;
      if (docFreqPercent > maxDocFreqPercent) {
        return false;
      }
    }

    return true;
  }

  /**
   * Calculates TF-IDF score for a term.
   *
   * @param termFreq   term frequency in source documents
   * @param docFreq    document frequency across the index
   * @param totalDocs  total number of documents in the index
   * @return TF-IDF score
   */
  private double calculateTfIdfScore(final int termFreq, final int docFreq, final int totalDocs) {
    // TF-IDF = termFreq * log(totalDocs / docFreq)
    return termFreq * Math.log10((double) totalDocs / docFreq);
  }

  /**
   * Internal record for tracking terms with their scores.
   */
  private record TermScore(String term, double score) {
  }
}
