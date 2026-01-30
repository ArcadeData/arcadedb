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
package com.arcadedb.index.fulltext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test suite for MoreLikeThisQueryBuilder.
 *
 * @author Frank Reale
 */
class MoreLikeThisQueryBuilderTest {

  @Test
  void selectTopTerms() {
    // Given: MLT config with default thresholds
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    config.setMinTermFreq(2);
    config.setMinDocFreq(1);
    config.setMaxDocFreqPercent(50.0f);
    config.setMinWordLen(3);
    config.setBoostByScore(true);

    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);

    // When: selecting terms from sample data
    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("java", 5);
    termFreqs.put("database", 3);
    termFreqs.put("the", 10);  // High doc freq should be filtered
    termFreqs.put("a", 8);     // Too short, should be filtered
    termFreqs.put("query", 1); // Below minTermFreq, should be filtered

    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("java", 10);
    docFreqs.put("database", 5);
    docFreqs.put("the", 60);  // 60/100 = 60% > maxDocFreqPercent
    docFreqs.put("a", 70);
    docFreqs.put("query", 2);

    final int totalDocs = 100;

    // Then: should return filtered and sorted terms
    final List<String> terms = builder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    assertThat(terms).isNotNull();
    assertThat(terms.size()).isEqualTo(2);
    assertThat(terms.contains("java")).isTrue();
    assertThat(terms.contains("database")).isTrue();
    assertThat(terms.contains("the")).isFalse();   // Filtered by maxDocFreqPercent
    assertThat(terms.contains("a")).isFalse();     // Filtered by minWordLen
    assertThat(terms.contains("query")).isFalse(); // Filtered by minTermFreq
  }

  @Test
  void selectTopTermsWithMaxQueryTerms() {
    // Given: MLT config with maxQueryTerms limit
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    config.setMinTermFreq(1);
    config.setMinDocFreq(1);
    config.setMaxQueryTerms(2);
    config.setBoostByScore(true);

    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);

    // When: selecting from many terms
    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("java", 10);
    termFreqs.put("database", 8);
    termFreqs.put("query", 6);
    termFreqs.put("index", 4);

    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("java", 5);
    docFreqs.put("database", 10);
    docFreqs.put("query", 15);
    docFreqs.put("index", 20);

    final int totalDocs = 100;

    // Then: should return only top N by TF-IDF score
    final List<String> terms = builder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    assertThat(terms).isNotNull();
    assertThat(terms.size()).isEqualTo(2);
    // "java" should have highest TF-IDF: 10 * log(100/5) = 10 * 1.301 = 13.01
    // "database" should have second highest: 8 * log(100/10) = 8 * 1.0 = 8.0
    assertThat(terms.get(0)).isEqualTo("java");
    assertThat(terms.get(1)).isEqualTo("database");
  }

  @Test
  void filterByMinTermFreq() {
    // Given: MLT config with higher minTermFreq
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    config.setMinTermFreq(5);
    config.setMinDocFreq(1);
    config.setBoostByScore(false);

    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);

    // When: selecting terms with various frequencies
    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("frequent", 10);
    termFreqs.put("rare", 2);

    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("frequent", 10);
    docFreqs.put("rare", 2);

    final int totalDocs = 100;

    // Then: should only return terms meeting minTermFreq
    final List<String> terms = builder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    assertThat(terms).isNotNull();
    assertThat(terms.size()).isEqualTo(1);
    assertThat(terms.get(0)).isEqualTo("frequent");
    assertThat(terms.contains("rare")).isFalse();
  }

  @Test
  void filterByMinDocFreq() {
    // Given: MLT config with minDocFreq threshold
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    config.setMinTermFreq(1);
    config.setMinDocFreq(5);
    config.setBoostByScore(false);

    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);

    // When: selecting terms with various document frequencies
    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("common", 3);
    termFreqs.put("unique", 2);

    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("common", 10);
    docFreqs.put("unique", 2);

    final int totalDocs = 100;

    // Then: should only return terms meeting minDocFreq
    final List<String> terms = builder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    assertThat(terms).isNotNull();
    assertThat(terms.size()).isEqualTo(1);
    assertThat(terms.get(0)).isEqualTo("common");
    assertThat(terms.contains("unique")).isFalse();
  }

  @Test
  void filterByWordLength() {
    // Given: MLT config with word length constraints
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    config.setMinTermFreq(1);
    config.setMinWordLen(4);
    config.setMaxWordLen(8);
    config.setBoostByScore(false);

    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);

    // When: selecting terms with various lengths
    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("sql", 5);           // Too short (3 < 4)
    termFreqs.put("java", 5);          // OK (4 <= 4 <= 8)
    termFreqs.put("database", 5);      // OK (4 <= 8 <= 8)
    termFreqs.put("extraordinarily", 5); // Too long (15 > 8)

    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("sql", 10);
    docFreqs.put("java", 10);
    docFreqs.put("database", 10);
    docFreqs.put("extraordinarily", 10);

    final int totalDocs = 100;

    // Then: should only return terms within length bounds
    final List<String> terms = builder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    assertThat(terms).isNotNull();
    assertThat(terms.size()).isEqualTo(2);
    assertThat(terms.contains("java")).isTrue();
    assertThat(terms.contains("database")).isTrue();
    assertThat(terms.contains("sql")).isFalse();
    assertThat(terms.contains("extraordinarily")).isFalse();
  }

  @Test
  void emptyInput() {
    // Given: MLT config with default settings
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);

    // When: selecting from empty term frequencies
    final Map<String, Integer> termFreqs = new HashMap<>();
    final Map<String, Integer> docFreqs = new HashMap<>();
    final int totalDocs = 100;

    // Then: should return empty list
    final List<String> terms = builder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    assertThat(terms).isNotNull();
    assertThat(terms.isEmpty()).isTrue();
  }

  @Test
  void boostByScoreDisabled() {
    // Given: MLT config with boostByScore disabled
    final MoreLikeThisConfig config = new MoreLikeThisConfig();
    config.setMinTermFreq(1);
    config.setBoostByScore(false);
    config.setMaxQueryTerms(10);

    final MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(config);

    // When: selecting terms
    final Map<String, Integer> termFreqs = new HashMap<>();
    termFreqs.put("java", 10);
    termFreqs.put("database", 5);

    final Map<String, Integer> docFreqs = new HashMap<>();
    docFreqs.put("java", 5);
    docFreqs.put("database", 10);

    final int totalDocs = 100;

    // Then: should return terms without specific ordering by TF-IDF
    final List<String> terms = builder.selectTopTerms(termFreqs, docFreqs, totalDocs);

    assertThat(terms).isNotNull();
    assertThat(terms.size()).isEqualTo(2);
    assertThat(terms.contains("java")).isTrue();
    assertThat(terms.contains("database")).isTrue();
  }
}
