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
package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10397: the BM25 corpus counters are persisted with the analyzer, and the token-counting rule, they were measured with.
 * The metadata-level contract of that signature, without a database: what is written, what is read back, what clears it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextIndexMetadataLengthSignatureTest {
  private static final String KEYWORD_ANALYZER = "org.apache.lucene.analysis.core.KeywordAnalyzer";

  private static FullTextIndexMetadata metadata() {
    return new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
  }

  private static FullTextIndexMetadata restored(final JSONObject definition) {
    final FullTextIndexMetadata restored = metadata();
    restored.fromJSON(definition);
    return restored;
  }

  @Test
  void theSignatureNamesTheIndexAnalyzerAndTheLengthRule() {
    final FullTextIndexMetadata metadata = metadata();
    assertThat(metadata.getLengthSignature()).isEqualTo(FullTextIndexMetadata.DEFAULT_ANALYZER + "#" + FullTextIndexMetadata.LENGTH_MEASURE_VERSION);

    // ONLY THE ANALYZER THAT MEASURES A LENGTH TAKES PART: THE QUERY ANALYZER AND THE PER-FIELD ONES DO NOT
    metadata.setQueryAnalyzerClass(KEYWORD_ANALYZER);
    metadata.setFieldAnalyzer("content", KEYWORD_ANALYZER);
    assertThat(metadata.getLengthSignature()).startsWith(FullTextIndexMetadata.DEFAULT_ANALYZER + "#");

    metadata.setIndexAnalyzerClass(KEYWORD_ANALYZER);
    assertThat(metadata.getLengthSignature()).startsWith(KEYWORD_ANALYZER + "#");
  }

  @Test
  void theSignatureRoundTripsThroughTheDefinition() {
    final FullTextIndexMetadata metadata = metadata();
    metadata.setIndexAnalyzerClass(KEYWORD_ANALYZER);
    metadata.setCounters(5, 5);

    final JSONObject definition = metadata.writeToJSON(new JSONObject());
    assertThat(definition.getString("ft_lengthSignature")).isEqualTo(metadata.getLengthSignature());

    final FullTextIndexMetadata restored = restored(definition);
    assertThat(restored.getMeasuredWith()).isEqualTo(metadata.getLengthSignature());
    assertThat(restored.isMeasuredWithAnotherAnalyzer()).isFalse();
  }

  @Test
  void anotherIndexAnalyzerThanTheOneThatMeasuredIsFound() {
    final FullTextIndexMetadata metadata = metadata();
    metadata.setCounters(5, 15);
    final JSONObject definition = metadata.writeToJSON(new JSONObject());

    definition.put("index_analyzer", KEYWORD_ANALYZER);

    final FullTextIndexMetadata restored = restored(definition);
    assertThat(restored.isMeasuredWithAnotherAnalyzer()).isTrue();
    assertThat(restored.getMeasuredWith()).startsWith(FullTextIndexMetadata.DEFAULT_ANALYZER + "#");
  }

  @Test
  void anotherLengthRuleThanTheOneThatMeasuredIsFound() {
    final FullTextIndexMetadata metadata = metadata();
    metadata.setCounters(5, 15);
    final JSONObject definition = metadata.writeToJSON(new JSONObject());

    // THE COUNTERS WERE MEASURED BY A RULE THAT COUNTED TOKENS DIFFERENTLY
    definition.put("ft_lengthSignature", definition.getString("ft_lengthSignature").replaceAll("#\\d+$", "#0"));

    assertThat(restored(definition).isMeasuredWithAnotherAnalyzer()).isTrue();
  }

  @Test
  void countersPersistedWithoutASignatureAreTrusted() {
    final FullTextIndexMetadata metadata = metadata();
    metadata.setCounters(5, 15);
    final JSONObject definition = metadata.writeToJSON(new JSONObject());
    definition.remove("ft_lengthSignature");
    definition.put("index_analyzer", KEYWORD_ANALYZER);

    final FullTextIndexMetadata restored = restored(definition);

    assertThat(restored.isMeasuredWithAnotherAnalyzer()).isFalse();
    // THE NEXT SAVE RECORDS THE ANALYZER THE INDEX HAS NOW
    assertThat(restored.writeToJSON(new JSONObject()).getString("ft_lengthSignature")).isEqualTo(restored.getLengthSignature());
  }

  @Test
  void aSaveBeforeTheRescanKeepsTheSignatureTheCountersWereMeasuredWith() {
    final FullTextIndexMetadata metadata = metadata();
    metadata.setCounters(5, 15);
    final JSONObject definition = metadata.writeToJSON(new JSONObject());
    definition.put("index_analyzer", KEYWORD_ANALYZER);
    final FullTextIndexMetadata restored = restored(definition);

    final JSONObject saved = restored.writeToJSON(new JSONObject());

    assertThat(saved.getString("ft_lengthSignature")).startsWith(FullTextIndexMetadata.DEFAULT_ANALYZER + "#");
    assertThat(restored(saved).isMeasuredWithAnotherAnalyzer()).isTrue();
  }

  @Test
  void aScanThatCouldNotBeProvenExactStillRecordsTheAnalyzer() {
    final FullTextIndexMetadata metadata = metadata();
    metadata.setCounters(5, 15);
    final JSONObject definition = metadata.writeToJSON(new JSONObject());
    definition.put("index_analyzer", KEYWORD_ANALYZER);
    final FullTextIndexMetadata restored = restored(definition);
    assertThat(restored.isMeasuredWithAnotherAnalyzer()).isTrue();

    // A WRITER MOVED THE COUNTERS WHILE THE SCAN RAN: THE RESULT IS NOT EXACT, AND THE DRIFT CHECK IS ARMED TO RESCAN
    final boolean exact = restored.publishScannedCounters(5, 5, 5, 15, restored.getMutations() - 1);

    assertThat(exact).isFalse();
    assertThat(restored.consumeRescanRequired()).isTrue();
    // THE ANALYZER IS NOT THE REASON ANY MORE, SO THE WARNING THAT NAMES IT IS NOT REPEATED
    assertThat(restored.isMeasuredWithAnotherAnalyzer()).isFalse();
  }

  @Test
  void adoptingTheCountersOfALeaderTakesTheAnalyzerTheyWereMeasuredWith() {
    final FullTextIndexMetadata follower = metadata();
    follower.setCounters(5, 15);

    final JSONObject leader = new JSONObject().put("ft_countersValid", true).put("ft_countersEpoch", 1L).put("ft_totalDocs", 7L)
        .put("ft_sumDocLength", 7L).put("ft_lengthSignature", KEYWORD_ANALYZER + "#" + FullTextIndexMetadata.LENGTH_MEASURE_VERSION);
    assertThat(follower.adoptPersistedCounters(leader)).isTrue();
    // THE FOLLOWER ANALYZES WITH THE STANDARD ANALYZER, THE LEADER MEASURED WITH THE KEYWORD ONE
    assertThat(follower.isMeasuredWithAnotherAnalyzer()).isTrue();

    // A LATER RECOMPUTE OF THE LEADER, SAVED BY A VERSION THAT DID NOT RECORD IT, LEAVES NOTHING TO COMPARE
    final JSONObject unrecorded = new JSONObject().put("ft_countersValid", true).put("ft_countersEpoch", 2L).put("ft_totalDocs", 7L)
        .put("ft_sumDocLength", 21L);
    assertThat(follower.adoptPersistedCounters(unrecorded)).isTrue();
    assertThat(follower.isMeasuredWithAnotherAnalyzer()).isFalse();
  }

  @Test
  void anExplicitSetOfTheCountersRecordsTheCurrentAnalyzer() {
    final FullTextIndexMetadata metadata = metadata();
    metadata.setCounters(5, 15);
    final JSONObject definition = metadata.writeToJSON(new JSONObject());
    definition.put("index_analyzer", KEYWORD_ANALYZER);
    final FullTextIndexMetadata restored = restored(definition);
    assertThat(restored.isMeasuredWithAnotherAnalyzer()).isTrue();

    restored.setCounters(5, 5);

    assertThat(restored.isMeasuredWithAnotherAnalyzer()).isFalse();
  }
}
