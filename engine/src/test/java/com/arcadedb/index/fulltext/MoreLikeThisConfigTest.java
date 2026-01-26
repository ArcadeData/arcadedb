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
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.fulltext;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MoreLikeThisConfigTest {

  @Test
  void testDefaultValues() {
    final MoreLikeThisConfig config = new MoreLikeThisConfig();

    assertThat(config.getMinTermFreq()).isEqualTo(2);
    assertThat(config.getMinDocFreq()).isEqualTo(5);
    assertThat(config.getMaxDocFreqPercent()).isNull();
    assertThat(config.getMaxQueryTerms()).isEqualTo(25);
    assertThat(config.getMinWordLen()).isEqualTo(0);
    assertThat(config.getMaxWordLen()).isEqualTo(0);
    assertThat(config.isBoostByScore()).isTrue();
    assertThat(config.isExcludeSource()).isTrue();
    assertThat(config.getMaxSourceDocs()).isEqualTo(25);
  }

  @Test
  void testFromJSON() {
    final JSONObject json = new JSONObject();
    json.put("minTermFreq", 1);
    json.put("minDocFreq", 3);
    json.put("maxDocFreqPercent", 0.5);
    json.put("maxQueryTerms", 50);
    json.put("excludeSource", false);

    final MoreLikeThisConfig config = MoreLikeThisConfig.fromJSON(json);

    assertThat(config.getMinTermFreq()).isEqualTo(1);
    assertThat(config.getMinDocFreq()).isEqualTo(3);
    assertThat(config.getMaxDocFreqPercent()).isEqualTo(0.5f);
    assertThat(config.getMaxQueryTerms()).isEqualTo(50);
    assertThat(config.isExcludeSource()).isFalse();
  }

  @Test
  void testFromNullJSON() {
    final MoreLikeThisConfig config = MoreLikeThisConfig.fromJSON(null);

    // Should use defaults
    assertThat(config.getMinTermFreq()).isEqualTo(2);
  }
}
